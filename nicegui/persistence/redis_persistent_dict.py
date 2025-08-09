import asyncio
from typing import Optional
from .. import background_tasks, core, json, optional_features
from ..logging import log
from .persistent_dict import PersistentDict

try:
    import redis as redis_sync  # sync standalone
    import redis.asyncio as redis_async  # async standalone
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster
    import redis.exceptions as redis_exceptions

    optional_features.register("redis")
except ImportError:
    pass


class RedisPersistentDict(PersistentDict):
    """
    PersistentDict backed by Redis (standalone or cluster), with optional
    injection of `kv_client` and `pubsub_client` to enable shared connection reuse.
    Adds:
      - robust pubsub reconnect loop (recreate pubsub on disconnect)
      - publish() retries on transient connection errors
      - avoids creating key when dict is empty and key doesn't exist yet
    """

    def __init__(
        self,
        *,
        url: str,
        id: str,
        key_prefix: str = "nicegui:",
        cluster: bool = False,
        kv_client: Optional[redis_async.Redis] = None,
        pubsub_client: Optional[redis_async.Redis] = None,
    ) -> None:
        if not optional_features.has("redis"):
            raise ImportError("Redis support is not installed. " 'Please run "pip install nicegui[redis]".')

        self.url = url
        self.key = key_prefix + id
        self.is_cluster = cluster
        self._should_listen = True

        # Track ownership so we only recreate what we created
        self._own_kv = kv_client is None
        self._own_pubsub_client = pubsub_client is None

        # KV client
        if kv_client is not None:
            self.kv_client = kv_client
        else:
            if cluster:
                self.kv_client = AsyncRedisCluster.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                )
            else:
                self.kv_client = redis_async.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                )

        # Pub/Sub client (separate from cluster client for reliability)
        if pubsub_client is not None:
            self.pubsub_client = pubsub_client
        else:
            if cluster:
                self.pubsub_client = redis_async.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                )
            else:
                self.pubsub_client = self.kv_client

        self.pubsub = self.pubsub_client.pubsub()
        super().__init__(data={}, on_change=self.publish)

    async def initialize(self) -> None:
        """Load data from Redis and start the change listener."""
        try:
            raw = await self.kv_client.get(self.key)
            self.update(json.loads(raw) if raw else {})
            self._start_listening()
        except Exception:
            # If Redis isn't up yet or key can't be read, just warn and continue.
            log.warning(f"Could not load data from Redis with key {self.key}")

    def initialize_sync(self) -> None:
        """Synchronous context: load data and subscribe to changes."""
        client_cls = SyncRedisCluster if self.is_cluster else redis_sync.Redis
        kwargs = {} if self.is_cluster else {"retry_on_timeout": True}
        with client_cls.from_url(
            self.url, health_check_interval=10, socket_connect_timeout=5, socket_keepalive=True, **kwargs
        ) as client:
            try:
                raw = client.get(self.key)
                self.update(json.loads(raw) if raw else {})
                self._start_listening()
            except Exception:
                log.warning(f"Could not load data from Redis with key {self.key}")

    def _new_pubsub(self):
        """Create a fresh pubsub bound to current pubsub_client."""
        try:
            # Best effort close old pubsub
            if getattr(self, "pubsub", None):
                try:
                    if getattr(self.pubsub, "subscribed", False):
                        # Don't await here (might be called outside task); listener does awaits.
                        pass
                except Exception:
                    pass
        finally:
            self.pubsub = self.pubsub_client.pubsub()

    def _start_listening(self) -> None:
        """Subscribe to the change channel and propagate updates with robust lifecycle handling."""

        async def listen():
            backoff = 0.2
            max_backoff = 5.0
            channel = self.key + "changes"
            while self._should_listen:
                try:
                    # (Re)create pubsub after any disconnect
                    self._new_pubsub()
                    await self.pubsub.subscribe(channel)
                    # Reset backoff on successful subscribe
                    backoff = 0.2

                    async for msg in self.pubsub.listen():
                        if not self._should_listen:
                            break
                        t = msg.get("type")
                        if t == "message":
                            new = json.loads(msg["data"])
                            if new != self:
                                self.update(new)
                        elif t in ("unsubscribe", "punsubscribe") and msg.get("data") == 0:
                            # No more subscriptions -> exit loop, will reconnect if still should listen
                            break

                except (redis_exceptions.ConnectionError, redis_exceptions.TimeoutError) as e:
                    if not self._should_listen:
                        return
                    log.warning(f"Redis pubsub disconnected for {self.key}: {e}; retrying in {backoff:.1f}s")
                    # Close and backoff, then loop to recreate and resubscribe
                    try:
                        await self.pubsub.close()
                    except Exception:
                        pass
                    await asyncio.sleep(backoff)
                    backoff = min(max_backoff, backoff * 2.0)
                    continue
                except Exception:
                    if not self._should_listen:
                        return
                    log.exception(f"Unexpected error in Redis listener for {self.key}")
                    # Small backoff to avoid hot loop on repeated error
                    await asyncio.sleep(0.5)
                finally:
                    # Ensure pubsub is closed before next iteration/exit
                    try:
                        if getattr(self.pubsub, "subscribed", False):
                            await self.pubsub.unsubscribe()
                    except Exception:
                        pass
                    try:
                        await self.pubsub.close()
                    except Exception:
                        pass

        if core.loop and core.loop.is_running():
            background_tasks.create(listen(), name=f"redis-listen-{self.key}")
        else:
            core.app.on_startup(listen())

    def publish(self) -> None:
        """Persist data and notify other instances, handling cluster pipeline limitations and retries."""

        async def backup() -> None:
            # Don't create an empty key on first write
            try:
                if not await self.kv_client.exists(self.key) and not self:
                    return
            except Exception:
                # If exists() fails (e.g., transient cluster state), proceed anyway
                pass

            data = json.dumps(self)

            async def _retry(coro_factory, attempts=3):
                delay = 0.2
                for i in range(1, attempts + 1):
                    try:
                        return await coro_factory()
                    except (
                        redis_exceptions.ConnectionError,
                        redis_exceptions.TimeoutError,
                        redis_exceptions.RedisError,
                    ) as e:
                        if i == attempts:
                            raise
                        log.warning(f"Redis write retry {i}/{attempts-1} for {self.key}: {e}")
                        await asyncio.sleep(delay)
                        delay = min(2.0, delay * 2.0)

            if self.is_cluster:
                # Cluster: publish and set sequentially (pipelines can be flaky across slots)
                async def do_cluster():
                    await self.kv_client.set(self.key, data)
                    await self.pubsub_client.publish(self.key + "changes", data)

                await _retry(lambda: do_cluster())
            else:
                # Standalone: pipeline both commands together
                async def do_standalone():
                    pipe = self.kv_client.pipeline()
                    pipe.set(self.key, data)
                    pipe.publish(self.key + "changes", data)
                    await pipe.execute()

                await _retry(lambda: do_standalone())

        if core.loop:
            background_tasks.create_lazy(backup(), name=f"redis-{self.key}")
        else:
            core.app.on_startup(backup())

    async def close(self) -> None:
        """Unsubscribe and close Redis connections cleanly."""
        self._should_listen = False
        try:
            if getattr(self, "pubsub", None):
                try:
                    if getattr(self.pubsub, "subscribed", False):
                        await self.pubsub.unsubscribe()
                except Exception:
                    pass
                try:
                    await self.pubsub.close()
                except Exception:
                    pass
        finally:
            try:
                await self.kv_client.close()
            finally:
                if self.pubsub_client is not self.kv_client:
                    try:
                        await self.pubsub_client.close()
                    except Exception:
                        pass

    def clear(self) -> None:
        """Clear in-memory data and delete the Redis key."""
        super().clear()
        if core.loop:
            background_tasks.create_lazy(self.kv_client.delete(self.key), name=f"redis-delete-{self.key}")
        else:
            core.app.on_startup(self.kv_client.delete(self.key))
