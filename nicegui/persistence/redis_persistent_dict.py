import os
from typing import Optional
from .. import background_tasks, core, json, optional_features
from ..logging import log
from .persistent_dict import PersistentDict

try:
    import redis as redis_sync                      # sync standalone
    import redis.asyncio as redis_async             # async standalone
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.cluster import RedisCluster as SyncRedisCluster
    import redis.exceptions as redis_exceptions
    optional_features.register('redis')
except ImportError:
    pass


class RedisPersistentDict(PersistentDict):
    """
    PersistentDict backed by Redis (standalone or cluster), with optional
    injection of `kv_client` and `pubsub_client` to enable shared connection reuse.
    Includes robust pubsub lifecycle handling and avoids creating empty keys.
    """

    def __init__(
        self,
        *,
        url: str,
        id: str,
        key_prefix: str = 'nicegui:',
        cluster: bool = False,
        kv_client: Optional[redis_async.Redis] = None,
        pubsub_client: Optional[redis_async.Redis] = None,
    ) -> None:
        if not optional_features.has('redis'):
            raise ImportError(
                'Redis support is not installed. '
                'Please run "pip install nicegui[redis]".'
            )

        self.url = url
        self.key = key_prefix + id
        self.is_cluster = cluster
        self._should_listen = True  # upstream improvement

        # 1) Key-Value client: use injected or create new
        if kv_client is not None:
            self.kv_client = kv_client
        else:
            if cluster:
                # AsyncRedisCluster.from_url() for cluster mode
                self.kv_client = AsyncRedisCluster.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                )
            else:
                # Standalone async client supports retry_on_timeout
                self.kv_client = redis_async.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                )

        # 2) Pub/Sub client: use injected or create new
        if pubsub_client is not None:
            self.pubsub_client = pubsub_client
        else:
            if cluster:
                # Cluster client lacks pubsub()/publish() reliability; use standalone for pubsub
                self.pubsub_client = redis_async.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                )
            else:
                self.pubsub_client = self.kv_client

        # 3) Create PubSub object from the chosen client
        self.pubsub = self.pubsub_client.pubsub()

        super().__init__(data={}, on_change=self.publish)

    async def initialize(self) -> None:
        """Load data from Redis and start the change listener."""
        try:
            raw = await self.kv_client.get(self.key)
            self.update(json.loads(raw) if raw else {})
            self._start_listening()
        except Exception:
            log.warning(f'Could not load data from Redis with key {self.key}')

    def initialize_sync(self) -> None:
        """Synchronous context: load data and subscribe to changes."""
        client_cls = SyncRedisCluster if self.is_cluster else redis_sync.Redis
        kwargs = {} if self.is_cluster else {'retry_on_timeout': True}
        with client_cls.from_url(
            self.url,
            health_check_interval=10,
            socket_connect_timeout=5,
            socket_keepalive=True,
            **kwargs
        ) as client:
            try:
                raw = client.get(self.key)
                self.update(json.loads(raw) if raw else {})
                self._start_listening()
            except Exception:
                log.warning(f'Could not load data from Redis with key {self.key}')

    def _start_listening(self) -> None:
        """Subscribe to the change channel and propagate updates with robust lifecycle handling."""
        async def listen():
            try:
                if not self._should_listen:
                    return
                await self.pubsub.subscribe(self.key + 'changes')
                if not self._should_listen:
                    await self.pubsub.unsubscribe()
                    return
                async for msg in self.pubsub.listen():
                    t = msg.get('type')
                    if t == 'message':
                        new = json.loads(msg['data'])
                        if new != self:
                            self.update(new)
                    elif t in ('unsubscribe', 'punsubscribe') and msg.get('data') == 0:
                        # last subscription removed -> exit
                        break
            except Exception as e:
                # tolerate connection close during shutdown
                if isinstance(e, redis_exceptions.ConnectionError) and not self._should_listen:
                    return
                log.exception(f'Unexpected error in Redis listener for {self.key}')

        if core.loop and core.loop.is_running():
            background_tasks.create(listen(), name=f'redis-listen-{self.key}')
        else:
            core.app.on_startup(listen())

    def publish(self) -> None:
        """Persist data and notify other instances, handling cluster pipeline restrictions."""
        async def backup() -> None:
            # Upstream behavior: don't create an empty key on first write
            try:
                if not await self.kv_client.exists(self.key) and not self:
                    return
            except Exception:
                # exists may fail on some cluster states; proceed with write
                pass

            data = json.dumps(self)
            if self.is_cluster:
                # Cluster mode: pipeline.publish is not supported reliably; do sequential writes
                await self.kv_client.set(self.key, data)
                await self.pubsub_client.publish(self.key + 'changes', data)
            else:
                # Standalone: pipeline both commands together
                pipe = self.kv_client.pipeline()
                pipe.set(self.key, data)
                pipe.publish(self.key + 'changes', data)
                await pipe.execute()

        if core.loop:
            background_tasks.create_lazy(backup(), name=f'redis-{self.key}')
        else:
            core.app.on_startup(backup())

    async def close(self) -> None:
        """Unsubscribe and close Redis connections cleanly."""
        self._should_listen = False
        try:
            # only attempt unsubscribe if currently subscribed
            if getattr(self.pubsub, 'subscribed', False):
                await self.pubsub.unsubscribe()
        except Exception:
            # ignore errors on shutdown
            pass
        try:
            await self.pubsub.close()
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
            background_tasks.create_lazy(
                self.kv_client.delete(self.key),
                name=f'redis-delete-{self.key}'
            )
        else:
            core.app.on_startup(self.kv_client.delete(self.key))
