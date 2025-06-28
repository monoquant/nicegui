import os
from .. import background_tasks, core, json, optional_features
from ..logging import log
from .persistent_dict import PersistentDict

try:
    import redis                        # sync standalone client
    import redis.asyncio as redis_async # async standalone client
    from redis.cluster import RedisCluster as SyncRedisCluster
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    optional_features.register('redis')
except ImportError:
    pass


class RedisPersistentDict(PersistentDict):
    """
    A dict persisted in Redis, with optional cluster mode.
    Accepts `cluster: bool`—when True, uses RedisCluster clients;
    and for async Pub/Sub falls back to a standalone client.
    """

    def __init__(
        self,
        *,
        url: str,
        id: str,
        key_prefix: str = 'nicegui:',
        cluster: bool = False,
    ) -> None:
        if not optional_features.has('redis'):
            raise ImportError(
                'Redis support is not installed. '
                'Please run "pip install nicegui[redis]".'
            )

        self.url = url
        self.key = key_prefix + id
        self.is_cluster = cluster

        # Async command client
        if cluster:
            self.redis_client = AsyncRedisCluster.from_url(
                url,
                health_check_interval=10,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            # Fallback to standalone for Pub/Sub
            self.pubsub = redis_async.from_url(
                url,
                health_check_interval=10,
                socket_connect_timeout=5,
                socket_keepalive=True,
            ).pubsub()
        else:
            self.redis_client = redis_async.from_url(
                url,
                health_check_interval=10,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                socket_keepalive=True,
            )
            self.pubsub = self.redis_client.pubsub()

        super().__init__(data={}, on_change=self.publish)

    async def initialize(self) -> None:
        """Load initial data from Redis and start listening for changes."""
        try:
            raw = await self.redis_client.get(self.key)
            self.update(json.loads(raw) if raw else {})
            self._start_listening()
        except Exception:
            log.warning(f'Could not load data from Redis with key {self.key}')

    def initialize_sync(self) -> None:
        """Synchronous context: load data and subscribe to changes."""
        client_cls = SyncRedisCluster if self.is_cluster else redis.Redis
        with client_cls.from_url(
            self.url,
            health_check_interval=10,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            socket_keepalive=True,
        ) as client:
            try:
                raw = client.get(self.key)
                self.update(json.loads(raw) if raw else {})
                self._start_listening()
            except Exception:
                log.warning(f'Could not load data from Redis with key {self.key}')

    def _start_listening(self) -> None:
        """Subscribe to change channel and apply updates from other instances."""
        async def listen():
            await self.pubsub.subscribe(self.key + 'changes')
            async for msg in self.pubsub.listen():
                if msg.get('type') == 'message':
                    new = json.loads(msg['data'])
                    if new != self:
                        self.update(new)

        if core.loop and core.loop.is_running():
            background_tasks.create(listen(), name=f'redis-listen-{self.key}')
        else:
            core.app.on_startup(listen())

    def publish(self) -> None:
        """Persist current dict to Redis and broadcast a change event."""
        async def backup() -> None:
            pipe = self.redis_client.pipeline()
            pipe.set(self.key, json.dumps(self))
            pipe.publish(self.key + 'changes', json.dumps(self))
            await pipe.execute()

        if core.loop:
            background_tasks.create_lazy(backup(), name=f'redis-{self.key}')
        else:
            core.app.on_startup(backup())

    async def close(self) -> None:
        """Unsubscribe and close Redis connections cleanly."""
        await self.pubsub.unsubscribe()
        await self.pubsub.close()
        await self.redis_client.close()

    def clear(self) -> None:
        """Clear in-memory dict and delete the key from Redis."""
        super().clear()
        if core.loop:
            background_tasks.create_lazy(
                self.redis_client.delete(self.key),
                name=f'redis-delete-{self.key}'
            )
        else:
            core.app.on_startup(self.redis_client.delete(self.key))
