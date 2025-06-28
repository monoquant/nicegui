import os
from .. import background_tasks, core, json, optional_features
from ..logging import log
from .persistent_dict import PersistentDict

try:
    import redis                        # sync standalone
    import redis.asyncio as redis_async # async standalone
    from redis.cluster import RedisCluster as SyncRedisCluster
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    optional_features.register('redis')
except ImportError:
    pass


class RedisPersistentDict(PersistentDict):
    """
    A dict persisted in Redis, supporting both standalone and cluster modes.
    Cluster mode is used only when WORK_ENV=KUBERNETES.
    """

    def __init__(self, *, url: str, id: str, key_prefix: str = 'nicegui:') -> None:
        if not optional_features.has('redis'):
            raise ImportError(
                'Redis support is not installed. '
                'Please run "pip install nicegui[redis]".'
            )

        self.url = url
        self.key = key_prefix + id
        # Detect cluster via envvar
        is_cluster = os.getenv('WORK_ENV', '').upper() == 'KUBERNETES'
        self.is_cluster = is_cluster

        # Async command client
        if is_cluster:
            # Cluster client (no retry_on_timeout arg) {{turn1view0}}
            self.redis_client = AsyncRedisCluster.from_url(
                url,
                health_check_interval=10,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            # Standalone pubsub for listening
            self.pubsub = redis_async.from_url(
                url,
                health_check_interval=10,
                socket_connect_timeout=5,
                socket_keepalive=True,
            ).pubsub()
        else:
            # Standalone async (supports retry_on_timeout) {{turn0search2}}
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
        """Load data and start listening for changes."""
        try:
            raw = await self.redis_client.get(self.key)
            self.update(json.loads(raw) if raw else {})
            self._start_listening()
        except Exception:
            log.warning(f'Could not load data from Redis with key {self.key}')

    def initialize_sync(self) -> None:
        """Sync load and subscribe in sync context."""
        client_cls = SyncRedisCluster if self.is_cluster else redis.Redis
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
        """Subscribe to Redis and apply remote changes."""
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
        """Persist data and notify others, handling cluster pipeline limits."""
        async def backup() -> None:
            data = json.dumps(self)
            if self.is_cluster:
                # In cluster mode, pipeline.publish is blocked {{turn0search3}}
                await self.redis_client.set(self.key, data)
                await self.redis_client.publish(self.key + 'changes', data)
            else:
                pipe = self.redis_client.pipeline()
                pipe.set(self.key, data)
                pipe.publish(self.key + 'changes', data)
                await pipe.execute()

        if core.loop:
            background_tasks.create_lazy(backup(), name=f'redis-{self.key}')
        else:
            core.app.on_startup(backup())

    async def close(self) -> None:
        """Cleanly close pubsub and connection."""
        await self.pubsub.unsubscribe()
        await self.pubsub.close()
        await self.redis_client.close()

    def clear(self) -> None:
        """Clear in-memory and delete the Redis key."""
        super().clear()
        if core.loop:
            background_tasks.create_lazy(
                self.redis_client.delete(self.key),
                name=f'redis-delete-{self.key}'
            )
        else:
            core.app.on_startup(self.redis_client.delete(self.key))
