from typing import Optional
from urllib.parse import urlparse, parse_qs

from .. import background_tasks, core, json, optional_features
from ..logging import log
from .persistent_dict import PersistentDict

try:
    import redis as redis_sync
    import redis.asyncio as redis
    import redis.exceptions as redis_exceptions
    from redis.sentinel import Sentinel
    from redis.asyncio.sentinel import Sentinel as AsyncSentinel

    optional_features.register("redis")
except ImportError:
    pass


class MappedSentinel(Sentinel):
    """Sentinel that remaps discovered hostnames for compatibility."""

    def __init__(self, *args, hostmap: Optional[dict] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._hostmap = hostmap or {}

    def discover_master(self, service_name):
        host, port = super().discover_master(service_name)
        return (self._hostmap.get(host, host), port)

    def discover_slaves(self, service_name):
        slaves = super().discover_slaves(service_name)
        return [(self._hostmap.get(h, h), p) for (h, p) in slaves]


class MappedAsyncSentinel(AsyncSentinel):
    """Async Sentinel that remaps discovered hostnames for compatibility."""

    def __init__(self, *args, hostmap: Optional[dict] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._hostmap = hostmap or {}

    async def discover_master(self, service_name):
        host, port = await super().discover_master(service_name)
        return (self._hostmap.get(host, host), port)

    async def discover_slaves(self, service_name):
        slaves = await super().discover_slaves(service_name)
        return [(self._hostmap.get(h, h), p) for (h, p) in slaves]


class RedisPersistentDict(PersistentDict):

    def __init__(
        self,
        *,
        url: str,
        id: str,
        key_prefix: str = "nicegui:",
        redis_client=None,
        hostmap: Optional[dict] = None,
    ) -> None:  # pylint: disable=redefined-builtin
        if not optional_features.has("redis"):
            raise ImportError('Redis is not installed. Please run "pip install nicegui[redis]".')

        self.url = url
        self.key = key_prefix + id
        self.redis_client = redis_client
        self._should_listen = True
        self._is_sentinel = url.startswith("redis+sentinel://")
        self._hostmap = hostmap or {}

        if self.redis_client is None:
            if self._is_sentinel:
                self.redis_client = self._create_sentinel_client(url)
            else:
                self.redis_client = redis.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                )
            self.pubsub = self.redis_client.pubsub()
        else:
            self.pubsub = self.redis_client.pubsub()

        super().__init__(data={}, on_change=self.publish)

    def _create_sentinel_client(self, url: str):
        """Create an async Redis client from Sentinel URL.

        Expected format: redis+sentinel://host1:port1,host2:port2/service_name?db=0&password=pass&hostmap=host:ip

        Query parameters:
        - db: database number (default: 0)
        - password: Redis password
        - hostmap: semicolon-separated host:ip mappings (e.g., hostmap=host.docker.internal:127.0.0.1;other:10.0.0.1)
        """
        parsed = urlparse(url)

        # Parse sentinel hosts from the netloc
        sentinels = []
        if parsed.netloc:
            for host_port in parsed.netloc.split(","):
                if ":" in host_port:
                    host, port = host_port.rsplit(":", 1)
                    sentinels.append((host, int(port)))
                else:
                    sentinels.append((host_port, 26379))  # Default Sentinel port

        # Extract service name from path
        service_name = parsed.path.lstrip("/").split("/")[0] if parsed.path else "mymaster"

        # Parse query parameters
        params = parse_qs(parsed.query)
        db = int(params.get("db", ["0"])[0])
        password = params.get("password", [None])[0]

        # Parse hostmap from query params if provided
        # Use semicolon as separator to avoid conflicts with commas in sentinel hosts
        hostmap = self._hostmap.copy()
        if "hostmap" in params:
            for mapping in params["hostmap"][0].split(";"):
                if ":" in mapping:
                    host, ip = mapping.split(":", 1)
                    hostmap[host.strip()] = ip.strip()

        # Create async sentinel with hostname mapping
        sentinel = MappedAsyncSentinel(
            sentinels,
            socket_timeout=5,
            socket_connect_timeout=5,
            socket_keepalive=True,
            hostmap=hostmap,
        )

        # Get master client
        master = sentinel.master_for(
            service_name,
            db=db,
            password=password,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            socket_keepalive=True,
            health_check_interval=10,
        )

        return master

    def _create_sentinel_client_sync(self, url: str):
        """Create a synchronous Redis client from Sentinel URL."""
        parsed = urlparse(url)

        # Parse sentinel hosts
        sentinels = []
        if parsed.netloc:
            for host_port in parsed.netloc.split(","):
                if ":" in host_port:
                    host, port = host_port.rsplit(":", 1)
                    sentinels.append((host, int(port)))
                else:
                    sentinels.append((host_port, 26379))

        # Extract service name and parameters
        service_name = parsed.path.lstrip("/").split("/")[0] if parsed.path else "mymaster"
        params = parse_qs(parsed.query)
        db = int(params.get("db", ["0"])[0])
        password = params.get("password", [None])[0]

        # Parse hostmap from query params if provided
        # Use semicolon as separator to avoid conflicts with commas in sentinel hosts
        hostmap = self._hostmap.copy()
        if "hostmap" in params:
            for mapping in params["hostmap"][0].split(";"):
                if ":" in mapping:
                    host, ip = mapping.split(":", 1)
                    hostmap[host.strip()] = ip.strip()

        # Create sync sentinel with hostname mapping
        sentinel = MappedSentinel(
            sentinels,
            socket_timeout=5,
            socket_connect_timeout=5,
            socket_keepalive=True,
            hostmap=hostmap,
        )

        # Get master client
        master = sentinel.master_for(
            service_name,
            db=db,
            password=password,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            socket_keepalive=True,
            health_check_interval=10,
        )

        return master

    async def initialize(self) -> None:
        """Load initial data from Redis and start listening for changes."""
        try:
            data = await self.redis_client.get(self.key)
            self.update(json.loads(data) if data else {})
            self._start_listening()
        except Exception:
            log.warning(f"Could not load data from Redis with key {self.key}")

    def initialize_sync(self) -> None:
        """Load initial data from Redis and start listening for changes in a synchronous context."""
        if self._is_sentinel:
            redis_client_sync = self._create_sentinel_client_sync(self.url)
        else:
            redis_client_sync = redis_sync.from_url(
                self.url,
                health_check_interval=10,
                socket_connect_timeout=5,
                retry_on_timeout=True,
                socket_keepalive=True,
            )
        try:
            data = redis_client_sync.get(self.key)
            self.update(json.loads(data) if data else {})
            self._start_listening()
        except Exception:
            log.warning(f"Could not load data from Redis with key {self.key}")
        finally:
            if self._is_sentinel:
                redis_client_sync.close()

    def _start_listening(self) -> None:
        async def listen():
            try:
                if not self._should_listen:
                    return
                await self.pubsub.subscribe(self.key + "changes")
                if not self._should_listen:
                    await self.pubsub.unsubscribe()
                    return
                async for message in self.pubsub.listen():
                    t = message["type"]
                    if t == "message":
                        new_data = json.loads(message["data"])
                        if new_data != self:
                            self.update(new_data)
                    elif t in ("unsubscribe", "punsubscribe") and message.get("data") == 0:
                        break
            except Exception as e:
                if isinstance(e, redis_exceptions.ConnectionError) and not self._should_listen:
                    return  # NOTE: on quick instantiation cycles, unsubscribe event might not be received before the connection is closed
                log.exception(f"Unexpected error in Redis listener for {self.key}")

        if core.loop and core.loop.is_running():
            background_tasks.create(listen(), name=f"redis-listen-{self.key}")
        else:
            core.app.on_startup(listen())

    def publish(self) -> None:
        """Publish the data to Redis and notify other instances."""

        async def backup() -> None:
            if not await self.redis_client.exists(self.key) and not self:
                return
            pipeline = self.redis_client.pipeline()
            pipeline.set(self.key, json.dumps(self))
            pipeline.publish(self.key + "changes", json.dumps(self))
            await pipeline.execute()

        if core.loop:
            background_tasks.create_lazy(backup(), name=f"redis-{self.key}")
        else:
            core.app.on_startup(backup())

    async def close(self) -> None:
        """Close Redis connection and subscription."""
        self._should_listen = False
        if self.pubsub.subscribed:
            await self.pubsub.unsubscribe()
        await self.pubsub.close()
        await self.redis_client.close()

    def clear(self) -> None:
        super().clear()
        if core.loop:
            background_tasks.create_lazy(self.redis_client.delete(self.key), name=f"redis-delete-{self.key}")
        else:
            core.app.on_startup(self.redis_client.delete(self.key))
