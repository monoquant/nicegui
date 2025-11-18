from typing import Optional
from urllib.parse import urlparse, parse_qs
import asyncio

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
    """
    Redis-backed persistent dictionary with support for connection pooling.

    Design:
    - Main redis_client is shared (pooled) for data operations (get/set/publish)
    - Pubsub connection is created separately per instance to avoid conflicts
    """

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
        self._sentinel = None  # Store sentinel reference for proper cleanup
        self._owns_client = redis_client is None  # Track if we created the main client

        # Separate tracking for pubsub connection
        self._pubsub_client = None
        self._pubsub_sentinel = None
        self.pubsub = None

        # Create or use provided main client (for data operations)
        if self.redis_client is None:
            if self._is_sentinel:
                self.redis_client, self._sentinel = self._create_sentinel_client(url)
            else:
                self.redis_client = redis.from_url(
                    url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                )

        # Note: We DON'T create pubsub here anymore - it's created in _start_listening
        # This allows the main client to be shared while pubsub is separate

        super().__init__(data={}, on_change=self.publish)

    def _create_sentinel_client(self, url: str):
        """Create an async Redis client from Sentinel URL.

        Expected format: redis+sentinel://host1:port1,host2:port2/service_name?db=0&password=pass&hostmap=host:ip

        Query parameters:
        - db: database number (default: 0)
        - password: Redis password
        - hostmap: semicolon-separated host:ip mappings (e.g., hostmap=host.docker.internal:127.0.0.1;other:10.0.0.1)

        Returns:
            Tuple of (master_client, sentinel) so sentinel can be properly closed later
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

        return master, sentinel

    def _create_sentinel_client_sync(self, url: str):
        """Create a synchronous Redis client from Sentinel URL.

        Returns:
            Tuple of (master_client, sentinel) so sentinel can be properly closed later
        """
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

        return master, sentinel

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
        redis_client_sync = None
        sentinel_sync = None

        try:
            if self._is_sentinel:
                redis_client_sync, sentinel_sync = self._create_sentinel_client_sync(self.url)
            else:
                redis_client_sync = redis_sync.from_url(
                    self.url,
                    health_check_interval=10,
                    socket_connect_timeout=5,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                )

            data = redis_client_sync.get(self.key)
            self.update(json.loads(data) if data else {})
            self._start_listening()
        except Exception:
            log.warning(f"Could not load data from Redis with key {self.key}")
        finally:
            # Properly close sync clients
            if redis_client_sync is not None:
                try:
                    redis_client_sync.close()
                except Exception:
                    pass
            if sentinel_sync is not None:
                try:
                    sentinel_sync.close()
                except Exception:
                    pass

    def _start_listening(self) -> None:
        """
        Start pubsub listener with a SEPARATE dedicated connection.
        FIXED: Uses timeout to check _should_listen periodically instead of blocking forever.
        """

        print(f"[DEBUG] _start_listening called for {self.key}")

        async def listen():
            print(f"[DEBUG] listen() coroutine started for {self.key}")
            try:
                # Create dedicated connection for pubsub
                if self._is_sentinel:
                    self._pubsub_client, self._pubsub_sentinel = self._create_sentinel_client(self.url)
                else:
                    self._pubsub_client = redis.from_url(
                        self.url,
                        health_check_interval=10,
                        socket_connect_timeout=5,
                        retry_on_timeout=True,
                        socket_keepalive=True,
                    )

                # Create pubsub from dedicated connection
                self.pubsub = self._pubsub_client.pubsub()

                if not self._should_listen:
                    return

                await self.pubsub.subscribe(self.key + "changes")

                if not self._should_listen:
                    await self.pubsub.unsubscribe()
                    return

                # ============ THE FIX ============
                # Use get_message() with timeout instead of async for loop
                # This allows checking _should_listen every 2 seconds

                log.info(f"Redis listener started for {self.key}")

                while self._should_listen:
                    try:
                        # Wait for message with 2 second timeout
                        message = await asyncio.wait_for(
                            self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0), timeout=2.0
                        )

                        if message is None:
                            # No message - loop will check _should_listen and continue
                            continue

                        # Process message
                        t = message.get("type")
                        if t == "message":
                            new_data = json.loads(message["data"])
                            if new_data != self:
                                self.update(new_data)
                        elif t in ("unsubscribe", "punsubscribe") and message.get("data") == 0:
                            break

                    except asyncio.TimeoutError:
                        # Timeout is EXPECTED - just continue to check _should_listen
                        continue

                    except redis_exceptions.ConnectionError:
                        if not self._should_listen:
                            # Connection closed intentionally
                            break
                        # Connection lost unexpectedly - try to reconnect once
                        log.warning(f"Redis connection lost for {self.key}, attempting reconnect...")
                        await asyncio.sleep(1)
                        try:
                            await self.pubsub.subscribe(self.key + "changes")
                        except Exception as e:
                            log.error(f"Failed to reconnect Redis listener for {self.key}: {e}")
                            break

                log.debug(f"Redis listener exited for {self.key} (_should_listen={self._should_listen})")
                # ============ END FIX ============

            except Exception as e:
                if isinstance(e, redis_exceptions.ConnectionError) and not self._should_listen:
                    return
                log.exception(f"Unexpected error in Redis listener for {self.key}")
            finally:
                # Clean up pubsub connection
                log.debug(f"Cleaning up Redis listener for {self.key}")
                if self.pubsub:
                    try:
                        await self.pubsub.unsubscribe()
                    except Exception:
                        pass
                    try:
                        await self.pubsub.close()
                    except Exception:
                        pass
                if self._pubsub_client:
                    try:
                        await self._pubsub_client.close()
                    except Exception:
                        pass
                if self._pubsub_sentinel:
                    try:
                        await self._pubsub_sentinel.close()
                    except Exception:
                        pass

        print(f"[DEBUG] About to schedule {self.key}")
        print(f"[DEBUG] core.loop exists: {core.loop is not None}")
        print(f"[DEBUG] core.loop.is_running(): {core.loop.is_running() if core.loop else 'N/A'}")

        if core.loop and core.loop.is_running():
            print(f"[DEBUG] 📋 Using background_tasks.create for {self.key}")
            background_tasks.create(listen(), name=f"redis-listen-{self.key}")
            print(f"[DEBUG] ✅ Task created for {self.key}")
        else:
            print(f"[DEBUG] 📋 Using app.on_startup for {self.key}")
            core.app.on_startup(listen())
            print(f"[DEBUG] ✅ Startup handler registered for {self.key}")

    def publish(self) -> None:
        """Publish the data to Redis and notify other instances using the shared client."""

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
        """Close Redis connections and subscription."""
        self._should_listen = False

        # Close pubsub and its dedicated connection (handled by listen() finally block)
        # Just wait a moment for cleanup
        if self.pubsub:
            try:
                if self.pubsub.subscribed:
                    await self.pubsub.unsubscribe()
            except Exception:
                pass

        # Close main redis client if we own it
        if self._owns_client and self.redis_client is not None:
            try:
                await self.redis_client.close()
            except Exception:
                pass

        # Close main sentinel if we created one
        if self._sentinel is not None:
            try:
                await self._sentinel.close()
            except Exception:
                pass

    def clear(self) -> None:
        super().clear()
        if core.loop:
            background_tasks.create_lazy(self.redis_client.delete(self.key), name=f"redis-delete-{self.key}")
        else:
            core.app.on_startup(self.redis_client.delete(self.key))
