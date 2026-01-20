import asyncio
import threading
from typing import Optional
import httpx
import bittensor as bt


class SharedAsyncRuntime:
    """
    Singleton that provides:
    1. A single event loop running in a background thread
    2. A single httpx.AsyncClient with a large connection pool

    All async operations from Timer threads go through this.
    """

    _instance: Optional["SharedAsyncRuntime"] = None
    _init_lock = threading.Lock()

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._started = False

    @classmethod
    def get_instance(cls) -> "SharedAsyncRuntime":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def start(self):
        """Start the shared event loop and create the HTTP client"""
        if self._started:
            return

        with self._init_lock:
            if self._started:
                return

            self._loop = asyncio.new_event_loop()

            def run_forever():
                asyncio.set_event_loop(self._loop)
                self._loop.run_forever()

            self._loop_thread = threading.Thread(
                target=run_forever,
                daemon=True,
                name="SharedAsyncRuntime",
            )
            self._loop_thread.start()

            # Create client INSIDE the shared loop
            future = asyncio.run_coroutine_threadsafe(
                self._create_client(),
                self._loop,
            )
            self._client = future.result(timeout=30)

            self._started = True
            bt.logging.info("SharedAsyncRuntime started")

    async def _create_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            http2=True,
            limits=httpx.Limits(
                max_connections=600,
                max_keepalive_connections=200,
            ),
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

    @property
    def client(self) -> httpx.AsyncClient:
        """Get the shared client - only use from within run_coroutine()"""
        if not self._started:
            self.start()
        return self._client

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if not self._started:
            self.start()
        return self._loop

    def run_coroutine(self, coro, timeout: float = 600):
        """
        Execute a coroutine in the shared event loop.
        Safe to call from any thread (e.g., Timer callbacks).
        """
        if not self._started:
            self.start()

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def shutdown(self):
        """Clean shutdown"""
        if not self._started:
            return

        try:
            if self._client:
                asyncio.run_coroutine_threadsafe(
                    self._client.aclose(),
                    self._loop,
                ).result(timeout=10)

            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=5)
        except Exception as e:
            bt.logging.warning(f"Shutdown error: {e}")
        finally:
            self._started = False


# Global singleton
shared_runtime = SharedAsyncRuntime.get_instance()
