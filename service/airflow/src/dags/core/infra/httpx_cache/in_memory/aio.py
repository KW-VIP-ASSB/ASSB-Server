import httpx
import pymongo
from ..base import IAsyncCacheTransport
from dataclasses import dataclass, field
from .base import InmemoryDBTransportMixin


@dataclass(init=True, repr=True, slots=True)
class AsyncInmemoryCacheTransport(
    IAsyncCacheTransport,
    InmemoryDBTransportMixin,
):
    client: pymongo.AsyncMongoClient = field(init=False, repr=False)

    def __post_init__(self):
        super(AsyncInmemoryCacheTransport, self).__post_init__()
        self.log.info("Connected to in-memory cache")

    def lookup(self, request: httpx.Request) -> httpx.Response | None:
        cache_key = self.generate_cache_key(request)
        cached_response = self.cache_store.get(cache_key, None)
        if cached_response is None:
            return None

        self.log.info(
            f"[CACHE HIT] {request.url}: {cache_key}",
        )

        headers = httpx.Headers(cached_response["headers"])
        headers.pop("content-encoding", None)
        headers.pop("Content-Encoding", None)

        return httpx.Response(
            status_code=cached_response["status_code"],
            content=cached_response["content"],
            headers=headers,
            request=request,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        cached_response = self.lookup(request)
        if cached_response is not None:
            return cached_response
        cache_key = self.generate_cache_key(request)
        self.log.info(f"[CACHE MISS] {request.url} : {cache_key}")
        response = await self.transport.handle_async_request(request)
        # Cacheing
        self.cache_store[cache_key] = {
            "request": {
                "method": request.method,
                "url": str(request.url),
                "body": request.content.decode() if request.content else None,
            },
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "content": response.read().decode(),
        }
        return response
