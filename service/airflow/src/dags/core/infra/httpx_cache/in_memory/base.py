import httpx
import json
import hashlib
from dataclasses import dataclass, field


@dataclass(init=True)
class InmemoryDBTransportMixin:
    cache_store: dict = field(init=True, repr=False, default_factory=dict)

    def lookup(self, request: httpx.Request) -> httpx.Response | None:
        cache_key = self.generate_cache_key(request)
        cached_response = self.cache_store.get(cache_key, None)
        if cached_response is None:
            return None

        self.log.info(
            f"[CACHE HIT] {request.url}: {cache_key}: {self.today}",
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

    def generate_cache_key(self, request: httpx.Request) -> str:
        """요청의 고유한 키를 생성"""
        key_data = {
            "method": request.method,
            "url": str(request.url),
            "body": request.content.decode() if request.content else None,
        }
        return hashlib.sha256(json.dumps(key_data, sort_keys=True, default=str).encode()).hexdigest()
