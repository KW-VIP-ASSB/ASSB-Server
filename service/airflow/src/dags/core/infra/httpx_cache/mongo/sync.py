from dataclasses import dataclass, field
import hashlib
import json

import httpx
import pymongo

from ..base import ICacheTransport

from .base import MongoDBTransportMixin


@dataclass(init=True, repr=True, slots=True)
class MongoDBTransport(
    ICacheTransport,
    MongoDBTransportMixin,
):
    client: pymongo.MongoClient = field(init=False, repr=False)

    def __post_init__(self):
        super(MongoDBTransport, self).__post_init__()

        self.client = pymongo.MongoClient(self.mongo_uri)
        self._cache_collection = self.client[self.db][self.collection]
        self.log.info(f"Connected to MongoDB: {self.db}.{self.collection}.{self.today}")
        if self.today is not None:
            self.today = self.today.replace(hour=0, minute=0, second=0, microsecond=0)

    def generate_cache_key(self, request: httpx.Request) -> str:
        key_data = {
            "method": request.method,
            "url": str(request.url),
            "body": request.content.decode() if request.content else None,
            "date": self.today,
        }
        if self.today is None:
            key_data.pop("date")
        return hashlib.sha256(json.dumps(key_data, sort_keys=True, default=str).encode()).hexdigest()

    def lookup(self, request: httpx.Request) -> httpx.Response | None:
        cache_key = self.generate_cache_key(request)
        cached_response = self._cache_collection.find_one({"_id": cache_key, "date": self.today})
        if cached_response is None:
            return None
        self.log.info(f"[CACHE HIT] {request.url}: {cache_key}: {self.today}")

        headers = httpx.Headers(cached_response["headers"])
        headers.pop("content-encoding", None)
        headers.pop("Content-Encoding", None)

        return httpx.Response(
            status_code=cached_response["status_code"],
            content=cached_response["content"],
            headers=headers,
            request=request,
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        cached_response = self.lookup(request)
        if cached_response is not None:
            return cached_response
        cache_key = self.generate_cache_key(request)
        self.log.info(f"[CACHE MISS] {request.url} : {cache_key} : {self.today}")

        response = self.transport.handle_request(request)

        # Cacheing
        self._cache_collection.insert_one(
            {
                "_id": cache_key,
                "request": {
                    "method": request.method,
                    "url": str(request.url),
                    "body": request.content.decode() if request.content else None,
                },
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "content": response.read().decode(),
                "date": self.today,
            }
        )
        return response
