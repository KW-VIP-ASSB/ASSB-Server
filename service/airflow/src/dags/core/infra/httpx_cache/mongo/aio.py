from dataclasses import dataclass, field
import hashlib
import json

import httpx
import pymongo

from ..base import IAsyncCacheTransport
from .base import MongoDBTransportMixin


@dataclass(init=True, repr=True, slots=True)
class AsyncMongoDBTransport(
    IAsyncCacheTransport,
    MongoDBTransportMixin,
):
    client: pymongo.MongoClient = field(init=False, repr=False)

    def __post_init__(self):
        super(AsyncMongoDBTransport, self).__post_init__()
        self.client = pymongo.MongoClient(self.mongo_uri)
        db = self.client[self.db]
        self.cache_collection = db[self.collection]
        self.log.info(f"Connected to MongoDB: {self.db}.{self.collection}.{self.today}")
        if self.today is not None:
            self.today = self.today.replace(hour=0, minute=0, second=0, microsecond=0)

    def generate_cache_key(self, request: httpx.Request) -> str:
        try:
            body = request._content 
        except AttributeError:
            body = None
            
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
        cached_response = self.cache_collection.find_one({"_id": cache_key})
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

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = None if request.method.upper() == "GET" else request.content.decode() if request.content else None
        cached_response = self.lookup(request)
        if cached_response is not None:
            return cached_response

        cache_key = self.generate_cache_key(request)
        self.log.info(f"[CACHE MISS] {cache_key} : {self.today}")
        response = await self.transport.handle_async_request(request)
        # Cacheing
        if response.is_success:
            content = await response.aread()
            try: 
                self.cache_collection.insert_one(
                {
                    "_id": cache_key,
                    "request": {
                        "method": request.method,
                        "url": str(request.url),
                        "body": request.content.decode() if request.content else None,
                    },
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "content": content.decode(),
                    "date": self.today,
                }
            )
                self.log.info(f"[CACHE INSERT] {cache_key}: {self.today}")
            except pymongo.errors.DuplicateKeyError:
                self.log.warning(f"[CACHE DUPLICATE] {cache_key} already exists, skipping insert.")
        return response
