import datetime
import httpx
import json
import hashlib
from pymongo.collation import Collation
from dataclasses import dataclass, field


@dataclass(init=True)
class MongoDBTransportMixin:
    mongo_uri: str = field(init=True, repr=True)
    db: str = field(init=True, repr=True)
    collection: str = field(init=True, repr=True)
    _cache_collection: Collation = field(init=False, repr=False)
    today: datetime.datetime | None = field(
        init=True,
        repr=True,
        default=None,
    )

    def generate_cache_key(self, request: httpx.Request) -> str:
        """요청의 고유한 키를 생성"""
        key_data = {
            "method": request.method,
            "url": str(request.url),
            "body": request.content.decode() if request.content else None,
            "date": self.today,
        }
        return hashlib.sha256(json.dumps(key_data, sort_keys=True, default=str).encode()).hexdigest()
