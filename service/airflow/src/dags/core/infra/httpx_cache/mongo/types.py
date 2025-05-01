from dataclasses import dataclass, field, asdict


@dataclass(frozen=True, kw_only=True, init=True, repr=True, slots=True)
class MongoDBCacheConfig:
    database: str = field(init=True)
    collection: str = field(init=True)

    @property
    def db(self) -> str:
        return self.database

    def to_dict(self) -> dict:
        return asdict(self)
