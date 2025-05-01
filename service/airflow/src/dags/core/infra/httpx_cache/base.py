import httpx
from abc import ABC, abstractmethod
import logging
from dataclasses import dataclass, field


@dataclass(init=True)
class ICacheTransport(httpx.BaseTransport, ABC):
    log: logging.Logger = field(init=False, repr=False)
    transport: httpx.HTTPTransport = field(init=True, default=httpx.HTTPTransport(retries=3), repr=False)

    def __post_init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def lookup(self, request: httpx.Request) -> httpx.Response | None:
        raise NotImplementedError

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        raise NotImplementedError


@dataclass(init=True)
class IAsyncCacheTransport(httpx.AsyncBaseTransport, ABC):
    log: logging.Logger = field(init=False, repr=False)
    transport: httpx.AsyncHTTPTransport = field(init=True, default=httpx.AsyncHTTPTransport(retries=3), repr=False)

    def __post_init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def lookup(self, request: httpx.Request) -> httpx.Response | None:
        raise NotImplementedError

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raise NotImplementedError
