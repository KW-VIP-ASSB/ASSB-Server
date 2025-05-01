import math
from dataclasses import dataclass, field
from random import shuffle
from typing import TypeVar

T = TypeVar("T")


def divide_list(data: list[T], n: int = 25) -> list[list[T]]:
    size = math.ceil(len(data) / n)
    divided = [data[i : i + size] for i in range(0, len(data), size)]
    return divided


def split_list_into_chunks(data: list[T], chunk_size: int = 100) -> list[list[T]]:
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


@dataclass(init=True, kw_only=True)
class DivideList:
    n: int | None = field(default=25)
    size: int | None = field(default=None)
    is_shuffle: bool = field(default=False)

    def deivide(self, data: list[T]) -> list[list[T]]:
        if self.is_shuffle:
            shuffle(data)
        if self.n is None and self.size is None:
            raise Exception("n or size should be given")
        if self.size:
            return self.split_list_into_chunks(data, self.size)
        elif self.n:
            return self.divide_list(data, self.n)

    @staticmethod
    def divide_list(data: list[T], n: int = 25) -> list[list[T]]:
        size = math.ceil(len(data) / n)
        divided = [data[i : i + size] for i in range(0, len(data), size)]
        return divided

    @staticmethod
    def split_list_into_chunks(data: list[T], chunk_size: int = 100) -> list[list[T]]:
        return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def parse_socks5_proxy(proxy_string: str) -> dict:
    proxy_string = proxy_string.replace("socks5://", "")
    credentials_server = proxy_string.split("@")
    if len(credentials_server) != 2:
        raise ValueError("Invalid proxy string format")
    credentials = credentials_server[0]
    server = credentials_server[1]
    username_password = credentials.split(":")
    if len(username_password) != 2:
        raise ValueError("Invalid credentials format")
    username, password = username_password
    return {"server": server, "username": username, "password": password}


# Example usage
