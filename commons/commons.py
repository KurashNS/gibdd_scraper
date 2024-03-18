from typing import Callable, Any


class Cache:
    def __init__(self):
        self._cache = {}

    def get(self, key: str) -> Any:
        return self._cache.get(key)

    def set(self, key: str, value: Any):
        self._cache[key] = value

    def delete(self, key: str):
        if key in self._cache:
            del self._cache[key]
