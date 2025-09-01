from __future__ import annotations

import hashlib
from typing import Any, Callable, Tuple

from diskcache import Cache


_cache = Cache(".cache")


def _hash_key(key: str) -> str:
	return hashlib.sha256(key.encode("utf-8")).hexdigest()


def cached_call(key_parts: Tuple[str, ...], fn: Callable[[], Any], expire: int = 3600) -> Any:
	key_raw = "|".join(key_parts)
	key = _hash_key(key_raw)
	if key in _cache:
		return _cache[key]
	value = fn()
	_cache.set(key, value, expire=expire)
	return value
