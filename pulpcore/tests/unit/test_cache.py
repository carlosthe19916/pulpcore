from time import sleep
from time import time as now
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp.web_exceptions import HTTPNotModified
from django.test import override_settings
from django.utils.http import http_date

import pulpcore.app.redis_connection
from pulpcore.cache import Cache
from pulpcore.cache.cache import AsyncContentCache


@pytest.fixture
def pulp_redisdb(settings, redisdb, monkeypatch):
    monkeypatch.setattr(pulpcore.app.redis_connection, "_conn", None)
    monkeypatch.setattr(pulpcore.app.redis_connection, "_a_conn", None)
    settings.CACHE_ENABLED = True
    settings.REDIS_URL = "unix://" + redisdb.get_connection_kwargs()["path"]
    return redisdb


def test_basic_set_get(pulp_redisdb):
    """Tests setting value, then getting it"""
    cache = Cache()
    cache.set("key", "hello")
    ret = cache.get("key")
    assert ret == b"hello"
    cache.set("key", "there")
    ret = cache.get("key")
    assert ret == b"there"


def test_basic_exists(pulp_redisdb):
    """Tests that keys already set exist"""
    cache = Cache()
    cache.set("key", "hello")
    assert cache.exists("key")
    assert not cache.exists("absent")


def test_basic_delete(pulp_redisdb):
    """Tests deleting value"""
    cache = Cache()
    cache.set("key", "hello")
    assert cache.exists("key")
    cache.delete("key")
    ret = cache.get("key")
    assert ret is None


def test_basic_expires(pulp_redisdb):
    """Tests setting values with expiration times"""
    cache = Cache()
    cache.set("key", "hi", expires=2)
    ret = cache.get("key")
    assert ret == b"hi"
    sleep(3)
    ret = cache.get("key")
    assert ret is None


def test_group_with_base_key(pulp_redisdb):
    """Tests grouping multiple key-values under one base-key"""
    cache = Cache()
    tuples = [
        ("key1", "hi", "base1"),
        ("key2", "friends", "base1"),
        ("key1", "hola", "base2"),
        ("key2", "amigos", "base2"),
    ]
    for key, value, base_key in tuples:
        cache.set(key, value, base_key=base_key)
    for key, value, base_key in tuples:
        assert value.encode() == cache.get(key, base_key=base_key)

    dict1 = {a.encode(): b.encode() for a, b, _ in tuples[:2]}
    dict2 = {a.encode(): b.encode() for a, b, _ in tuples[2:]}
    assert cache.get(None, base_key="base1") == dict1
    assert cache.get(None, base_key="base2") == dict2
    assert cache.exists(base_key="base1")
    assert cache.exists(base_key="base2")
    assert cache.exists(base_key=["base1", "base2"]) == 2


def test_delete_base_key(pulp_redisdb):
    """Tests deleting multiple key-values under one base-key"""
    cache = Cache()
    cache.delete(base_key="base1")
    assert not cache.exists("key1", base_key="base1")
    assert not cache.exists("key2", base_key="base1")
    assert not cache.exists(base_key="base1")

    cache.set("key1", "hi", base_key="base1")
    assert cache.exists("key1", base_key="base1")
    # multi delete
    cache.delete(base_key=["base1", "base2"])
    assert cache.exists(base_key=["base1", "base2"]) == 0


def test_clear(pulp_redisdb):
    """Tests clearing the cache"""
    cache = Cache()
    tuples = [
        ("key", "hi", None),
        ("key1", "there", None),
        ("key", "hey", "base"),
        ("key1", "now", "base"),
    ]
    for key, value, base_key in tuples:
        cache.set(key, value, base_key=base_key)
    cache.redis.flushdb()
    for key, _, base_key in tuples:
        assert not cache.exists(key, base_key=base_key)


def _request_with_ims(value):
    return Mock(headers={"If-Modified-Since": value} if value else {})


def test_async_content_cache_not_modified():
    """_not_modified compares If-Modified-Since to the stored Last-Modified at second resolution."""
    last_modified = http_date(1_000_000_000)

    # Client copy is as new or newer -> not modified.
    assert AsyncContentCache._not_modified(_request_with_ims(last_modified), last_modified) is True
    assert (
        AsyncContentCache._not_modified(_request_with_ims(http_date(1_000_000_060)), last_modified)
        is True
    )
    # Content changed after the client's copy -> modified.
    assert (
        AsyncContentCache._not_modified(_request_with_ims(http_date(999_999_940)), last_modified)
        is False
    )
    # Missing/invalid values -> modified (200).
    assert AsyncContentCache._not_modified(_request_with_ims(None), last_modified) is False
    assert AsyncContentCache._not_modified(_request_with_ims(last_modified), None) is False
    assert AsyncContentCache._not_modified(_request_with_ims("garbage"), last_modified) is False
    # RFC 9110: If-None-Match takes precedence over If-Modified-Since.
    both = Mock(headers={"If-Modified-Since": last_modified, "If-None-Match": '"abc"'})
    assert AsyncContentCache._not_modified(both, last_modified) is False
    # RFC 7232: ignore IMS in the future relative to the origin clock.
    future = http_date(now() + 86400)
    assert AsyncContentCache._not_modified(_request_with_ims(future), last_modified) is False


def test_async_content_cache_make_not_modified_echoes_metadata():
    """The 304 carries only validator/caching metadata already present on the source."""
    last_modified = http_date(1_000_000_000)
    source = {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "Content-Length": "1024",
        "X-PULP-CACHE": "HIT",
    }

    exc = AsyncContentCache._make_not_modified(source, last_modified)

    assert isinstance(exc, HTTPNotModified)
    assert exc.headers["Last-Modified"] == last_modified
    assert exc.headers["Cache-Control"] == "public, max-age=0, must-revalidate"
    assert exc.headers["X-PULP-CACHE"] == "HIT"
    assert "Content-Length" not in exc.headers

    bare = AsyncContentCache._make_not_modified({}, last_modified)
    assert "X-PULP-CACHE" not in bare.headers
    assert "Cache-Control" not in bare.headers


def test_async_content_cache_build_response_pops_last_modified():
    """build_response must not pass the stored last_modified field to the response constructor."""
    cache = AsyncContentCache.__new__(AsyncContentCache)  # avoid Redis connection in __init__
    entry = {
        "type": "Response",
        "status": 200,
        "headers": {"Last-Modified": http_date(1_000_000_000)},
        "last_modified": http_date(1_000_000_000),
        "body": b"hello".hex(),
    }

    response = cache.build_response(entry)

    assert response.status == 200
    assert response.body == b"hello"
    assert response.headers["Last-Modified"] == http_date(1_000_000_000)
    assert response.headers["X-PULP-CACHE"] == "HIT"


def _cache_for_decorator():
    """AsyncContentCache instance that skips Redis and uses injected request/key helpers."""
    cache = AsyncContentCache.__new__(AsyncContentCache)
    cache.auth = None
    cache.default_base_key = "base"
    cache.keys = ()
    cache.default_expires_ttl = 60
    cache.get_request_from_args = lambda args: args[0]
    cache.make_key = lambda req: "key"
    return cache


@pytest.mark.asyncio
async def test_cache_hit_304_does_not_rebuild_response():
    """A matching IMS on a cache hit 304s without constructing the cached response."""
    last_modified = http_date(1_000_000_000)
    entry = {
        "type": "Response",
        "status": 200,
        "headers": {
            "Last-Modified": last_modified,
            "Cache-Control": "public, max-age=0, must-revalidate",
        },
        "last_modified": last_modified,
        "body": b"payload".hex(),
        "expires": None,
    }
    cache = _cache_for_decorator()
    cache.get_entry = AsyncMock(return_value=entry)
    cache.build_response = Mock(side_effect=AssertionError("must not reconstruct"))
    request = Mock(headers={"If-Modified-Since": last_modified})

    async def handler(req):
        raise AssertionError("handler must not run on cache hit")

    with override_settings(CACHE_ENABLED=True):
        wrapped = AsyncContentCache.__call__(cache, handler)
        with pytest.raises(HTTPNotModified) as exc:
            await wrapped(request)

    cache.build_response.assert_not_called()
    assert exc.value.headers["Last-Modified"] == last_modified
    assert exc.value.headers["X-PULP-CACHE"] == "HIT"


@pytest.mark.asyncio
async def test_cache_hit_304_falls_back_to_header_without_last_modified_field():
    """Entries cached before last_modified was stored still 304 from the Last-Modified header."""
    last_modified = http_date(1_000_000_000)
    entry = {
        "type": "Response",
        "status": 200,
        "headers": {"Last-Modified": last_modified},
        "body": b"payload".hex(),
        "expires": None,
    }
    cache = _cache_for_decorator()
    cache.get_entry = AsyncMock(return_value=entry)
    cache.build_response = Mock(side_effect=AssertionError("must not reconstruct"))
    request = Mock(headers={"If-Modified-Since": last_modified})

    async def handler(req):
        raise AssertionError("handler must not run on cache hit")

    with override_settings(CACHE_ENABLED=True):
        wrapped = AsyncContentCache.__call__(cache, handler)
        with pytest.raises(HTTPNotModified):
            await wrapped(request)

    cache.build_response.assert_not_called()


@pytest.mark.asyncio
async def test_cache_hit_stale_ims_rebuilds_response():
    """An older If-Modified-Since on a cache hit still reconstructs the full cached response."""
    last_modified = http_date(1_000_000_000)
    older = http_date(999_999_000)
    entry = {
        "type": "Response",
        "status": 200,
        "headers": {"Last-Modified": last_modified},
        "last_modified": last_modified,
        "body": b"payload".hex(),
        "expires": None,
    }
    rebuilt = Mock(headers={"X-PULP-ARTIFACT-SIZE": None})
    cache = _cache_for_decorator()
    cache.get_entry = AsyncMock(return_value=entry)
    cache.build_response = Mock(return_value=rebuilt)
    request = Mock(headers={"If-Modified-Since": older})

    async def handler(req):
        raise AssertionError("handler must not run on cache hit")

    with override_settings(CACHE_ENABLED=True):
        wrapped = AsyncContentCache.__call__(cache, handler)
        response = await wrapped(request)

    cache.build_response.assert_called_once_with(entry)
    assert response is rebuilt


@pytest.mark.asyncio
async def test_cache_miss_304_after_make_entry():
    """A matching IMS on a cache miss 304s from the fresh response after it is stored."""
    last_modified = http_date(1_000_000_000)
    built = Mock(headers={"Last-Modified": last_modified}, prepared=False, status=200)
    cache = _cache_for_decorator()
    cache.get_entry = AsyncMock(return_value=None)
    cache.make_entry = AsyncMock(return_value=built)
    request = Mock(headers={"If-Modified-Since": last_modified})

    async def handler(req):
        raise AssertionError("handler is invoked via make_entry")

    with override_settings(CACHE_ENABLED=True):
        wrapped = AsyncContentCache.__call__(cache, handler)
        with pytest.raises(HTTPNotModified) as exc:
            await wrapped(request)

    cache.make_entry.assert_awaited_once()
    assert exc.value.headers["Last-Modified"] == last_modified


@pytest.mark.asyncio
async def test_cache_miss_does_not_304_prepared_stream():
    """A live stream that already started writing must not be converted into a 304."""
    last_modified = http_date(1_000_000_000)
    stream = Mock(headers={"Last-Modified": last_modified}, prepared=True, status=200)
    cache = _cache_for_decorator()
    cache.get_entry = AsyncMock(return_value=None)
    cache.make_entry = AsyncMock(return_value=stream)
    request = Mock(headers={"If-Modified-Since": last_modified})

    async def handler(req):
        raise AssertionError("handler is invoked via make_entry")

    with override_settings(CACHE_ENABLED=True):
        wrapped = AsyncContentCache.__call__(cache, handler)
        response = await wrapped(request)

    assert response is stream


@pytest.mark.asyncio
async def test_make_entry_does_not_cache_304():
    """HTTPNotModified is HTTPSuccessful but must never be written to Redis."""
    cache = _cache_for_decorator()
    cache.set = AsyncMock()

    async def handler():
        raise HTTPNotModified(headers={"Last-Modified": http_date(1_000_000_000)})

    with pytest.raises(HTTPNotModified):
        await cache.make_entry("k", "b", handler, (), {}, 60)

    cache.set.assert_not_called()
