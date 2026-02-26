"""Integration tests for RedisDAO lifecycle with mocked Redis I/O."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis.asyncio as aioredis

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.mem.redis_dao import RedisDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

HOST, PORT, DB = "localhost", 6379, "0"
TTL_DAY, TTL_HR = 86400, 3600
UID_A, UID_B = "abc-123", "def-456"
NAME, STATUS = "widget-alpha", "active"
PFX, META, IDX = "sw:", "sw:meta:", "sw:idx:"
PAIR, TRIPLE, FIVE = 2, 3, 5
REDIS_CLS = "ami.implementations.mem.redis_dao.redis.Redis"


class SampleWidget(StorageModel):
    """Test model: uid, name, status, created_at."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="sw")
    name: str | None = None
    status: str | None = None


def _cfg() -> StorageConfig:
    return StorageConfig(
        storage_type=StorageType.INMEM,
        host=HOST,
        port=PORT,
        database=DB,
    )


def _j(d: dict[str, Any]) -> str:
    return json.dumps(d)


def _ai(items: list[Any]) -> MagicMock:
    async def _g() -> Any:
        for x in items:
            yield x

    return _g()


@pytest.fixture
def mr() -> AsyncMock:
    """Mock redis.asyncio.Redis at I/O boundary."""
    c = AsyncMock()
    for a, v in [
        ("ping", True),
        ("get", None),
        ("expire", True),
        ("delete", 1),
        ("hgetall", {}),
        ("hget", None),
        ("keys", []),
        ("info", {}),
        ("type", "string"),
        ("hkeys", []),
        ("smembers", set()),
    ]:
        setattr(c, a, AsyncMock(return_value=v))
    for a in ("aclose", "setex", "hset", "sadd", "srem", "set"):
        setattr(c, a, AsyncMock())
    c.scan_iter = MagicMock(return_value=_ai([]))
    return c


@pytest.fixture
def dao(mr: AsyncMock) -> RedisDAO:
    """RedisDAO with mock client injected."""
    d = RedisDAO(SampleWidget, _cfg())
    d.client = mr
    return d


class TestInit:
    """StorageConfig(INMEM) wires collection, prefix, TTL."""

    def test_collection(self) -> None:
        assert RedisDAO(SampleWidget, _cfg()).collection_name == "sw"

    def test_prefix(self) -> None:
        assert RedisDAO(SampleWidget, _cfg())._key_prefix == PFX

    def test_client_none(self) -> None:
        assert RedisDAO(SampleWidget, _cfg()).client is None

    def test_ttl(self) -> None:
        assert RedisDAO.DEFAULT_TTL == TTL_DAY


class TestConnect:
    """connect / disconnect / test_connection lifecycle."""

    @pytest.mark.asyncio
    async def test_connect(self) -> None:
        d = RedisDAO(SampleWidget, _cfg())
        mc = AsyncMock(ping=AsyncMock(return_value=True))
        with patch(REDIS_CLS, return_value=mc):
            await d.connect()
        assert d.client is mc

    @pytest.mark.asyncio
    async def test_skip_connected(self, dao: RedisDAO, mr: AsyncMock) -> None:
        await dao.connect()
        mr.ping.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_config(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await RedisDAO(SampleWidget, None).connect()

    @pytest.mark.asyncio
    async def test_no_host(self) -> None:
        c = StorageConfig(storage_type=StorageType.INMEM, host=None, port=PORT)
        with pytest.raises(StorageError, match="host"):
            await RedisDAO(SampleWidget, c).connect()

    @pytest.mark.asyncio
    async def test_no_port(self) -> None:
        c = _cfg()
        c.port = None
        with pytest.raises(StorageError, match="port"):
            await RedisDAO(SampleWidget, c).connect()

    @pytest.mark.asyncio
    async def test_redis_err(self) -> None:
        mc = AsyncMock(ping=AsyncMock(side_effect=aioredis.RedisError("x")))
        with (
            patch(REDIS_CLS, return_value=mc),
            pytest.raises(StorageConnectionError),
        ):
            await RedisDAO(SampleWidget, _cfg()).connect()

    @pytest.mark.asyncio
    async def test_disconnect(self, dao: RedisDAO, mr: AsyncMock) -> None:
        await dao.disconnect()
        mr.aclose.assert_awaited_once()
        assert dao.client is None

    @pytest.mark.asyncio
    async def test_test_conn(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.ping = AsyncMock(return_value=True)
        assert await dao.test_connection() is True

    @pytest.mark.asyncio
    async def test_test_conn_no_client(self) -> None:
        with pytest.raises(StorageError, match="Client not"):
            await RedisDAO(SampleWidget, _cfg()).test_connection()


class TestCreate:
    """create: normalize -> id/timestamps -> TTL -> store -> index."""

    @pytest.mark.asyncio
    async def test_returns_uid(self, dao: RedisDAO, mr: AsyncMock) -> None:
        assert await dao.create({"uid": UID_A, "name": NAME, "ttl": TTL_HR}) == UID_A

    @pytest.mark.asyncio
    async def test_setex(self, dao: RedisDAO, mr: AsyncMock) -> None:
        await dao.create({"uid": UID_A, "name": NAME, "ttl": TTL_HR})
        a = mr.setex.call_args[0]
        assert a[0] == f"{PFX}{UID_A}"
        assert a[1] == TTL_HR

    @pytest.mark.asyncio
    async def test_metadata(self, dao: RedisDAO, mr: AsyncMock) -> None:
        await dao.create({"uid": UID_A, "name": NAME, "ttl": TTL_HR})
        assert mr.hset.call_args_list[0][0][0] == f"{META}{UID_A}"

    @pytest.mark.asyncio
    async def test_indexes(self, dao: RedisDAO, mr: AsyncMock) -> None:
        await dao.create({"uid": UID_A, "name": NAME, "ttl": TTL_HR})
        mr.sadd.assert_awaited()

    @pytest.mark.asyncio
    async def test_no_ttl(self, dao: RedisDAO) -> None:
        with pytest.raises(StorageError, match="TTL"):
            await dao.create({"uid": UID_A, "name": NAME})

    @pytest.mark.asyncio
    async def test_id_fallback(self, dao: RedisDAO, mr: AsyncMock) -> None:
        assert await dao.create({"id": UID_B, "name": NAME, "ttl": TTL_HR}) == UID_B

    @pytest.mark.asyncio
    async def test_timestamps(self, dao: RedisDAO, mr: AsyncMock) -> None:
        before = datetime.now(UTC)
        await dao.create({"uid": UID_A, "name": NAME, "ttl": TTL_HR})
        s = json.loads(mr.setex.call_args[0][2])
        assert datetime.fromisoformat(s["created_at"]) >= before


class TestRead:
    """find_by_id -> get key -> deserialize."""

    @pytest.mark.asyncio
    async def test_found(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=_j({"uid": UID_A, "name": NAME}))
        r = await dao.find_by_id(UID_A)
        assert r is not None
        assert r.uid == UID_A

    @pytest.mark.asyncio
    async def test_miss(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=None)
        assert await dao.find_by_id(UID_A) is None

    @pytest.mark.asyncio
    async def test_access_meta(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=_j({"uid": UID_A}))
        await dao.read(UID_A)
        mr.hset.assert_awaited()

    @pytest.mark.asyncio
    async def test_bad_json(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value="<<bad>>")
        with pytest.raises(StorageError, match="corruption"):
            await dao.read(UID_A)

    @pytest.mark.asyncio
    async def test_get_meta(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.hgetall = AsyncMock(return_value={"ttl": "3600"})
        m = await dao.get_metadata(UID_A)
        assert m is not None
        assert m["ttl"] == "3600"


class TestQuery:
    """query -> scan + filter; find / find_one wrap query."""

    @pytest.mark.asyncio
    async def test_filter(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.smembers = AsyncMock(return_value={UID_A})
        mr.get = AsyncMock(return_value=_j({"uid": UID_A, "name": NAME}))
        r = await dao.query({"name": NAME})
        assert len(r) == 1
        assert r[0]["uid"] == UID_A

    @pytest.mark.asyncio
    async def test_scan_all(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.scan_iter = MagicMock(return_value=_ai([f"{PFX}{UID_A}"]))
        mr.get = AsyncMock(return_value=_j({"uid": UID_A}))
        assert len(await dao.query(None)) == 1

    @pytest.mark.asyncio
    async def test_find_one(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.smembers = AsyncMock(return_value={UID_A})
        mr.get = AsyncMock(return_value=_j({"uid": UID_A, "name": NAME}))
        assert isinstance(await dao.find_one({"name": NAME}), SampleWidget)

    @pytest.mark.asyncio
    async def test_find_one_miss(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.smembers = AsyncMock(return_value=set())
        assert await dao.find_one({"name": "x"}) is None

    @pytest.mark.asyncio
    async def test_find(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.smembers = AsyncMock(return_value={UID_A})
        mr.get = AsyncMock(return_value=_j({"uid": UID_A, "name": NAME}))
        r = await dao.find({"name": NAME})
        assert len(r) == 1
        assert isinstance(r[0], SampleWidget)


class TestUpdate:
    """update -> merge existing -> prepare_data_with_ttl -> indexes."""

    @pytest.mark.asyncio
    async def test_merge(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(
            return_value=_j(
                {
                    "uid": UID_A,
                    "name": NAME,
                    "status": STATUS,
                    "_ttl": TTL_HR,
                }
            )
        )
        await dao.update(UID_A, {"status": "off"})
        s = json.loads(mr.setex.call_args[0][2])
        assert s["name"] == NAME
        assert s["status"] == "off"

    @pytest.mark.asyncio
    async def test_upsert(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=None)
        await dao.update(UID_A, {"name": NAME})
        mr.setex.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_custom_ttl(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=None)
        await dao.update(UID_A, {"name": NAME, "_ttl": TTL_HR})
        assert mr.setex.call_args[0][1] == TTL_HR

    @pytest.mark.asyncio
    async def test_updated_at(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=None)
        before = datetime.now(UTC)
        await dao.update(UID_A, {"name": NAME})
        s = json.loads(mr.setex.call_args[0][2])
        assert datetime.fromisoformat(s["updated_at"]) >= before


class TestDelete:
    """delete -> remove key + metadata + indexes."""

    @pytest.mark.asyncio
    async def test_ok(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.delete = AsyncMock(return_value=1)
        mr.scan_iter = MagicMock(return_value=_ai([]))
        assert await dao.delete(UID_A) is True

    @pytest.mark.asyncio
    async def test_miss(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.delete = AsyncMock(return_value=0)
        mr.scan_iter = MagicMock(return_value=_ai([]))
        assert await dao.delete(UID_A) is False

    @pytest.mark.asyncio
    async def test_idx_cleanup(self, dao: RedisDAO, mr: AsyncMock) -> None:
        k = f"{IDX}name:{NAME}"
        mr.delete = AsyncMock(return_value=1)
        mr.scan_iter = MagicMock(return_value=_ai([k]))
        await dao.delete(UID_A)
        mr.srem.assert_awaited_once_with(k, UID_A)


class TestExpireTouch:
    """expire / touch TTL management."""

    @pytest.mark.asyncio
    async def test_expire(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.expire = AsyncMock(return_value=True)
        assert await dao.expire(UID_A, TTL_HR) is True
        mr.expire.assert_awaited_once_with(f"{PFX}{UID_A}", TTL_HR)

    @pytest.mark.asyncio
    async def test_expire_miss(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.expire = AsyncMock(return_value=False)
        assert await dao.expire(UID_A, TTL_HR) is False

    @pytest.mark.asyncio
    async def test_touch(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.hget = AsyncMock(return_value=str(TTL_HR))
        mr.expire = AsyncMock(return_value=True)
        assert await dao.touch(UID_A) is True

    @pytest.mark.asyncio
    async def test_touch_no_meta(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.hget = AsyncMock(return_value=None)
        assert await dao.touch(UID_A) is False


class TestCountExists:
    """count / exists."""

    @pytest.mark.asyncio
    async def test_count_scan(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.scan_iter = MagicMock(return_value=_ai([f"{PFX}{UID_A}"]))
        assert await dao.count() == 1

    @pytest.mark.asyncio
    async def test_count_filter(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.smembers = AsyncMock(return_value={UID_A, UID_B})
        assert await dao.count({"status": STATUS}) == PAIR

    @pytest.mark.asyncio
    async def test_exists_true(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=_j({"uid": UID_A}))
        assert await dao.exists(UID_A) is True

    @pytest.mark.asyncio
    async def test_exists_false(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=None)
        assert await dao.exists(UID_A) is False


class TestBulk:
    """bulk_create / bulk_delete."""

    @pytest.mark.asyncio
    async def test_create(self, dao: RedisDAO, mr: AsyncMock) -> None:
        ids = await dao.bulk_create(
            [
                {"uid": UID_A, "name": "a", "ttl": TTL_HR},
                {"uid": UID_B, "name": "b", "ttl": TTL_HR},
            ]
        )
        assert len(ids) == PAIR
        assert ids[0] == UID_A

    @pytest.mark.asyncio
    async def test_create_fail(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.setex = AsyncMock(side_effect=Exception("x"))
        with pytest.raises(StorageError, match="Bulk create"):
            await dao.bulk_create([{"uid": UID_A, "name": "x", "ttl": TTL_HR}])

    @pytest.mark.asyncio
    async def test_delete(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.delete = AsyncMock(return_value=1)
        mr.scan_iter = MagicMock(return_value=_ai([]))
        assert await dao.bulk_delete([UID_A, UID_B]) == PAIR

    @pytest.mark.asyncio
    async def test_delete_fail(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.delete = AsyncMock(return_value=0)
        mr.scan_iter = MagicMock(return_value=_ai([]))
        with pytest.raises(StorageError, match="Bulk delete"):
            await dao.bulk_delete([UID_A])


class TestClear:
    """clear_collection -> scan_iter + delete."""

    @pytest.mark.asyncio
    async def test_all(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.scan_iter = MagicMock(return_value=_ai([f"{PFX}{i}" for i in range(FIVE)]))
        mr.delete = AsyncMock(return_value=FIVE)
        assert await dao.clear_collection() == FIVE

    @pytest.mark.asyncio
    async def test_empty(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.scan_iter = MagicMock(return_value=_ai([]))
        assert await dao.clear_collection() == 0


class TestRaw:
    """raw_read_query (GET/KEYS) / raw_write_query (SET/DEL)."""

    @pytest.mark.asyncio
    async def test_get(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value="hello")
        r = await dao.raw_read_query("GET k")
        assert len(r) == 1
        assert r[0]["value"] == "hello"

    @pytest.mark.asyncio
    async def test_get_miss(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.get = AsyncMock(return_value=None)
        assert len(await dao.raw_read_query("GET k")) == 0

    @pytest.mark.asyncio
    async def test_keys(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.scan_iter = MagicMock(return_value=_ai(["a", "b"]))
        assert len(await dao.raw_read_query("KEYS *")) == PAIR

    @pytest.mark.asyncio
    async def test_read_bad_cmd(self, dao: RedisDAO) -> None:
        with pytest.raises(StorageError, match="Unsupported"):
            await dao.raw_read_query("HGET k f")

    @pytest.mark.asyncio
    async def test_set(self, dao: RedisDAO, mr: AsyncMock) -> None:
        assert await dao.raw_write_query("SET k v") == 1

    @pytest.mark.asyncio
    async def test_del(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.delete = AsyncMock(return_value=PAIR)
        assert await dao.raw_write_query("DEL a b") == PAIR

    @pytest.mark.asyncio
    async def test_write_bad_cmd(self, dao: RedisDAO) -> None:
        with pytest.raises(StorageError, match="Unsupported"):
            await dao.raw_write_query("LPUSH l v")


class TestIntrospection:
    """list_models / get_model_info."""

    @pytest.mark.asyncio
    async def test_list_models(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.keys = AsyncMock(return_value=["w:1", "t:1"])
        m = await dao.list_models()
        assert "w" in m
        assert "t" in m

    @pytest.mark.asyncio
    async def test_model_info(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.keys = AsyncMock(return_value=["m:a", "m:b", "m:c"])
        mr.type = AsyncMock(return_value="string")
        info = await dao.get_model_info("m")
        assert info["key_count"] == TRIPLE
        assert info["key_type"] == "string"

    @pytest.mark.asyncio
    async def test_model_info_empty(self, dao: RedisDAO, mr: AsyncMock) -> None:
        mr.keys = AsyncMock(return_value=[])
        info = await dao.get_model_info("e")
        assert info["key_count"] == 0
        assert info["key_type"] is None
