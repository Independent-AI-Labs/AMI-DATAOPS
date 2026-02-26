"""Integration tests for PostgreSQLDAO full lifecycle."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import (
    QueryError,
    StorageConnectionError,
    StorageError,
    StorageValidationError,
)
from ami.core.storage_types import StorageType
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_TEST_PORT = 5432
_TEST_UID = "abc-123"
_SECOND_UID = "def-456"
_THIRD_UID = "ghi-789"
_ITEM_COUNT = 5
_BULK_DELETE_FOUND = 2
_SELECT_ONE = 1
_RAW_WRITE_AFFECTED = 4
_ESTIMATED_ROWS = 1000
_PAIR_COUNT = 2
_POOL_PATH = "ami.implementations.sql.postgresql_dao.asyncpg.create_pool"
_COL_DICTS = [
    {"column_name": "id"},
    {"column_name": "name"},
    {"column_name": "count"},
    {"column_name": "active"},
    {"column_name": "created_at"},
    {"column_name": "updated_at"},
]


class _TestItem(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = ""
    count: int = 0
    active: bool = True


def _cfg() -> StorageConfig:
    return StorageConfig(
        storage_type=StorageType.RELATIONAL,
        host="localhost",
        port=_TEST_PORT,
        database="testdb",
        username="user",
        password="pass",
    )


def _conn() -> AsyncMock:
    c = AsyncMock()
    c.fetchval = AsyncMock(return_value=None)
    c.fetchrow = AsyncMock(return_value=None)
    c.fetch = AsyncMock(return_value=[])
    c.execute = AsyncMock(return_value="UPDATE 1")
    return c


def _pool(conn: AsyncMock) -> MagicMock:
    p = MagicMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    p.acquire.return_value = ctx
    p.close = AsyncMock()
    return p


def _dao(pool: MagicMock | None = None) -> PostgreSQLDAO:
    d = PostgreSQLDAO(_TestItem, _cfg())
    if pool is not None:
        d.pool = pool
        d._table_created = True
    return d


def _row(
    uid: str = _TEST_UID,
    name: str = "alice",
    count: int = _ITEM_COUNT,
    active: bool = True,
) -> dict[str, Any]:
    return {
        "id": uid,
        "uid": uid,
        "name": name,
        "count": count,
        "active": active,
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }


class TestInit:
    def test_collection_name(self) -> None:
        assert _dao().collection_name == "test_items"

    def test_pool_and_table_flag_defaults(self) -> None:
        d = PostgreSQLDAO(_TestItem, _cfg())
        assert d.pool is None
        assert d._table_created is False

    def test_config_and_model_cls_stored(self) -> None:
        cfg = _cfg()
        dao = PostgreSQLDAO(_TestItem, cfg)
        assert dao.config is cfg
        assert dao.model_cls is _TestItem


class TestConnect:
    @pytest.mark.asyncio
    @patch(_POOL_PATH, new_callable=AsyncMock)
    async def test_creates_pool(self, mock_cp: AsyncMock) -> None:
        mp = MagicMock()
        mock_cp.return_value = mp
        d = PostgreSQLDAO(_TestItem, _cfg())
        await d.connect()
        mock_cp.assert_awaited_once()
        assert d.pool is mp

    @pytest.mark.asyncio
    @patch(_POOL_PATH, new_callable=AsyncMock)
    async def test_idempotent(self, mock_cp: AsyncMock) -> None:
        ep = MagicMock()
        d = PostgreSQLDAO(_TestItem, _cfg())
        d.pool = ep
        await d.connect()
        mock_cp.assert_not_awaited()
        assert d.pool is ep

    @pytest.mark.asyncio
    async def test_raises_without_config(self) -> None:
        with pytest.raises(StorageError, match="StorageConfig is required"):
            await PostgreSQLDAO(_TestItem, None).connect()

    @pytest.mark.asyncio
    @patch(_POOL_PATH, new_callable=AsyncMock)
    async def test_wraps_os_error(self, mock_cp: AsyncMock) -> None:
        mock_cp.side_effect = OSError("refused")
        with pytest.raises(StorageConnectionError):
            await PostgreSQLDAO(_TestItem, _cfg()).connect()


class TestDisconnect:
    """disconnect() closes pool."""

    @pytest.mark.asyncio
    async def test_closes_pool(self) -> None:
        p = _pool(_conn())
        d = _dao(p)
        await d.disconnect()
        p.close.assert_awaited_once()
        assert d.pool is None

    @pytest.mark.asyncio
    async def test_noop_when_no_pool(self) -> None:
        d = PostgreSQLDAO(_TestItem, _cfg())
        await d.disconnect()
        assert d.pool is None


class TestTestConnection:
    """test_connection() verifies SELECT 1."""

    @pytest.mark.asyncio
    async def test_returns_true(self) -> None:
        c = _conn()
        c.fetchval.return_value = _SELECT_ONE
        d = _dao(_pool(c))
        assert await d.test_connection() is True
        c.fetchval.assert_awaited_once_with("SELECT 1")


class TestCreate:
    """create() delegates through postgresql_create."""

    @pytest.mark.asyncio
    async def test_with_model_instance(self) -> None:
        c = _conn()
        c.fetchval.return_value = _TEST_UID
        c.fetch.return_value = _COL_DICTS
        inst = _TestItem(uid=_TEST_UID, name="alice", count=_ITEM_COUNT)
        assert await _dao(_pool(c)).create(inst) == _TEST_UID

    @pytest.mark.asyncio
    async def test_with_dict(self) -> None:
        c = _conn()
        c.fetchval.return_value = _SECOND_UID
        c.fetch.return_value = _COL_DICTS[:_PAIR_COUNT]
        result = await _dao(_pool(c)).create(
            {"uid": _SECOND_UID, "name": "bob"},
        )
        assert result == _SECOND_UID

    @pytest.mark.asyncio
    async def test_rejects_invalid_type(self) -> None:
        with pytest.raises(StorageError, match="StorageModel or dict"):
            await _dao(_pool(_conn())).create(42)


class TestFindById:
    """find_by_id() delegates through postgresql_read.read."""

    @pytest.mark.asyncio
    async def test_returns_model(self) -> None:
        c = _conn()
        c.fetchrow.return_value = _row()
        result = await _dao(_pool(c)).find_by_id(_TEST_UID)
        assert result is not None
        assert result.uid == _TEST_UID
        assert result.name == "alice"

    @pytest.mark.asyncio
    async def test_returns_none(self) -> None:
        c = _conn()
        c.fetchrow.return_value = None
        assert await _dao(_pool(c)).find_by_id("nonexistent") is None


class TestFind:
    """find() delegates through postgresql_read.query."""

    @pytest.mark.asyncio
    async def test_returns_matching(self) -> None:
        c = _conn()
        c.fetch.return_value = [_row(_TEST_UID), _row(_SECOND_UID, "bob")]
        assert len(await _dao(_pool(c)).find({"active": True})) == _PAIR_COUNT

    @pytest.mark.asyncio
    async def test_limit(self) -> None:
        c = _conn()
        c.fetch.return_value = [_row(_TEST_UID), _row(_SECOND_UID)]
        results = await _dao(_pool(c)).find({"active": True}, limit=1)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_skip(self) -> None:
        c = _conn()
        c.fetch.return_value = [_row(_TEST_UID), _row(_SECOND_UID)]
        results = await _dao(_pool(c)).find({"active": True}, skip=1)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        c = _conn()
        c.fetch.return_value = []
        assert await _dao(_pool(c)).find({"active": False}) == []


class TestFindOne:
    """find_one() returns first match or None."""

    @pytest.mark.asyncio
    async def test_returns_first(self) -> None:
        c = _conn()
        c.fetch.return_value = [_row()]
        result = await _dao(_pool(c)).find_one({"name": "alice"})
        assert result is not None
        assert result.name == "alice"

    @pytest.mark.asyncio
    async def test_returns_none(self) -> None:
        c = _conn()
        c.fetch.return_value = []
        assert await _dao(_pool(c)).find_one({"name": "nobody"}) is None


class TestUpdate:
    """update() delegates through postgresql_update."""

    @pytest.mark.asyncio
    async def test_successful(self) -> None:
        c = _conn()
        c.execute.return_value = "UPDATE 1"
        d = _dao(_pool(c))
        await d.update(_TEST_UID, {"name": "alice-updated"})
        c.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_found_raises(self) -> None:
        c = _conn()
        c.execute.return_value = "UPDATE 0"
        with pytest.raises(StorageError, match="Record not found"):
            await _dao(_pool(c)).update("missing", {"name": "nope"})


class TestDelete:
    """delete() delegates through postgresql_delete."""

    @pytest.mark.asyncio
    async def test_returns_true(self) -> None:
        c = _conn()
        c.execute.return_value = "DELETE 1"
        assert await _dao(_pool(c)).delete(_TEST_UID) is True

    @pytest.mark.asyncio
    async def test_returns_false(self) -> None:
        c = _conn()
        c.execute.return_value = "DELETE 0"
        assert await _dao(_pool(c)).delete("nonexistent") is False


class TestCount:
    """count() delegates through postgresql_read.count."""

    @pytest.mark.asyncio
    async def test_returns_count(self) -> None:
        c = _conn()
        c.fetchval.return_value = _ITEM_COUNT
        assert await _dao(_pool(c)).count({"active": True}) == _ITEM_COUNT

    @pytest.mark.asyncio
    async def test_empty_query(self) -> None:
        c = _conn()
        c.fetchval.return_value = 0
        assert await _dao(_pool(c)).count({}) == 0


class TestExists:
    """exists() checks via postgresql_read.read."""

    @pytest.mark.asyncio
    async def test_true_when_found(self) -> None:
        c = _conn()
        c.fetchrow.return_value = _row()
        assert await _dao(_pool(c)).exists(_TEST_UID) is True

    @pytest.mark.asyncio
    async def test_false_when_missing(self) -> None:
        c = _conn()
        c.fetchrow.return_value = None
        assert await _dao(_pool(c)).exists("nope") is False


class TestBulkCreate:
    """bulk_create() iterates create() for each instance."""

    @pytest.mark.asyncio
    async def test_returns_all_ids(self) -> None:
        c = _conn()
        c.fetchval.side_effect = [_TEST_UID, _SECOND_UID]
        c.fetch.return_value = _COL_DICTS[:_PAIR_COUNT]
        d = _dao(_pool(c))
        items = [
            _TestItem(uid=_TEST_UID, name="a"),
            _TestItem(uid=_SECOND_UID, name="b"),
        ]
        ids = await d.bulk_create(items)
        assert len(ids) == _PAIR_COUNT
        assert ids[0] == _TEST_UID
        assert ids[1] == _SECOND_UID


class TestBulkDelete:
    """bulk_delete() returns count of successful deletions."""

    @pytest.mark.asyncio
    async def test_counts_deleted(self) -> None:
        c = _conn()
        c.execute.side_effect = ["DELETE 1", "DELETE 0", "DELETE 1"]
        deleted = await _dao(_pool(c)).bulk_delete(
            [_TEST_UID, _SECOND_UID, _THIRD_UID],
        )
        assert deleted == _BULK_DELETE_FOUND


class TestRawReadQuery:
    """raw_read_query() executes arbitrary SELECT."""

    @pytest.mark.asyncio
    async def test_returns_rows(self) -> None:
        c = _conn()
        c.fetch.return_value = [{"id": _TEST_UID}]
        results = await _dao(_pool(c)).raw_read_query(
            "SELECT id FROM test_items",
        )
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_rejects_dict_params(self) -> None:
        with pytest.raises(StorageValidationError):
            await _dao(_pool(_conn())).raw_read_query(
                "SELECT * FROM t WHERE id = $1",
                {"id": _TEST_UID},
            )

    @pytest.mark.asyncio
    async def test_no_pool_raises(self) -> None:
        with pytest.raises((StorageConnectionError, StorageError)):
            await PostgreSQLDAO(_TestItem, None).raw_read_query(
                "SELECT 1",
            )


class TestRawWriteQuery:
    """raw_write_query() executes arbitrary DML."""

    @pytest.mark.asyncio
    async def test_returns_affected_count(self) -> None:
        c = _conn()
        c.execute.return_value = f"DELETE {_RAW_WRITE_AFFECTED}"
        affected = await _dao(_pool(c)).raw_write_query(
            "DELETE FROM test_items WHERE active = false",
        )
        assert affected == _RAW_WRITE_AFFECTED

    @pytest.mark.asyncio
    async def test_returns_zero_for_empty(self) -> None:
        c = _conn()
        c.execute.return_value = ""
        affected = await _dao(_pool(c)).raw_write_query(
            "DELETE FROM test_items WHERE 1=0",
        )
        assert affected == 0


class TestListModels:
    """list_models() queries information_schema.tables."""

    @pytest.mark.asyncio
    async def test_returns_table_names(self) -> None:
        c = _conn()
        c.fetch.return_value = [
            {"table_name": "users"},
            {"table_name": "orders"},
        ]
        assert await _dao(_pool(c)).list_models() == ["users", "orders"]


class TestGetModelInfo:
    """get_model_info() combines row count with schema."""

    @pytest.mark.asyncio
    async def test_returns_info_dict(self) -> None:
        c = _conn()
        c.fetchval.return_value = _ESTIMATED_ROWS
        c.fetch.return_value = [
            {
                "column_name": "id",
                "data_type": "text",
                "is_nullable": "NO",
                "column_default": None,
            }
        ]
        info = await _dao(_pool(c)).get_model_info("test_items")
        assert info["name"] == "test_items"
        assert info["estimated_rows"] == _ESTIMATED_ROWS
        assert "schema" in info


class TestGetModelSchema:
    """get_model_schema() queries column metadata."""

    @pytest.mark.asyncio
    async def test_returns_fields(self) -> None:
        c = _conn()
        c.fetch.return_value = [
            {
                "column_name": "id",
                "data_type": "text",
                "is_nullable": "NO",
                "column_default": None,
            },
            {
                "column_name": "name",
                "data_type": "text",
                "is_nullable": "YES",
                "column_default": None,
            },
        ]
        schema = await _dao(_pool(c)).get_model_schema("test_items")
        assert schema["table_name"] == "test_items"
        assert len(schema["fields"]) == _PAIR_COUNT
        assert schema["fields"][0]["name"] == "id"


class TestErrorPaths:
    """Verify error handling across the DAO."""

    @pytest.mark.asyncio
    async def test_hydration_error(self) -> None:
        """from_storage_dict failure wraps as QueryError."""
        c = _conn()
        bad = {"id": _TEST_UID, "uid": _TEST_UID, "count": "not-int"}
        c.fetchrow.return_value = bad
        with pytest.raises(QueryError):
            await _dao(_pool(c)).find_by_id(_TEST_UID)

    @pytest.mark.asyncio
    async def test_create_db_error(self) -> None:
        """Database exception during INSERT wraps as StorageError."""
        c = _conn()
        c.fetchval.side_effect = RuntimeError("db down")
        c.fetch.return_value = [{"column_name": "id"}]
        with pytest.raises(StorageError):
            await _dao(_pool(c)).create({"uid": _TEST_UID, "name": "x"})
