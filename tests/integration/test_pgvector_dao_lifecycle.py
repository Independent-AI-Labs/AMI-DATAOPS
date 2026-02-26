"""Integration tests for PgVectorDAO lifecycle."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import QueryError, StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_E = "ami.implementations.vec.pgvector_dao.get_embedding_service"
_P = "ami.implementations.vec.pgvector_dao.asyncpg.create_pool"
DIM = 384
V = 0.1
PORT = 5432
LIM5 = 5
LIM10 = 10
SKIP2 = 2
BULK3 = 3
DEL2 = 2
RAW7 = 7
CDIST = 0.15
CSCORE = 0.85
CNT42 = 42
NROWS = 2
V2 = 0.2
V3 = 0.3


class _M(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="vec_items")
    title: str = "default"
    content: str | None = None


def _svc() -> MagicMock:
    s = MagicMock()
    s.embedding_dim = DIM
    s.generate_embedding = AsyncMock(return_value=[V] * DIM)
    return s


def _cfg(**kw: Any) -> StorageConfig:
    d: dict[str, Any] = {
        "storage_type": StorageType.VECTOR,
        "host": "localhost",
        "port": PORT,
        "database": "vectordb",
        "username": "user",
        "password": "pass",
    }
    d.update(kw)
    return StorageConfig(**d)


def _dao(pool: bool = True) -> PgVectorDAO:
    with patch(_E, return_value=_svc()):
        dao = PgVectorDAO(model_cls=_M, config=_cfg())
    if pool:
        _mkpool(dao)
    return dao


def _mkpool(dao: PgVectorDAO) -> None:
    p, cn, ctx = MagicMock(), AsyncMock(), MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=cn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    p.acquire = MagicMock(return_value=ctx)
    p.close = AsyncMock()
    dao.pool = p


def _cn(dao: PgVectorDAO) -> AsyncMock:
    return dao.pool.acquire.return_value.__aenter__.return_value


def _nopool() -> PgVectorDAO:
    dao = _dao(pool=False)
    dao.connect = AsyncMock(side_effect=StorageConnectionError("no"))
    return dao


class TestInitConfig:
    def test_init_fields(self) -> None:
        dao = _dao()
        assert dao.model_cls is _M
        assert dao.config is not None
        assert dao.config.storage_type == StorageType.VECTOR
        assert dao.collection_name == "vec_items"
        assert dao.embedding_dim == DIM

    def test_pool_none_before_connect(self) -> None:
        assert _dao(pool=False).pool is None

    def test_build_dsn(self) -> None:
        dsn = _dao()._build_dsn()
        assert dsn == f"postgresql://user:pass@localhost:{PORT}/vectordb"

    def test_build_dsn_conn_string(self) -> None:
        with patch(_E, return_value=_svc()):
            dao = PgVectorDAO(
                model_cls=_M,
                config=_cfg(connection_string="postgresql://c@h/d"),
            )
        assert dao._build_dsn() == "postgresql://c@h/d"

    def test_build_dsn_no_config(self) -> None:
        with patch(_E, return_value=_svc()):
            dao = PgVectorDAO(model_cls=_M, config=None)
        with pytest.raises(StorageError, match="No storage config"):
            dao._build_dsn()

    def test_safe_dsn_redacts(self) -> None:
        safe = _dao()._safe_dsn()
        assert "***" in safe
        assert "pass" not in safe

    def test_safe_dsn_no_config(self) -> None:
        with patch(_E, return_value=_svc()):
            dao = PgVectorDAO(model_cls=_M, config=None)
        assert dao._safe_dsn() == "<no config>"


class TestConnection:
    @pytest.mark.asyncio
    async def test_connect_creates_pool(self) -> None:
        dao = _dao(pool=False)
        mp = MagicMock()
        with patch(_P, new_callable=AsyncMock, return_value=mp):
            await dao.connect()
        assert dao.pool is mp

    @pytest.mark.asyncio
    async def test_connect_skips_existing(self) -> None:
        dao = _dao()
        with patch(_P, new_callable=AsyncMock) as cr:
            await dao.connect()
            cr.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connect_failure(self) -> None:
        dao = _dao(pool=False)
        se = OSError("refused")
        with (
            patch(_P, new_callable=AsyncMock, side_effect=se),
            pytest.raises(StorageConnectionError),
        ):
            await dao.connect()

    @pytest.mark.asyncio
    async def test_disconnect(self) -> None:
        dao = _dao()
        dao._ensured_tables.add("vec_items")
        await dao.disconnect()
        assert dao.pool is None
        assert len(dao._ensured_tables) == 0
        dao2 = _dao(pool=False)
        await dao2.disconnect()
        assert dao2.pool is None

    @pytest.mark.asyncio
    async def test_test_connection_true(self) -> None:
        dao = _dao()
        _cn(dao).fetchval = AsyncMock(return_value=1)
        assert await dao.test_connection() is True

    @pytest.mark.asyncio
    async def test_test_connection_false(self) -> None:
        dao = _dao()
        _cn(dao).fetchval = AsyncMock(side_effect=OSError("x"))
        assert await dao.test_connection() is False

    @pytest.mark.asyncio
    async def test_ensure_pool_auto_connects(self) -> None:
        dao = _dao(pool=False)
        mp = MagicMock()
        mc = AsyncMock()
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=mc)
        ctx.__aexit__ = AsyncMock(return_value=False)
        mp.acquire = MagicMock(return_value=ctx)
        mc.fetchval = AsyncMock(return_value=1)
        with patch(_P, new_callable=AsyncMock, return_value=mp):
            assert await dao.test_connection() is True
        assert dao.pool is mp


class TestCrud:
    @pytest.mark.asyncio
    async def test_create_uid(self) -> None:
        dao = _dao()
        dao._generate_embedding_for_record = AsyncMock(return_value=None)
        _cn(dao).execute = AsyncMock()
        assert await dao.create({"uid": "abc", "title": "t"}) == "abc"

    @pytest.mark.asyncio
    async def test_create_generates_uid(self) -> None:
        dao = _dao()
        dao._generate_embedding_for_record = AsyncMock(return_value=None)
        _cn(dao).execute = AsyncMock()
        assert len(await dao.create({"title": "no-uid"})) > 0

    @pytest.mark.asyncio
    async def test_find_by_id_found(self) -> None:
        dao = _dao()
        _cn(dao).fetchrow = AsyncMock(return_value={"uid": "r1", "title": "f"})
        dao.model_cls.from_storage_dict = AsyncMock(return_value=_M(uid="r1"))
        r = await dao.find_by_id("r1")
        assert r is not None
        assert r.uid == "r1"

    @pytest.mark.asyncio
    async def test_find_by_id_none(self) -> None:
        dao = _dao()
        _cn(dao).fetchrow = AsyncMock(return_value=None)
        assert await dao.find_by_id("x") is None

    @pytest.mark.asyncio
    async def test_find_paginated(self) -> None:
        dao = _dao()
        _cn(dao).fetch = AsyncMock(return_value=[{"uid": "a"}, {"uid": "b"}])
        dao.model_cls.from_storage_dict = AsyncMock(
            side_effect=[_M(uid="a"), _M(uid="b")],
        )
        r = await dao.find({"title": "x"}, limit=LIM10, skip=SKIP2)
        assert len(r) == NROWS

    @pytest.mark.asyncio
    async def test_update(self) -> None:
        dao = _dao()
        dao._generate_embedding_for_record = AsyncMock(return_value=None)
        _cn(dao).execute = AsyncMock()
        await dao.update("u1", {"title": "new"})
        _cn(dao).execute.assert_awaited()

    @pytest.mark.asyncio
    async def test_update_empty_noop(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock()
        await dao.update("u1", {})
        _cn(dao).execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete_true(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock(return_value="DELETE 1")
        assert await dao.delete("d1") is True

    @pytest.mark.asyncio
    async def test_delete_false(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock(return_value="DELETE 0")
        assert await dao.delete("nope") is False

    @pytest.mark.asyncio
    async def test_count(self) -> None:
        dao = _dao()
        _cn(dao).fetchval = AsyncMock(return_value=CNT42)
        assert await dao.count({"title": "x"}) == CNT42

    @pytest.mark.asyncio
    async def test_count_zero(self) -> None:
        dao = _dao()
        _cn(dao).fetchval = AsyncMock(return_value=None)
        assert await dao.count({}) == 0

    @pytest.mark.asyncio
    async def test_exists_true(self) -> None:
        dao = _dao()
        _cn(dao).fetchval = AsyncMock(return_value=1)
        assert await dao.exists("e1") is True

    @pytest.mark.asyncio
    async def test_exists_false(self) -> None:
        dao = _dao()
        _cn(dao).fetchval = AsyncMock(return_value=None)
        assert await dao.exists("e2") is False


class TestBulk:
    @pytest.mark.asyncio
    async def test_bulk_create_empty(self) -> None:
        assert await _dao().bulk_create([]) == []

    @pytest.mark.asyncio
    async def test_bulk_create(self) -> None:
        dao = _dao()
        dao._generate_embedding_for_record = AsyncMock(return_value=None)
        _cn(dao).execute = AsyncMock()
        items = [{"title": f"i{i}"} for i in range(BULK3)]
        assert len(await dao.bulk_create(items)) == BULK3

    @pytest.mark.asyncio
    async def test_bulk_delete_empty(self) -> None:
        assert await _dao().bulk_delete([]) == 0

    @pytest.mark.asyncio
    async def test_bulk_delete(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock(return_value=f"DELETE {DEL2}")
        assert await dao.bulk_delete(["a", "b"]) == DEL2


class TestVector:
    @pytest.mark.asyncio
    async def test_similarity_search(self) -> None:
        dao = _dao()
        dao._get_query_embedding = AsyncMock(return_value=[V] * DIM)
        row = {"uid": "v1", "title": "hit", "distance": CDIST, "embedding": None}
        _cn(dao).fetch = AsyncMock(return_value=[row])
        r = await dao.similarity_search("q", limit=LIM5)
        assert len(r) == 1
        assert r[0]["score"] == pytest.approx(CSCORE)

    @pytest.mark.asyncio
    async def test_search_by_vector(self) -> None:
        dao = _dao()
        row = {"uid": "v2", "distance": CDIST, "embedding": None}
        _cn(dao).fetch = AsyncMock(return_value=[row])
        r = await dao.similarity_search_by_vector(
            [V] * DIM,
            limit=LIM5,
            metric="cosine",
        )
        assert len(r) == 1
        assert r[0]["score"] == pytest.approx(CSCORE)

    @pytest.mark.asyncio
    async def test_search_with_filters(self) -> None:
        dao = _dao()
        _cn(dao).fetch = AsyncMock(return_value=[])
        r = await dao.similarity_search_by_vector([V], filters={"title": "x"})
        assert r == []

    @pytest.mark.asyncio
    async def test_fetch_embedding(self) -> None:
        dao = _dao()
        expected = [V, V2, V3]
        _cn(dao).fetchrow = AsyncMock(return_value={"embedding": expected})
        assert await dao.fetch_embedding("i1") == expected

    @pytest.mark.asyncio
    async def test_fetch_embedding_none_and_null(self) -> None:
        d1 = _dao()
        _cn(d1).fetchrow = AsyncMock(return_value=None)
        assert await d1.fetch_embedding("x") is None
        d2 = _dao()
        _cn(d2).fetchrow = AsyncMock(return_value={"embedding": None})
        assert await d2.fetch_embedding("n") is None


class TestEmbedding:
    def test_extract_default_fields(self) -> None:
        t = _dao()._extract_text_for_embedding(
            {"title": "Hello", "content": "World", "other": 42},
        )
        assert "Hello" in t
        assert "World" in t

    def test_extract_non_string_and_empty(self) -> None:
        assert _dao()._extract_text_for_embedding({"title": 123}) == ""
        assert _dao()._extract_text_for_embedding({}) == ""

    @pytest.mark.asyncio
    async def test_generate_success(self) -> None:
        r = await _dao()._generate_embedding_for_record({"title": "doc"})
        assert r is not None
        assert len(r) == DIM

    @pytest.mark.asyncio
    async def test_generate_no_text(self) -> None:
        r = await _dao()._generate_embedding_for_record({"count": 99})
        assert r is None

    @pytest.mark.asyncio
    async def test_generate_whitespace(self) -> None:
        r = await _dao()._generate_embedding_for_record({"title": "   "})
        assert r is None

    @pytest.mark.asyncio
    async def test_generate_error(self) -> None:
        dao = _dao()
        dao._embedding_service.generate_embedding = AsyncMock(
            side_effect=RuntimeError("down"),
        )
        with pytest.raises(QueryError, match="Embedding generation"):
            await dao._generate_embedding_for_record({"title": "fail"})


class TestIndexes:
    @pytest.mark.asyncio
    async def test_creates_hnsw(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock()
        meta = MagicMock()
        meta.indexes = [
            {"columns": ["title"], "unique": False, "type": "btree"},
        ]
        dao.model_cls.get_metadata = MagicMock(return_value=meta)
        await dao.create_indexes()
        sqls = [c[0][0] for c in _cn(dao).execute.call_args_list]
        assert any("hnsw" in s.lower() for s in sqls)

    @pytest.mark.asyncio
    async def test_no_pool(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().create_indexes()


class TestRaw:
    @pytest.mark.asyncio
    async def test_read_params(self) -> None:
        dao = _dao()
        _cn(dao).fetch = AsyncMock(return_value=[{"uid": "r1"}])
        rows = await dao.raw_read_query("SELECT 1", {"uid": "r1"})
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_read_no_params(self) -> None:
        dao = _dao()
        _cn(dao).fetch = AsyncMock(return_value=[])
        assert await dao.raw_read_query("SELECT 1") == []

    @pytest.mark.asyncio
    async def test_write_params(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock(return_value=f"UPDATE {RAW7}")
        assert await dao.raw_write_query("U", {"n": "x"}) == RAW7

    @pytest.mark.asyncio
    async def test_write_none(self) -> None:
        dao = _dao()
        _cn(dao).execute = AsyncMock(return_value=None)
        assert await dao.raw_write_query("D") == 0


class TestSchema:
    @pytest.mark.asyncio
    async def test_list_models(self) -> None:
        dao = _dao()
        rows = [{"table_name": "vec_items"}, {"table_name": "other"}]
        _cn(dao).fetch = AsyncMock(return_value=rows)
        tables = await dao.list_models()
        assert "vec_items" in tables
        assert "other" in tables

    @pytest.mark.asyncio
    async def test_model_info_found(self) -> None:
        dao = _dao()
        row = {"table_name": "vec_items", "table_type": "BASE TABLE"}
        _cn(dao).fetchrow = AsyncMock(return_value=row)
        info = await dao.get_model_info("vec_items")
        assert info["name"] == "vec_items"
        assert info["type"] == "BASE TABLE"

    @pytest.mark.asyncio
    async def test_model_info_missing(self) -> None:
        dao = _dao()
        _cn(dao).fetchrow = AsyncMock(return_value=None)
        assert "error" in await dao.get_model_info("nope")

    @pytest.mark.asyncio
    async def test_model_info_invalid(self) -> None:
        dao = _dao()
        r = await dao.get_model_info("drop;--")
        assert r == {"error": "Invalid table name"}


class TestNoPool:
    @pytest.mark.asyncio
    async def test_create(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().create({"title": "fail"})

    @pytest.mark.asyncio
    async def test_find_by_id(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().find_by_id("x")

    @pytest.mark.asyncio
    async def test_delete(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().delete("x")

    @pytest.mark.asyncio
    async def test_sim_search(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().similarity_search("q")

    @pytest.mark.asyncio
    async def test_fetch_emb(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().fetch_embedding("x")

    @pytest.mark.asyncio
    async def test_raw_queries(self) -> None:
        with pytest.raises(StorageConnectionError):
            await _nopool().raw_read_query("SELECT 1")
        with pytest.raises(StorageConnectionError):
            await _nopool().raw_write_query("U")
