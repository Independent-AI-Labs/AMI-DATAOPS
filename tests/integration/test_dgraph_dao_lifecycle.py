"""Integration tests for the DgraphDAO lifecycle through sub-modules."""

from __future__ import annotations

import json
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

HOST = "localhost"
PORT = 9080
UID_1 = "0x1"
UID_2 = "0x2"
UID_3 = "0x3"
COLL = "test_nodes"
COUNT_VAL = 7
LIMIT = 10
SKIP = 2
BULK_N = 3
FIND_COUNT = 2
FAIL_INDEX = 2
PARTIAL_OK = 2
_B = "ami.implementations.graph"


class _N(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path=COLL)
    name: str = "untitled"


def _cfg() -> StorageConfig:
    return StorageConfig(
        storage_type=StorageType.GRAPH,
        host=HOST,
        port=PORT,
    )


def _txn(
    *,
    qj: str | None = None,
    uids: dict[str, str] | None = None,
) -> MagicMock:
    t = MagicMock()
    if qj is not None:
        r = MagicMock()
        r.json = qj
        t.query.return_value = r
    if uids is not None:
        mr = MagicMock()
        mr.uids = uids
        t.mutate.return_value = mr
    return t


def _ready() -> tuple[DgraphDAO, MagicMock]:
    dao = DgraphDAO(_N, _cfg())
    c = MagicMock()
    dao.client = c
    dao._grpc_client_conn = MagicMock()
    return dao, c


def _nd(uid: str, name: str, app: str) -> dict[str, Any]:
    return {
        "uid": uid,
        f"{COLL}.name": name,
        f"{COLL}.app_uid": app,
        "dgraph.type": [COLL],
    }


class TestInit:
    def test_defaults(self) -> None:
        c = _cfg()
        dao = DgraphDAO(_N, c)
        assert dao.collection_name == COLL
        assert dao.client is None
        assert dao._grpc_client_conn is None
        assert dao.config is c


class TestConnect:
    @pytest.mark.asyncio
    async def test_creates_stub_and_client(self) -> None:
        dao = DgraphDAO(_N, _cfg())
        stub, cli = MagicMock(), MagicMock()
        tgt = f"{_B}.dgraph_dao.pydgraph"
        with (
            patch(f"{tgt}.DgraphClientStub", return_value=stub) as sc,
            patch(f"{tgt}.DgraphClient", return_value=cli) as cc,
            patch(f"{_B}.dgraph_dao.ensure_schema"),
        ):
            await dao.connect()
        sc.assert_called_once_with(f"{HOST}:{PORT}")
        cc.assert_called_once_with(stub)
        assert dao.client is cli
        assert dao._grpc_client_conn is stub

    @pytest.mark.asyncio
    async def test_no_host_raises(self) -> None:
        c = StorageConfig(
            storage_type=StorageType.GRAPH,
            host=None,
            port=PORT,
        )
        with pytest.raises(StorageError, match="host"):
            await DgraphDAO(_N, c).connect()

    @pytest.mark.asyncio
    async def test_no_port_raises(self) -> None:
        c = _cfg()
        c.port = None
        with pytest.raises(StorageError, match="port"):
            await DgraphDAO(_N, c).connect()


class TestDisconnect:
    @pytest.mark.asyncio
    async def test_closes_and_clears(self) -> None:
        dao, _ = _ready()
        conn = dao._grpc_client_conn
        await dao.disconnect()
        assert conn is not None
        conn.close.assert_called_once()
        assert dao.client is None

    @pytest.mark.asyncio
    async def test_noop_no_conn(self) -> None:
        dao = DgraphDAO(_N, _cfg())
        dao._grpc_client_conn = None
        await dao.disconnect()


class TestTestConnection:
    @pytest.mark.asyncio
    async def test_success(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj='{"schema": []}')
        assert await dao.test_connection() is True

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Client not init"):
            await DgraphDAO(_N, _cfg()).test_connection()

    @pytest.mark.asyncio
    async def test_query_failure(self) -> None:
        dao, c = _ready()
        t = MagicMock()
        t.query.side_effect = RuntimeError("down")
        c.txn.return_value = t
        with pytest.raises(StorageError, match="Health check"):
            await dao.test_connection()


class TestCreate:
    @pytest.mark.asyncio
    async def test_returns_app_uid(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(uids={"blank-0": UID_1})
        inst = _N(name="alpha")
        assert await dao.create(inst) == inst.uid

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).create(_N())

    @pytest.mark.asyncio
    async def test_mutation_failure(self) -> None:
        dao, c = _ready()
        t = MagicMock()
        t.mutate.side_effect = RuntimeError("boom")
        c.txn.return_value = t
        with pytest.raises(StorageError, match="Failed to create"):
            await dao.create(_N(name="bad"))
        t.discard.assert_called_once()


class TestFindById:
    @pytest.mark.asyncio
    async def test_dgraph_uid(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(
            qj=json.dumps({"node": [_nd(UID_1, "f", "a1")]}),
        )
        r = await dao.find_by_id(UID_1)
        assert r is not None
        assert r.name == "f"

    @pytest.mark.asyncio
    async def test_app_uid(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(
            qj=json.dumps({"node": [_nd(UID_1, "af", "mid")]}),
        )
        r = await dao.find_by_id("mid")
        assert r is not None
        assert r.name == "af"

    @pytest.mark.asyncio
    async def test_not_found(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"node": []}))
        assert await dao.find_by_id(UID_1) is None

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).find_by_id(UID_1)


class TestFind:
    @pytest.mark.asyncio
    async def test_returns_list(self) -> None:
        dao, c = _ready()
        k = f"{COLL}_results"
        c.txn.return_value = _txn(
            qj=json.dumps({k: [_nd(UID_1, "a", "u"), _nd(UID_2, "b", "v")]}),
        )
        r = await dao.find({"name": "a"}, limit=LIMIT, skip=SKIP)
        assert len(r) == FIND_COUNT
        assert r[0].name == "a"

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(
            qj=json.dumps({f"{COLL}_results": []}),
        )
        assert await dao.find({"name": "x"}) == []


class TestFindOne:
    @pytest.mark.asyncio
    async def test_returns_model(self) -> None:
        dao, c = _ready()
        k = f"{COLL}_results"
        c.txn.return_value = _txn(qj=json.dumps({k: [_nd(UID_1, "s", "u")]}))
        r = await dao.find_one({"name": "s"})
        assert r is not None
        assert r.name == "s"

    @pytest.mark.asyncio
    async def test_returns_none(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({f"{COLL}_results": []}))
        assert await dao.find_one({"name": "m"}) is None


class TestCount:
    @pytest.mark.asyncio
    async def test_returns_int(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"count": [{"total": COUNT_VAL}]}))
        assert await dao.count({"name": "x"}) == COUNT_VAL

    @pytest.mark.asyncio
    async def test_zero(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"count": [{"total": 0}]}))
        assert await dao.count({}) == 0


class TestExists:
    @pytest.mark.asyncio
    async def test_true_dgraph_uid(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"node": [_nd(UID_1, "h", "uh")]}))
        assert await dao.exists(UID_1) is True

    @pytest.mark.asyncio
    async def test_false_dgraph_uid(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"node": []}))
        assert await dao.exists(UID_1) is False

    @pytest.mark.asyncio
    async def test_app_uid(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"node": [{"uid": UID_1}]}))
        assert await dao.exists("my-app-uid") is True

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).exists(UID_1)


class TestUpdate:
    @pytest.mark.asyncio
    async def test_dgraph_uid(self) -> None:
        dao, c = _ready()
        t = MagicMock()
        t.mutate.return_value = MagicMock()
        t.commit.return_value = None
        c.txn.return_value = t
        await dao.update(UID_1, {"name": "up"})
        assert t.mutate.call_count >= 1
        t.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).update(UID_1, {"name": "n"})


class TestDelete:
    @pytest.mark.asyncio
    async def test_dgraph_uid(self) -> None:
        dao, c = _ready()
        t = MagicMock()
        t.mutate.return_value = MagicMock()
        t.commit.return_value = None
        c.txn.return_value = t
        assert await dao.delete(UID_1) is True

    @pytest.mark.asyncio
    async def test_app_uid_not_found(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=json.dumps({"node": []}))
        assert await dao.delete("no-such") is False

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).delete(UID_1)


class TestBulkCreate:
    @pytest.mark.asyncio
    async def test_returns_app_uids(self) -> None:
        dao, c = _ready()
        uids = {f"blank-{i}": f"0x{i + 10}" for i in range(BULK_N)}
        c.txn.return_value = _txn(uids=uids)
        items = [_N(name=f"n{i}") for i in range(BULK_N)]
        expected = [it.uid for it in items]
        assert await dao.bulk_create(items) == expected

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).bulk_create([_N()])


class TestBulkDelete:
    @pytest.mark.asyncio
    async def test_returns_count(self) -> None:
        dao, c = _ready()
        t = MagicMock()
        t.mutate.return_value = MagicMock()
        t.commit.return_value = None
        c.txn.return_value = t
        assert await dao.bulk_delete([UID_1, UID_2, UID_3]) == BULK_N

    @pytest.mark.asyncio
    async def test_partial_failure(self) -> None:
        dao, c = _ready()
        n = 0

        def _factory(**_kw: Any) -> MagicMock:
            nonlocal n
            n += 1
            t = MagicMock()
            if n == FAIL_INDEX:
                t.mutate.side_effect = RuntimeError("fail")
            else:
                t.mutate.return_value = MagicMock()
                t.commit.return_value = None
            return t

        c.txn.side_effect = _factory
        assert await dao.bulk_delete([UID_1, UID_2, UID_3]) == PARTIAL_OK


class TestCreateIndexes:
    @pytest.mark.asyncio
    async def test_calls_ensure_schema(self) -> None:
        dao, c = _ready()
        c.alter.return_value = None
        with patch(f"{_B}.dgraph_create.ensure_schema") as m:
            await dao.create_indexes()
        m.assert_called_once()


class TestRawReadQuery:
    @pytest.mark.asyncio
    async def test_returns_list(self) -> None:
        dao, c = _ready()
        pay = {"data": [{"uid": UID_1}]}
        c.txn.return_value = _txn(qj=json.dumps(pay))
        r = await dao.raw_read_query("{schema {}}")
        assert isinstance(r, list)
        assert r[0] == pay

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).raw_read_query("{q{}}")


class TestRawWriteQuery:
    @pytest.mark.asyncio
    async def test_returns_count(self) -> None:
        dao, c = _ready()
        t = MagicMock()
        mr = MagicMock()
        mr.uids = {"blank-0": UID_1}
        t.mutate.return_value = mr
        t.commit.return_value = None
        c.txn.return_value = t
        assert await dao.raw_write_query('<0x1> <n> "v" .') == 1

    @pytest.mark.asyncio
    async def test_params_raises(self) -> None:
        dao, _ = _ready()
        with pytest.raises(NotImplementedError):
            await dao.raw_write_query("nq", {"k": "v"})

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).raw_write_query("nq")


_GRP_J = json.dumps({"types": [{"@groupby": [{"dgraph.type": COLL}]}]})
_INFO_J = json.dumps({"type_info": [{"count(uid)": COUNT_VAL}]})
_PREDS = [
    {"predicate": f"{COLL}.name", "type": "string", "index": True},
    {"predicate": f"{COLL}.desc", "type": "string"},
    {"predicate": "other.f", "type": "int"},
]
_SCH_J = json.dumps({"schema": _PREDS})
_NO_IDX_J = json.dumps({"schema": [_PREDS[1]]})


class TestSchemaIntrospection:
    @pytest.mark.asyncio
    async def test_list_databases(self) -> None:
        dao, _ = _ready()
        assert await dao.list_databases() == ["default"]

    @pytest.mark.asyncio
    async def test_list_schemas(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_GRP_J)
        assert COLL in await dao.list_schemas()

    @pytest.mark.asyncio
    async def test_list_schemas_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await DgraphDAO(_N, _cfg()).list_schemas()

    @pytest.mark.asyncio
    async def test_list_models(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_GRP_J)
        assert COLL in await dao.list_models()

    @pytest.mark.asyncio
    async def test_get_model_info(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_INFO_J)
        info = await dao.get_model_info(COLL)
        assert info["type"] == COLL
        assert info["count"] == COUNT_VAL

    @pytest.mark.asyncio
    async def test_get_model_schema_filtered(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_SCH_J)
        s = await dao.get_model_schema(COLL)
        assert f"{COLL}.name" in s
        assert "other.f" not in s

    @pytest.mark.asyncio
    async def test_get_model_fields(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_SCH_J)
        fs = await dao.get_model_fields(COLL)
        assert len(fs) >= 1
        assert any(f["name"] == "name" for f in fs)

    @pytest.mark.asyncio
    async def test_get_model_indexes(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_SCH_J)
        ix = await dao.get_model_indexes(COLL)
        assert len(ix) == 1
        assert ix[0]["field"] == "name"

    @pytest.mark.asyncio
    async def test_get_model_indexes_empty(self) -> None:
        dao, c = _ready()
        c.txn.return_value = _txn(qj=_NO_IDX_J)
        assert await dao.get_model_indexes(COLL) == []
