"""Integration tests for dgraph_graph, dgraph_traversal, dgraph_relations."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.implementations.graph.dgraph_graph import (
    _count_degrees,
    _format_degree_result,
    _validate_direction,
    _validate_identifier,
    _validate_positive_int,
    _validate_uid,
)
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig

UID_A, UID_B, UID_C = "0x1", "0x2", "0x3"
UID_D, UID_E, UID_F = "0x4", "0x5", "0x9"
DEPTH_5 = 5
IN_DEG, OUT_DEG, TOTAL_DEG = 2, 3, 5
COLL = "TestNode"
EF, EK, EL = "friends", "knows", "likes"
INTER_CT, CT_2, CT_3 = 3, 2, 3
DT = ["P"]


def _r(p: dict[str, Any]) -> MagicMock:
    m = MagicMock()
    m.json = json.dumps(p)
    return m


def _mr(u: dict[str, str] | None = None) -> MagicMock:
    m = MagicMock()
    m.uids = u or {}
    return m


class _S(StorageModel):
    name: str = "s"


def _dao() -> DgraphDAO:
    c = StorageConfig(
        storage_type=StorageType.GRAPH,
        host="localhost",
        port=9080,
        database="t",
    )
    d = DgraphDAO(model_cls=_S, config=c)
    d.collection_name = COLL
    d.client = MagicMock()
    return d


def _wq(d: DgraphDAO, r: MagicMock) -> MagicMock:
    t = MagicMock()
    t.query.return_value = r
    d.client.txn.return_value = t
    return t


def _wm(d: DgraphDAO, r: MagicMock) -> MagicMock:
    t = MagicMock()
    t.mutate.return_value = r
    d.client.txn.return_value = t
    return t


def _nd(uid: str, ex: dict[str, Any] | None = None) -> dict[str, Any]:
    n: dict[str, Any] = {"uid": uid, "dgraph.type": DT}
    if ex:
        n.update(ex)
    return n


def _ccq(nr: MagicMock, nb: dict[str, dict[str, Any]]) -> Any:
    def _q(qs: str, variables: Any = None, **kw: Any) -> MagicMock:
        if variables and "$node_uid" in variables:
            return _r(nb[variables["$node_uid"]])
        return nr

    return _q


class TestValidateUid:
    def test_hex(self) -> None:
        assert _validate_uid("0x1a2b") == "0x1a2b"

    def test_alphanum(self) -> None:
        assert _validate_uid("node-42_abc") == "node-42_abc"

    def test_empty(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_uid("")

    def test_spaces(self) -> None:
        with pytest.raises(ValueError, match="Invalid UID"):
            _validate_uid("uid with spaces")

    def test_injection(self) -> None:
        with pytest.raises(ValueError, match="Invalid UID"):
            _validate_uid("uid;DROP")


class TestValidateIdentifier:
    def test_ok(self) -> None:
        assert _validate_identifier("Person") == "Person"

    def test_dotted(self) -> None:
        assert _validate_identifier("my.type") == "my.type"

    def test_empty(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_identifier("")

    def test_spaces(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("bad name")


class TestValidatePositiveInt:
    def test_ok(self) -> None:
        assert _validate_positive_int(1) == 1

    def test_zero(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            _validate_positive_int(0)

    def test_negative(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            _validate_positive_int(-3)


class TestValidateDirection:
    def test_valid(self) -> None:
        for v in ("in", "out", "all"):
            _validate_direction(v)

    def test_invalid(self) -> None:
        with pytest.raises(ValueError, match="Invalid direction"):
            _validate_direction("sideways")


class TestCountDegrees:
    def test_out_only(self) -> None:
        nd = _nd(UID_A, {EF: [{"uid": UID_B}, {"uid": UID_C}]})
        i, o = _count_degrees(nd)
        assert i == 0
        assert o == CT_2

    def test_reverse_as_in(self) -> None:
        nd = _nd(UID_A, {f"~{EF}": [{"uid": UID_B}, {"uid": UID_C}, {"uid": UID_D}]})
        i, o = _count_degrees(nd)
        assert i == CT_3
        assert o == 0

    def test_mixed(self) -> None:
        nd = _nd(
            UID_A, {EF: [{"uid": UID_B}], f"~{EK}": [{"uid": UID_C}, {"uid": UID_D}]}
        )
        i, o = _count_degrees(nd)
        assert i == CT_2
        assert o == 1


class TestFormatDegreeResult:
    def test_in(self) -> None:
        assert _format_degree_result("in", IN_DEG, OUT_DEG) == {"in": IN_DEG}

    def test_out(self) -> None:
        assert _format_degree_result("out", IN_DEG, OUT_DEG) == {"out": OUT_DEG}

    def test_all(self) -> None:
        r = _format_degree_result("all", IN_DEG, OUT_DEG)
        assert r == {"in": IN_DEG, "out": OUT_DEG, "total": TOTAL_DEG}


class TestOneHopNeighbors:
    @pytest.mark.asyncio
    async def test_hex_uid(self) -> None:
        d = _dao()
        payload = {"path": [_nd(UID_A, {EF: [{"uid": UID_B}, {"uid": UID_C}]})]}
        _wq(d, _r(payload))
        r = await d.one_hop_neighbors(UID_A)
        assert isinstance(r, list)
        assert r[0]["uid"] == UID_A

    @pytest.mark.asyncio
    async def test_app_uid_resolves(self) -> None:
        d = _dao()
        t = MagicMock()
        t.query.side_effect = [
            _r({"find_node": [{"uid": UID_A}]}),
            _r({"path": [{"uid": UID_A}]}),
        ]
        d.client.txn.return_value = t
        r = await d.one_hop_neighbors("app-uid-123")
        assert t.query.call_count == CT_2
        assert isinstance(r, list)

    @pytest.mark.asyncio
    async def test_not_found(self) -> None:
        d = _dao()
        _wq(d, _r({"find_node": []}))
        with pytest.raises(StorageError, match="Node not found"):
            await d.one_hop_neighbors("missing")

    @pytest.mark.asyncio
    async def test_empty_uid(self) -> None:
        d = _dao()
        with pytest.raises(StorageError, match="Invalid input"):
            await d.one_hop_neighbors("")


class TestShortestPath:
    @pytest.mark.asyncio
    async def test_returns_uids(self) -> None:
        d = _dao()
        nodes = [{"uid": UID_A}, {"uid": UID_E}, {"uid": UID_F}]
        _wq(d, _r({"path_nodes": nodes}))
        assert await d.shortest_path(UID_A, UID_F) == [UID_A, UID_E, UID_F]

    @pytest.mark.asyncio
    async def test_custom_depth(self) -> None:
        d = _dao()
        t = _wq(d, _r({"path_nodes": [{"uid": UID_A}]}))
        await d.shortest_path(UID_A, UID_F, max_depth=DEPTH_5)
        kw = t.query.call_args
        vs = kw[1].get("variables", kw[0][1] if len(kw[0]) > 1 else {})
        assert vs["$depth"] == DEPTH_5

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        _wq(d, _r({"path_nodes": []}))
        assert await d.shortest_path(UID_A, UID_F) == []

    @pytest.mark.asyncio
    async def test_invalid_start(self) -> None:
        d = _dao()
        with pytest.raises(StorageError, match="Invalid input"):
            await d.shortest_path("", UID_F)

    @pytest.mark.asyncio
    async def test_zero_depth(self) -> None:
        d = _dao()
        with pytest.raises(StorageError, match="Invalid input"):
            await d.shortest_path(UID_A, UID_F, max_depth=0)


class TestConnectedComponents:
    @pytest.mark.asyncio
    async def test_two_components(self) -> None:
        d = _dao()
        nr = _r({"nodes": [{"uid": UID_A}, {"uid": UID_B}, {"uid": UID_C}]})
        nb = {
            UID_A: {"node": [{EF: [{"uid": UID_B}]}]},
            UID_B: {"node": [{EF: [{"uid": UID_A}]}]},
            UID_C: {"node": [{}]},
        }
        t = MagicMock()
        t.query.side_effect = _ccq(nr, nb)
        d.client.txn.return_value = t
        r = await d.find_connected_components(node_type="Person")
        assert len(r) == CT_2
        s = [set(c) for c in r]
        assert {UID_A, UID_B} in s
        assert {UID_C} in s

    @pytest.mark.asyncio
    async def test_single_component(self) -> None:
        d = _dao()
        nr = _r({"nodes": [{"uid": UID_A}, {"uid": UID_B}]})
        nb = {
            UID_A: {"node": [{EF: [{"uid": UID_B}]}]},
            UID_B: {"node": [{EF: [{"uid": UID_A}]}]},
        }
        t = MagicMock()
        t.query.side_effect = _ccq(nr, nb)
        d.client.txn.return_value = t
        r = await d.find_connected_components()
        assert len(r) == 1
        assert set(r[0]) == {UID_A, UID_B}

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        _wq(d, _r({"nodes": []}))
        assert await d.find_connected_components() == []


class TestGetNodeDegree:
    @pytest.mark.asyncio
    async def test_all(self) -> None:
        d = _dao()
        nd = _nd(
            UID_A, {EF: [{"uid": UID_B}], f"~{EK}": [{"uid": UID_C}, {"uid": UID_D}]}
        )
        _wq(d, _r({"node": [nd]}))
        r = await d.get_node_degree(UID_A, direction="all")
        assert r["in"] == CT_2
        assert r["out"] == 1
        assert r["total"] == CT_3

    @pytest.mark.asyncio
    async def test_in_only(self) -> None:
        d = _dao()
        nd = _nd(UID_A, {f"~{EF}": [{"uid": UID_B}]})
        _wq(d, _r({"node": [nd]}))
        assert await d.get_node_degree(UID_A, "in") == {"in": 1}

    @pytest.mark.asyncio
    async def test_out_only(self) -> None:
        d = _dao()
        nd = _nd(UID_A, {EF: [{"uid": UID_B}, {"uid": UID_C}]})
        _wq(d, _r({"node": [nd]}))
        assert await d.get_node_degree(UID_A, "out") == {"out": CT_2}

    @pytest.mark.asyncio
    async def test_missing_node(self) -> None:
        d = _dao()
        _wq(d, _r({"node": []}))
        assert await d.get_node_degree(UID_A) == {"in": 0, "out": 0, "total": 0}

    @pytest.mark.asyncio
    async def test_bad_direction(self) -> None:
        d = _dao()
        with pytest.raises(StorageError, match="Invalid input"):
            await d.get_node_degree(UID_A, direction="left")


class TestTraverse:
    @pytest.mark.asyncio
    async def test_single_edge(self) -> None:
        d = _dao()
        nd = _nd(UID_A, {EF: [{"uid": UID_B, "name": "B"}, {"uid": UID_C}]})
        _wq(d, _r({"path": [nd]}))
        r = await d.traverse(UID_A, [EF])
        assert len(r) == CT_2
        assert {n["uid"] for n in r} == {UID_B, UID_C}

    @pytest.mark.asyncio
    async def test_multi_hop(self) -> None:
        d = _dao()
        inner = {"uid": UID_B, EK: [{"uid": UID_D}]}
        _wq(d, _r({"path": [_nd(UID_A, {EF: [inner]})]}))
        r = await d.traverse(UID_A, [EF, EK])
        assert len(r) == 1
        assert r[0]["uid"] == UID_D

    @pytest.mark.asyncio
    async def test_empty_path_raises(self) -> None:
        d = _dao()
        with pytest.raises(ValueError, match="cannot be empty"):
            await d.traverse(UID_A, [])

    @pytest.mark.asyncio
    async def test_no_results(self) -> None:
        d = _dao()
        _wq(d, _r({"path": [{"uid": UID_A}]}))
        assert await d.traverse(UID_A, [EF]) == []

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        d = _dao()
        d.client = None
        with pytest.raises(StorageError, match="Not connected"):
            await d.traverse(UID_A, [EF])

    @pytest.mark.asyncio
    async def test_missing_path_key(self) -> None:
        d = _dao()
        _wq(d, _r({"data": []}))
        with pytest.raises(StorageError, match="path"):
            await d.traverse(UID_A, [EF])


class TestGetEdges:
    @pytest.mark.asyncio
    async def test_out_by_name(self) -> None:
        d = _dao()
        nd = {"uid": UID_A, EF: [{"uid": UID_B}, {"uid": UID_C}]}
        _wq(d, _r({"node": [nd]}))
        r = await d.get_edges(UID_A, edge_name=EF, direction="out")
        assert len(r) == 1
        assert r[0]["uid"] == UID_A

    @pytest.mark.asyncio
    async def test_in_edges(self) -> None:
        d = _dao()
        nd = {"uid": UID_A, f"~{EF}": [{"uid": UID_B}]}
        _wq(d, _r({"node": [nd]}))
        r = await d.get_edges(UID_A, edge_name=EF, direction="in")
        assert len(r) == 1

    @pytest.mark.asyncio
    async def test_both(self) -> None:
        d = _dao()
        nd = {"uid": UID_A, EF: [{"uid": UID_B}], f"~{EF}": [{"uid": UID_C}]}
        _wq(d, _r({"node": [nd]}))
        r = await d.get_edges(UID_A, EF, direction="both")
        assert len(r) == 1

    @pytest.mark.asyncio
    async def test_all_no_name(self) -> None:
        d = _dao()
        nd = {"uid": UID_A, EF: [{"uid": UID_B}], EL: [{"uid": UID_C}]}
        _wq(d, _r({"node": [nd]}))
        r = await d.get_edges(UID_A)
        assert len(r) == 1
        assert EF in r[0]
        assert EL in r[0]

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        d = _dao()
        d.client = None
        with pytest.raises(StorageError, match="Not connected"):
            await d.get_edges(UID_A)

    @pytest.mark.asyncio
    async def test_node_key_missing(self) -> None:
        d = _dao()
        _wq(d, _r({"other": []}))
        with pytest.raises(StorageError, match="not found"):
            await d.get_edges(UID_A)

    @pytest.mark.asyncio
    async def test_bad_json(self) -> None:
        d = _dao()
        r = MagicMock()
        r.json = "not-json{{"
        _wq(d, r)
        with pytest.raises(StorageError, match="Failed to parse"):
            await d.get_edges(UID_A, edge_name=EF)


class TestAddEdge:
    @pytest.mark.asyncio
    async def test_simple(self) -> None:
        d = _dao()
        t = _wm(d, _mr())
        await d.add_edge(UID_A, UID_B, EF)
        t.mutate.assert_called_once()
        t.commit.assert_called_once()
        t.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_with_properties(self) -> None:
        d = _dao()
        t = _wm(d, _mr())
        await d.add_edge(UID_A, UID_B, EF, properties={"w": 0.9})
        data = json.loads(t.mutate.call_args[0][0].set_json)
        assert isinstance(data, list)
        assert len(data) == INTER_CT

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        d = _dao()
        d.client = None
        with pytest.raises(StorageError, match="Not connected"):
            await d.add_edge(UID_A, UID_B, EF)

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        d = _dao()
        t = MagicMock()
        t.mutate.side_effect = RuntimeError("grpc")
        d.client.txn.return_value = t
        with pytest.raises(StorageError, match="Failed to add edge"):
            await d.add_edge(UID_A, UID_B, EF)
        t.discard.assert_called_once()


class TestRemoveEdge:
    @pytest.mark.asyncio
    async def test_simple(self) -> None:
        d = _dao()
        t = _wm(d, _mr())
        await d.remove_edge(UID_A, UID_B, EF)
        t.mutate.assert_called_once()
        data = json.loads(t.mutate.call_args[0][0].delete_json)
        assert data["uid"] == UID_A
        assert data[EF]["uid"] == UID_B
        t.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        d = _dao()
        d.client = None
        with pytest.raises(StorageError, match="Not connected"):
            await d.remove_edge(UID_A, UID_B, EF)

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        d = _dao()
        t = MagicMock()
        t.mutate.side_effect = RuntimeError("grpc")
        d.client.txn.return_value = t
        with pytest.raises(StorageError, match="Failed to remove"):
            await d.remove_edge(UID_A, UID_B, EF)
        t.discard.assert_called_once()
