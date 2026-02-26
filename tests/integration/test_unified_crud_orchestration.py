"""Integration: UnifiedCRUD, StorageValidator, http_client retry."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from ami.core.exceptions import NotFoundError, StorageError
from ami.core.storage_types import StorageType
from ami.core.unified_crud import UnifiedCRUD
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig
from ami.storage.registry import ModelStorageUsage, StorageRegistry
from ami.storage.validator import StorageValidator
from ami.utils.http_client import DEFAULT_MAX_RETRIES, RetryConfig, request_with_retry

_IDX0 = 0
_IDX1 = 1
_LIMIT = 25
_SKIP = 5
_COUNT = 99
_YEAR = 2025
_OK = 200
_RATE = 429
_ERR = 500
_RETRIES = 3
_INIT_DLY = 0.1
_V_PORT = 8200
_ONE = 1
_TWO = 2
_GFX = StorageConfig(storage_type=StorageType.GRAPH, host="localhost", port=9080)
_REL = StorageConfig(
    storage_type=StorageType.RELATIONAL,
    host="localhost",
    port=5432,
    database="testdb",
    username="user",
    password="pass",
)


class _G(StorageModel):
    """Graph model."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="g",
        storage_configs={"dg": _GFX},
    )
    label: str = ""


class _R(StorageModel):
    """Relational model."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="r",
        storage_configs={"pg": _REL},
    )
    title: str = ""


class _M(StorageModel):
    """Multi-backend model."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="m",
        storage_configs={"a": _GFX, "b": _REL},
    )
    value: str = ""


class _E(StorageModel):
    """Empty-config model."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="e")
    data: str = ""


def _dao(
    uid: str = "uid-001",
    found: Any = None,
    many: list[Any] | None = None,
    cnt: int = _COUNT,
    ok: bool = True,
) -> AsyncMock:
    """Build mock DAO."""
    d = AsyncMock()
    d.connect = AsyncMock()
    d.disconnect = AsyncMock()
    d.create = AsyncMock(return_value=uid)
    d.find_by_id = AsyncMock(return_value=found)
    d.find = AsyncMock(return_value=many or [])
    d.update = AsyncMock()
    d.delete = AsyncMock(return_value=ok)
    d.count = AsyncMock(return_value=cnt)
    d.test_connection = AsyncMock(return_value=True)
    return d


_P = "ami.core.unified_crud.DAOFactory.create"


class TestResolveHelpers:
    """_resolve_model_class and _resolve_storage_configs."""

    def test_type(self) -> None:
        """Class returned unchanged."""
        assert UnifiedCRUD._resolve_model_class(_G) is _G

    def test_instance(self) -> None:
        """Instance returns its class."""
        assert UnifiedCRUD._resolve_model_class(_R(title="x")) is _R

    def test_bare(self) -> None:
        """Bare StorageModel rejected."""
        with pytest.raises(ValueError, match="bare StorageModel"):
            UnifiedCRUD._resolve_model_class(StorageModel())

    def test_dict_configs(self) -> None:
        """Dict metadata returned as list."""
        cfgs = UnifiedCRUD()._resolve_storage_configs(_G, _G)
        assert len(cfgs) >= _ONE
        assert isinstance(cfgs[0], StorageConfig)

    def test_multi_configs(self) -> None:
        """Two-backend model returns both."""
        assert len(UnifiedCRUD()._resolve_storage_configs(_M, _M)) == _TWO

    def test_empty_configs(self) -> None:
        """Empty metadata raises."""
        with pytest.raises(ValueError, match="No storage configs"):
            UnifiedCRUD()._resolve_storage_configs(_E, _E)


class TestGetDao:
    """_get_dao caching and dispatch."""

    async def test_cached(self) -> None:
        """Second call returns cached DAO."""
        c, d = UnifiedCRUD(), _dao()
        with patch(_P, return_value=d) as f:
            a = await c._get_dao(_G, _IDX0)
            b = await c._get_dao(_G, _IDX0)
        assert a is d
        assert b is d
        f.assert_called_once()
        d.connect.assert_awaited_once()

    async def test_diff_index(self) -> None:
        """Different index creates separate DAO."""
        c, da, db = UnifiedCRUD(), _dao(uid="a"), _dao(uid="b")
        with patch(_P, side_effect=[da, db]):
            assert await c._get_dao(_M, _IDX0) is da
            assert await c._get_dao(_M, _IDX1) is db

    async def test_eviction(self) -> None:
        """Closed loop triggers eviction."""
        c, old, new = UnifiedCRUD(), _dao(), _dao()
        lp = MagicMock()
        lp.is_closed.return_value = True
        c._dao_cache[(_G, _IDX0)] = old
        c._dao_loop_cache[(_G, _IDX0)] = lp
        with patch(_P, return_value=new):
            assert await c._get_dao(_G, _IDX0) is new
        old.disconnect.assert_awaited_once()


class TestCrudOps:
    """Create, read, update, delete, query, count."""

    async def test_create(self) -> None:
        """Create delegates, sets uid, registers."""
        c, m, d = UnifiedCRUD(), _G(label="n"), _dao(uid="uid-g1")
        with patch(_P, return_value=d):
            uid = await c.create(m, _IDX0)
        assert uid == "uid-g1"
        assert m.uid == "uid-g1"
        assert uid in c._uid_registry
        d.create.assert_awaited_once_with(m)

    async def test_read_found(self) -> None:
        """Found model returned."""
        c, exp = UnifiedCRUD(), _R(uid="uid-r1", title="ok")
        with patch(_P, return_value=_dao(found=exp)):
            assert await c.read(_R, "uid-r1", _IDX0) is exp

    async def test_read_missing(self) -> None:
        """Missing raises NotFoundError."""
        with (
            patch(_P, return_value=_dao(found=None)),
            pytest.raises(NotFoundError, match="not found"),
        ):
            await UnifiedCRUD().read(_R, "uid-x", _IDX0)

    async def test_update(self) -> None:
        """Update sets timestamp and delegates."""
        c, m, d = UnifiedCRUD(), _R(uid="uid-u1", title="v"), _dao()
        before = m.updated_at
        with patch(_P, return_value=d):
            await c.update(m, _IDX0)
        assert m.updated_at is not None
        assert m.updated_at >= (before or datetime.min.replace(tzinfo=UTC))
        d.update.assert_awaited_once()

    async def test_update_no_uid(self) -> None:
        """No uid raises."""
        with pytest.raises(ValueError, match="without UID"):
            await UnifiedCRUD().update(_R(uid=None, title="x"), _IDX0)

    async def test_delete(self) -> None:
        """Delete delegates to dao."""
        c, m, d = UnifiedCRUD(), _G(uid="uid-d1", label="b"), _dao()
        with patch(_P, return_value=d):
            assert await c.delete(m, _IDX0) is True
        d.delete.assert_awaited_once_with("uid-d1")

    async def test_delete_no_uid(self) -> None:
        """No uid raises."""
        with pytest.raises(ValueError, match="without UID"):
            await UnifiedCRUD().delete(_G(uid=None, label="x"), _IDX0)

    async def test_query(self) -> None:
        """Query passes filters."""
        c, items = UnifiedCRUD(), [_R(title="a"), _R(title="b")]
        d = _dao(many=items)
        with patch(_P, return_value=d):
            r = await c.query(_R, {"title": "a"}, limit=_LIMIT, skip=_SKIP)
        assert r is items
        d.find.assert_awaited_once_with({"title": "a"}, limit=_LIMIT, skip=_SKIP)

    async def test_count(self) -> None:
        """Count forwards to DAO."""
        c, d = UnifiedCRUD(), _dao(cnt=_COUNT)
        with patch(_P, return_value=d):
            assert await c.count(_G, {"on": True}, _IDX0) == _COUNT


class TestUidRegistry:
    """read_by_uid and delete_by_uid."""

    async def test_hit(self) -> None:
        """Registry hit reads from DAO."""
        c, exp = UnifiedCRUD(), _G(uid="uid-rh", label="hit")
        c._uid_registry["uid-rh"] = (_G, _IDX0)
        with patch(_P, return_value=_dao(found=exp)):
            assert await c.read_by_uid("uid-rh") is exp

    async def test_scan(self) -> None:
        """Cache scan finds and registers UID."""
        c, found = UnifiedCRUD(), _R(uid="uid-sf", title="s")
        c._dao_cache[(_R, _IDX0)] = _dao(found=found)
        assert await c.read_by_uid("uid-sf") is found
        assert "uid-sf" in c._uid_registry

    async def test_miss(self) -> None:
        """No match returns None."""
        assert await UnifiedCRUD().read_by_uid("uid-ghost") is None

    async def test_del_ok(self) -> None:
        """Delete removes from registry."""
        c = UnifiedCRUD()
        c._uid_registry["uid-bye"] = (_G, _IDX0)
        with patch(_P, return_value=_dao(ok=True)):
            assert await c.delete_by_uid("uid-bye") is True
        assert "uid-bye" not in c._uid_registry

    async def test_del_unknown(self) -> None:
        """Unknown UID raises."""
        with pytest.raises(NotFoundError, match="not found"):
            await UnifiedCRUD().delete_by_uid("uid-no")


class TestFieldMapping:
    """_map_to_storage and _map_from_storage."""

    def test_rel_uid_to_id(self) -> None:
        """Relational renames uid to id."""
        data = UnifiedCRUD._map_to_storage(
            _R(uid="r01", title="x", storage_configs=[_REL]),
        )
        assert data["id"] == "r01"
        assert "uid" not in data

    def test_graph_gen_uid(self) -> None:
        """Graph generates uid when None."""
        data = UnifiedCRUD._map_to_storage(
            _G(uid=None, label="a", storage_configs=[_GFX]),
        )
        assert data["uid"] is not None
        assert len(data["uid"]) > 0

    def test_dt_iso(self) -> None:
        """Datetime serialized to ISO."""
        val = UnifiedCRUD._map_to_storage(
            _G(label="t", storage_configs=[_GFX]),
        ).get("updated_at")
        if val is not None:
            assert isinstance(val, str)

    def test_from_id(self) -> None:
        """Storage id maps to uid."""
        r = UnifiedCRUD._map_from_storage(_G, {"id": "x1", "label": "z"})
        assert r.uid == "x1"
        assert r.label == "z"

    def test_from_dt(self) -> None:
        """ISO string parsed to datetime."""
        r = UnifiedCRUD._map_from_storage(
            _R,
            {"uid": "d", "title": "t", "updated_at": "2025-06-15T12:30:00+00:00"},
        )
        assert isinstance(r.updated_at, datetime)
        assert r.updated_at.year == _YEAR


class TestMissingFields:
    """_missing_required_fields per StorageType."""

    def test_rel_ok(self, make_storage_config: Any) -> None:
        """Complete relational has none missing."""
        cfg = make_storage_config(storage_type=StorageType.RELATIONAL)
        assert StorageValidator._missing_required_fields(cfg) == []

    def test_rel_no_host(self, make_storage_config: Any) -> None:
        """Missing host detected."""
        cfg = make_storage_config(storage_type=StorageType.RELATIONAL, host=None)
        assert "host" in StorageValidator._missing_required_fields(cfg)

    def test_graph(self) -> None:
        """Graph missing host detected (port auto-defaults)."""
        m = StorageValidator._missing_required_fields(
            StorageConfig(storage_type=StorageType.GRAPH, host=None),
        )
        assert "host" in m

    def test_vault(self) -> None:
        """Vault needs options.token."""
        cfg = StorageConfig(
            storage_type=StorageType.VAULT,
            host="v",
            port=_V_PORT,
            options={},
        )
        assert "options.token" in StorageValidator._missing_required_fields(cfg)

    def test_none_type(self) -> None:
        """None type flagged."""
        cfg = StorageConfig(storage_type=None, port=None)
        assert StorageValidator._missing_required_fields(cfg) == ["storage_type"]


class TestValidateAll:
    """validate_all end-to-end."""

    async def test_ok(self, make_storage_config: Any) -> None:
        """Good connection yields ok."""
        cfg = make_storage_config(name="pg")
        reg = MagicMock(spec=StorageRegistry)
        reg.list_configs.return_value = {"pg": cfg}
        reg.get_config_usage_index.return_value = {"pg": ["app.U"]}
        v = StorageValidator(registry=reg, dao_factory=MagicMock(return_value=_dao()))
        res = await v.validate_all()
        assert len(res) == _ONE
        assert res[0].status == "ok"
        assert res[0].models == ["app.U"]

    async def test_conn_fail(self, make_storage_config: Any) -> None:
        """Connect failure yields error."""
        cfg = make_storage_config(name="pg")
        d = AsyncMock()
        d.connect.side_effect = StorageError("refused")
        d.disconnect = AsyncMock()
        reg = MagicMock(spec=StorageRegistry)
        reg.list_configs.return_value = {"pg": cfg}
        reg.get_config_usage_index.return_value = {}
        res = await StorageValidator(
            registry=reg,
            dao_factory=MagicMock(return_value=d),
        ).validate_all()
        assert res[0].status == "error"
        assert "refused" in (res[0].details or "")

    async def test_missing_skip(self, make_storage_config: Any) -> None:
        """Missing fields skip DAO creation."""
        cfg = make_storage_config(storage_type=StorageType.RELATIONAL, host=None)
        factory = MagicMock()
        reg = MagicMock(spec=StorageRegistry)
        reg.list_configs.return_value = {"pg": cfg}
        reg.get_config_usage_index.return_value = {}
        res = await StorageValidator(registry=reg, dao_factory=factory).validate_all()
        assert res[0].status == "error"
        assert "host" in res[0].missing_fields
        factory.assert_not_called()


class TestValidateForModel:
    """validate_for_model resolution."""

    async def test_matched(self, make_storage_config: Any) -> None:
        """Known model triggers validation."""
        cfg = make_storage_config(name="pg")
        usage = [
            ModelStorageUsage(
                model="app.User",
                storages=[{"name": "pg", "storage_type": "postgres", "primary": True}],
            )
        ]
        reg = MagicMock(spec=StorageRegistry)
        reg.get_model_usage.return_value = usage
        reg.list_configs.return_value = {"pg": cfg}
        reg.get_config_usage_index.return_value = {"pg": ["app.User"]}
        v = StorageValidator(registry=reg, dao_factory=MagicMock(return_value=_dao()))
        res = await v.validate_for_model("User")
        assert len(res) == _ONE
        assert res[0].name == "pg"

    async def test_unmatched(self) -> None:
        """Unknown model returns empty."""
        reg = MagicMock(spec=StorageRegistry)
        reg.get_model_usage.return_value = []
        assert await StorageValidator(registry=reg).validate_for_model("X") == []


def _resp(status: int = _OK, reason: str = "OK", text: str = "") -> MagicMock:
    """Build mock HTTP response."""
    r = MagicMock()
    r.status, r.reason = status, reason
    r.text = AsyncMock(return_value=text)
    r.release = AsyncMock()
    return r


def _sess(fx: list[MagicMock | Exception]) -> MagicMock:
    """Build mock aiohttp session."""
    s = MagicMock(spec=aiohttp.ClientSession)
    s.request = AsyncMock(side_effect=fx)
    return s


_URL = "https://api.test/v1"


class TestRequestRetry:
    """RetryConfig and request_with_retry scenarios."""

    def test_config(self) -> None:
        """Defaults and custom values work."""
        assert RetryConfig().max_retries == DEFAULT_MAX_RETRIES
        cfg = RetryConfig(max_retries=_RETRIES, initial_delay=_INIT_DLY)
        assert cfg.max_retries == _RETRIES
        assert cfg.initial_delay == _INIT_DLY

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_ok(self, slp: AsyncMock) -> None:
        """200 returned with zero sleeps."""
        ok = _resp(_OK)
        s = _sess([ok])
        assert await request_with_retry(s, "GET", _URL) is ok
        assert s.request.call_count == _ONE
        slp.assert_not_awaited()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_500_then_ok(self, slp: AsyncMock) -> None:
        """500 then 200."""
        e, ok = _resp(_ERR, "ISE"), _resp(_OK)
        s = _sess([e, ok])
        assert await request_with_retry(s, "GET", _URL) is ok
        assert s.request.call_count == _TWO
        e.release.assert_awaited_once()
        slp.assert_awaited_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_429_then_ok(self, slp: AsyncMock) -> None:
        """429 then 200."""
        r, ok = _resp(_RATE, "Rate"), _resp(_OK)
        s = _sess([r, ok])
        assert await request_with_retry(s, "POST", _URL) is ok
        assert s.request.call_count == _TWO
        slp.assert_awaited_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_timeout_then_ok(self, slp: AsyncMock) -> None:
        """TimeoutError then 200."""
        ok = _resp(_OK)
        s = _sess([TimeoutError("t"), ok])
        assert await request_with_retry(s, "GET", _URL) is ok
        assert s.request.call_count == _TWO
        slp.assert_awaited_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_exhaust_500(self, slp: AsyncMock) -> None:
        """Persistent 500 raises StorageError."""
        cfg = RetryConfig(max_retries=_RETRIES)
        s = _sess([_resp(_ERR, "ISE", "f") for _ in range(_RETRIES)])
        with pytest.raises(StorageError, match="HTTP"):
            await request_with_retry(s, "GET", _URL, retry_cfg=cfg)
        assert s.request.call_count == _RETRIES

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_exhaust_timeout(self, slp: AsyncMock) -> None:
        """Persistent timeout raises StorageError."""
        cfg = RetryConfig(max_retries=_RETRIES)
        s = _sess([TimeoutError("t") for _ in range(_RETRIES)])
        with pytest.raises(StorageError, match="failed after"):
            await request_with_retry(s, "GET", _URL, retry_cfg=cfg)
        assert s.request.call_count == _RETRIES
