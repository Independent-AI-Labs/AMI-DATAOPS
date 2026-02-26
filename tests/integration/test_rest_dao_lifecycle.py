from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import QueryError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.rest.rest_dao import (
    HTTP_CREATED,
    HTTP_NO_CONTENT,
    HTTP_NOT_FOUND,
    HTTP_OK,
    RestDAO,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_HTTP_500 = 500
_PORT_HTTP = 8080
_PORT_HTTPS = 443
_TWO = 2
_THREE = 3
_AFFECTED = 5
_RETRY = "ami.implementations.rest.rest_dao.request_with_retry"
_DISC = "ami.implementations.rest.rest_discovery.request_with_retry"


class _W(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="widgets")
    name: str = "default"


def _cfg(**kw: Any) -> StorageConfig:
    base: dict[str, Any] = {
        "storage_type": StorageType.REST,
        "host": "api.example.com",
        "port": _PORT_HTTPS,
    } | kw
    return StorageConfig(**base)


def _resp(
    status: int = HTTP_OK,
    json_data: Any = None,
    text_data: str = "",
) -> MagicMock:
    r = MagicMock(status=status)
    r.json = AsyncMock(return_value=json_data)
    r.text = AsyncMock(return_value=text_data)
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


def _dao(config: StorageConfig | None = None) -> RestDAO:
    dao = RestDAO(_W, config or _cfg())
    dao.session = MagicMock(closed=False, close=AsyncMock())
    dao._connected = True
    return dao


class TestInit:
    def test_with_config(self) -> None:
        dao = RestDAO(_W, _cfg())
        assert dao.base_url != ""
        assert dao.session is None
        assert dao._connected is False

    def test_without_config(self) -> None:
        assert RestDAO(_W, config=None).base_url == ""

    def test_collection_name(self) -> None:
        assert RestDAO(_W, _cfg()).collection_name == "widgets"

    def test_default_port(self) -> None:
        cfg = StorageConfig(storage_type=StorageType.REST, host="h")
        assert cfg.port == _PORT_HTTPS


class TestBuildBaseUrl:
    def test_connection_string(self) -> None:
        cfg = _cfg(connection_string="https://c.io/v2/")
        assert RestDAO._build_base_url(cfg) == "https://c.io/v2"

    def test_https_443(self) -> None:
        url = RestDAO._build_base_url(_cfg(host="s.io", port=_PORT_HTTPS))
        assert url.startswith("https://")

    def test_http_non_443(self) -> None:
        url = RestDAO._build_base_url(_cfg(host="l.dev", port=_PORT_HTTP))
        assert url.startswith("http://")

    def test_database(self) -> None:
        url = RestDAO._build_base_url(_cfg(database="api/v1"))
        assert url.endswith("/api/v1")

    def test_no_database(self) -> None:
        url = RestDAO._build_base_url(_cfg(host="h", port=_PORT_HTTP))
        assert url == "http://h:8080"


class TestPrepareHeaders:
    def test_defaults(self) -> None:
        h = _dao()._prepare_headers()
        assert h["Content-Type"] == "application/json"
        assert h["Accept"] == "application/json"

    def test_bearer_token(self) -> None:
        cfg = _cfg(options={"auth_token": "t"})
        h = _dao(cfg)._prepare_headers()
        assert h["Authorization"] == "Bearer t"

    def test_api_key(self) -> None:
        cfg = _cfg(options={"api_key": "k"})
        assert _dao(cfg)._prepare_headers()["X-API-Key"] == "k"

    def test_extra_headers(self) -> None:
        cfg = _cfg(options={"headers": {"X-C": "v"}})
        assert _dao(cfg)._prepare_headers()["X-C"] == "v"

    def test_no_options(self) -> None:
        h = _dao(_cfg(options=None))._prepare_headers()
        assert "Authorization" not in h


class TestMapFields:
    def test_identity(self) -> None:
        assert _dao()._map_fields({"name": "a"}) == {"name": "a"}

    def test_reverse(self) -> None:
        mapping = {"field_mapping": {"name": "display_name"}}
        r = _dao(_cfg(options=mapping))._map_fields({"display_name": "b"})
        assert r == {"name": "b"}

    def test_unmapped_passthrough(self) -> None:
        cfg = _cfg(options={"field_mapping": {"name": "title"}})
        r = _dao(cfg)._map_fields({"title": "X", "extra": 1})
        assert r["name"] == "X"
        assert r["extra"] == 1


class TestExtractData:
    def test_data_key(self) -> None:
        assert _dao()._extract_data({"data": [1]}) == [1]

    def test_results_key(self) -> None:
        assert _dao()._extract_data({"results": [2]}) == [2]

    def test_items_key(self) -> None:
        assert _dao()._extract_data({"items": [3]}) == [3]

    def test_records_key(self) -> None:
        assert _dao()._extract_data({"records": [4]}) == [4]

    def test_explicit_config_key(self) -> None:
        cfg = _cfg(options={"response_data_key": "payload"})
        d = _dao(cfg)
        assert d._extract_data({"payload": [5], "data": [6]}) == [5]

    def test_list_passthrough(self) -> None:
        assert _dao()._extract_data([7, 8]) == [7, 8]

    def test_no_envelope(self) -> None:
        assert _dao()._extract_data({"x": 9}) == {"x": 9}


class TestConnectDisconnect:
    @pytest.mark.asyncio
    async def test_roundtrip(self) -> None:
        dao = RestDAO(_W, _cfg())
        await dao.connect()
        assert dao.session is not None
        assert dao._connected is True
        await dao.disconnect()
        assert dao.session is None
        assert dao._connected is False

    @pytest.mark.asyncio
    async def test_disconnect_noop(self) -> None:
        dao = RestDAO(_W, _cfg())
        await dao.disconnect()
        assert dao._connected is False


class TestCreate:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_uid(self, m: AsyncMock) -> None:
        m.return_value = _resp(HTTP_CREATED, {"data": {"uid": "n1"}})
        assert await _dao().create({"name": "x"}) == "n1"

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_model(self, m: AsyncMock) -> None:
        m.return_value = _resp(HTTP_CREATED, {"data": {"uid": "m1"}})
        assert await _dao().create(_W(name="w")) == "m1"

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_http_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="e")
        with pytest.raises(StorageError, match="create failed"):
            await _dao().create({"name": "bad"})

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_no_id(self, m: AsyncMock) -> None:
        m.return_value = _resp(HTTP_OK, {"data": {"no_id": True}})
        with pytest.raises(QueryError, match="no ID"):
            await _dao().create({"name": "x"})

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_network_error(self, m: AsyncMock) -> None:
        m.side_effect = StorageError("connection refused")
        with pytest.raises(StorageError, match="refused"):
            await _dao().create({"name": "err"})


class TestFindById:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_found(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={"data": {"uid": "x", "name": "found"}},
        )
        r = await _dao().find_by_id("x")
        assert r is not None
        assert r.name == "found"

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_not_found(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NOT_FOUND)
        assert await _dao().find_by_id("miss") is None

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="x")
        with pytest.raises(StorageError, match="find_by_id"):
            await _dao().find_by_id("err")


class TestFind:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_list(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={
                "data": [
                    {"uid": "a", "name": "a"},
                    {"uid": "b", "name": "b"},
                ]
            }
        )
        assert len(await _dao().find({"s": "active"})) == _TWO

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_limit_skip(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={"data": [{"uid": "c", "name": "c"}]},
        )
        await _dao().find({"q": "x"}, limit=10, skip=20)
        kw = m.call_args.kwargs
        assert kw["params"]["limit"] == "10"
        assert kw["params"]["offset"] == "20"

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="e")
        with pytest.raises(StorageError, match="find failed"):
            await _dao().find({})


class TestUpdate:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_patch_default(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NO_CONTENT)
        await _dao().update("u1", {"name": "p"})
        assert m.call_args[0][1] == "PATCH"

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_put_override(self, m: AsyncMock) -> None:
        cfg = _cfg(options={"update_method": "PUT"})
        m.return_value = _resp(status=HTTP_OK)
        await _dao(cfg).update("u2", {"name": "p"})
        assert m.call_args[0][1] == "PUT"

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="f")
        with pytest.raises(StorageError, match="update failed"):
            await _dao().update("u3", {"x": 1})


class TestDelete:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_success(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NO_CONTENT)
        assert await _dao().delete("d1") is True

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_not_found(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NOT_FOUND)
        assert await _dao().delete("ghost") is False

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="e")
        with pytest.raises(StorageError, match="delete failed"):
            await _dao().delete("d-err")


class TestCount:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_endpoint(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={"data": {"count": _THREE}},
        )
        assert await _dao().count({"a": "1"}) == _THREE

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_int_response(self, m: AsyncMock) -> None:
        m.return_value = _resp(json_data=_THREE)
        assert await _dao().count({}) == _THREE

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_fallback(self, m: AsyncMock) -> None:
        m.side_effect = [
            _resp(status=HTTP_NOT_FOUND),
            _resp(
                json_data={
                    "data": [
                        {"uid": "a", "name": "a"},
                        {"uid": "b", "name": "b"},
                    ]
                }
            ),
        ]
        assert await _dao().count({}) == _TWO


class TestExists:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_head_ok(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_OK)
        assert await _dao().exists("e1") is True

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_head_miss(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NOT_FOUND)
        assert await _dao().exists("e-g") is False

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_fallback(self, m: AsyncMock) -> None:
        m.side_effect = [
            StorageError("HEAD fail"),
            _resp(
                json_data={
                    "data": {"uid": "e2", "name": "fb"},
                }
            ),
        ]
        assert await _dao().exists("e2") is True


class TestBulkOps:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_bulk_create(self, m: AsyncMock) -> None:
        m.side_effect = [
            _resp(HTTP_CREATED, {"data": {"uid": "b1"}}),
            _resp(HTTP_CREATED, {"data": {"uid": "b2"}}),
        ]
        r = await _dao().bulk_create([{"n": "x"}, {"n": "y"}])
        assert r == ["b1", "b2"]

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_bulk_update(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NO_CONTENT)
        await _dao().bulk_update(
            [
                {"uid": "u1", "name": "a"},
                {"id": "u2", "name": "b"},
            ]
        )
        assert m.call_count == _TWO

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_bulk_update_skip_no_id(
        self,
        m: AsyncMock,
    ) -> None:
        m.return_value = _resp(status=HTTP_NO_CONTENT)
        await _dao().bulk_update([{"name": "orphan"}])
        m.assert_not_called()

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_bulk_delete(self, m: AsyncMock) -> None:
        m.side_effect = [
            _resp(status=HTTP_NO_CONTENT),
            _resp(status=HTTP_NOT_FOUND),
            _resp(status=HTTP_NO_CONTENT),
        ]
        assert await _dao().bulk_delete(["a", "b", "c"]) == _TWO


class TestRawQueries:
    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_read_list(self, m: AsyncMock) -> None:
        m.return_value = _resp(json_data={"data": [{"id": 1}]})
        assert await _dao().raw_read_query("ep") == [{"id": 1}]

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_read_wraps_dict(self, m: AsyncMock) -> None:
        m.return_value = _resp(json_data={"data": {"id": 1}})
        assert await _dao().raw_read_query("s") == [{"id": 1}]

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_read_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="e")
        with pytest.raises(StorageError, match="raw_read_query"):
            await _dao().raw_read_query("fail")

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_write_affected(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={"data": {"affected": _AFFECTED}},
        )
        r = await _dao().raw_write_query("batch/op", {"x": 1})
        assert r == _AFFECTED

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_write_no_content(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_NO_CONTENT)
        assert await _dao().raw_write_query("fire") == 1

    @pytest.mark.asyncio
    @patch(_RETRY, new_callable=AsyncMock)
    async def test_write_error(self, m: AsyncMock) -> None:
        m.return_value = _resp(_HTTP_500, text_data="e")
        with pytest.raises(StorageError, match="raw_write_query"):
            await _dao().raw_write_query("fail")


class TestDiscovery:
    @pytest.mark.asyncio
    @patch(_DISC, new_callable=AsyncMock)
    async def test_list_models(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={"data": [{"name": "users"}]},
        )
        assert "users" in await _dao().list_models()

    @pytest.mark.asyncio
    @patch(_DISC, new_callable=AsyncMock)
    async def test_model_info(self, m: AsyncMock) -> None:
        m.return_value = _resp(
            json_data={"data": {"name": "w", "count": _THREE}},
        )
        info = await _dao().get_model_info("widgets")
        assert info["name"] == "w"

    @pytest.mark.asyncio
    @patch(_DISC, new_callable=AsyncMock)
    async def test_connection_ok(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_OK)
        assert await _dao().test_connection() is True

    @pytest.mark.asyncio
    @patch(_DISC, new_callable=AsyncMock)
    async def test_connection_fail(self, m: AsyncMock) -> None:
        m.side_effect = StorageError("unreachable")
        assert await _dao().test_connection() is False
