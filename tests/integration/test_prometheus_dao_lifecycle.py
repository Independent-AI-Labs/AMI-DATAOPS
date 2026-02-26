"""Integration tests for the Prometheus DAO lifecycle.
Covers PrometheusDAO + connection, read, write, metadata, and models
sub-modules.  Mocks ``aiohttp.ClientSession`` at the I/O edge.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import Field

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.timeseries.prometheus_connection import (
    build_base_url,
)
from ami.implementations.timeseries.prometheus_dao import PrometheusDAO
from ami.implementations.timeseries.prometheus_models import (
    PrometheusMetric,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

PROM_HOST = "localhost"
PROM_PORT = 9090
PROM_BASE_URL = f"http://{PROM_HOST}:{PROM_PORT}"
HTTP_OK = 200
HTTP_NO_CONTENT = 204
HTTP_SERVER_ERROR = 500
METRIC_VALUE = 1.5
METRIC_TS_EPOCH = 1_700_000_000.0
BULK_COUNT = 3
RAW_LINE_COUNT = 2
RAW_QUERY_VALUE = 99.0
CUSTOM_PORT = 8428
CUSTOM_TIMEOUT = 60
_CONN_PATCH = (
    "ami.implementations.timeseries.prometheus_connection.aiohttp.ClientSession"
)
_DAO_SESSION_PATCH = "ami.implementations.timeseries.prometheus_dao.create_session"


# -- Helpers ----------------------------------------------------------
def _cfg(**kw: Any) -> StorageConfig:
    defaults: dict[str, Any] = {
        "storage_type": StorageType.TIMESERIES,
        "host": PROM_HOST,
        "port": PROM_PORT,
    }
    defaults.update(kw)
    return StorageConfig(**defaults)


def _vec(
    name: str = "http_requests_total",
    labels: dict[str, str] | None = None,
    value: str = "1.5",
    ts: float = METRIC_TS_EPOCH,
) -> dict[str, Any]:
    merged = {"__name__": name, **(labels or {})}
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": merged, "value": [ts, value]}],
        },
    }


def _empty_vec() -> dict[str, Any]:
    return {
        "status": "success",
        "data": {"resultType": "vector", "result": []},
    }


def _resp(
    status: int = HTTP_OK,
    json_data: dict[str, Any] | None = None,
    text: str = "",
) -> AsyncMock:
    r = AsyncMock()
    r.status = status
    r.json = AsyncMock(return_value=json_data or {})
    r.text = AsyncMock(return_value=text)
    r.release = AsyncMock()
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


def _sess(response: AsyncMock | None = None) -> AsyncMock:
    s = AsyncMock()
    s.closed = False
    s.close = AsyncMock()
    if response is not None:
        s.request = AsyncMock(return_value=response)
    return s


def _dao_ready(
    model: type[Any] = PrometheusMetric,
    json_data: dict[str, Any] | None = None,
    status: int = HTTP_OK,
) -> PrometheusDAO:
    """Return a DAO with a mock session already connected."""
    dao = PrometheusDAO(model, _cfg())
    dao.session = _sess(_resp(status=status, json_data=json_data))
    dao._connected = True
    return dao


# -- Stub model classes -----------------------------------------------
class _MetricNameModel(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="custom_path",
    )
    metric_name: ClassVar[str] = "explicit_metric_total"
    value: float = 0.0


class _MetadataPathModel(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="metadata_metric_total",
    )
    count: int = 0


class _BareModel(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata()
    count: int = 0


class _GenericModel(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="generic")
    metric_name: str = "gen_metric"
    labels: dict[str, str] = Field(default_factory=dict)
    value: float = 0.0
    timestamp: datetime | None = None


class TestBuildBaseUrl:
    def test_from_host_and_port(self) -> None:
        assert build_base_url(_cfg()) == PROM_BASE_URL

    def test_custom_port(self) -> None:
        url = build_base_url(_cfg(port=CUSTOM_PORT))
        assert url == f"http://{PROM_HOST}:{CUSTOM_PORT}"

    def test_connection_string_override(self) -> None:
        cfg = _cfg(connection_string="https://prom.example.com:443/")
        assert build_base_url(cfg) == "https://prom.example.com:443"

    def test_none_config_defaults(self) -> None:
        assert build_base_url(None) == PROM_BASE_URL


class TestInit:
    def test_explicit_metric_name_attribute(self) -> None:
        dao = PrometheusDAO(_MetricNameModel, _cfg())
        assert dao._metric_name == "explicit_metric_total"

    def test_metadata_path_fallback(self) -> None:
        dao = PrometheusDAO(_MetadataPathModel, _cfg())
        assert dao._metric_name == "metadata_metric_total"

    def test_classname_fallback(self) -> None:
        dao = PrometheusDAO(_BareModel, _cfg())
        assert dao._metric_name == "_baremodel_total"

    def test_base_url_set(self) -> None:
        assert PrometheusDAO(PrometheusMetric, _cfg()).base_url == PROM_BASE_URL

    def test_session_initially_none(self) -> None:
        assert PrometheusDAO(PrometheusMetric, _cfg()).session is None

    def test_connected_initially_false(self) -> None:
        assert PrometheusDAO(PrometheusMetric, _cfg())._connected is False


class TestConnectionLifecycle:
    @patch(_CONN_PATCH)
    async def test_connect_creates_session(self, mc: MagicMock) -> None:
        mc.return_value = _sess()
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        await dao.connect()
        assert dao.session is not None
        assert dao._connected is True

    @patch(_CONN_PATCH)
    async def test_disconnect_clears_session(self, mc: MagicMock) -> None:
        mc.return_value = _sess()
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        await dao.connect()
        await dao.disconnect()
        assert dao.session is None
        assert dao._connected is False

    @patch(_CONN_PATCH)
    async def test_connect_idempotent(self, mc: MagicMock) -> None:
        mc.return_value = _sess()
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        await dao.connect()
        first = dao.session
        await dao.connect()
        assert dao.session is first

    @patch(_CONN_PATCH)
    async def test_ensure_session_auto_connects(self, mc: MagicMock) -> None:
        mc.return_value = _sess()
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        result = await dao._ensure_session()
        assert result is not None
        assert dao._connected is True

    async def test_ensure_session_raises_when_creation_fails(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        with (
            patch(_DAO_SESSION_PATCH, new_callable=AsyncMock, return_value=None),
            pytest.raises(StorageConnectionError),
        ):
            await dao._ensure_session()


class TestCreate:
    async def test_create_prometheus_metric(self) -> None:
        dao = _dao_ready(json_data={"status": "success"})
        metric = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"method": "GET"},
            value=METRIC_VALUE,
        )
        uid = await dao.create(metric)
        assert "http_requests_total" in uid
        assert "method=GET" in uid

    async def test_create_dict_instance(self) -> None:
        dao = _dao_ready(json_data={"status": "success"})
        data = {
            "metric_name": "cpu_usage",
            "labels": {"host": "web1"},
            "value": METRIC_VALUE,
        }
        uid = await dao.create(data)
        assert "cpu_usage" in uid
        assert "host=web1" in uid

    async def test_create_dict_defaults_metric_name(self) -> None:
        dao = _dao_ready(json_data={"status": "success"})
        data: dict[str, Any] = {"labels": {"env": "prod"}, "value": METRIC_VALUE}
        uid = await dao.create(data)
        assert dao._metric_name in uid

    async def test_create_generic_model(self) -> None:
        dao = _dao_ready(model=_GenericModel)
        inst = _GenericModel(
            metric_name="gen_metric",
            labels={"zone": "us-east"},
            value=METRIC_VALUE,
        )
        uid = await dao.create(inst)
        assert "gen_metric" in uid


class TestBulkCreate:
    async def test_bulk_create_returns_ids(self) -> None:
        dao = _dao_ready()
        metrics = [
            PrometheusMetric(
                metric_name="req_total",
                labels={"code": str(i)},
                value=float(i),
            )
            for i in range(BULK_COUNT)
        ]
        ids = await dao.bulk_create(metrics)
        assert len(ids) == BULK_COUNT

    async def test_bulk_create_dict_instances(self) -> None:
        dao = _dao_ready()
        dicts = [
            {"metric_name": "mem_used", "labels": {}, "value": 100.0}
            for _ in range(BULK_COUNT)
        ]
        ids = await dao.bulk_create(dicts)
        assert len(ids) == BULK_COUNT
        for item_id in ids:
            assert "mem_used" in item_id


class TestReadOperations:
    async def test_find_by_id_returns_metric(self) -> None:
        dao = _dao_ready(json_data=_vec(labels={"method": "GET"}))
        result = await dao.find_by_id('http_requests_total{method="GET"}')
        assert result is not None
        assert result.metric_name == "http_requests_total"

    async def test_find_by_id_returns_none_on_empty(self) -> None:
        dao = _dao_ready(json_data=_empty_vec())
        assert await dao.find_by_id("nonexistent_metric") is None

    async def test_find_one_returns_first(self) -> None:
        dao = _dao_ready(json_data=_vec(labels={"env": "prod"}))
        assert await dao.find_one({"env": "prod"}) is not None

    async def test_find_one_returns_none_on_empty(self) -> None:
        dao = _dao_ready(json_data=_empty_vec())
        assert await dao.find_one({"env": "staging"}) is None

    async def test_find_returns_list(self) -> None:
        dao = _dao_ready(json_data=_vec(labels={"region": "us"}))
        assert len(await dao.find({"region": "us"})) == 1

    async def test_find_with_limit(self) -> None:
        dao = _dao_ready(json_data=_vec())
        assert len(await dao.find({}, limit=1)) <= 1

    async def test_count_returns_int(self) -> None:
        dao = _dao_ready(json_data=_vec())
        assert await dao.count({}) == 1

    async def test_exists_true(self) -> None:
        dao = _dao_ready(json_data=_vec())
        assert await dao.exists("http_requests_total") is True

    async def test_exists_false(self) -> None:
        dao = _dao_ready(json_data=_empty_vec())
        assert await dao.exists("missing_metric") is False


class TestUnsupportedMutations:
    async def test_update_raises(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        with pytest.raises(NotImplementedError, match="append-only"):
            await dao.update("some_id", {"value": 2.0})

    async def test_bulk_update_raises(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        with pytest.raises(NotImplementedError, match="append-only"):
            await dao.bulk_update([{"id": "x", "value": 1.0}])

    async def test_delete_raises(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        with pytest.raises(NotImplementedError, match="append-only"):
            await dao.delete("some_id")

    async def test_bulk_delete_raises(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        with pytest.raises(NotImplementedError, match="append-only"):
            await dao.bulk_delete(["id_a", "id_b"])


class TestRawQueries:
    async def test_raw_read_query(self) -> None:
        dao = _dao_ready(json_data=_vec(value=str(RAW_QUERY_VALUE)))
        results = await dao.raw_read_query("up")
        assert len(results) == 1
        assert results[0]["value"] == RAW_QUERY_VALUE

    async def test_raw_read_query_with_time_param(self) -> None:
        dao = _dao_ready(json_data=_vec())
        results = await dao.raw_read_query(
            "up",
            params={"time": str(METRIC_TS_EPOCH)},
        )
        assert len(results) >= 1

    async def test_raw_write_query_success(self) -> None:
        dao = _dao_ready(status=HTTP_NO_CONTENT)
        count = await dao.raw_write_query("my_metric 42\nmy_metric2 99")
        assert count == RAW_LINE_COUNT

    async def test_raw_write_query_error_raises(self) -> None:
        dao = _dao_ready(status=HTTP_SERVER_ERROR)
        with pytest.raises(StorageError, match="retries"):
            await dao.raw_write_query("bad_metric 0")


class TestCreateIndexes:
    async def test_create_indexes_noop(self) -> None:
        await PrometheusDAO(PrometheusMetric, _cfg()).create_indexes()


class TestMetadata:
    async def test_list_databases_from_config(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg(database="mydb"))
        assert await dao.list_databases() == ["mydb"]

    async def test_list_databases_fallback_to_url(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        assert await dao.list_databases() == [PROM_BASE_URL]

    async def test_list_schemas_returns_labels(self) -> None:
        label_data = {
            "status": "success",
            "data": ["__name__", "instance", "job"],
        }
        dao = _dao_ready(json_data=label_data)
        schemas = await dao.list_schemas()
        assert "__name__" in schemas
        assert "job" in schemas

    async def test_list_models_returns_metric_names(self) -> None:
        names_data = {
            "status": "success",
            "data": ["up", "http_requests_total"],
        }
        dao = _dao_ready(json_data=names_data)
        models = await dao.list_models()
        assert "up" in models
        assert "http_requests_total" in models

    async def test_get_model_info(self) -> None:
        meta_data = {
            "status": "success",
            "data": {
                "up": [{"type": "gauge", "help": "Target is up", "unit": ""}],
            },
        }
        dao = _dao_ready(json_data=meta_data)
        info = await dao.get_model_info("up")
        assert info["name"] == "up"
        assert info["type"] == "gauge"

    async def test_test_connection_healthy(self) -> None:
        r = _resp(status=HTTP_OK)
        session = _sess(response=r)
        session.get = MagicMock(return_value=r)
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        dao.session = session
        dao._connected = True
        assert await dao.test_connection() is True

    async def test_test_connection_unhealthy(self) -> None:
        r = _resp(status=HTTP_SERVER_ERROR)
        session = _sess(response=r)
        session.get = MagicMock(return_value=r)
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        dao.session = session
        dao._connected = True
        assert await dao.test_connection() is False


class TestErrorPaths:
    async def test_read_without_session_raises(self) -> None:
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        dao.session = None
        dao._connected = False
        with (
            patch(_DAO_SESSION_PATCH, new_callable=AsyncMock, return_value=None),
            pytest.raises(StorageConnectionError),
        ):
            await dao.find_by_id("test_metric")

    async def test_api_error_on_query(self) -> None:
        dao = _dao_ready(status=HTTP_SERVER_ERROR)
        with pytest.raises(StorageError):
            await dao.raw_read_query("up")

    async def test_create_without_session_auto_connects(self) -> None:
        ok_session = _sess(_resp(status=HTTP_OK))
        dao = PrometheusDAO(PrometheusMetric, _cfg())
        dao.session = None
        dao._connected = False
        with patch(_DAO_SESSION_PATCH, new_callable=AsyncMock, return_value=ok_session):
            metric = PrometheusMetric(
                metric_name="auto_connect_metric",
                value=METRIC_VALUE,
            )
            uid = await dao.create(metric)
        assert "auto_connect_metric" in uid


class TestSessionManagement:
    async def test_session_with_auth_token(self) -> None:
        cfg = _cfg(options={"auth_token": "secret-tok"})
        with patch(_CONN_PATCH) as mc:
            mc.return_value = _sess()
            dao = PrometheusDAO(PrometheusMetric, cfg)
            await dao.connect()
            headers = mc.call_args.kwargs.get(
                "headers",
                mc.call_args[1].get("headers", {}),
            )
            assert headers["Authorization"] == "Bearer secret-tok"

    async def test_session_with_api_key(self) -> None:
        cfg = _cfg(options={"api_key": "my-key"})
        with patch(_CONN_PATCH) as mc:
            mc.return_value = _sess()
            dao = PrometheusDAO(PrometheusMetric, cfg)
            await dao.connect()
            headers = mc.call_args.kwargs.get(
                "headers",
                mc.call_args[1].get("headers", {}),
            )
            assert headers["X-API-Key"] == "my-key"

    async def test_session_custom_timeout(self) -> None:
        cfg = _cfg(options={"timeout": str(CUSTOM_TIMEOUT)})
        with patch(_CONN_PATCH) as mc:
            mc.return_value = _sess()
            dao = PrometheusDAO(PrometheusMetric, cfg)
            await dao.connect()
            timeout = mc.call_args.kwargs.get(
                "timeout",
                mc.call_args[1].get("timeout"),
            )
            assert timeout.total == CUSTOM_TIMEOUT
