"""Prometheus / VictoriaMetrics Data Access Object implementation.

Core CRUD operations.  Discovery and metadata methods are delegated
to ``prometheus_metadata``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import aiohttp

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageError
from ami.implementations.timeseries.prometheus_connection import (
    build_base_url,
    close_session,
    create_session,
)
from ami.implementations.timeseries.prometheus_models import (
    PrometheusMetric,
)
from ami.implementations.timeseries.prometheus_read import (
    find_metrics,
    instant_query,
)
from ami.implementations.timeseries.prometheus_write import (
    delete_series,
    write_metrics,
    write_single_metric,
)
from ami.models.storage_config import StorageConfig
from ami.utils.http_client import request_with_retry

logger = logging.getLogger(__name__)

HTTP_OK = 200


class PrometheusDAO(BaseDAO):
    """DAO for Prometheus / VictoriaMetrics time-series backends.

    Prometheus is primarily a pull-based monitoring system, so "create"
    and "update" operations go through the remote-write API or a
    Pushgateway.  Reads are translated to PromQL queries.
    """

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        super().__init__(model_cls, config)
        self.session: aiohttp.ClientSession | None = None
        self.base_url: str = build_base_url(config)
        self._connected: bool = False

        # Derive the metric name from model metadata or class attribute
        self._metric_name: str = self._resolve_metric_name()

    def _resolve_metric_name(self) -> str:
        """Determine the Prometheus metric name for this DAO.

        Checks (in order):
        1. ``metric_name`` attribute on model class
        2. ``path`` from model metadata
        3. Lowercased class name + ``_total``
        """
        # Check for explicit metric_name class attribute
        metric = getattr(self.model_cls, "metric_name", None)
        if metric:
            return str(metric)

        # Check model metadata path
        if hasattr(self.model_cls, "get_metadata"):
            meta = self.model_cls.get_metadata()
            if meta.path:
                return str(meta.path)

        # Fallback
        return self.model_cls.__name__.lower() + "_total"

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open an HTTP session for Prometheus API calls."""
        if self.session is not None and not self.session.closed:
            return
        self.session = await create_session(self.config)
        self._connected = True
        logger.info("Prometheus session opened for %s", self.base_url)

    async def disconnect(self) -> None:
        """Close the HTTP session."""
        await close_session(self.session)
        self.session = None
        self._connected = False
        logger.info("Prometheus session closed for %s", self.base_url)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            await self.connect()
        assert self.session is not None
        return self.session

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create(self, instance: Any) -> str:
        """Write a metric sample (remote-write / pushgateway)."""
        await self._ensure_session()

        if isinstance(instance, PrometheusMetric):
            metric_data: dict[str, Any] = {
                "metric_name": instance.metric_name,
                "labels": instance.labels,
                "value": instance.value,
                "timestamp": instance.timestamp,
            }
        elif isinstance(instance, dict):
            metric_data = instance.copy()
            metric_data.setdefault("metric_name", self._metric_name)
        else:
            data = (
                instance.model_dump(mode="json")
                if hasattr(instance, "model_dump")
                else dict(instance)
            )
            metric_data = {
                "metric_name": data.get("metric_name", self._metric_name),
                "labels": data.get("labels", {}),
                "value": data.get("value", 0),
                "timestamp": data.get("timestamp"),
            }

        uid = await write_single_metric(
            self,
            metric_data["metric_name"],
            float(metric_data.get("value", 0)),
            labels=metric_data.get("labels"),
            timestamp=metric_data.get("timestamp"),
        )
        return uid

    async def bulk_create(self, instances: list[Any]) -> list[str]:
        """Write multiple metric samples."""
        await self._ensure_session()
        metrics: list[dict[str, Any]] = []
        for inst in instances:
            if isinstance(inst, PrometheusMetric):
                metrics.append(
                    {
                        "metric_name": inst.metric_name,
                        "labels": inst.labels,
                        "value": inst.value,
                        "timestamp": inst.timestamp,
                    }
                )
            elif isinstance(inst, dict):
                inst.setdefault("metric_name", self._metric_name)
                metrics.append(inst)
            else:
                data = (
                    inst.model_dump(mode="json")
                    if hasattr(inst, "model_dump")
                    else dict(inst)
                )
                metrics.append(
                    {
                        "metric_name": data.get("metric_name", self._metric_name),
                        "labels": data.get("labels", {}),
                        "value": data.get("value", 0),
                        "timestamp": data.get("timestamp"),
                    }
                )

        count = await write_metrics(self, metrics)
        logger.debug("bulk_create wrote %d/%d metrics", count, len(metrics))

        # Return synthetic IDs
        ids: list[str] = []
        for m in metrics:
            name = m.get("metric_name", "unknown")
            labels = m.get("labels", {})
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            ids.append(f"{name}{{{label_str}}}")
        return ids

    async def find_by_id(self, item_id: str) -> Any | None:
        """Find a metric by its synthetic ID (metric_name{labels}).

        The *item_id* is expected to be a PromQL selector.
        """
        await self._ensure_session()
        results = await instant_query(self, item_id)
        if not results:
            return None
        record = results[0]
        if issubclass(self.model_cls, PrometheusMetric):
            return self.model_cls(**record)
        return self.model_cls.from_storage_dict(record)

    async def find_one(self, query: dict[str, Any]) -> Any | None:
        """Find a single metric matching *query*."""
        results = await self.find(query, limit=1)
        return results[0] if results else None

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        """Query metrics using dict-based filters."""
        await self._ensure_session()
        metric_name = query.pop("metric_name", self._metric_name)
        results = await find_metrics(
            self,
            metric_name,
            query,
            limit=limit,
            skip=skip,
        )
        output: list[Any] = [
            self.model_cls(**record)
            if issubclass(self.model_cls, PrometheusMetric)
            else self.model_cls.from_storage_dict(record)
            for record in results
        ]
        return output

    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """'Update' a metric by writing a new sample.

        Prometheus is append-only; updates are new data points.
        """
        await self._ensure_session()
        metric_name = data.get("metric_name", self._metric_name)
        labels = data.get("labels", {})
        value = data.get("value", 0)
        timestamp = data.get("timestamp")
        await write_single_metric(
            self,
            metric_name,
            float(value),
            labels=labels,
            timestamp=timestamp,
        )

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Bulk 'update' -- writes new samples for each entry."""
        metrics: list[dict[str, Any]] = [
            {
                "metric_name": upd.get("metric_name", self._metric_name),
                "labels": upd.get("labels", {}),
                "value": upd.get("value", 0),
                "timestamp": upd.get("timestamp"),
            }
            for upd in updates
        ]
        await self._ensure_session()
        await write_metrics(self, metrics)

    async def delete(self, item_id: str) -> bool:
        """Delete series matching the given selector."""
        await self._ensure_session()
        try:
            count = await delete_series(self, match=[item_id])
        except StorageError:
            logger.warning("Delete failed for selector %s", item_id)
            return False
        else:
            return count > 0

    async def bulk_delete(self, ids: list[str]) -> int:
        """Delete series for multiple selectors."""
        await self._ensure_session()
        deleted = 0
        for item_id in ids:
            if await self.delete(item_id):
                deleted += 1
        return deleted

    async def count(self, query: dict[str, Any]) -> int:
        """Count matching series."""
        results = await self.find(query)
        return len(results)

    async def exists(self, item_id: str) -> bool:
        """Check if a metric series exists."""
        result = await self.find_by_id(item_id)
        return result is not None

    # ------------------------------------------------------------------
    # Raw queries
    # ------------------------------------------------------------------

    async def create_indexes(self) -> None:
        """No-op: Prometheus manages its own indexing."""
        logger.debug("create_indexes is a no-op for Prometheus DAO")

    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a raw PromQL instant query."""
        await self._ensure_session()
        time_param = None
        if params and "time" in params:
            time_param = datetime.fromtimestamp(float(params["time"]), tz=UTC)
        return await instant_query(self, query, time=time_param)

    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a raw write (exposition-format lines)."""
        await self._ensure_session()
        session = self.session
        assert session is not None

        url = f"{self.base_url}/api/v1/import/prometheus"
        payload = query
        resp = await request_with_retry(
            session,
            "POST",
            url,
            data=payload,
            headers={"Content-Type": "text/plain"},
        )
        async with resp:
            if resp.status not in (HTTP_OK, 204):
                body = await resp.text()
                msg = f"raw_write_query failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
        return query.count("\n") + 1

    # ------------------------------------------------------------------
    # Discovery / metadata (delegated to prometheus_metadata)
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[str]:
        from ami.implementations.timeseries.prometheus_metadata import (
            list_databases,
        )

        return await list_databases(self)

    async def list_schemas(self, database: str | None = None) -> list[str]:
        from ami.implementations.timeseries.prometheus_metadata import (
            list_schemas,
        )

        return await list_schemas(self, database)

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        from ami.implementations.timeseries.prometheus_metadata import (
            list_models,
        )

        return await list_models(self, database, schema)

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        from ami.implementations.timeseries.prometheus_metadata import (
            get_model_info,
        )

        return await get_model_info(self, path, database, schema)

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        from ami.implementations.timeseries.prometheus_metadata import (
            get_model_schema,
        )

        return await get_model_schema(self, path, database, schema)

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        from ami.implementations.timeseries.prometheus_metadata import (
            get_model_fields,
        )

        return await get_model_fields(self, path, database, schema)

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        from ami.implementations.timeseries.prometheus_metadata import (
            get_model_indexes,
        )

        return await get_model_indexes(self, path, database, schema)

    async def test_connection(self) -> bool:
        from ami.implementations.timeseries.prometheus_metadata import (
            test_connection,
        )

        return await test_connection(self)
