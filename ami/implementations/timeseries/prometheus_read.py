"""Read / query operations for the Prometheus DAO.

Functions accept a *dao* reference (carrying ``session`` and ``base_url``)
and return parsed results.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.timeseries.prometheus_models import (
    dict_query_to_promql,
    parse_prometheus_response,
)
from ami.utils.http_client import request_with_retry

logger = logging.getLogger(__name__)

HTTP_OK = 200


# ------------------------------------------------------------------
# Instant / range queries
# ------------------------------------------------------------------


async def instant_query(
    dao: Any,
    promql: str,
    time: datetime | None = None,
) -> list[dict[str, Any]]:
    """Execute a Prometheus instant query.

    Args:
        dao: DAO instance with ``session`` and ``base_url``.
        promql: PromQL expression.
        time: Evaluation timestamp (defaults to server time).

    Returns:
        List of parsed result records.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/query"
    params: dict[str, str] = {"query": promql}
    if time is not None:
        params["time"] = str(time.timestamp())

    resp = await request_with_retry(session, "GET", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus instant query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    return parse_prometheus_response(data)


async def range_query(
    dao: Any,
    promql: str,
    start: datetime,
    end: datetime,
    step: str = "15s",
) -> list[dict[str, Any]]:
    """Execute a Prometheus range query.

    Args:
        dao: DAO instance with ``session`` and ``base_url``.
        promql: PromQL expression.
        start: Range start time.
        end: Range end time.
        step: Query resolution step (e.g. ``"15s"``, ``"1m"``).

    Returns:
        List of parsed result records (may contain multiple values per series).
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/query_range"
    params: dict[str, str] = {
        "query": promql,
        "start": str(start.timestamp()),
        "end": str(end.timestamp()),
        "step": step,
    }

    resp = await request_with_retry(session, "GET", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus range query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    return parse_prometheus_response(data)


# ------------------------------------------------------------------
# Dict-query based reads
# ------------------------------------------------------------------


async def find_metrics(
    dao: Any,
    metric_name: str,
    query: dict[str, Any] | None = None,
    *,
    limit: int | None = None,
    skip: int = 0,
) -> list[dict[str, Any]]:
    """Query metrics using a dict-based filter.

    Converts *query* to PromQL using ``dict_query_to_promql`` and
    executes an instant query.
    """
    promql = dict_query_to_promql(metric_name, query or {})
    results = await instant_query(dao, promql)

    if skip:
        results = results[skip:]
    if limit is not None:
        results = results[:limit]
    return results


async def find_metric_by_labels(
    dao: Any,
    metric_name: str,
    labels: dict[str, str],
    *,
    time: datetime | None = None,
) -> dict[str, Any] | None:
    """Find a single metric matching exact label values."""
    promql = dict_query_to_promql(metric_name, dict(labels.items()))
    results = await instant_query(dao, promql, time=time)
    return results[0] if results else None


# ------------------------------------------------------------------
# Series / label metadata
# ------------------------------------------------------------------


async def get_series(
    dao: Any,
    match: list[str],
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict[str, str]]:
    """Retrieve time series matching one or more selectors.

    Calls ``/api/v1/series``.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/series"
    params: dict[str, Any] = {"match[]": match}
    if start:
        params["start"] = str(start.timestamp())
    if end:
        params["end"] = str(end.timestamp())

    resp = await request_with_retry(session, "GET", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus series query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    result: list[dict[str, str]] = data.get("data", [])
    return result


async def get_label_names(
    dao: Any,
    match: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[str]:
    """Retrieve all label names, optionally filtered by series selectors."""
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/labels"
    params: dict[str, Any] = {}
    if match:
        params["match[]"] = match
    if start:
        params["start"] = str(start.timestamp())
    if end:
        params["end"] = str(end.timestamp())

    resp = await request_with_retry(session, "GET", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus labels query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    label_names: list[str] = data.get("data", [])
    return label_names


async def get_label_values(
    dao: Any,
    label_name: str,
    match: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[str]:
    """Retrieve all values for a given label name."""
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/label/{label_name}/values"
    params: dict[str, Any] = {}
    if match:
        params["match[]"] = match
    if start:
        params["start"] = str(start.timestamp())
    if end:
        params["end"] = str(end.timestamp())

    resp = await request_with_retry(session, "GET", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus label values query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    label_values: list[str] = data.get("data", [])
    return label_values


# ------------------------------------------------------------------
# Metadata endpoints
# ------------------------------------------------------------------


async def get_metric_metadata(
    dao: Any,
    metric_name: str | None = None,
) -> dict[str, Any]:
    """Retrieve metric metadata from ``/api/v1/metadata``."""
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/metadata"
    params: dict[str, str] = {}
    if metric_name:
        params["metric"] = metric_name

    resp = await request_with_retry(session, "GET", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus metadata query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    metadata: dict[str, Any] = data.get("data", {})
    return metadata


async def get_targets(dao: Any) -> dict[str, Any]:
    """Retrieve active targets from ``/api/v1/targets``."""
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/targets"
    resp = await request_with_retry(session, "GET", url)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus targets query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    targets: dict[str, Any] = data.get("data", {})
    return targets


async def get_rules(dao: Any) -> dict[str, Any]:
    """Retrieve alerting and recording rules from ``/api/v1/rules``."""
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/rules"
    resp = await request_with_retry(session, "GET", url)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus rules query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    rules: dict[str, Any] = data.get("data", {})
    return rules


async def get_alerts(dao: Any) -> list[dict[str, Any]]:
    """Retrieve active alerts from ``/api/v1/alerts``."""
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/alerts"
    resp = await request_with_retry(session, "GET", url)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus alerts query failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    alerts: list[dict[str, Any]] = data.get("data", {}).get("alerts", [])
    return alerts
