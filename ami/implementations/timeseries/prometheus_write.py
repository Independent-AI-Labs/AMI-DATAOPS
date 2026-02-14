"""Write operations for the Prometheus DAO.

Prometheus is primarily a pull-based system, so "writes" go through:
1. The remote-write API (VictoriaMetrics / Prometheus remote-write receiver).
2. The Pushgateway for batch metrics.
3. Local exposition formatting for scrape-based ingestion.

Functions accept a *dao* reference (carrying ``session``, ``base_url``,
and configuration) and return results.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ami.core.exceptions import StorageError
from ami.utils.http_client import request_with_retry

logger = logging.getLogger(__name__)

HTTP_OK = 200
HTTP_NO_CONTENT = 204


# ------------------------------------------------------------------
# Remote-write helpers
# ------------------------------------------------------------------


async def write_metrics(
    dao: Any,
    metrics: list[dict[str, Any]],
) -> int:
    """Write metric samples via remote-write or import API.

    Each dict in *metrics* should contain:
    - ``metric_name`` (str)
    - ``labels`` (dict[str, str])
    - ``value`` (float)
    - ``timestamp`` (datetime | None)

    Returns:
        Number of successfully written samples.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    if not metrics:
        return 0

    # Try VictoriaMetrics import/prometheus API first
    import_url = f"{dao.base_url}/api/v1/import/prometheus"
    lines = _format_exposition_lines(metrics)
    payload = "\n".join(lines)

    try:
        resp = await request_with_retry(
            session,
            "POST",
            import_url,
            data=payload,
            headers={"Content-Type": "text/plain"},
        )
        async with resp:
            if resp.status in (HTTP_OK, HTTP_NO_CONTENT):
                logger.debug("Wrote %d metrics via remote import", len(metrics))
                return len(metrics)
    except StorageError:
        logger.debug("Remote import not available, trying pushgateway")

    # Fallback to pushgateway
    return await _push_to_gateway(dao, metrics)


async def write_single_metric(
    dao: Any,
    metric_name: str,
    value: float,
    labels: dict[str, str] | None = None,
    timestamp: datetime | None = None,
) -> str:
    """Write a single metric sample.

    Returns:
        A synthetic ID for the written sample.
    """
    metric: dict[str, Any] = {
        "metric_name": metric_name,
        "labels": labels or {},
        "value": value,
        "timestamp": timestamp,
    }
    count = await write_metrics(dao, [metric])
    if count == 0:
        msg = f"Failed to write metric {metric_name}"
        raise StorageError(msg)

    # Construct a deterministic ID from name + labels
    label_str = ",".join(f"{k}={v}" for k, v in sorted((labels or {}).items()))
    return f"{metric_name}{{{label_str}}}"


async def delete_series(
    dao: Any,
    match: list[str],
    start: datetime | None = None,
    end: datetime | None = None,
) -> int:
    """Delete time series matching the given selectors.

    Uses the ``/api/v1/admin/tsdb/delete_series`` endpoint.
    Requires ``--web.enable-admin-api`` on the Prometheus server.

    Returns:
        Number of matchers applied (not actual series count).
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/admin/tsdb/delete_series"
    params: dict[str, Any] = {"match[]": match}
    if start:
        params["start"] = str(start.timestamp())
    if end:
        params["end"] = str(end.timestamp())

    resp = await request_with_retry(session, "POST", url, params=params)
    async with resp:
        if resp.status not in (HTTP_OK, HTTP_NO_CONTENT):
            body = await resp.text()
            msg = f"Prometheus delete_series failed: {resp.status} {body[:200]}"
            raise StorageError(msg)

    logger.info("Deleted series matching %s", match)
    return len(match)


async def clean_tombstones(dao: Any) -> None:
    """Trigger tombstone cleanup on the Prometheus server.

    Uses the ``/api/v1/admin/tsdb/clean_tombstones`` endpoint.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/admin/tsdb/clean_tombstones"
    resp = await request_with_retry(session, "POST", url)
    async with resp:
        if resp.status not in (HTTP_OK, HTTP_NO_CONTENT):
            body = await resp.text()
            msg = f"Prometheus clean_tombstones failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
    logger.info("Tombstone cleanup triggered")


async def snapshot(dao: Any, skip_head: bool = False) -> str:
    """Create a TSDB snapshot.

    Returns:
        The snapshot name.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    url = f"{dao.base_url}/api/v1/admin/tsdb/snapshot"
    params: dict[str, str] = {}
    if skip_head:
        params["skip_head"] = "true"

    resp = await request_with_retry(session, "POST", url, params=params)
    async with resp:
        if resp.status != HTTP_OK:
            body = await resp.text()
            msg = f"Prometheus snapshot failed: {resp.status} {body[:200]}"
            raise StorageError(msg)
        data = await resp.json()

    name: str = data.get("data", {}).get("name", "")
    logger.info("Created TSDB snapshot: %s", name)
    return name


# ------------------------------------------------------------------
# Pushgateway helpers
# ------------------------------------------------------------------


async def _push_to_gateway(
    dao: Any,
    metrics: list[dict[str, Any]],
) -> int:
    """Push metrics to a Pushgateway instance.

    The pushgateway URL is derived from config options or defaults to
    ``<base_url>:9091``.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    gw_url = _get_pushgateway_url(dao)
    job_name = "ami"
    if dao.config and dao.config.options:
        job_name = dao.config.options.get("pushgateway_job", job_name)

    url = f"{gw_url}/metrics/job/{job_name}"
    lines = _format_exposition_lines(metrics)
    payload = "\n".join(lines) + "\n"

    try:
        resp = await request_with_retry(
            session,
            "POST",
            url,
            data=payload,
            headers={"Content-Type": "text/plain"},
        )
        async with resp:
            if resp.status in (HTTP_OK, HTTP_NO_CONTENT):
                logger.debug(
                    "Pushed %d metrics to gateway",
                    len(metrics),
                )
                return len(metrics)
            body = await resp.text()
            status = resp.status
    except StorageError:
        raise
    except Exception as exc:
        msg = f"Pushgateway write failed: {exc}"
        raise StorageError(msg) from exc
    else:
        msg = f"Pushgateway write failed: {status} {body[:200]}"
        raise StorageError(msg)


async def delete_from_gateway(
    dao: Any,
    job_name: str | None = None,
    grouping: dict[str, str] | None = None,
) -> bool:
    """Delete metrics from the Pushgateway.

    Returns *True* on success.
    """
    session = dao.session
    if session is None:
        msg = "Prometheus session not connected"
        raise StorageError(msg)

    gw_url = _get_pushgateway_url(dao)
    job = job_name or "ami"
    url = f"{gw_url}/metrics/job/{job}"
    if grouping:
        for k, v in sorted(grouping.items()):
            url = f"{url}/{k}/{v}"

    resp = await request_with_retry(session, "DELETE", url)
    async with resp:
        if resp.status in (HTTP_OK, HTTP_NO_CONTENT, 202):
            logger.info("Deleted pushgateway metrics for job %s", job)
            return True
        body = await resp.text()
        logger.warning(
            "Pushgateway delete returned %d: %s",
            resp.status,
            body[:200],
        )
        return False


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _get_pushgateway_url(dao: Any) -> str:
    """Derive the Pushgateway URL from DAO config."""
    if dao.config and dao.config.options:
        gw = dao.config.options.get("pushgateway_url")
        if gw:
            return str(gw).rstrip("/")
    # Default: same host, port 9091
    host = (dao.config.host if dao.config else None) or "localhost"
    return f"http://{host}:9091"


def _format_exposition_lines(
    metrics: list[dict[str, Any]],
) -> list[str]:
    """Format a list of metric dicts as Prometheus text-exposition lines."""
    lines: list[str] = []
    for m in metrics:
        name = m.get("metric_name", "unknown")
        labels = m.get("labels", {})
        value = m.get("value", 0)
        ts = m.get("timestamp")

        if labels:
            parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
            selector = f"{name}{{{','.join(parts)}}}"
        else:
            selector = name

        line = f"{selector} {value}"
        if ts is not None:
            if isinstance(ts, datetime):
                line = f"{line} {int(ts.timestamp() * 1000)}"
            else:
                line = f"{line} {ts}"
        lines.append(line)
    return lines
