"""Prometheus-specific data models and query helpers.

Defines the ``PrometheusMetric`` storage model and utilities for
converting between dict-based queries and PromQL selectors.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, ClassVar

from pydantic import Field

from ami.models.base_model import ModelMetadata, StorageModel

logger = logging.getLogger(__name__)

# Minimum number of elements in a timestamp-value pair [timestamp, value]
_MIN_TS_VAL_LEN = 2


class PrometheusMetric(StorageModel):
    """Model representing a Prometheus metric data point."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="metrics")

    metric_name: str = Field(
        ...,
        description="Prometheus metric name (e.g. http_requests_total)",
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="Label key-value pairs",
    )
    value: float = Field(
        default=0.0,
        description="Metric value",
    )
    timestamp: datetime | None = Field(
        default=None,
        description="Metric timestamp (defaults to server time)",
    )

    def to_promql_selector(self) -> str:
        """Build a PromQL label selector string.

        Example: ``http_requests_total{method="GET",status="200"}``
        """
        if not self.labels:
            return self.metric_name
        parts = [f'{k}="{v}"' for k, v in sorted(self.labels.items())]
        return f"{self.metric_name}{{{','.join(parts)}}}"

    def to_exposition_line(self) -> str:
        """Format as a Prometheus text-exposition line.

        Example: ``http_requests_total{method="GET"} 42.0 1620000000000``
        """
        selector = self.to_promql_selector()
        ts_ms = ""
        if self.timestamp:
            ts_ms = f" {int(self.timestamp.timestamp() * 1000)}"
        return f"{selector} {self.value}{ts_ms}"


class PromQLBuilder:
    """Helper to construct PromQL query strings from structured data."""

    @staticmethod
    def instant_query(metric: str, labels: dict[str, str] | None = None) -> str:
        """Build an instant query selector."""
        if not labels:
            return metric
        parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
        return f"{metric}{{{','.join(parts)}}}"

    @staticmethod
    def range_query(
        metric: str,
        labels: dict[str, str] | None = None,
        duration: str = "5m",
    ) -> str:
        """Build a range vector selector."""
        base = PromQLBuilder.instant_query(metric, labels)
        return f"{base}[{duration}]"

    @staticmethod
    def rate_query(
        metric: str,
        labels: dict[str, str] | None = None,
        duration: str = "5m",
    ) -> str:
        """Build a ``rate()`` query."""
        range_sel = PromQLBuilder.range_query(metric, labels, duration)
        return f"rate({range_sel})"

    @staticmethod
    def aggregation_query(
        func: str,
        metric: str,
        labels: dict[str, str] | None = None,
        by: list[str] | None = None,
    ) -> str:
        """Build an aggregation query like ``sum by (code) (metric{...})``."""
        selector = PromQLBuilder.instant_query(metric, labels)
        if by:
            by_clause = ", ".join(by)
            return f"{func} by ({by_clause}) ({selector})"
        return f"{func}({selector})"


# ------------------------------------------------------------------
# Dict-query to PromQL conversion
# ------------------------------------------------------------------


def _validate_regex_pattern(pattern: str) -> None:
    """Validate that a regex pattern has no PromQL structural chars."""
    if any(c in pattern for c in "{}"):
        msg = f"Invalid regex pattern: {pattern}"
        raise ValueError(msg)


def _build_dict_selector(
    key: str,
    value: Any,
) -> str | None:
    """Build a single label selector from a dict operator."""
    if "$ne" in value:
        return f'{key}!="{value["$ne"]}"'
    if "$regex" in value:
        _validate_regex_pattern(value["$regex"])
        return f'{key}=~"{value["$regex"]}"'
    if "$nregex" in value:
        _validate_regex_pattern(value["$nregex"])
        return f'{key}!~"{value["$nregex"]}"'
    logger.warning("Unsupported operator in query for key %s", key)
    return None


def dict_query_to_promql(
    metric_name: str,
    query: dict[str, Any],
) -> str:
    """Convert a MongoDB-style dict query to a PromQL selector.

    Supported operators:
    - Equality: ``{"method": "GET"}`` -> ``method="GET"``
    - ``$ne``:  ``{"method": {"$ne": "POST"}}`` -> ``method!="POST"``
    - ``$regex``: ``{"path": {"$regex": "/api.*"}}`` -> ``path=~"/api.*"``
    - ``$nregex``: ``{"path": {"$nregex": "/health"}}`` -> ``path!~"/health"``

    Raises ``ValueError`` if a regex pattern contains PromQL structural chars.
    """
    if not query:
        return metric_name

    label_selectors: list[str] = []

    for key, value in query.items():
        if key.startswith("$"):
            logger.debug(
                "Skipping unsupported top-level operator %s",
                key,
            )
            continue

        if isinstance(value, dict):
            selector = _build_dict_selector(key, value)
            if selector:
                label_selectors.append(selector)
        else:
            label_selectors.append(f'{key}="{value}"')

    if not label_selectors:
        return metric_name
    return f"{metric_name}{{{','.join(label_selectors)}}}"


def parse_prometheus_response(
    response_data: dict[str, Any],
) -> list[dict[str, Any]]:
    """Parse a Prometheus API query response into a flat list of records.

    Handles both ``vector`` and ``matrix`` result types.
    """
    status = response_data.get("status")
    if status != "success":
        logger.warning("Prometheus query returned status: %s", status)
        return []

    data = response_data.get("data", {})
    result_type = data.get("resultType", "")
    results_raw = data.get("result", [])

    records: list[dict[str, Any]] = []

    if result_type == "vector":
        for item in results_raw:
            metric = item.get("metric", {})
            ts_val = item.get("value", [])
            record: dict[str, Any] = {
                "metric_name": metric.get("__name__", ""),
                "labels": {k: v for k, v in metric.items() if k != "__name__"},
            }
            if len(ts_val) >= _MIN_TS_VAL_LEN:
                record["timestamp"] = datetime.fromtimestamp(
                    float(ts_val[0]),
                    tz=UTC,
                )
                record["value"] = float(ts_val[1])
            records.append(record)

    elif result_type == "matrix":
        for item in results_raw:
            metric = item.get("metric", {})
            base_labels = {k: v for k, v in metric.items() if k != "__name__"}
            metric_name = metric.get("__name__", "")
            records.extend(
                {
                    "metric_name": metric_name,
                    "labels": base_labels.copy(),
                    "timestamp": datetime.fromtimestamp(
                        float(ts_val[0]),
                        tz=UTC,
                    ),
                    "value": float(ts_val[1]),
                }
                for ts_val in item.get("values", [])
                if len(ts_val) >= _MIN_TS_VAL_LEN
            )
    else:
        logger.debug("Unhandled Prometheus result type: %s", result_type)

    return records
