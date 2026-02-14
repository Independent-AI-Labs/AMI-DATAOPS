"""Enhanced decorators for event recording and sensitive field handling."""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, ClassVar, TypeVar

from pydantic import Field
from uuid_utils import uuid7

from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import DataClassification
from ami.models.storage_config import StorageConfig
from ami.secrets.config import SensitiveFieldConfig

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=StorageModel)


class EventRecord(StorageModel):
    """Generic event record for capturing function calls."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="events",
        storage_configs={
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "timeseries": StorageConfig(
                storage_type=StorageType.TIMESERIES,
            ),
            "document": StorageConfig(
                storage_type=StorageType.DOCUMENT,
            ),
        },
    )

    event_id: str = Field(
        default_factory=lambda: f"event_{uuid7()}",
    )
    event_type: str
    function_name: str

    input: dict[str, Any] = Field(default_factory=dict)
    output: Any = None

    start_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    end_time: datetime | None = None
    duration_ms: int | None = None

    success: bool = True
    error: str | None = None
    error_type: str | None = None

    context_user: str | None = None
    context_session: str | None = None


def sensitive_field(
    field_name: str,
    mask_pattern: str = "{field}_uid",
    *,
    classification: DataClassification | None = None,
    namespace: str | None = None,
    auto_rotate_days: int | None = None,
) -> Callable[[type], type]:
    """Mark a field as sensitive for vault-backed persistence."""
    config = SensitiveFieldConfig(
        mask_pattern=mask_pattern,
        classification=classification,
        namespace=namespace,
        auto_rotate_days=auto_rotate_days,
    )

    def decorator(cls: type[T]) -> type[T]:
        attr = "_sensitive_fields"
        if not hasattr(cls, attr):
            setattr(cls, attr, {})
        sensitive_fields = getattr(cls, attr)
        if isinstance(sensitive_fields, dict):
            sensitive_fields[field_name] = config
        return cls

    return decorator


def sanitize_for_mcp(
    instance: StorageModel,
    caller: str = "mcp",
) -> dict[str, Any]:
    """Sanitize model instance for MCP server output.

    Replaces sensitive field values with masked versions.
    """
    data = instance.model_dump()

    if hasattr(instance.__class__, "_sensitive_fields"):
        for field_name, config in instance.__class__._sensitive_fields.items():
            if field_name in data:
                mask_value = config.mask_value(field_name)
                if "uid" in mask_value.lower():
                    mask_value = f"{mask_value}_{uuid7()}"
                data[field_name] = mask_value
                logger.debug(
                    "Masked sensitive field '%s' for %s",
                    field_name,
                    caller,
                )

    return data


def record_event(
    event_type: type[StorageModel] | str,
    **options: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Simplified event recording decorator."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                if inspect.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
            except Exception:
                logger.exception(
                    "Event %s: %s failed",
                    event_type,
                    func.__name__,
                )
                raise
            else:
                logger.debug(
                    "Event %s: %s succeeded",
                    event_type,
                    func.__name__,
                )
                return result

        return wrapper

    return decorator


def cached_result(
    ttl: int = 300,
    cache_key: Callable[..., str] | None = None,
    backend: str = "memory",
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Cache function results to avoid repeated expensive operations.

    Args:
        ttl: Time to live in seconds
        cache_key: Function to generate cache key
        backend: Cache backend (memory or redis)
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        cache: dict[str, Any] = {}

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = (
                cache_key(*args, **kwargs)
                if cache_key
                else f"{func.__name__}:{args!s}:{kwargs!s}"
            )

            if backend == "memory" and key in cache:
                cached_data, cached_time = cache[key]
                if datetime.now(UTC).timestamp() - cached_time < ttl:
                    logger.debug("Cache hit for %s", key)
                    return cached_data

            result = await func(*args, **kwargs)

            if backend == "memory":
                cache[key] = (result, datetime.now(UTC).timestamp())

            return result

        return wrapper

    return decorator
