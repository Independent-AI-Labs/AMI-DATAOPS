"""Helpers for persisting and hydrating sensitive fields via the secrets broker."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextvars import ContextVar
from copy import deepcopy
from typing import Any

from pydantic import SecretStr

from ami.models.security import SecurityContext
from ami.secrets.client import (
    compute_integrity_hash,
    get_secrets_broker_client,
)
from ami.secrets.config import SensitiveFieldConfig
from ami.secrets.pointer import VaultFieldPointer

logger = logging.getLogger(__name__)

_POINTER_CONTEXT: ContextVar[dict[str, VaultFieldPointer] | None] = ContextVar(
    "_POINTER_CONTEXT", default=None
)


def prepare_instance_for_storage(
    instance: Any,
    data: dict[str, Any],
    context: SecurityContext | None = None,
) -> dict[str, Any]:
    """Replace sensitive field values with vault pointers before persistence."""
    sensitive_map: Mapping[str, SensitiveFieldConfig] | None = getattr(
        instance.__class__,
        "_sensitive_fields",
        None,
    )
    if not sensitive_map:
        return data

    client = get_secrets_broker_client()
    namespace_default = instance.__class__.__module__

    payload = deepcopy(data)
    pointer_cache = getattr(instance, "_vault_pointer_cache", {})

    for field_name, config in sensitive_map.items():
        raw_value = getattr(instance, field_name, None)
        if raw_value in (None, ""):
            continue
        if isinstance(raw_value, VaultFieldPointer):
            payload[field_name] = raw_value.to_storage()
            pointer_cache[field_name] = raw_value
            continue
        if isinstance(raw_value, dict) and "vault_reference" in raw_value:
            pointer = VaultFieldPointer.model_validate(raw_value)
            payload[field_name] = pointer.to_storage()
            pointer_cache[field_name] = pointer
            continue

        if isinstance(raw_value, SecretStr):
            raw_value = raw_value.get_secret_value()
        elif not isinstance(raw_value, str):
            raw_value = str(raw_value)

        integrity_hash = compute_integrity_hash(raw_value)
        cached_pointer: VaultFieldPointer | None = pointer_cache.get(
            field_name,
        )
        if cached_pointer and cached_pointer.integrity_hash == integrity_hash:
            payload[field_name] = cached_pointer.to_storage()
            continue

        pointer = client.ensure_secret(
            namespace=config.namespace or namespace_default,
            model=instance.__class__.__name__,
            field=field_name,
            value=raw_value,
            classification=config.classification,
        )
        payload[field_name] = pointer.to_storage()
        pointer_cache[field_name] = pointer

        logger.debug(
            "Persisted sensitive field via broker: %s ref=%s v=%d",
            field_name,
            pointer.vault_reference,
            pointer.version,
        )

    return payload


def hydrate_sensitive_fields(
    model_cls: type[Any],
    data: dict[str, Any],
    context: SecurityContext | None = None,
) -> dict[str, Any]:
    """Hydrate sensitive fields from vault pointers when loading."""
    sensitive_map: Mapping[str, SensitiveFieldConfig] | None = getattr(
        model_cls,
        "_sensitive_fields",
        None,
    )
    if not sensitive_map:
        return data

    client = get_secrets_broker_client()
    hydrated = deepcopy(data)
    pointer_map: dict[str, VaultFieldPointer] = {}

    for field_name in sensitive_map:
        value = hydrated.get(field_name)
        if not isinstance(value, Mapping) or "vault_reference" not in value:
            continue

        pointer = VaultFieldPointer.model_validate(value)
        secret_value, integrity_hash = client.retrieve_secret(
            pointer.vault_reference,
        )
        if integrity_hash != pointer.integrity_hash:
            msg = (
                f"Integrity mismatch for "
                f"{model_cls.__name__}.{field_name}: "
                f"{pointer.vault_reference}"
            )
            raise ValueError(msg)

        hydrated[field_name] = secret_value
        pointer_map[field_name] = pointer

    if pointer_map:
        _POINTER_CONTEXT.set(pointer_map)

    return hydrated


def consume_pointer_cache() -> dict[str, VaultFieldPointer] | None:
    """Return and clear the pointer cache collected during hydration."""
    cache = _POINTER_CONTEXT.get()
    _POINTER_CONTEXT.set(None)
    return cache
