"""Update operations for Dgraph DAO."""

import json
import logging
from datetime import datetime
from typing import Any

import pydgraph

from ami.core.exceptions import StorageError, StorageValidationError

logger = logging.getLogger(__name__)

# Maximum allowed length for collection names
MAX_COLLECTION_NAME_LENGTH = 64


def _validate_collection_name(collection_name: str) -> None:
    """Validate collection name to prevent injection.

    Raises:
        StorageValidationError: If collection name contains invalid characters
    """
    if not isinstance(collection_name, str):
        msg = f"Collection name must be a string, got {type(collection_name).__name__}"
        raise StorageValidationError(msg)
    if not collection_name:
        msg = "Collection name cannot be empty"
        raise StorageValidationError(msg)
    if not collection_name.replace("_", "").isalnum():
        msg = (
            f"Invalid collection name '{collection_name}':"
            " only alphanumeric characters"
            " and underscores allowed"
        )
        raise StorageValidationError(msg)
    if collection_name.startswith("_") or collection_name.endswith("_"):
        msg = (
            f"Invalid collection name '{collection_name}':"
            " cannot start or end with underscore"
        )
        raise StorageValidationError(msg)
    if "__" in collection_name:
        msg = (
            f"Invalid collection name '{collection_name}':"
            " cannot contain consecutive underscores"
        )
        raise StorageValidationError(msg)
    if len(collection_name) > MAX_COLLECTION_NAME_LENGTH:
        msg = (
            f"Collection name too long:"
            f" {len(collection_name)} characters"
            f" (max {MAX_COLLECTION_NAME_LENGTH})"
        )
        raise StorageValidationError(msg)


def _validate_item_id(item_id: str) -> None:
    """Validate item ID format."""
    if not isinstance(item_id, str):
        msg = f"Item ID must be a string, got {type(item_id).__name__}"
        raise StorageValidationError(msg)
    if not item_id:
        msg = "Item ID cannot be empty"
        raise StorageValidationError(msg)


def _parse_uid_response(response_json: str, item_id: str) -> str:
    """Parse UID from Dgraph response.

    Raises:
        StorageError: If parsing fails or UID not found
    """
    try:
        result = json.loads(response_json)
    except json.JSONDecodeError as e:
        msg = f"Failed to parse Dgraph response: {e}"
        raise StorageError(msg) from e

    if not isinstance(result, dict):
        msg = f"Invalid Dgraph response: expected dict, got {type(result).__name__}"
        raise StorageError(msg)
    nodes = result.get("node")
    if not nodes:
        msg = f"Node not found: {item_id}"
        raise StorageError(msg)
    if not isinstance(nodes, list):
        msg = (
            f"Invalid Dgraph response: 'node' must be list, got {type(nodes).__name__}"
        )
        raise StorageError(msg)
    if len(nodes) == 0:
        msg = f"Node not found: {item_id}"
        raise StorageError(msg)

    uid_value = nodes[0].get("uid")
    if not uid_value:
        msg = f"Node found but UID missing for item: {item_id}"
        raise StorageError(msg)
    if not isinstance(uid_value, str):
        msg = f"Invalid UID type: expected str, got {type(uid_value).__name__}"
        raise StorageError(msg)
    return uid_value


def _get_actual_uid(dao: Any, item_id: str) -> str:
    """Get actual Dgraph UID from item ID.

    Raises:
        StorageError: If query execution fails or node not found
        StorageValidationError: If collection_name or item_id is invalid
    """
    _validate_item_id(item_id)

    if item_id.startswith("0x"):
        return item_id

    _validate_collection_name(dao.collection_name)

    # SECURITY: collection_name validated above (alphanumeric + underscore only).
    # User-provided item_id is parameterized via $item_id variable.
    collection_name = dao.collection_name
    cn = collection_name
    query = (
        "query find_node($item_id: string) {\n"
        f"    node(func: eq({cn}.app_uid, $item_id))"
        f" @filter(type({cn})) {{\n"
        "        uid\n"
        "    }\n"
        "}"
    )

    txn = dao.client.txn(read_only=True)
    try:
        try:
            response = txn.query(query, variables={"$item_id": item_id})
        except Exception as e:
            msg = f"Failed to execute Dgraph query: {e}"
            raise StorageError(msg) from e
        return _parse_uid_response(response.json, item_id)
    finally:
        txn.discard()


def _validate_uid_str(actual_uid: str) -> None:
    """Validate that actual_uid is a non-empty string."""
    if not isinstance(actual_uid, str):
        msg = f"UID must be a string, got {type(actual_uid).__name__}"
        raise StorageValidationError(msg)
    if not actual_uid:
        msg = "UID cannot be empty"
        raise StorageValidationError(msg)


def _validate_field_key(key: str) -> None:
    """Validate a field key string."""
    if not isinstance(key, str):
        msg = f"Field key must be a string, got {type(key).__name__}"
        raise StorageValidationError(msg)
    if not key.replace("_", "").replace(".", "").isalnum():
        msg = (
            f"Invalid field name '{key}':"
            " only alphanumeric, underscore,"
            " and dot allowed"
        )
        raise StorageValidationError(msg)


def _prepare_delete_data(
    dao: Any, actual_uid: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Prepare delete mutation data."""
    _validate_collection_name(dao.collection_name)
    _validate_uid_str(actual_uid)

    collection_name = dao.collection_name
    delete_data: dict[str, Any] = {"uid": actual_uid}
    for key in data:
        _validate_field_key(key)
        if key != "id":
            delete_data[f"{collection_name}.{key}"] = None
    return delete_data


def _prepare_update_data(
    dao: Any, actual_uid: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Prepare update mutation data."""
    _validate_collection_name(dao.collection_name)
    _validate_uid_str(actual_uid)

    collection_name = dao.collection_name
    update_data = {"uid": actual_uid}

    for key, value in data.items():
        _validate_field_key(key)
        if key == "uid":
            update_data[f"{collection_name}.app_uid"] = value
        elif key != "id" and value is not None:
            update_data[f"{collection_name}.{key}"] = _format_value(key, value)
    return update_data


def _format_value(key: str, value: Any) -> Any:
    """Format value for Dgraph storage."""
    if isinstance(value, list | dict):
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            msg = f"Failed to serialize {key} to JSON: {e}"
            raise StorageError(msg) from e

    if isinstance(value, datetime):
        try:
            return value.isoformat()
        except (ValueError, AttributeError) as e:
            msg = f"Failed to convert datetime to ISO format for {key}: {e}"
            raise StorageValidationError(msg) from e
    if isinstance(value, str) and key in [
        "created_at",
        "updated_at",
        "verified_at",
        "last_login",
    ]:
        try:
            dt = datetime.fromisoformat(value.replace(" ", "T"))
            return dt.isoformat()
        except (ValueError, AttributeError) as e:
            msg = f"Invalid datetime format for {key}: {value}"
            raise StorageValidationError(msg) from e

    return value


async def update(dao: Any, item_id: str, data: dict[str, Any]) -> None:
    """Update node in Dgraph.

    Raises:
        StorageError: If not connected, node not found, or update fails
        StorageValidationError: If data validation fails
    """
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    actual_uid = _get_actual_uid(dao, item_id)

    txn = dao.client.txn()
    try:
        delete_data = _prepare_delete_data(dao, actual_uid, data)
        try:
            delete_json = json.dumps(delete_data, default=str).encode()
        except (TypeError, ValueError) as e:
            msg = f"Failed to serialize delete data to JSON: {e}"
            raise StorageError(msg) from e
        try:
            del_mutation = pydgraph.Mutation(delete_json=delete_json)
            txn.mutate(del_mutation)
        except Exception as e:
            msg = f"Failed to execute delete mutation: {e}"
            raise StorageError(msg) from e

        update_data = _prepare_update_data(dao, actual_uid, data)
        try:
            update_json = json.dumps(update_data, default=str).encode()
        except (TypeError, ValueError) as e:
            msg = f"Failed to serialize update data to JSON: {e}"
            raise StorageError(msg) from e
        try:
            mutation = pydgraph.Mutation(set_json=update_json)
            txn.mutate(mutation)
        except Exception as e:
            msg = f"Failed to execute set mutation: {e}"
            raise StorageError(msg) from e
        try:
            txn.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise StorageError(msg) from e

    except (StorageValidationError, StorageError):
        txn.discard()
        raise
    except Exception as e:
        txn.discard()
        msg = f"Failed to update in Dgraph: {e}"
        raise StorageError(msg) from e
    finally:
        txn.discard()


async def bulk_update(dao: Any, updates: list[dict[str, Any]]) -> None:
    """Bulk update nodes.

    Raises:
        StorageError: If not connected or any update fails
        StorageValidationError: If any data validation fails
    """
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    failed_items: list[tuple[str, Exception]] = []

    for idx, update_item in enumerate(updates):
        uid = update_item.get("uid")
        if uid is None:
            uid = update_item.get("id")
        if not uid:
            msg = f"Update item at index {idx} is missing required 'uid' or 'id' field"
            raise StorageValidationError(msg)
        try:
            await update(dao, uid, update_item)
        except (StorageError, StorageValidationError) as e:
            failed_items.append((uid, e))

    if failed_items:
        error_details = "; ".join(f"{uid}: {e}" for uid, e in failed_items)
        msg = (
            f"Bulk update failed for"
            f" {len(failed_items)}/{len(updates)}"
            f" items: {error_details}"
        )
        raise StorageError(msg)


def _validate_raw_query(query: str) -> None:
    """Validate raw query format."""
    if not isinstance(query, str):
        msg = f"Query must be a string, got {type(query).__name__}"
        raise StorageValidationError(msg)
    if not query.strip():
        msg = "Query cannot be empty"
        raise StorageValidationError(msg)


def _encode_query(query: str) -> bytes:
    """Encode query string to bytes."""
    try:
        return query.encode("utf-8")
    except UnicodeEncodeError as e:
        msg = f"Failed to encode query string: {e}"
        raise StorageValidationError(msg) from e
    except AttributeError as e:
        msg = f"Query must be a string with encode method: {e}"
        raise StorageValidationError(msg) from e
    except Exception as e:
        msg = f"Unexpected error encoding query: {e}"
        raise StorageValidationError(msg) from e


def _extract_mutation_count(response: Any) -> int:
    """Extract count of affected nodes from mutation response.

    NOTE: Called ONLY after successful transaction commit. The count is
    purely informational about a completed operation.
    """
    if not hasattr(response, "uids"):
        msg = "Invalid mutation response: missing 'uids' attribute"
        raise StorageError(msg)
    if response.uids is None or not response.uids:
        return 0
    if not isinstance(response.uids, dict):
        msg = (
            "Invalid mutation response: 'uids' is"
            f" {type(response.uids).__name__},"
            " expected dict"
        )
        raise StorageError(msg)
    try:
        count_val = len(response.uids)
    except TypeError as e:
        msg = f"Failed to count affected nodes: {e}"
        raise StorageError(msg) from e
    if not isinstance(count_val, int):
        msg = f"Count must be int, got {type(count_val).__name__}"
        raise StorageError(msg)
    if count_val < 0:
        msg = f"Invalid negative count: {count_val}"
        raise StorageError(msg)
    return count_val


async def raw_write_query(
    dao: Any, query: str, _params: dict[str, Any] | None = None
) -> int:
    """Execute raw mutation. Returns count after successful commit.

    Raises:
        StorageError: If not connected, mutation/commit fails, or bad response
        StorageValidationError: If query encoding fails
    """
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    _validate_raw_query(query)
    query_bytes = _encode_query(query)

    txn = dao.client.txn()
    try:
        try:
            mutation = pydgraph.Mutation(set_nquads=query_bytes)
            response = txn.mutate(mutation)
        except Exception as e:
            msg = f"Failed to execute mutation: {e}"
            raise StorageError(msg) from e
        try:
            txn.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise StorageError(msg) from e
        return _extract_mutation_count(response)

    except (StorageError, StorageValidationError):
        txn.discard()
        raise
    except Exception as e:
        txn.discard()
        msg = f"Failed to execute mutation: {e}"
        raise StorageError(msg) from e
    finally:
        txn.discard()
