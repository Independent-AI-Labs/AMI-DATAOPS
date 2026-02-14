"""Delete operations for Dgraph DAO."""

import json
import logging
from typing import Any

import pydgraph

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_update import (
    _validate_collection_name,
)

logger = logging.getLogger(__name__)


async def delete(dao: Any, item_id: str) -> bool:
    """Delete node from Dgraph."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Validate collection name to prevent DQL injection
    _validate_collection_name(dao.collection_name)

    # Check if item_id is a Dgraph UID (starts with 0x) or a regular UUID
    actual_uid = item_id
    if not item_id.startswith("0x"):
        # It's an application UID, need to find the Dgraph UID
        # Use parameterized query to prevent DQL injection
        collection_name = dao.collection_name  # Already validated above
        query = (
            "query find_node($item_id: string) {\n"
            f"    node(func: eq({collection_name}.app_uid, "
            f"$item_id)) @filter(type({collection_name})) {{\n"
            "        uid\n"
            "    }\n"
            "}"
        )

        txn = dao.client.txn(read_only=True)
        try:
            response = txn.query(query, variables={"$item_id": item_id})
            result = json.loads(response.json)
            if result.get("node") and len(result["node"]) > 0:
                actual_uid = result["node"][0]["uid"]
            else:
                # Node not found
                return False
        except StorageError:
            raise
        except Exception as e:
            msg = f"Failed to query Dgraph for item {item_id}: {e}"
            raise StorageError(msg) from e
        finally:
            txn.discard()

    txn = dao.client.txn()
    try:
        # Delete mutation using delete_json
        mutation = pydgraph.Mutation(
            delete_json=json.dumps([{"uid": actual_uid}]).encode()
        )

        # Execute mutation and commit
        txn.mutate(mutation)
        txn.commit()

    except Exception as e:
        msg = f"Failed to delete from Dgraph: {e}"
        raise StorageError(msg) from e
    else:
        return True
    finally:
        txn.discard()


async def bulk_delete(dao: Any, ids: list[str]) -> dict[str, Any]:
    """Bulk delete nodes.

    Returns:
        Dictionary containing:
        - success_count: Number of successfully deleted items
        - failed_ids: List of IDs that failed to delete
        - total: Total number of IDs processed
    """
    success_count = 0
    failed_ids = []

    for uid in ids:
        try:
            success = await delete(dao, uid)
            if success:
                success_count += 1
            else:
                failed_ids.append(uid)
        except Exception:
            logger.exception("Failed to delete item %s", uid)
            failed_ids.append(uid)

    return {
        "success_count": success_count,
        "failed_ids": failed_ids,
        "total": len(ids),
    }
