"""Create operations for Dgraph DAO."""

import json
import logging
from typing import Any

import pydgraph

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_util import (
    ensure_schema,
    json_encoder,
    to_dgraph_format,
)
from ami.models.base_model import StorageModel

logger = logging.getLogger(__name__)


async def create(dao: Any, instance: StorageModel) -> str:
    """Create new node in Dgraph."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Store the application's uid before we send to Dgraph
    app_uid = instance.uid
    if not app_uid:
        msg = "Instance must have a uid before creation"
        raise StorageError(msg)

    txn = dao.client.txn()
    try:
        # Convert to Dgraph format
        dgraph_data = to_dgraph_format(instance, dao.collection_name)

        # Add type
        dgraph_data["dgraph.type"] = dao.collection_name

        # Create mutation with a blank node
        dgraph_data["uid"] = "_:blank-0"

        logger.debug("Creating node with data: %s", dgraph_data)
        mutation = pydgraph.Mutation(
            set_json=json.dumps(dgraph_data, default=json_encoder).encode()
        )

        # Commit (not async - pydgraph uses sync methods)
        response = txn.mutate(mutation)
        txn.commit()

        # Get Dgraph's internal UID
        dgraph_uid = response.uids.get("blank-0")

    except Exception as e:
        txn.discard()
        msg = f"Failed to create in Dgraph: {e}"
        raise StorageError(msg) from e
    else:
        if not dgraph_uid:
            logger.error(
                "No UID returned. Response UIDs: %s",
                response.uids,
            )
            msg = "Failed to get UID from Dgraph"
            raise StorageError(msg)
        # Return the application uid, not Dgraph's internal uid
        return app_uid


async def bulk_create(dao: Any, instances: list[StorageModel]) -> list[str]:
    """Bulk create nodes."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Store the application uids before we send to Dgraph
    app_uids = []
    for instance in instances:
        if not instance.uid:
            msg = "All instances must have a uid before creation"
            raise StorageError(msg)
        app_uids.append(instance.uid)

    txn = dao.client.txn()
    try:
        # Prepare data
        nodes = []
        for i, instance in enumerate(instances):
            dgraph_data = to_dgraph_format(instance, dao.collection_name)
            dgraph_data["dgraph.type"] = dao.collection_name
            dgraph_data["uid"] = f"_:blank-{i}"
            nodes.append(dgraph_data)

        # Create mutation
        mutation = pydgraph.Mutation(set_json=json.dumps(nodes).encode())

        # Commit
        response = txn.mutate(mutation)
        txn.commit()

        # Verify all nodes were created
        for i in range(len(instances)):
            dgraph_uid = response.uids.get(f"blank-{i}")
            if not dgraph_uid:
                logger.warning("No UID returned for node %d", i)

    except Exception as e:
        txn.discard()
        msg = f"Failed to bulk create in Dgraph: {e}"
        raise StorageError(msg) from e
    else:
        # Return the application uids, not Dgraph's internal uids
        return app_uids


async def create_indexes(dao: Any) -> None:
    """Indexes are created with schema in Dgraph."""
    metadata = (
        dao.model_cls.get_metadata() if hasattr(dao.model_cls, "get_metadata") else None
    )
    ensure_schema(dao.client, dao.model_cls, metadata, dao.collection_name)
