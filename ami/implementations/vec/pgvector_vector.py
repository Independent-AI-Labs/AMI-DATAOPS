"""PgVector vector-similarity search operations.

Provides cosine, L2 (Euclidean), and inner-product similarity searches
backed by the ``pgvector`` extension.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from ami.implementations.vec.pgvector_util import (
    build_where_clause,
    deserialize_row,
    get_safe_table_name,
)

if TYPE_CHECKING:
    from ami.implementations.vec.pgvector_dao import PgVectorDAO

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Public search APIs
# ------------------------------------------------------------------


async def similarity_search(
    dao: PgVectorDAO,
    query_text: str,
    *,
    limit: int = 10,
    filters: dict[str, Any] | None = None,
    metric: str = "cosine",
) -> list[dict[str, Any]]:
    """Search records by semantic similarity to *query_text*.

    Parameters
    ----------
    query_text:
        Natural-language query to embed and compare.
    limit:
        Maximum number of results.
    filters:
        Optional column filters to narrow the search.
    metric:
        Distance metric — ``"cosine"`` (default), ``"l2"``, or ``"ip"``.
    """
    embedding = await dao._get_query_embedding(query_text)
    return await similarity_search_by_vector(
        dao,
        embedding,
        limit=limit,
        filters=filters,
        metric=metric,
    )


async def similarity_search_by_vector(
    dao: PgVectorDAO,
    embedding: list[float],
    *,
    limit: int = 10,
    filters: dict[str, Any] | None = None,
    metric: str = "cosine",
) -> list[dict[str, Any]]:
    """Search by a pre-computed *embedding* vector."""
    table = get_safe_table_name(dao.collection_name)
    operator = _metric_operator(metric)
    vec_literal = _to_vector_literal(embedding)

    # Build optional WHERE clause
    where_sql = ""
    params: list[Any] = []
    if filters:
        where_fragment, params = build_where_clause(filters)
        if where_fragment:
            where_sql = f"WHERE {where_fragment}"

    # Distance is the first param after any filter params
    distance_param_idx = len(params) + 1
    params.append(vec_literal)

    sql = (
        f"SELECT *, (embedding {operator} ${distance_param_idx}::vector)"
        f" AS distance "
        f"FROM {table} {where_sql} "
        f"ORDER BY embedding {operator} ${distance_param_idx}::vector "
        f"LIMIT {limit}"
    )

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    results: list[dict[str, Any]] = []
    for row in rows:
        data = deserialize_row(dict(row))
        distance = data.pop("distance", None)
        raw_embedding = data.pop("embedding", None)

        # Convert distance to a similarity score
        score: float
        if metric == "cosine":
            score = 1.0 - float(distance) if distance is not None else 0.0
        elif metric == "ip":
            score = -float(distance) if distance is not None else 0.0
        else:
            score = float(distance) if distance is not None else 0.0

        results.append(
            {
                "data": data,
                "score": score,
                "distance": float(distance) if distance is not None else None,
                "embedding": _parse_embedding(raw_embedding),
            }
        )

    logger.debug(
        "Similarity search on %s returned %d results (metric=%s)",
        table,
        len(results),
        metric,
    )
    return results


# ------------------------------------------------------------------
# Fetch stored embedding
# ------------------------------------------------------------------


async def fetch_embedding(
    dao: PgVectorDAO,
    item_id: str,
) -> list[float] | None:
    """Retrieve the stored embedding for *item_id*."""
    table = get_safe_table_name(dao.collection_name)
    sql = f"SELECT embedding FROM {table} WHERE uid = $1"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        row = await conn.fetchrow(sql, item_id)

    if row is None or row["embedding"] is None:
        return None
    return _parse_embedding(row["embedding"])


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _metric_operator(metric: str) -> str:
    """Return the pgvector distance operator for *metric*."""
    operators = {
        "cosine": "<=>",
        "l2": "<->",
        "ip": "<#>",
    }
    op = operators.get(metric)
    if op is None:
        msg = f"Unsupported metric: {metric} (use cosine, l2, or ip)"
        raise ValueError(msg)
    return op


def _to_vector_literal(embedding: list[float]) -> str:
    """Convert a Python list of floats to a pgvector literal string."""
    return "[{}]".format(",".join(str(v) for v in embedding))


def _parse_embedding(raw: Any) -> list[float] | None:
    """Parse an embedding value from various pgvector return types."""
    if raw is None:
        return None

    # list or tuple — already usable
    if isinstance(raw, list | tuple):
        return [float(v) for v in raw]

    # memoryview (returned by some asyncpg + pgvector combos)
    if isinstance(raw, memoryview):
        return [float(v) for v in bytes(raw)]

    # string representation e.g. "[0.1,0.2,0.3]"
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [float(v) for v in parsed]
        except (json.JSONDecodeError, TypeError):
            pass

    logger.warning("Unable to parse embedding of type %s", type(raw).__name__)
    return None
