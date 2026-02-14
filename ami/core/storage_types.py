"""Storage type definitions and enumerations."""

from enum import Enum


class StorageType(Enum):
    """Supported storage backend types."""

    RELATIONAL = "postgres"
    DOCUMENT = "mongodb"
    TIMESERIES = "prometheus"
    VECTOR = "pgvector"
    GRAPH = "dgraph"
    INMEM = "redis"
    FILE = "file"
    REST = "rest"
    VAULT = "vault"


class OperationType(Enum):
    """CRUD operation types."""

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    QUERY = "query"


class SyncStrategy(Enum):
    """Synchronization strategies for multi-storage operations."""

    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    PRIMARY_FIRST = "primary_first"
    EVENTUAL = "eventual"
