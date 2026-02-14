"""Custom exceptions for the storage framework."""


class StorageError(Exception):
    """Base exception for storage-related errors."""


class StorageConnectionError(StorageError):
    """Raised when connection to storage backend fails."""


class NotFoundError(StorageError):
    """Raised when a requested resource is not found."""


class DuplicateError(StorageError):
    """Raised when attempting to create a duplicate resource."""


class StorageValidationError(StorageError):
    """Raised when data validation fails.

    Named StorageValidationError to avoid shadowing pydantic.ValidationError.
    """


class QueryError(StorageError):
    """Raised when a query operation fails."""


class TransactionError(StorageError):
    """Raised when a transaction fails."""


class ConfigurationError(StorageError):
    """Raised when storage configuration is invalid."""
