"""Tests for the exception hierarchy."""

import pydantic
import pytest

from ami.core.exceptions import (
    ConfigurationError,
    DuplicateError,
    NotFoundError,
    QueryError,
    StorageConnectionError,
    StorageError,
    StorageValidationError,
    TransactionError,
)


class TestExceptionHierarchy:
    """Verify exception hierarchy and no Pydantic shadow."""

    def test_all_inherit_from_storage_error(self) -> None:
        for exc_cls in (
            StorageConnectionError,
            NotFoundError,
            DuplicateError,
            StorageValidationError,
            QueryError,
            TransactionError,
            ConfigurationError,
        ):
            assert issubclass(exc_cls, StorageError)

    def test_storage_error_inherits_from_exception(self) -> None:
        assert issubclass(StorageError, Exception)

    def test_no_pydantic_shadow(self) -> None:
        """StorageValidationError must NOT shadow pydantic.ValidationError."""
        assert StorageValidationError is not pydantic.ValidationError
        assert not issubclass(
            StorageValidationError,
            pydantic.ValidationError,
        )

    def test_can_catch_storage_validation_error(self) -> None:
        with_msg = StorageValidationError("bad data")
        assert str(with_msg) == "bad data"
        assert isinstance(with_msg, StorageError)

    def test_exceptions_are_raisable(self) -> None:
        msg = "test message"
        for exc_cls in (
            StorageError,
            StorageConnectionError,
            NotFoundError,
            DuplicateError,
            StorageValidationError,
            QueryError,
            TransactionError,
            ConfigurationError,
        ):
            with pytest.raises(StorageError, match=msg):
                raise exc_cls(msg)
