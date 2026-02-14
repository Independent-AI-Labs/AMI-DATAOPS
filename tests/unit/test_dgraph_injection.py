"""Tests verifying DQL injection prevention in Dgraph operations.

The original code interpolated user-provided values directly into DQL
query strings. The fix uses parameterized queries and validates
collection names and item IDs.
"""

import sys
import types
from typing import Any, cast

import pytest

# pydgraph is imported at module level in dgraph_update but the
# validation helpers we test here don't use it.  Provide a stub so the
# module can be imported even if pydgraph has protobuf conflicts.
if "pydgraph" not in sys.modules:
    sys.modules["pydgraph"] = types.ModuleType("pydgraph")

from ami.core.exceptions import StorageValidationError
from ami.implementations.graph.dgraph_update import (
    _validate_collection_name,
    _validate_item_id,
)


class TestCollectionNameValidation:
    """Test _validate_collection_name rejects injection payloads."""

    def test_valid_names(self) -> None:
        _validate_collection_name("users")
        _validate_collection_name("my_model")
        _validate_collection_name("Users123")
        _validate_collection_name("A")

    def test_rejects_empty(self) -> None:
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            _validate_collection_name("")

    def test_rejects_non_string(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="must be a string",
        ):
            _validate_collection_name(cast(Any, 123))

    def test_rejects_special_chars(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="alphanumeric",
        ):
            _validate_collection_name("users; DROP ALL;")

    def test_rejects_dots(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="alphanumeric",
        ):
            _validate_collection_name("schema.type")

    def test_rejects_parens(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="alphanumeric",
        ):
            _validate_collection_name("func()")

    def test_rejects_leading_underscore(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="cannot start",
        ):
            _validate_collection_name("_private")

    def test_rejects_trailing_underscore(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="cannot start or end",
        ):
            _validate_collection_name("bad_")

    def test_rejects_consecutive_underscores(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="consecutive",
        ):
            _validate_collection_name("bad__name")

    def test_rejects_too_long(self) -> None:
        with pytest.raises(StorageValidationError, match="too long"):
            _validate_collection_name("a" * 65)


class TestItemIdValidation:
    """Test _validate_item_id rejects injection payloads."""

    def test_valid_ids(self) -> None:
        _validate_item_id("0x123abc")
        _validate_item_id("some-uuid-value")
        _validate_item_id("abc123")

    def test_rejects_empty(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="cannot be empty",
        ):
            _validate_item_id("")

    def test_rejects_non_string(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="must be a string",
        ):
            _validate_item_id(cast(Any, 42))
