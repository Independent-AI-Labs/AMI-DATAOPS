"""Tests for DataOps decorators."""

import asyncio
from datetime import datetime
from typing import Any, ClassVar

import pytest

from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import SecurityContext
from ami.models.storage_config import StorageConfig
from ami.secrets.client import (
    InMemorySecretsBackend,
    SecretsBrokerClient,
    reset_secrets_broker_client,
    set_secrets_broker_client,
)
from ami.secrets.pointer import VaultFieldPointer
from ami.services.decorators import (
    EventRecord,
    cached_result,
    record_event,
    sanitize_for_mcp,
    sensitive_field,
)

TEST_PASSWORD = "secret123"
TEST_API_KEY = "key_abc123"

EXPECTED_ENSURE_CALLS = 2
EXPECTED_DOUBLE_VALUE = 10
EXPECTED_TWO_CALLS = 2
EXPECTED_THREE_CALLS = 3


@sensitive_field("password", mask_pattern="pwd_masked")
@sensitive_field("api_key", mask_pattern="{field}_hidden")
class SampleUser(StorageModel):
    """Test user model with sensitive fields."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_users",
        storage_configs={
            "memory": StorageConfig(storage_type=StorageType.INMEM),
        },
    )

    username: str
    password: str
    api_key: str = "secret_key_123"
    email: str = "test@example.com"


class TestDecorators:
    """Test decorator functionality."""

    @pytest.fixture
    def security_context(self) -> SecurityContext:
        return SecurityContext(user_id="test_user", roles=["member"])

    def test_sensitive_field_decorator(self) -> None:
        assert hasattr(SampleUser, "_sensitive_fields")
        assert "password" in SampleUser._sensitive_fields
        assert "api_key" in SampleUser._sensitive_fields

        password_config = SampleUser._sensitive_fields["password"]
        api_key_config = SampleUser._sensitive_fields["api_key"]

        assert password_config.mask_pattern == "pwd_masked"
        assert api_key_config.mask_pattern == "{field}_hidden"

    def test_sanitize_for_mcp(self) -> None:
        user = SampleUser(
            username="john",
            password=TEST_PASSWORD,
            api_key=TEST_API_KEY,
        )
        sanitized = sanitize_for_mcp(user, caller="mcp")
        assert sanitized["username"] == "john"
        assert sanitized["password"] == "pwd_masked"
        assert "api_key_hidden" in sanitized["api_key"]
        assert sanitized["email"] == "test@example.com"

    def test_sensitive_field_storage_and_hydration(self) -> None:
        class CountingBackend(InMemorySecretsBackend):
            def __init__(self) -> None:
                super().__init__()
                self.ensure_calls = 0

            def ensure_secret(
                self,
                *,
                namespace: str,
                model: str,
                field: str,
                value: str,
                classification: Any | None = None,
            ) -> VaultFieldPointer:
                self.ensure_calls += 1
                return super().ensure_secret(
                    namespace=namespace,
                    model=model,
                    field=field,
                    value=value,
                    classification=classification,
                )

        backend = CountingBackend()
        client = SecretsBrokerClient(backend=backend)
        set_secrets_broker_client(client)
        try:
            user = SampleUser(
                username="john",
                password=TEST_PASSWORD,
                api_key=TEST_API_KEY,
            )
            payload = user.to_storage_dict()

            pw_pointer = payload["password"]
            ak_pointer = payload["api_key"]
            assert isinstance(pw_pointer, dict)
            assert isinstance(ak_pointer, dict)
            assert "vault_reference" in pw_pointer
            assert "integrity_hash" in pw_pointer
            assert backend.ensure_calls == EXPECTED_ENSURE_CALLS

            user.to_storage_dict()
            assert backend.ensure_calls == EXPECTED_ENSURE_CALLS

            hydrated = SampleUser(**payload)
            assert hydrated.password == TEST_PASSWORD
            assert hydrated.api_key == TEST_API_KEY
        finally:
            reset_secrets_broker_client()

    @pytest.mark.asyncio
    async def test_record_event_decorator(
        self,
        security_context: SecurityContext,
    ) -> None:
        @record_event("TestEvent", capture_output=True)
        async def test_function(
            username: str,
            password: str,
            data: dict[str, Any],
        ) -> dict[str, Any]:
            return {"result": "success", "user": username}

        result = await test_function(
            username="alice",
            password="secret",
            data={"key": "value"},
        )
        assert result["result"] == "success"
        assert result["user"] == "alice"

    @pytest.mark.asyncio
    async def test_record_event_with_error(self) -> None:
        @record_event("ErrorEvent", capture_errors=True)
        async def failing_function(value: int) -> int:
            if value < 0:
                msg = "Negative value not allowed"
                raise ValueError(msg)
            return value * 2

        result = await failing_function(5)
        assert result == EXPECTED_DOUBLE_VALUE

        with pytest.raises(ValueError, match="Negative value"):
            await failing_function(-1)

    @pytest.mark.asyncio
    async def test_cached_result_decorator(self) -> None:
        call_count = 0

        @cached_result(ttl=1, backend="memory")
        async def expensive_operation(
            user_id: str,
        ) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return {"user_id": user_id, "data": "expensive_result"}

        result1 = await expensive_operation("user_123")
        assert result1["user_id"] == "user_123"
        assert call_count == 1

        result2 = await expensive_operation("user_123")
        assert result2["user_id"] == "user_123"
        assert call_count == 1

        result3 = await expensive_operation("user_456")
        assert result3["user_id"] == "user_456"
        assert call_count == EXPECTED_TWO_CALLS

        await asyncio.sleep(1.1)

        result4 = await expensive_operation("user_123")
        assert result4["user_id"] == "user_123"
        assert call_count == EXPECTED_THREE_CALLS

    def test_event_record_model(self) -> None:
        event = EventRecord(
            event_type="TestEvent",
            function_name="test_func",
            input={"arg1": "value1", "arg2": 42},
            output={"result": "success"},
            success=True,
        )
        assert event.event_type == "TestEvent"
        assert event.function_name == "test_func"
        assert event.input["arg1"] == "value1"
        assert event.output["result"] == "success"
        assert event.success is True
        assert event.event_id.startswith("event_")
        assert isinstance(event.start_time, datetime)

    def test_event_record_with_error(self) -> None:
        event = EventRecord(
            event_type="ErrorEvent",
            function_name="failing_func",
            input={"bad_input": "value"},
            success=False,
            error="Something went wrong",
            error_type="ValueError",
        )
        assert event.success is False
        assert event.error == "Something went wrong"
        assert event.error_type == "ValueError"
