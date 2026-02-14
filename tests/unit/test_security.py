"""Tests for the DataOps security model."""

from datetime import UTC, datetime, timedelta
from typing import ClassVar

import pytest

from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import (
    ACLEntry,
    Permission,
    SecurityContext,
)
from ami.models.storage_config import StorageConfig

EXPECTED_TEST_VALUE = 42
EXPECTED_UUID_LENGTH = 36


class SampleModel(StorageModel):
    """Test model for security tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_models",
        storage_configs={
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
        },
    )

    name: str
    value: int = 0


class TestSecurityModel:
    """Test security features."""

    @pytest.fixture
    def security_context(self) -> SecurityContext:
        return SecurityContext(
            user_id="user_123",
            roles=["member"],
            groups=["developers"],
        )

    @pytest.fixture
    def admin_context(self) -> SecurityContext:
        return SecurityContext(
            user_id="admin_user",
            roles=["admin"],
            groups=["administrators"],
        )

    def test_security_context_creation(
        self,
        security_context: SecurityContext,
    ) -> None:
        assert security_context.user_id == "user_123"
        assert "member" in security_context.roles
        assert "developers" in security_context.groups

        principal_ids = security_context.principal_ids
        assert "user_123" in principal_ids
        assert "member" in principal_ids
        assert "developers" in principal_ids

    def test_acl_entry_creation(self) -> None:
        acl = ACLEntry(
            principal_id="user_123",
            principal_type="user",
            permissions=[Permission.READ, Permission.WRITE],
            granted_by="admin",
        )
        assert acl.principal_id == "user_123"
        assert Permission.READ in acl.permissions
        assert Permission.WRITE in acl.permissions
        assert acl.granted_by == "admin"
        assert acl.has_permission(Permission.READ)
        assert acl.has_permission(Permission.WRITE)
        assert not acl.has_permission(Permission.DELETE)

    def test_acl_entry_with_admin(self) -> None:
        acl = ACLEntry(
            principal_id="admin_user",
            principal_type="user",
            permissions=[Permission.ADMIN],
            granted_by="system",
        )
        assert acl.has_permission(Permission.READ)
        assert acl.has_permission(Permission.WRITE)
        assert acl.has_permission(Permission.DELETE)
        assert acl.has_permission(Permission.ADMIN)

    def test_acl_entry_expiration(self) -> None:
        expired_acl = ACLEntry(
            principal_id="user_123",
            principal_type="user",
            permissions=[Permission.READ],
            expires_at=datetime.now(UTC) - timedelta(hours=1),
        )
        valid_acl = ACLEntry(
            principal_id="user_123",
            principal_type="user",
            permissions=[Permission.READ],
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
        assert expired_acl.is_expired()
        assert not valid_acl.is_expired()

    @pytest.mark.asyncio
    async def test_secured_model_creation(
        self,
        security_context: SecurityContext,
    ) -> None:
        model = SampleModel(
            name="test",
            value=EXPECTED_TEST_VALUE,
        )
        model.owner_id = security_context.user_id
        model.created_by = security_context.user_id

        assert model.name == "test"
        assert model.value == EXPECTED_TEST_VALUE
        assert model.owner_id == "user_123"
        assert model.created_by == "user_123"
        assert model.acl == []

    @pytest.mark.asyncio
    async def test_check_permission_owner(
        self,
        security_context: SecurityContext,
    ) -> None:
        model = SampleModel(name="test")
        model.owner_id = security_context.user_id

        assert await model.check_permission(
            security_context,
            Permission.READ,
        )
        assert await model.check_permission(
            security_context,
            Permission.WRITE,
        )
        assert await model.check_permission(
            security_context,
            Permission.DELETE,
        )

    @pytest.mark.asyncio
    async def test_check_permission_with_acl(
        self,
        security_context: SecurityContext,
    ) -> None:
        model = SampleModel(name="test")
        model.owner_id = "other_user"
        model.acl = [
            ACLEntry(
                principal_id="user_123",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="owner",
            ),
        ]

        assert await model.check_permission(
            security_context,
            Permission.READ,
        )
        assert not await model.check_permission(
            security_context,
            Permission.WRITE,
            raise_on_deny=False,
        )

    @pytest.mark.asyncio
    async def test_check_permission_with_role(
        self,
        security_context: SecurityContext,
    ) -> None:
        model = SampleModel(name="test")
        model.owner_id = "other_user"
        model.acl = [
            ACLEntry(
                principal_id="member",
                principal_type="role",
                permissions=[Permission.READ, Permission.WRITE],
                granted_by="owner",
            ),
        ]

        assert await model.check_permission(
            security_context,
            Permission.READ,
        )
        assert await model.check_permission(
            security_context,
            Permission.WRITE,
        )
        assert not await model.check_permission(
            security_context,
            Permission.DELETE,
            raise_on_deny=False,
        )

    @pytest.mark.asyncio
    async def test_check_permission_with_group(
        self,
        security_context: SecurityContext,
    ) -> None:
        model = SampleModel(name="test")
        model.owner_id = "other_user"
        model.acl = [
            ACLEntry(
                principal_id="developers",
                principal_type="group",
                permissions=[Permission.READ],
                granted_by="owner",
            ),
        ]

        assert await model.check_permission(
            security_context,
            Permission.READ,
        )
        assert not await model.check_permission(
            security_context,
            Permission.WRITE,
            raise_on_deny=False,
        )


class TestStorageModel:
    """Test base storage model."""

    def test_model_id_generation(self) -> None:
        model = SampleModel(name="test")
        assert model.uid is not None
        assert len(model.uid) == EXPECTED_UUID_LENGTH

    def test_model_timestamps(self) -> None:
        model = SampleModel(name="test")
        assert model.created_at is not None
        assert model.updated_at is not None
        assert isinstance(model.created_at, datetime)
        assert isinstance(model.updated_at, datetime)

    def test_model_metadata(self) -> None:
        metadata = SampleModel.get_metadata()
        assert metadata.path == "test_models"
        assert "graph" in metadata.storage_configs
        graph_config = metadata.storage_configs["graph"]
        assert graph_config.storage_type == StorageType.GRAPH

    def test_model_collection_name(self) -> None:
        model = SampleModel(name="test", path="test_models")
        assert model.get_collection_name() == "test_models"
