"""Tests verifying DENY-before-ALLOW permission check order.

The original code checked ALLOW rules first, which allowed an explicit
ALLOW entry to bypass a DENY rule. The fix ensures DENY entries are
evaluated before ALLOW entries.
"""

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


class _DenyTestModel(StorageModel):
    """Model used only for deny-rule tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="deny_test",
        storage_configs={
            "inmem": StorageConfig(storage_type=StorageType.INMEM),
        },
    )

    name: str = ""


class TestDenyBeforeAllow:
    """Verify DENY rules take precedence over ALLOW rules."""

    @pytest.fixture
    def ctx(self) -> SecurityContext:
        return SecurityContext(
            user_id="user_1",
            roles=["editor"],
            groups=["team_a"],
        )

    @pytest.mark.asyncio
    async def test_deny_overrides_allow_same_principal(
        self,
        ctx: SecurityContext,
    ) -> None:
        """An explicit DENY for a user must block even if ALLOW exists."""
        model = _DenyTestModel(name="secret-doc")
        model.owner_id = "other_user"
        model.acl = [
            ACLEntry(
                principal_id="user_1",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="owner",
                is_deny_rule=False,
            ),
            ACLEntry(
                principal_id="user_1",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=True,
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_deny_overrides_allow_regardless_of_order(
        self,
        ctx: SecurityContext,
    ) -> None:
        """DENY rule listed AFTER ALLOW must still take precedence."""
        model = _DenyTestModel(name="doc")
        model.owner_id = "other_user"

        # ALLOW first, DENY second -- DENY must still win
        model.acl = [
            ACLEntry(
                principal_id="editor",
                principal_type="role",
                permissions=[Permission.WRITE],
                granted_by="owner",
                is_deny_rule=False,
            ),
            ACLEntry(
                principal_id="editor",
                principal_type="role",
                permissions=[Permission.WRITE],
                granted_by="admin",
                is_deny_rule=True,
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.WRITE,
            raise_on_deny=False,
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_deny_raises_permission_error(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _DenyTestModel(name="locked")
        model.owner_id = "other_user"
        model.acl = [
            ACLEntry(
                principal_id="user_1",
                principal_type="user",
                permissions=[Permission.DELETE],
                granted_by="admin",
                is_deny_rule=True,
            ),
        ]

        with pytest.raises(PermissionError, match="denied"):
            await model.check_permission(ctx, Permission.DELETE)

    @pytest.mark.asyncio
    async def test_allow_works_when_no_deny(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _DenyTestModel(name="open")
        model.owner_id = "other_user"
        model.acl = [
            ACLEntry(
                principal_id="user_1",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="owner",
                is_deny_rule=False,
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_owner_bypasses_deny(
        self,
        ctx: SecurityContext,
    ) -> None:
        """Owner always has full access regardless of deny rules."""
        model = _DenyTestModel(name="owned")
        model.owner_id = "user_1"
        model.acl = [
            ACLEntry(
                principal_id="user_1",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=True,
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )
        assert result is True
