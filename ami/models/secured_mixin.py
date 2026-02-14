"""SecuredModelMixin — security mixin for StorageModel classes.

FIX: DENY rules are now checked BEFORE ALLOW rules to prevent
allow-bypasses-deny vulnerabilities.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import Field

from ami.models.security import (
    ACLEntry,
    DataClassification,
    Permission,
    SecurityContext,
)

_MAX_ACCESS_ENTRIES = 100


class SecuredModelMixin:
    """Mixin that adds ownership, ACL, audit, and classification."""

    owner_id: str | None = None
    owner_type: str = "user"

    acl: list[ACLEntry] = Field(default_factory=list)

    created_by: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    modified_by: str | None = None
    modified_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    accessed_by: list[str] = Field(default_factory=list)
    accessed_at: list[datetime] = Field(default_factory=list)

    classification: DataClassification = DataClassification.INTERNAL
    encrypted_fields: list[str] = Field(default_factory=list)
    tenant_isolation_level: str = "shared"

    # ------------------------------------------------------------------
    # Permission checking (DENY-first)
    # ------------------------------------------------------------------

    async def check_permission(
        self,
        context: SecurityContext,
        permission: Permission,
        raise_on_deny: bool = True,
    ) -> bool:
        """Check if *context* has *permission* on this resource.

        Order: owner → DENY rules → ALLOW rules → no-match.
        """
        # Owner always has full access
        if context.user_id == self.owner_id:
            self._log_access(context, permission, "GRANTED")
            return True

        # FIX: Check DENY rules FIRST (explicit deny overrides allow)
        for entry in self.acl:
            if (
                entry.is_deny_rule
                and not entry.is_expired()
                and self._matches_principal(context, entry)
                and permission in entry.permissions
            ):
                self._log_access(context, permission, "DENIED")
                if raise_on_deny:
                    msg = f"Access denied: {permission}"
                    raise PermissionError(msg)
                return False

        # Then check ALLOW rules
        for entry in self.acl:
            if (
                not entry.is_deny_rule
                and not entry.is_expired()
                and self._matches_principal(context, entry)
                and permission in entry.permissions
            ):
                self._log_access(context, permission, "GRANTED")
                return True

        # No matching rule
        self._log_access(context, permission, "NO_MATCH")
        if raise_on_deny:
            msg = f"No permission: {permission}"
            raise PermissionError(msg)
        return False

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _matches_principal(
        self,
        context: SecurityContext,
        acl_entry: ACLEntry,
    ) -> bool:
        if acl_entry.principal_id == context.user_id:
            return True
        if (
            acl_entry.principal_type == "role"
            and acl_entry.principal_id in context.roles
        ):
            return True
        if (
            acl_entry.principal_type == "group"
            and acl_entry.principal_id in context.groups
        ):
            return True
        return bool(
            acl_entry.principal_type == "service"
            and acl_entry.principal_id in context.principal_ids
        )

    def _log_access(
        self,
        context: SecurityContext,
        _permission: Permission,
        _result: str,
    ) -> None:
        if context.user_id not in self.accessed_by:
            self.accessed_by.append(context.user_id)
        self.accessed_at.append(datetime.now(UTC))
        if len(self.accessed_by) > _MAX_ACCESS_ENTRIES:
            self.accessed_by = self.accessed_by[-_MAX_ACCESS_ENTRIES:]
            self.accessed_at = self.accessed_at[-_MAX_ACCESS_ENTRIES:]

    # ------------------------------------------------------------------
    # Permission management
    # ------------------------------------------------------------------

    async def grant_permission(
        self,
        context: SecurityContext,
        principal_id: str,
        permissions: list[Permission],
        principal_type: str = "user",
        expires_at: datetime | None = None,
    ) -> ACLEntry:
        """Grant permissions to a principal."""
        if not await self.check_permission(
            context,
            Permission.ADMIN,
            raise_on_deny=False,
        ):
            msg = "No admin permission to grant access"
            raise PermissionError(msg)

        entry = ACLEntry(
            principal_id=principal_id,
            principal_type=principal_type,
            permissions=permissions,
            granted_by=context.user_id,
            expires_at=expires_at,
        )
        self.acl.append(entry)
        self.modified_by = context.user_id
        self.modified_at = datetime.now(UTC)
        return entry

    async def revoke_permission(
        self,
        context: SecurityContext,
        principal_id: str,
    ) -> bool:
        """Revoke all permissions for a principal."""
        if not await self.check_permission(
            context,
            Permission.ADMIN,
            raise_on_deny=False,
        ):
            msg = "No admin permission to revoke access"
            raise PermissionError(msg)

        original = len(self.acl)
        self.acl = [e for e in self.acl if e.principal_id != principal_id]
        if len(self.acl) < original:
            self.modified_by = context.user_id
            self.modified_at = datetime.now(UTC)
            return True
        return False

    def set_owner(
        self,
        user_id: str,
        owner_type: str = "user",
    ) -> None:
        """Set the owner of the resource."""
        self.owner_id = user_id
        self.owner_type = owner_type
        has_admin = any(
            e.principal_id == user_id and Permission.ADMIN in e.permissions
            for e in self.acl
        )
        if not has_admin:
            self.acl.append(
                ACLEntry(
                    principal_id=user_id,
                    principal_type=owner_type,
                    permissions=[Permission.ADMIN],
                    granted_by="system",
                ),
            )

    def set_classification(
        self,
        classification: DataClassification,
    ) -> None:
        """Set data classification level."""
        self.classification = classification

    def apply_row_level_security(
        self,
        query: dict[str, Any],
        context: SecurityContext,
    ) -> dict[str, Any]:
        """Apply row-level security filter for tenant isolation."""
        if not context.tenant_id:
            return query
        tenant_filter = {"tenant_id": context.tenant_id}
        if query:
            return {"$and": [query, tenant_filter]}
        return tenant_filter
