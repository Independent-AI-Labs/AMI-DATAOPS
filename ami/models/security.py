"""Security models: ACL, permissions, roles, and security context."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum, StrEnum
from typing import Any

from pydantic import BaseModel, Field


class DataClassification(Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"

    def __ge__(self, other: object) -> bool:
        if isinstance(other, DataClassification):
            levels = list(DataClassification)
            return levels.index(self) >= levels.index(other)
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, DataClassification):
            levels = list(DataClassification)
            return levels.index(self) < levels.index(other)
        return NotImplemented


class Permission(StrEnum):
    """Permissions for ACL."""

    READ = "r"
    WRITE = "w"
    DELETE = "d"
    EXECUTE = "x"
    ADMIN = "a"
    SHARE = "s"
    AUDIT = "u"
    EXPORT = "e"
    IMPORT = "i"
    DECRYPT = "c"
    SIGN = "g"
    APPROVE = "p"


class RoleType(StrEnum):
    """Built-in role types."""

    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"
    GUEST = "guest"
    SERVICE = "service"


class ACLEntry(BaseModel):
    """Access Control List entry."""

    principal_id: str
    principal_type: str = "user"
    permissions: list[Permission]
    resource_path: str | None = None
    conditions: dict[str, Any] = Field(default_factory=dict)
    granted_by: str | None = None
    granted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    is_deny_rule: bool = False

    def has_permission(self, permission: Permission) -> bool:
        """Check if this ACL entry grants a specific permission."""
        return permission in self.permissions or Permission.ADMIN in self.permissions

    def is_expired(self) -> bool:
        """Check if this ACL entry has expired."""
        if self.expires_at:
            return datetime.now(UTC) > self.expires_at
        return False


class SecurityContext(BaseModel):
    """Security context for operations."""

    user_id: str
    roles: list[str] = Field(default_factory=list)
    groups: list[str] = Field(default_factory=list)
    tenant_id: str | None = None

    @property
    def is_admin(self) -> bool:
        return "admin" in self.roles or "system" in self.roles

    @property
    def principal_ids(self) -> list[str]:
        principals = [self.user_id]
        principals.extend(self.roles)
        principals.extend(self.groups)
        return principals


class Role(BaseModel):
    """Role model with permissions.

    Note: when used as a StorageModel subclass, set ``_model_meta``
    with the appropriate ``ModelMetadata`` instance.
    """

    name: str
    role_type: RoleType
    permissions: list[Permission]
    description: str | None = None

    # Example storage binding (activate in a StorageModel subclass):
    # _model_meta = ModelMetadata(
    #     path="roles",
    #     storage_configs={
    #         "graph": StorageConfig(storage_type=StorageType.GRAPH),
    #         "inmem": StorageConfig(storage_type=StorageType.INMEM),
    #     },
    # )


class SecurityGroup(BaseModel):
    """Security group for organizing users."""

    name: str
    description: str | None = None
    member_ids: list[str] = Field(default_factory=list)
    role_ids: list[str] = Field(default_factory=list)
    parent_group_id: str | None = None


class AuthRule(BaseModel):
    """Reusable authentication rule."""

    name: str
    description: str | None = None
    rule_type: str = "jwt"
    rule_config: dict[str, Any] = Field(default_factory=dict)

    def to_dgraph_rule(self) -> str:
        """Convert to Dgraph @auth rule string."""
        if self.rule_type == "jwt":
            return str(self.rule_config.get("query", ""))
        if self.rule_type == "graph_traversal":
            return str(self.rule_config.get("traversal", ""))
        return ""
