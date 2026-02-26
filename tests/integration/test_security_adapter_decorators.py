"""Integration: security mixin, secrets adapter, decorators, embedding."""

from __future__ import annotations

import sys
import types
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from ami.core.storage_types import StorageType
from ami.implementations.embedding_service import EmbeddingService
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import (
    ACLEntry,
    DataClassification,
    Permission,
    SecurityContext,
)
from ami.models.storage_config import StorageConfig
from ami.secrets.adapter import (
    _POINTER_CONTEXT,
    hydrate_sensitive_fields,
    pointer_context,
    prepare_instance_for_storage,
)
from ami.secrets.client import (
    InMemorySecretsBackend,
    SecretsBrokerClient,
    reset_secrets_broker_client,
    set_secrets_broker_client,
)
from ami.secrets.pointer import VaultFieldPointer
from ami.services.decorators import sanitize_for_mcp, sensitive_field
from tests.helpers.embedding import build_test_embedding_service

VERSION_ONE = 1
EXPIRE_HOURS = 2
DIM_384 = 384
DIM_768 = 768
EMB_DIM = 32
_IC = {"im": StorageConfig(storage_type=StorageType.INMEM)}


@sensitive_field("secret_token", mask_pattern="{field}_uid")
@sensitive_field("api_key", mask_pattern="key_masked")
class _SM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="s",
        storage_configs=_IC,
    )
    name: str = ""
    secret_token: str = ""
    api_key: str = ""


class _PM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="p",
        storage_configs=_IC,
    )
    label: str = ""


@sensitive_field("password", mask_pattern="pw_redacted")
class _SSM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="ss",
        storage_configs=_IC,
    )
    username: str = ""
    password: Any = ""


class _NSM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="ns",
        storage_configs=_IC,
    )
    title: str = ""


def _acl(pid: str, perms: list[Permission], **kw: Any) -> ACLEntry:
    return ACLEntry(
        principal_id=pid,
        permissions=perms,
        granted_by=kw.pop("by", "a1"),
        **kw,
    )


def _ctx(uid: str, **kw: Any) -> SecurityContext:
    return SecurityContext(
        user_id=uid,
        roles=kw.pop("roles", []),
        groups=kw.pop("groups", []),
        **kw,
    )


async def _chk(m: Any, ctx: SecurityContext, p: Permission) -> bool:
    return await m.check_permission(ctx, p, raise_on_deny=False)


def _raise_in_ptr_ctx() -> None:
    with pointer_context():
        msg = "boom"
        raise RuntimeError(msg)


@pytest.fixture(autouse=True)
def _reset():
    reset_secrets_broker_client()
    EmbeddingService._instance = None
    yield
    reset_secrets_broker_client()
    EmbeddingService._instance = None


@pytest.fixture
def broker() -> SecretsBrokerClient:
    bk = InMemorySecretsBackend(master_key=b"int-test-key")
    cl = SecretsBrokerClient(backend=bk)
    set_secrets_broker_client(cl)
    return cl


class TestACLLifecycle:
    @pytest.mark.asyncio
    async def test_grant_then_check(self) -> None:
        m = _PM(label="lc")
        m.set_owner("o1")
        await m.grant_permission(
            _ctx("o1"),
            principal_id="r1",
            permissions=[Permission.READ],
        )
        assert await _chk(m, _ctx("r1"), Permission.READ) is True

    @pytest.mark.asyncio
    async def test_no_grant(self) -> None:
        m = _PM(label="ng")
        m.set_owner("o1")
        assert await _chk(m, _ctx("x"), Permission.WRITE) is False


class TestDenyFirst:
    @pytest.mark.asyncio
    async def test_deny_overrides_allow(self) -> None:
        m = _PM(label="df")
        m.set_owner("a1")
        m.acl.extend(
            [
                _acl("ux", [Permission.READ], is_deny_rule=False),
                _acl("ux", [Permission.READ], is_deny_rule=True),
            ]
        )
        assert await _chk(m, _ctx("ux"), Permission.READ) is False

    @pytest.mark.asyncio
    async def test_deny_raises(self) -> None:
        m = _PM(label="dr")
        m.set_owner("a1")
        m.acl.append(
            _acl("ux", [Permission.READ], is_deny_rule=True),
        )
        with pytest.raises(PermissionError, match="Access denied"):
            await m.check_permission(
                _ctx("ux"),
                Permission.READ,
                raise_on_deny=True,
            )


class TestExpiredACL:
    @pytest.mark.asyncio
    async def test_expired_allow_ignored(self) -> None:
        m = _PM(label="ea")
        m.set_owner("a1")
        past = datetime.now(UTC) - timedelta(hours=EXPIRE_HOURS)
        m.acl.append(
            _acl("ue", [Permission.READ], expires_at=past),
        )
        assert await _chk(m, _ctx("ue"), Permission.READ) is False

    @pytest.mark.asyncio
    async def test_expired_deny_ignored(self) -> None:
        m = _PM(label="ed")
        m.set_owner("a1")
        past = datetime.now(UTC) - timedelta(hours=EXPIRE_HOURS)
        m.acl.extend(
            [
                _acl("ue", [Permission.READ], is_deny_rule=True, expires_at=past),
                _acl("ue", [Permission.READ], is_deny_rule=False),
            ]
        )
        assert await _chk(m, _ctx("ue"), Permission.READ) is True


class TestMatchesPrincipal:
    @pytest.mark.asyncio
    async def test_role(self) -> None:
        m = _PM(label="rm")
        m.set_owner("a1")
        m.acl.append(
            _acl("ed", [Permission.WRITE], principal_type="role"),
        )
        ctx = _ctx("ur", roles=["ed"])
        assert await _chk(m, ctx, Permission.WRITE) is True

    @pytest.mark.asyncio
    async def test_group(self) -> None:
        m = _PM(label="gm")
        m.set_owner("a1")
        m.acl.append(
            _acl("eg", [Permission.READ], principal_type="group"),
        )
        ctx = _ctx("ug", groups=["eg"])
        assert await _chk(m, ctx, Permission.READ) is True

    @pytest.mark.asyncio
    async def test_service(self) -> None:
        m = _PM(label="sm")
        m.set_owner("a1")
        m.acl.append(
            _acl("ix", [Permission.READ], principal_type="service"),
        )
        ctx = _ctx("sv", roles=["ix"])
        assert await _chk(m, ctx, Permission.READ) is True

    @pytest.mark.asyncio
    async def test_no_match(self) -> None:
        m = _PM(label="nm")
        m.set_owner("a1")
        m.acl.append(
            _acl("ot", [Permission.READ], principal_type="role"),
        )
        ctx = _ctx("uz", roles=["viewer"])
        assert await _chk(m, ctx, Permission.READ) is False


class TestRevoke:
    @pytest.mark.asyncio
    async def test_owner_revokes(self) -> None:
        m = _PM(label="rv")
        m.set_owner("o1")
        await m.grant_permission(
            _ctx("o1"),
            principal_id="ur",
            permissions=[Permission.READ],
        )
        assert await m.revoke_permission(_ctx("o1"), "ur") is True
        ctx = _ctx("ur", roles=["v"], groups=["t"])
        assert await _chk(m, ctx, Permission.READ) is False

    @pytest.mark.asyncio
    async def test_non_owner_fails(self) -> None:
        m = _PM(label="rf")
        m.set_owner("o1")
        with pytest.raises(PermissionError, match="admin"):
            await m.revoke_permission(
                _ctx("x", roles=["viewer"]),
                "any",
            )


class TestClassRLS:
    def test_set_classification(self) -> None:
        m = _PM(label="cl")
        m.set_classification(DataClassification.RESTRICTED)
        assert m.classification == DataClassification.RESTRICTED.value

    def test_rls_tenant(self) -> None:
        s = _PM(label="r").apply_row_level_security(
            {"status": "ok"},
            _ctx("u", tenant_id="t-42"),
        )
        assert "$and" in s
        assert {"tenant_id": "t-42"} in s["$and"]

    def test_rls_empty(self) -> None:
        s = _PM(label="e").apply_row_level_security(
            {},
            _ctx("u", tenant_id="t-99"),
        )
        assert s == {"tenant_id": "t-99"}

    def test_rls_no_tenant(self) -> None:
        q = {"foo": "bar"}
        r = _PM(label="n").apply_row_level_security(q, _ctx("u"))
        assert r is q


class TestPrepare:
    @pytest.mark.asyncio
    async def test_pointers(self, broker: SecretsBrokerClient) -> None:
        i = _SM(name="t", secret_token="tok", api_key="key")
        r = await prepare_instance_for_storage(i, i.model_dump())
        assert isinstance(r["secret_token"], dict)
        assert "vault_reference" in r["secret_token"]
        assert "integrity_hash" in r["secret_token"]

    @pytest.mark.asyncio
    async def test_ptr_pass(self, broker: SecretsBrokerClient) -> None:
        ptr = VaultFieldPointer(
            vault_reference="ref-x",
            integrity_hash="h-x",
            version=VERSION_ONE,
        )
        i = _SM.model_construct(
            name="p",
            secret_token=ptr,
            api_key="k",
        )
        d = {"name": "p", "secret_token": "x", "api_key": "k"}
        r = await prepare_instance_for_storage(i, d)
        assert r["secret_token"]["vault_reference"] == "ref-x"

    @pytest.mark.asyncio
    async def test_secretstr(self, broker: SecretsBrokerClient) -> None:
        i = _SSM(username="al", password=SecretStr("s3c"))
        r = await prepare_instance_for_storage(i, i.model_dump())
        assert isinstance(r["password"], dict)
        assert "vault_reference" in r["password"]

    @pytest.mark.asyncio
    async def test_no_sensitive(self) -> None:
        i = _NSM(title="pub")
        r = await prepare_instance_for_storage(i, i.model_dump())
        assert r["title"] == "pub"


class TestHydrate:
    @pytest.mark.asyncio
    async def test_roundtrip(self, broker: SecretsBrokerClient) -> None:
        i = _SM(name="rt", secret_token="ot", api_key="ok")
        s = await prepare_instance_for_storage(i, i.model_dump())
        h = await hydrate_sensitive_fields(_SM, s)
        assert h["secret_token"] == "ot"
        assert h["api_key"] == "ok"

    @pytest.mark.asyncio
    async def test_integrity_mismatch(
        self,
        broker: SecretsBrokerClient,
    ) -> None:
        i = _SM(name="bh", secret_token="st", api_key="sk")
        s = await prepare_instance_for_storage(i, i.model_dump())
        s["secret_token"]["integrity_hash"] = "tampered"
        with pytest.raises(ValueError, match="Integrity mismatch"):
            await hydrate_sensitive_fields(_SM, s)

    @pytest.mark.asyncio
    async def test_non_pointer_skipped(
        self,
        broker: SecretsBrokerClient,
    ) -> None:
        d = {"name": "p", "secret_token": "s", "api_key": "s2"}
        h = await hydrate_sensitive_fields(_SM, d)
        assert h["secret_token"] == "s"


class TestPointerCtx:
    def test_normal_exit(self) -> None:
        _POINTER_CONTEXT.set({"l": "o"})
        with pointer_context():
            assert _POINTER_CONTEXT.get(None) is None
        assert _POINTER_CONTEXT.get(None) is None

    def test_exception_exit(self) -> None:
        _POINTER_CONTEXT.set({"s": True})
        with pytest.raises(RuntimeError, match="boom"):
            _raise_in_ptr_ctx()
        assert _POINTER_CONTEXT.get(None) is None


class TestSensFieldDec:
    def test_registers(self) -> None:
        f = _SM._sensitive_fields
        assert "secret_token" in f
        assert "api_key" in f

    def test_mask_patterns(self) -> None:
        f = _SM._sensitive_fields
        assert f["secret_token"].mask_pattern == "{field}_uid"
        assert f["api_key"].mask_pattern == "key_masked"

    def test_classification(self) -> None:
        @sensitive_field(
            "cred",
            mask_pattern="cm",
            classification=DataClassification.RESTRICTED,
        )
        class _C(StorageModel):
            cred: str = ""

        cfg = _C._sensitive_fields["cred"]
        assert cfg.classification == DataClassification.RESTRICTED


class TestSanitizeMcp:
    def test_masks(self) -> None:
        s = sanitize_for_mcp(
            _SM(name="u", secret_token="r", api_key="r"),
        )
        assert s["name"] == "u"
        assert s["api_key"] == "key_masked"
        assert "secret_token_uid" in s["secret_token"]

    def test_no_sensitive(self) -> None:
        assert sanitize_for_mcp(_NSM(title="p"))["title"] == "p"


class TestEmbSingleton:
    def test_identity(self) -> None:
        a = EmbeddingService.get_instance()
        assert a is EmbeddingService.get_instance()

    def test_diff_model(self) -> None:
        a = EmbeddingService.get_instance(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
        )
        b = EmbeddingService.get_instance(
            model_name="sentence-transformers/all-mpnet-base-v2",
        )
        assert a is not b


class TestEmbDim:
    def test_minilm(self) -> None:
        svc = EmbeddingService(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
        )
        assert svc.embedding_dim == DIM_384

    def test_mpnet(self) -> None:
        svc = EmbeddingService(
            model_name="sentence-transformers/all-mpnet-base-v2",
        )
        assert svc.embedding_dim == DIM_768

    def test_unknown(self) -> None:
        svc = EmbeddingService(model_name="custom/x")
        assert svc.embedding_dim == DIM_768


class TestExtractText:
    def _s(self) -> EmbeddingService:
        return EmbeddingService(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
        )

    def test_flat(self) -> None:
        r = self._s()._extract_text_from_dict({"t": "h", "b": "w"})
        assert "t: h" in r
        assert "b: w" in r

    def test_nested(self) -> None:
        r = self._s()._extract_text_from_dict({"m": {"a": "Ada"}})
        assert "a: Ada" in r

    def test_list(self) -> None:
        r = self._s()._extract_text_from_dict({"t": ["a", "b"]})
        assert "a" in r

    def test_empty(self) -> None:
        assert self._s()._extract_text_from_dict({}) == ""


class TestGenFromDict:
    @pytest.fixture(autouse=True)
    def _mock_ml(self) -> None:
        t = types.ModuleType("torch")
        t.sum = MagicMock()
        t.clamp = MagicMock()
        t.nn = MagicMock()
        sys.modules["torch"] = t
        o = types.ModuleType("optimum.onnxruntime")
        o.ORTModelForFeatureExtraction = MagicMock()
        sys.modules["optimum"] = types.ModuleType("optimum")
        sys.modules["optimum.onnxruntime"] = o
        tr = types.ModuleType("transformers")
        tr.AutoTokenizer = MagicMock()
        sys.modules["transformers"] = tr

    @pytest.mark.asyncio
    async def test_text(self) -> None:
        svc = build_test_embedding_service(embedding_dim=EMB_DIM)
        r = await svc.generate_from_dict({"t": "hi", "b": "wo"})
        assert isinstance(r, list)
        assert len(r) == EMB_DIM

    @pytest.mark.asyncio
    async def test_empty_raises(self) -> None:
        svc = build_test_embedding_service(embedding_dim=EMB_DIM)
        with pytest.raises(ValueError, match="No text content"):
            await svc.generate_from_dict({"count": 42})

    @pytest.mark.asyncio
    async def test_nested(self) -> None:
        svc = build_test_embedding_service(embedding_dim=EMB_DIM)
        r = await svc.generate_from_dict({"m": {"a": "Ada"}})
        assert len(r) == EMB_DIM

    @pytest.mark.asyncio
    async def test_list(self) -> None:
        svc = build_test_embedding_service(embedding_dim=EMB_DIM)
        r = await svc.generate_from_dict({"t": ["ml", "ai"]})
        assert len(r) == EMB_DIM
