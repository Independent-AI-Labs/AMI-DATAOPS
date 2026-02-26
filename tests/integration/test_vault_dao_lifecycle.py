"""Integration tests for OpenBaoDAO lifecycle and CRUD operations."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest
from hvac.exceptions import VaultError as OpenBaoError

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.vault.openbao_dao import OpenBaoDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

VAULT_HOST = "localhost"
VAULT_PORT = 8200
VAULT_TOKEN = "test-token"
VAULT_MOUNT = "secret"
COLLECTION = "vaultsecrets"
UID_A = "abc-123"
UID_B = "def-456"
UID_C = "ghi-789"
BULK_COUNT = 3
PAIR_COUNT = 2


class VaultSecret(StorageModel):
    """Minimal model used by all tests in this module."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path=COLLECTION)
    name: str = ""
    secret_key: str = ""


def _vault_config(**overrides: Any) -> StorageConfig:
    base = {
        "storage_type": StorageType.VAULT,
        "host": VAULT_HOST,
        "port": VAULT_PORT,
        "options": {"token": VAULT_TOKEN},
    }
    return StorageConfig(**{**base, **overrides})


def _kv2(client: MagicMock) -> MagicMock:
    return client.secrets.kv.v2


def _resp(data: dict[str, Any]) -> dict[str, Any]:
    return {"data": {"data": data}}


def _payload(uid: str = UID_A, **kw: Any) -> dict[str, Any]:
    return {"uid": uid, "name": kw.get("name", "x"), "secret_key": "k"}


@pytest.fixture
def config() -> StorageConfig:
    return _vault_config()


@pytest.fixture
def dao(config: StorageConfig) -> OpenBaoDAO:
    return OpenBaoDAO(VaultSecret, config)


@pytest.fixture
def cdao(dao: OpenBaoDAO, mock_hvac_client: MagicMock) -> OpenBaoDAO:
    """DAO with an injected mock hvac client."""
    dao.client = mock_hvac_client
    dao._connected = True
    return dao


class TestInit:
    """OpenBaoDAO.__init__ with real StorageConfig."""

    def test_default_mount(self, dao: OpenBaoDAO) -> None:
        assert dao._mount == VAULT_MOUNT

    def test_custom_mount(self) -> None:
        cfg = _vault_config(options={"token": VAULT_TOKEN, "mount": "kv"})
        assert OpenBaoDAO(VaultSecret, cfg)._mount == "kv"

    def test_collection_name(self, dao: OpenBaoDAO) -> None:
        assert dao.collection_name == COLLECTION

    def test_client_starts_none(self, dao: OpenBaoDAO) -> None:
        assert dao.client is None

    def test_connected_starts_false(self, dao: OpenBaoDAO) -> None:
        assert dao._connected is False


class TestConnect:
    """connect / disconnect / test_connection."""

    @pytest.mark.asyncio
    async def test_connect_creates_client(self, dao: OpenBaoDAO) -> None:
        target = "ami.implementations.vault.openbao_dao.OpenBaoClient"
        with patch(target) as mock_cls:
            mock_cls.return_value = MagicMock()
            await dao.connect()
            assert dao.client is not None
            assert dao._connected is True

    @pytest.mark.asyncio
    async def test_connect_idempotent(self, cdao: OpenBaoDAO) -> None:
        original = cdao.client
        await cdao.connect()
        assert cdao.client is original

    @pytest.mark.asyncio
    async def test_disconnect_clears(self, cdao: OpenBaoDAO) -> None:
        await cdao.disconnect()
        assert cdao.client is None
        assert cdao._connected is False

    @pytest.mark.asyncio
    async def test_health_check(self, cdao: OpenBaoDAO) -> None:
        assert await cdao.test_connection() is True

    @pytest.mark.asyncio
    async def test_fallback_is_authenticated(
        self,
        cdao: OpenBaoDAO,
    ) -> None:
        del cdao.client.sys
        cdao.client.is_authenticated.return_value = True
        assert await cdao.test_connection() is True

    @pytest.mark.asyncio
    async def test_connection_error_returns_false(
        self,
        cdao: OpenBaoDAO,
    ) -> None:
        cdao.client.sys.read_health_status.side_effect = OpenBaoError("down")
        assert await cdao.test_connection() is False


class TestReference:
    """Path building and traversal rejection."""

    def test_valid_path(self, cdao: OpenBaoDAO) -> None:
        assert cdao._reference("my-secret") == f"{COLLECTION}/my-secret"

    def test_traversal_rejected(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            cdao._reference("../etc/passwd")

    def test_leading_slash_rejected(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            cdao._reference("/absolute")

    def test_control_char_rejected(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            cdao._reference("bad\x00id")

    def test_empty_string_rejected(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            cdao._reference("")


class TestCreate:
    """Secret creation via to_storage_dict, dict, and model_dump."""

    @pytest.mark.asyncio
    async def test_via_to_storage_dict(self, cdao: OpenBaoDAO) -> None:
        inst = VaultSecret(uid=UID_A, name="alpha", secret_key="s3cr3t")
        uid = await cdao.create(inst)
        assert uid == UID_A
        _kv2(cdao.client).create_or_update_secret.assert_called_once()

    @pytest.mark.asyncio
    async def test_from_dict(self, cdao: OpenBaoDAO) -> None:
        data = {"uid": UID_B, "name": "beta", "secret_key": "k"}
        assert await cdao.create(data) == UID_B

    @pytest.mark.asyncio
    async def test_generates_uid_when_missing(
        self,
        cdao: OpenBaoDAO,
    ) -> None:
        uid = await cdao.create({"name": "gamma"})
        assert uid  # non-empty UUID string

    @pytest.mark.asyncio
    async def test_vault_error_raises(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).create_or_update_secret.side_effect = OpenBaoError("denied")
        with pytest.raises(StorageError, match="Failed to create"):
            await cdao.create({"uid": UID_A, "name": "x"})

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, dao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="not connected"):
            await dao.create({"name": "x"})


class TestFindById:
    """Read a secret by ID -- found and not-found paths."""

    @pytest.mark.asyncio
    async def test_found(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp(_payload())
        result = await cdao.find_by_id(UID_A)
        assert result is not None
        assert result.uid == UID_A

    @pytest.mark.asyncio
    async def test_not_found_returns_none(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.side_effect = OpenBaoError("404")
        assert await cdao.find_by_id("missing") is None

    @pytest.mark.asyncio
    async def test_empty_data_returns_none(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp({})
        assert await cdao.find_by_id(UID_A) is None


class TestFindOne:
    """Query routing: uid shortcut, id shortcut, general query."""

    @pytest.mark.asyncio
    async def test_by_uid(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp(_payload())
        result = await cdao.find_one({"uid": UID_A})
        assert result is not None
        assert result.uid == UID_A

    @pytest.mark.asyncio
    async def test_by_id(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp(_payload())
        assert await cdao.find_one({"id": UID_A}) is not None

    @pytest.mark.asyncio
    async def test_general_query(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.list_secrets.return_value = {"data": {"keys": [UID_A]}}
        kv2.read_secret_version.return_value = _resp(_payload(name="match"))
        result = await cdao.find_one({"name": "match"})
        assert result is not None
        assert result.name == "match"


class TestFind:
    """List-all and filtered find."""

    @pytest.mark.asyncio
    async def test_find_all(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.list_secrets.return_value = {"data": {"keys": [UID_A, UID_B]}}
        kv2.read_secret_version.side_effect = lambda path, mount_point: (
            _resp(
                {
                    "uid": path.rsplit("/", maxsplit=1)[-1],
                    "name": "n",
                    "secret_key": "k",
                }
            )
        )
        assert len(await cdao.find({})) == PAIR_COUNT

    @pytest.mark.asyncio
    async def test_with_filter(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.list_secrets.return_value = {"data": {"keys": [UID_A, UID_B]}}
        kv2.read_secret_version.side_effect = lambda path, mount_point: (
            _resp(
                {
                    "uid": path.rsplit("/", maxsplit=1)[-1],
                    "name": "target" if UID_A in path else "other",
                    "secret_key": "k",
                }
            )
        )
        results = await cdao.find({"name": "target"})
        assert len(results) == 1
        assert results[0].name == "target"

    @pytest.mark.asyncio
    async def test_vault_error_raises(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).list_secrets.side_effect = OpenBaoError("fail")
        with pytest.raises(StorageConnectionError):
            await cdao.find({})


class TestUpdate:
    """Merge existing data and write."""

    @pytest.mark.asyncio
    async def test_merges_and_writes(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.read_secret_version.return_value = _resp(_payload(name="old"))
        await cdao.update(UID_A, {"name": "new"})
        call_kw = kv2.create_or_update_secret.call_args.kwargs
        assert call_kw["secret"]["name"] == "new"

    @pytest.mark.asyncio
    async def test_not_found_raises(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.side_effect = OpenBaoError("404")
        with pytest.raises(StorageError, match="Secret not found"):
            await cdao.update("missing", {"name": "x"})

    @pytest.mark.asyncio
    async def test_write_error_raises(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.read_secret_version.return_value = _resp(_payload())
        kv2.create_or_update_secret.side_effect = OpenBaoError("denied")
        with pytest.raises(StorageError, match="Failed to update"):
            await cdao.update(UID_A, {"name": "x"})


class TestDelete:
    """Delete success and failure paths."""

    @pytest.mark.asyncio
    async def test_success(self, cdao: OpenBaoDAO) -> None:
        assert await cdao.delete(UID_A) is True

    @pytest.mark.asyncio
    async def test_failure_returns_false(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).delete_metadata_and_all_versions.side_effect = OpenBaoError(
            "404"
        )
        assert await cdao.delete(UID_A) is False


class TestBulkOperations:
    """bulk_create, bulk_update, bulk_delete."""

    @pytest.mark.asyncio
    async def test_bulk_create(self, cdao: OpenBaoDAO) -> None:
        items = [
            {"uid": UID_A, "name": "a", "secret_key": "k"},
            {"uid": UID_B, "name": "b", "secret_key": "k"},
            {"uid": UID_C, "name": "c", "secret_key": "k"},
        ]
        ids = await cdao.bulk_create(items)
        assert len(ids) == BULK_COUNT
        assert ids[0] == UID_A

    @pytest.mark.asyncio
    async def test_bulk_update(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.read_secret_version.return_value = _resp(_payload())
        await cdao.bulk_update([{"uid": UID_A, "name": "new"}])
        kv2.create_or_update_secret.assert_called_once()

    @pytest.mark.asyncio
    async def test_bulk_delete(self, cdao: OpenBaoDAO) -> None:
        assert await cdao.bulk_delete([UID_A, UID_B]) == PAIR_COUNT


class TestCountExists:
    """Counting and existence checks."""

    @pytest.mark.asyncio
    async def test_count(self, cdao: OpenBaoDAO) -> None:
        kv2 = _kv2(cdao.client)
        kv2.list_secrets.return_value = {"data": {"keys": [UID_A]}}
        kv2.read_secret_version.return_value = _resp(_payload())
        assert await cdao.count({}) == 1

    @pytest.mark.asyncio
    async def test_exists_true(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp(_payload())
        assert await cdao.exists(UID_A) is True

    @pytest.mark.asyncio
    async def test_exists_false(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.side_effect = OpenBaoError("404")
        assert await cdao.exists("missing") is False


class TestRawQueries:
    """Raw read and write against arbitrary paths."""

    @pytest.mark.asyncio
    async def test_raw_read_data(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp({"key": "val"})
        rows = await cdao.raw_read_query("some/path")
        assert len(rows) == 1
        assert rows[0]["key"] == "val"

    @pytest.mark.asyncio
    async def test_raw_read_empty(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.return_value = _resp({})
        assert await cdao.raw_read_query("empty/path") == []

    @pytest.mark.asyncio
    async def test_raw_read_error(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).read_secret_version.side_effect = OpenBaoError("fail")
        with pytest.raises(StorageError, match="Raw read failed"):
            await cdao.raw_read_query("bad/path")

    @pytest.mark.asyncio
    async def test_raw_write_returns_one(self, cdao: OpenBaoDAO) -> None:
        assert await cdao.raw_write_query("p", {"key": "val"}) == 1

    @pytest.mark.asyncio
    async def test_raw_write_error(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).create_or_update_secret.side_effect = OpenBaoError("fail")
        with pytest.raises(StorageError, match="Raw write failed"):
            await cdao.raw_write_query("bad/path")


class TestDiscovery:
    """list_databases, list_schemas, list_models, get_model_*."""

    @pytest.mark.asyncio
    async def test_list_databases_from_mounts(
        self,
        cdao: OpenBaoDAO,
    ) -> None:
        cdao.client.sys.list_mounted_secrets_engines.return_value = {
            "data": {"secret/": {}, "transit/": {}},
        }
        dbs = await cdao.list_databases()
        assert "secret" in dbs
        assert "transit" in dbs

    @pytest.mark.asyncio
    async def test_list_databases_fallback(
        self,
        cdao: OpenBaoDAO,
    ) -> None:
        cdao.client.sys.list_mounted_secrets_engines.side_effect = OpenBaoError(
            "no access"
        )
        assert await cdao.list_databases() == [VAULT_MOUNT]

    @pytest.mark.asyncio
    async def test_list_schemas(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).list_secrets.return_value = {
            "data": {"keys": ["apps/", "infra/"]},
        }
        assert await cdao.list_schemas() == ["apps", "infra"]

    @pytest.mark.asyncio
    async def test_list_schemas_error(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).list_secrets.side_effect = OpenBaoError("fail")
        with pytest.raises(StorageConnectionError):
            await cdao.list_schemas()

    @pytest.mark.asyncio
    async def test_list_models(self, cdao: OpenBaoDAO) -> None:
        _kv2(cdao.client).list_secrets.return_value = {
            "data": {"keys": ["key-a", "key-b/"]},
        }
        assert await cdao.list_models() == ["key-a", "key-b"]

    @pytest.mark.asyncio
    async def test_get_model_info(self, cdao: OpenBaoDAO) -> None:
        info = await cdao.get_model_info("my-secret")
        assert info["name"] == "my-secret"
        assert info["type"] == "vault_secret"
        assert info["mount"] == VAULT_MOUNT

    @pytest.mark.asyncio
    async def test_get_model_schema(self, cdao: OpenBaoDAO) -> None:
        schema = await cdao.get_model_schema("my-secret")
        assert "name" in schema["fields"]
        assert "secret_key" in schema["fields"]

    @pytest.mark.asyncio
    async def test_get_model_fields(self, cdao: OpenBaoDAO) -> None:
        names = [f["name"] for f in await cdao.get_model_fields("s")]
        assert "name" in names
        assert "secret_key" in names

    @pytest.mark.asyncio
    async def test_get_model_indexes_empty(
        self,
        cdao: OpenBaoDAO,
    ) -> None:
        assert await cdao.get_model_indexes("s") == []


class TestInvalidItemId:
    """Operations that reach _reference with bad IDs."""

    @pytest.mark.asyncio
    async def test_find_by_id_traversal(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            await cdao.find_by_id("../../root")

    @pytest.mark.asyncio
    async def test_update_traversal(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            await cdao.update("../etc", {"name": "x"})

    @pytest.mark.asyncio
    async def test_delete_traversal(self, cdao: OpenBaoDAO) -> None:
        with pytest.raises(StorageError, match="Invalid item ID"):
            await cdao.delete("../secret")
