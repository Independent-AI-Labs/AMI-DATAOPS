"""Field-level encryption for PII and sensitive data."""

import base64
import hashlib
import logging
from typing import Any, ClassVar

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ami.models.security import (
    DataClassification,
    Permission,
    SecurityContext,
)

logger = logging.getLogger(__name__)


class KeyManager:
    """Manage encryption keys."""

    _keys: ClassVar[dict[str, bytes]] = {}
    _master_key: bytes | None = None

    @classmethod
    def initialize(cls, master_key: str | None = None) -> None:
        """Initialize key manager with master key."""
        if master_key:
            cls._master_key = master_key.encode()
        else:
            cls._master_key = Fernet.generate_key()
            logger.warning(
                "Using generated master key - not for production!",
            )

    @classmethod
    def get_field_key(cls, field_name: str) -> bytes:
        """Get or derive key for field."""
        if field_name not in cls._keys:
            if not cls._master_key:
                cls.initialize()

            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=field_name.encode(),
                iterations=100000,
                backend=default_backend(),
            )

            if cls._master_key is None:
                msg = "Master key not initialized"
                raise ValueError(msg)
            derived = kdf.derive(cls._master_key)
            cls._keys[field_name] = base64.urlsafe_b64encode(derived)

        return cls._keys[field_name]

    @classmethod
    def rotate_keys(cls) -> None:
        """Rotate encryption keys."""
        cls._master_key = Fernet.generate_key()
        cls._keys.clear()
        logger.info("Encryption keys rotated")


class TokenEncryption:
    """Token-based encryption for fields."""

    def __init__(self, key: bytes | None = None) -> None:
        if key:
            self.cipher = Fernet(key)
        else:
            self.cipher = Fernet(Fernet.generate_key())

    def encrypt(self, value: str) -> str:
        """Encrypt value and return base64-encoded token."""
        if not value:
            return value
        encrypted = self.cipher.encrypt(value.encode())
        return base64.urlsafe_b64encode(encrypted).decode()

    def decrypt(self, token: str) -> str:
        """Decrypt token and return original value."""
        if not token:
            return token
        try:
            encrypted = base64.urlsafe_b64decode(token.encode())
            decrypted: bytes = self.cipher.decrypt(encrypted)
            return decrypted.decode()
        except Exception:
            logger.exception("Decryption failed")
            return "[DECRYPTION_FAILED]"


class FieldEncryption:
    """Encrypt specific fields in models."""

    @staticmethod
    def encrypt_field(
        value: Any,
        field_name: str,
        classification: DataClassification,
    ) -> str:
        """Encrypt field based on classification."""
        if classification >= DataClassification.CONFIDENTIAL:
            key = KeyManager.get_field_key(field_name)
            encryptor = TokenEncryption(key)
            return encryptor.encrypt(str(value))
        return str(value)

    @staticmethod
    def decrypt_field(
        encrypted: str,
        field_name: str,
        context: SecurityContext,
    ) -> Any:
        """Decrypt field with permission check."""
        if (
            not hasattr(context, "permissions")
            or Permission.DECRYPT not in context.permissions
        ):
            return "[ENCRYPTED]"
        key = KeyManager.get_field_key(field_name)
        decryptor = TokenEncryption(key)
        return decryptor.decrypt(encrypted)

    @staticmethod
    def hash_field(value: str, salt: str | None = None) -> str:
        """One-way hash for fields like passwords."""
        if salt:
            value = f"{salt}{value}"
        hash_obj = hashlib.sha256(value.encode())
        return hash_obj.hexdigest()

    @staticmethod
    def verify_hash(
        value: str,
        hashed: str,
        salt: str | None = None,
    ) -> bool:
        """Verify hashed value."""
        return FieldEncryption.hash_field(value, salt) == hashed


class PIIEncryption:
    """Special handling for PII fields."""

    PII_FIELDS: ClassVar[set[str]] = {
        "ssn",
        "social_security",
        "tax_id",
        "passport",
        "driver_license",
        "credit_card",
        "bank_account",
        "email",
        "phone",
        "address",
        "date_of_birth",
        "medical_record",
        "health_info",
    }

    @classmethod
    def is_pii_field(cls, field_name: str) -> bool:
        """Check if field contains PII."""
        field_lower = field_name.lower()
        return any(pii in field_lower for pii in cls.PII_FIELDS)

    @classmethod
    def _mask_ssn(cls, value: str) -> str | None:
        """Mask SSN showing last 4 digits."""
        mask_last_digits = 4
        if len(value) >= mask_last_digits:
            return f"***-**-{value[-mask_last_digits:]}"
        return None

    @classmethod
    def _mask_credit_card(cls, value: str) -> str | None:
        """Mask credit card showing last 4 digits."""
        mask_last_digits = 4
        if len(value) >= mask_last_digits:
            return f"****-****-****-{value[-mask_last_digits:]}"
        return None

    @classmethod
    def _mask_email(cls, value: str) -> str | None:
        """Mask email showing first char and domain."""
        if "@" in value:
            local, domain = value.split("@", 1)
            if len(local) > 1:
                return f"{local[0]}***@{domain}"
        return None

    @classmethod
    def _mask_phone(cls, value: str) -> str | None:
        """Mask phone showing area code."""
        phone_min_length = 10
        if len(value) >= phone_min_length:
            return f"({value[:3]}) ***-****"
        return None

    @classmethod
    def _mask_generic(cls, value: str) -> str:
        """Generic masking showing first and last char."""
        min_length_for_partial = 2
        if len(value) > min_length_for_partial:
            return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"
        return "*" * len(value)

    @classmethod
    def mask_pii(cls, value: str, field_type: str = "generic") -> str:
        """Mask PII for display."""
        if not value:
            return value
        maskers = {
            "ssn": cls._mask_ssn,
            "credit_card": cls._mask_credit_card,
            "email": cls._mask_email,
            "phone": cls._mask_phone,
        }
        if field_type in maskers:
            result = maskers[field_type](value)
            if result is not None:
                return result
        return cls._mask_generic(value)


class TransparentEncryption:
    """Transparent encryption for database fields.

    Automatically encrypts/decrypts on save/load.
    """

    def __init__(
        self,
        model_class: type,
        encrypted_fields: list[str],
    ) -> None:
        self.model_class = model_class
        self.encrypted_fields = encrypted_fields

    def encrypt_model(self, instance: Any) -> Any:
        """Encrypt fields in model instance."""
        for field in self.encrypted_fields:
            if hasattr(instance, field):
                value = getattr(instance, field)
                if value and not value.startswith("[ENC:"):
                    key = KeyManager.get_field_key(field)
                    encryptor = TokenEncryption(key)
                    encrypted = encryptor.encrypt(str(value))
                    setattr(instance, field, f"[ENC:{encrypted}]")
        return instance

    def decrypt_model(
        self,
        instance: Any,
        context: SecurityContext,
    ) -> Any:
        """Decrypt fields in model instance."""
        for field in self.encrypted_fields:
            if hasattr(instance, field):
                value = getattr(instance, field)
                if value and value.startswith("[ENC:"):
                    encrypted = value[5:-1]
                    if (
                        hasattr(context, "permissions")
                        and Permission.DECRYPT in context.permissions
                    ):
                        key = KeyManager.get_field_key(field)
                        decryptor = TokenEncryption(key)
                        decrypted = decryptor.decrypt(encrypted)
                        setattr(instance, field, decrypted)
                    elif PIIEncryption.is_pii_field(field):
                        setattr(instance, field, "[PII_ENCRYPTED]")
                    else:
                        setattr(instance, field, "[ENCRYPTED]")
        return instance
