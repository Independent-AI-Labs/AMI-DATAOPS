"""Shared types for DataOps models."""

from enum import StrEnum


class AuthProviderType(StrEnum):
    """Supported authentication provider types."""

    GOOGLE = "google"
    GITHUB = "github"
    AZURE_AD = "azure_ad"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    SSH = "ssh"


class TokenType(StrEnum):
    """Token types."""

    ACCESS = "access"
    REFRESH = "refresh"
    ID_TOKEN = "id_token"
    API_KEY = "api_key"
