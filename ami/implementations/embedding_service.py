"""Embedding service for generating vector embeddings."""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating text embeddings using ONNX-optimized models."""

    _instance: "EmbeddingService | None" = None

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    ) -> None:
        self.model_name = model_name
        self._model: Any | None = None
        self._tokenizer: Any | None = None
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(
        cls,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    ) -> "EmbeddingService":
        if cls._instance is None or cls._instance.model_name != model_name:
            cls._instance = cls(model_name)
        return cls._instance

    @property
    def embedding_dim(self) -> int:
        """Get embedding dimension for the model."""
        if "all-MiniLM-L6-v2" in self.model_name:
            return 384
        if any(
            name in self.model_name
            for name in ["all-mpnet-base-v2", "all-distilroberta-v1"]
        ):
            return 768
        return 768

    def _mean_pooling(self, model_output: Any, attention_mask: Any) -> Any:
        """Apply mean pooling to token embeddings using attention mask."""
        import torch

        token_embeddings = model_output[0]
        input_mask_expanded = (
            attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        )
        return torch.sum(
            token_embeddings * input_mask_expanded,
            1,
        ) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

    def _get_model(self) -> tuple[Any, Any]:
        """Get or initialize the model and tokenizer on first use."""
        if self._model is None or self._tokenizer is None:
            from optimum.onnxruntime import (
                ORTModelForFeatureExtraction,
            )
            from transformers import AutoTokenizer

            logger.info("Loading ONNX embedding model: %s", self.model_name)
            self._model = ORTModelForFeatureExtraction.from_pretrained(
                self.model_name,
                export=True,
            )
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            logger.info(
                "Model loaded, embedding dimension: %d",
                self.embedding_dim,
            )
        return self._model, self._tokenizer

    async def generate_embedding(self, text: str) -> list[float]:
        """Generate embedding for a single text string."""
        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self._generate_embedding_sync,
                text,
            )

    def _generate_embedding_sync(self, text: str) -> list[float]:
        """Synchronous embedding generation."""
        import torch

        if not text or not text.strip():
            msg = "Cannot generate embedding for empty or whitespace-only text"
            raise ValueError(msg)

        model, tokenizer = self._get_model()
        inputs = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True,
        )
        outputs = model(**inputs)
        embeddings = self._mean_pooling(outputs, inputs["attention_mask"])
        embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        result: list[float] = embeddings[0].tolist()
        return result

    async def generate_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts."""
        async with self._lock:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self._generate_embeddings_sync,
                texts,
            )

    def _generate_embeddings_sync(self, texts: list[str]) -> list[list[float]]:
        """Synchronous batch embedding generation."""
        import torch

        if not texts:
            msg = "Cannot generate embeddings for empty list"
            raise ValueError(msg)

        for i, text in enumerate(texts):
            if not text or not text.strip():
                msg = (
                    "Cannot generate embedding for empty or "
                    f"whitespace-only text at index {i}"
                )
                raise ValueError(msg)

        model, tokenizer = self._get_model()
        inputs = tokenizer(
            texts,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=512,
        )
        outputs = model(**inputs)
        embeddings = self._mean_pooling(outputs, inputs["attention_mask"])
        embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        return [emb.tolist() for emb in embeddings]

    async def generate_from_dict(self, data: dict[str, Any]) -> list[float]:
        """Generate embedding from dictionary data."""
        text_parts: list[str] = []
        for key, value in data.items():
            if isinstance(value, str):
                text_parts.append(f"{key}: {value}")
            elif isinstance(value, list):
                text_parts.extend(
                    item if isinstance(item, str) else item["text"]
                    for item in value
                    if isinstance(item, str)
                    or (isinstance(item, dict) and "text" in item)
                )
            elif isinstance(value, dict):
                nested_text = self._extract_text_from_dict(value)
                if nested_text:
                    text_parts.append(nested_text)

        combined_text = " ".join(text_parts)
        if not combined_text.strip():
            msg = "No text content found in dictionary for embedding generation"
            raise ValueError(msg)
        return await self.generate_embedding(combined_text)

    def _extract_text_from_dict(self, data: dict[str, Any]) -> str:
        """Recursively extract text from nested dictionary."""
        text_parts: list[str] = []
        for key, value in data.items():
            if isinstance(value, str):
                text_parts.append(f"{key}: {value}")
            elif isinstance(value, list):
                text_parts.extend(item for item in value if isinstance(item, str))
            elif isinstance(value, dict):
                nested_text = self._extract_text_from_dict(value)
                if nested_text:
                    text_parts.append(nested_text)
        return " ".join(text_parts)


def get_embedding_service(
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> EmbeddingService:
    """Get or create singleton embedding service instance."""
    return EmbeddingService.get_instance(model_name)


class TestEmbeddingService(EmbeddingService):
    """Test-only embedding service that returns deterministic vectors."""

    def __init__(self, embedding_dim: int = 32) -> None:
        super().__init__(model_name="test")
        self._embedding_dim_override = embedding_dim

    @property
    def embedding_dim(self) -> int:
        return self._embedding_dim_override

    def _get_model(self) -> Any:
        """Return None (avoid loading external models)."""
        return None

    def _generate_embedding_sync(self, text: str) -> list[float]:
        """Generate deterministic vector derived from input length."""
        if not text or not text.strip():
            msg = "Cannot generate embedding for empty or whitespace-only text"
            raise ValueError(msg)
        vector = [0.0] * self.embedding_dim
        if vector:
            vector[0] = float(len(text))
            if self.embedding_dim > 1:
                vector[1] = 1.0
        return vector

    def _generate_embeddings_sync(self, texts: list[str]) -> list[list[float]]:
        """Generate deterministic vectors for all entries."""
        return [self._generate_embedding_sync(text) for text in texts]


def build_test_embedding_service(
    embedding_dim: int = 32,
) -> TestEmbeddingService:
    """Factory helper for tests that require deterministic embeddings."""
    return TestEmbeddingService(embedding_dim=embedding_dim)
