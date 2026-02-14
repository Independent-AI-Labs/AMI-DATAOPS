"""Configuration objects for DataOps sensitive field handling."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from ami.models.security import DataClassification


class SensitiveFieldConfig(BaseModel):
    """Metadata registered for a sensitive field."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    mask_pattern: str
    classification: DataClassification | None = None
    namespace: str | None = None
    auto_rotate_days: int | None = None

    def mask_value(self, field_name: str) -> str:
        """Render the mask pattern for a field."""
        pattern = self.mask_pattern
        if "{field}" in pattern:
            return pattern.format(field=field_name)
        return pattern
