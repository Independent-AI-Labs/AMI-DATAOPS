"""Graph relationship annotations and utilities for ORM-style graph persistence."""

from __future__ import annotations

from typing import Any, TypeVar, cast, get_args, get_origin, get_type_hints

from pydantic import BaseModel

T = TypeVar("T")


class GraphRelation:
    """Annotation for marking fields as graph edges."""

    def __init__(
        self,
        edge_name: str | None = None,
        reverse_name: str | None = None,
        target_type: str | type | None = None,
        cascade_delete: bool = False,
        eager_load: bool = False,
    ) -> None:
        self.edge_name = edge_name
        self.reverse_name = reverse_name
        self.target_type = target_type
        self.cascade_delete = cascade_delete
        self.eager_load = eager_load


class GraphNode:
    """Marker class for explicit node declaration."""


class GraphEdge:
    """Marker class for edge with properties (intermediate node)."""

    def __init__(self, properties: dict[str, Any] | None = None) -> None:
        self.properties = properties or {}


class GraphSchemaAnalyzer:
    """Analyzes Pydantic models to detect graph structure."""

    @classmethod
    def analyze_model(cls, model_cls: type[BaseModel]) -> dict[str, Any]:
        """Analyze a model to determine its graph semantics.

        Returns dict with: is_node, edges, properties, reverse_edges.
        """
        schema: dict[str, Any] = {
            "is_node": True,
            "edges": {},
            "properties": {},
            "reverse_edges": {},
            "model_name": model_cls.__name__,
        }
        hints = get_type_hints(model_cls, include_extras=True)

        for field_name, field_type in hints.items():
            if field_name.startswith("_"):
                continue
            if hasattr(field_type, "__metadata__"):
                graph_relation = None
                for metadata in field_type.__metadata__:
                    if isinstance(metadata, GraphRelation):
                        graph_relation = metadata
                        break
                if graph_relation:
                    edge_config = cls._build_edge_config(
                        field_name,
                        field_type,
                        graph_relation,
                    )
                    schema["edges"][field_name] = edge_config
                    if graph_relation.reverse_name:
                        schema["reverse_edges"][graph_relation.reverse_name] = {
                            "field": field_name,
                            "source_type": model_cls.__name__,
                        }
                else:
                    schema["properties"][field_name] = cls._get_base_type(
                        field_type,
                    )
            else:
                schema["properties"][field_name] = cls._get_base_type(
                    field_type,
                )
        return schema

    @classmethod
    def _build_edge_config(
        cls,
        field_name: str,
        field_type: Any,
        graph_relation: GraphRelation,
    ) -> dict[str, Any]:
        """Build edge configuration from field and GraphRelation."""
        base_type = cls._get_base_type(field_type)
        is_list = get_origin(base_type) in (list, tuple, set)

        if is_list:
            target_type: Any = get_args(base_type)[0] if get_args(base_type) else str
        else:
            target_type = base_type

        # FIX: check target_type directly instead of get_origin
        if target_type is type(None):
            target_type = get_args(target_type)[0] if get_args(target_type) else str

        if graph_relation.target_type:
            target_type = graph_relation.target_type

        return {
            "edge_name": graph_relation.edge_name or field_name,
            "target_type": cls._type_to_string(target_type),
            "is_list": is_list,
            "reverse_name": graph_relation.reverse_name,
            "cascade_delete": graph_relation.cascade_delete,
            "eager_load": graph_relation.eager_load,
            "field_name": field_name,
        }

    @classmethod
    def _get_base_type(cls, field_type: Any) -> Any:
        """Extract base type from Annotated."""
        if hasattr(field_type, "__metadata__"):
            return get_args(field_type)[0] if get_args(field_type) else field_type
        return field_type

    @classmethod
    def _type_to_string(cls, type_obj: Any) -> str:
        """Convert type to string representation."""
        if isinstance(type_obj, str):
            return type_obj
        if isinstance(type_obj, type):
            return type_obj.__name__
        if hasattr(type_obj, "__name__"):
            return cast(str, type_obj.__name__)
        msg = f"Cannot convert type {type(type_obj)} to string: {type_obj!r}"
        raise TypeError(msg)

    @classmethod
    def is_edge_field(
        cls,
        model_cls: type[BaseModel],
        field_name: str,
    ) -> bool:
        """Check if a field is an edge."""
        schema = cls.analyze_model(model_cls)
        return field_name in schema["edges"]

    @classmethod
    def get_edge_config(
        cls,
        model_cls: type[BaseModel],
        field_name: str,
    ) -> dict[str, Any]:
        """Get edge configuration for a field."""
        schema = cls.analyze_model(model_cls)
        edge_config = schema["edges"].get(field_name)
        if edge_config is None:
            msg = (
                f"Field '{field_name}' is not an edge field "
                f"in model {model_cls.__name__}"
            )
            raise ValueError(msg)
        return cast(dict[str, Any], edge_config)


class RelationalField:
    """Descriptor for deferred-load relational fields."""

    def __init__(
        self,
        edge_config: dict[str, Any],
        model_cls: type | None = None,
    ) -> None:
        self.edge_config = edge_config
        self.model_cls = model_cls
        self.field_name: str = edge_config["field_name"]
        self.edge_name: str = edge_config["edge_name"]
        self.is_list: bool = edge_config["is_list"]
        self.target_type: str = edge_config["target_type"]
        self.name: str = ""

        self._cache_attr = f"_{self.field_name}_cache"
        self._ids_attr = f"_{self.field_name}_ids"
        self._loaded_attr = f"_{self.field_name}_loaded"

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name
        if not self.model_cls:
            self.model_cls = owner

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        if hasattr(obj, self._cache_attr):
            return getattr(obj, self._cache_attr)
        if self.edge_config.get("eager_load") and not getattr(
            obj,
            self._loaded_attr,
            False,
        ):
            msg = (
                f"Eager load requested for '{self.field_name}' "
                "but objects not loaded. Call load_related() first."
            )
            raise RuntimeError(msg)
        return getattr(
            obj,
            self._ids_attr,
            [] if self.is_list else None,
        )

    def __set__(self, obj: Any, value: Any) -> None:
        if value is None:
            setattr(
                obj,
                self._ids_attr,
                [] if self.is_list else None,
            )
            if hasattr(obj, self._cache_attr):
                delattr(obj, self._cache_attr)
            return

        if self.is_list:
            if all(isinstance(v, str) for v in value):
                setattr(obj, self._ids_attr, value)
                if hasattr(obj, self._cache_attr):
                    delattr(obj, self._cache_attr)
            else:
                setattr(obj, self._cache_attr, value)
                setattr(
                    obj,
                    self._ids_attr,
                    [v.uid if hasattr(v, "uid") else str(v) for v in value],
                )
        elif isinstance(value, str):
            setattr(obj, self._ids_attr, value)
            if hasattr(obj, self._cache_attr):
                delattr(obj, self._cache_attr)
        else:
            setattr(obj, self._cache_attr, value)
            setattr(
                obj,
                self._ids_attr,
                value.uid if hasattr(value, "uid") else str(value),
            )

    async def load_related(self, obj: Any, dao: Any) -> Any:
        """Load related objects from the database."""
        if hasattr(obj, self._cache_attr):
            return getattr(obj, self._cache_attr)

        ids = getattr(
            obj,
            self._ids_attr,
            [] if self.is_list else None,
        )
        if not ids:
            return [] if self.is_list else None

        if self.is_list:
            objects = []
            missing_ids = []
            for obj_id in ids:
                related = await dao.find_by_id(obj_id)
                if related:
                    objects.append(related)
                else:
                    missing_ids.append(obj_id)
            if missing_ids:
                msg = f"Missing related objects for '{self.field_name}': {missing_ids}"
                raise ValueError(msg)
            setattr(obj, self._cache_attr, objects)
            return objects

        related = await dao.find_by_id(ids)
        if related is None:
            msg = f"Missing related object for '{self.field_name}': ID {ids}"
            raise ValueError(msg)
        setattr(obj, self._cache_attr, related)
        return related


class GraphQueryBuilder:
    """Build Dgraph queries with relationship traversal."""

    def __init__(self, model_cls: type[BaseModel]) -> None:
        self.model_cls = model_cls
        self.schema = GraphSchemaAnalyzer.analyze_model(model_cls)
        self.query_parts: list[dict[str, Any]] = []
        self.filters: list[dict[str, Any]] = []

    def with_edges(self, *edge_names: str) -> GraphQueryBuilder:
        """Include specific edges in the query."""
        for edge_name in edge_names:
            edge_config = self.schema["edges"].get(edge_name)
            if edge_config:
                self.query_parts.append(
                    {
                        "type": "edge",
                        "name": edge_config["edge_name"],
                        "config": edge_config,
                    }
                )
        return self

    def with_all_edges(self) -> GraphQueryBuilder:
        """Include all edges in the query."""
        for edge_config in self.schema["edges"].values():
            self.query_parts.append(
                {
                    "type": "edge",
                    "name": edge_config["edge_name"],
                    "config": edge_config,
                }
            )
        return self

    def filter_by(self, **kwargs: Any) -> GraphQueryBuilder:
        """Add filters to the query."""
        for field, value in kwargs.items():
            self.filters.append(
                {
                    "field": field,
                    "value": value,
                    "op": "eq",
                }
            )
        return self

    def build(self) -> str:
        """Build the final Dgraph query."""
        lines = ["{"]
        if self.filters:
            func_parts = [
                f'eq({self.model_cls.__name__}.{f["field"]}, "{f["value"]}")'
                for f in self.filters
            ]
            func = " AND ".join(func_parts)
            lines.append(f"  result(func: {func}) {{")
        else:
            lines.append(
                f"  result(func: type({self.model_cls.__name__})) {{",
            )

        lines.append("    uid")
        lines.append("    expand(_all_)")
        for part in self.query_parts:
            if part["type"] == "edge":
                edge_name = part["name"]
                lines.append(f"    {edge_name} {{")
                lines.append("      uid")
                lines.append("      expand(_all_)")
                lines.append("    }")
        lines.append("  }")
        lines.append("}")
        return "\n".join(lines)
