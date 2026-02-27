from typing import Any, Dict, Optional, Union

from google.protobuf.json_format import MessageToDict, ParseDict
from pydantic import BaseModel, ConfigDict, Field as PydanticField, field_serializer, field_validator
from typing_extensions import Self

from feast.field import Field
from feast.protos.feast.types import Value_pb2 as ValueProto
from feast.types import Array, PrimitiveFeastType


class FieldModel(BaseModel):
    """
    Pydantic Model of a Feast Field.
    """

    name: str
    dtype: Union[Array, PrimitiveFeastType]
    description: str = ""
    tags: Optional[Dict[str, str]] = {}
    vector_index: bool = False
    vector_length: int = 0
    vector_search_metric: Optional[str] = None
    default_value: Optional[ValueProto.Value] = PydanticField(
        default=None,
        serialization_alias="defaultValue",
        validation_alias="defaultValue"
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_schema_serialization_defaults_required=False,
        # Exclude None values from JSON to maintain backwards compatibility
        # Fields without defaults won't have defaultValue key in JSON
        ser_json_timedelta='float',
        ser_json_bytes='base64'
    )

    @field_serializer("default_value", when_used='json')
    def serialize_default_value(self, value: Optional[ValueProto.Value]) -> Optional[Dict[str, Any]]:
        """
        Serialize proto Value to JSON-compatible dict using MessageToDict.
        Returns camelCase keys (int64Val, stringVal, etc.) per proto JSON format.
        Returns None for fields without defaults (will be excluded from JSON via mode='omit' below).
        """
        if value is None:
            return None
        return MessageToDict(value, preserving_proto_field_name=False)

    @field_validator("default_value", mode="before")
    @classmethod
    def validate_default_value(cls, v: Any) -> Optional[ValueProto.Value]:
        """
        Validate default_value: accepts proto Value object or dict.
        When receiving dict (from JSON), convert to proto Value using ParseDict.
        Note: ParseDict handles base64-encoded bytes automatically for bytesVal fields.
        """
        if v is None:
            return None
        if isinstance(v, ValueProto.Value):
            return v
        if isinstance(v, dict):
            return ParseDict(v, ValueProto.Value())
        return v

    def to_field(self) -> Field:
        """
        Given a Pydantic FieldModel, create and return a Field.

        Returns:
            A Field.
        """
        return Field(
            name=self.name,
            dtype=self.dtype,
            description=self.description,
            tags=self.tags,
            vector_index=self.vector_index,
            vector_length=self.vector_length,
            vector_search_metric=self.vector_search_metric,
            default_value=self.default_value,
        )

    @classmethod
    def from_field(
        cls,
        field: Field,
    ) -> Self:  # type: ignore
        """
        Converts a Field object to its pydantic FieldModel representation.

        Returns:
            A FieldModel.
        """
        return cls(
            name=field.name,
            dtype=field.dtype,  # type: ignore
            description=field.description,
            tags=field.tags,
            vector_index=field.vector_index,
            vector_length=field.vector_length,
            vector_search_metric=field.vector_search_metric,
            default_value=field.default_value,
        )
