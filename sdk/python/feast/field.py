# Copyright 2022 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, field_validator
from typeguard import check_type, typechecked

from feast.feature import Feature
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FieldProto
from feast.protos.feast.types import Value_pb2 as ValueProto
from feast.types import FeastType, from_string, from_value_type
from feast.value_type import ValueType


@typechecked
class Field(BaseModel):
    """
    A Field represents a set of values with the same structure.

    Attributes:
        name: The name of the field.
        dtype: The type of the field, such as string or float.
        description: A human-readable description.
        tags: User-defined metadata in dictionary form.
        vector_index: If set to True the field will be indexed for vector similarity search.
        vector_length: The length of the vector if the vector index is set to True.
        vector_search_metric: The metric used for vector similarity search.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    name: str
    dtype: FeastType
    description: str = ""
    tags: Optional[Dict[str, str]] = {}
    vector_index: bool = False
    vector_length: int = 0
    vector_search_metric: Optional[str] = ""
    default_value: Optional[ValueProto.Value] = None

    @field_validator("default_value")
    @classmethod
    def validate_default_value_type(
        cls, v: Optional[ValueProto.Value], info: Any
    ) -> Optional[ValueProto.Value]:
        """
        Validate that default_value type matches the field's dtype.
        """
        if v is None:
            return v

            # Get dtype from the model data
        dtype = info.data.get("dtype")
        if dtype is None:
            # dtype will be validated by its own validator, skip for now
            return v

            # Validate type compatibility
        value_type = dtype.to_value_type()
        val_case = v.WhichOneof("val")

        if val_case is None:
            # Empty Value proto
            return v

            # Map proto value types to ValueType enums
        type_mapping: Dict[str, ValueType] = {
            "int32_val": ValueType.INT32,
            "int64_val": ValueType.INT64,
            "double_val": ValueType.DOUBLE,
            "float_val": ValueType.FLOAT,
            "string_val": ValueType.STRING,
            "bytes_val": ValueType.BYTES,
            "bool_val": ValueType.BOOL,
            "unix_timestamp_val": ValueType.UNIX_TIMESTAMP,
            "int32_list_val": ValueType.INT32_LIST,
            "int64_list_val": ValueType.INT64_LIST,
            "double_list_val": ValueType.DOUBLE_LIST,
            "float_list_val": ValueType.FLOAT_LIST,
            "string_list_val": ValueType.STRING_LIST,
            "bytes_list_val": ValueType.BYTES_LIST,
            "bool_list_val": ValueType.BOOL_LIST,
            "unix_timestamp_list_val": ValueType.UNIX_TIMESTAMP_LIST,
        }

        expected_type = type_mapping.get(val_case)
        if expected_type != value_type:
            raise ValueError(
                f"default_value type '{val_case}' does not match field dtype '{dtype}' "
                f"(expected ValueType.{value_type.name})"
            )

        return v

    @field_validator("dtype", mode="before")
    def dtype_is_feasttype_or_string_feasttype(cls, v):
        """
        dtype must be a FeastType, but to allow wire transmission,
        it is necessary to allow string representations of FeastTypes.
        We therefore allow dtypes to be specified as strings which are
        converted to FeastTypes at time of definition.
        TO-DO: Investigate whether FeastType can be refactored to a json compatible
        format.
        """
        try:
            check_type(v, FeastType)  # type: ignore
        except TypeError:
            try:
                check_type(v, str)
                return from_string(v)
            except TypeError:
                raise TypeError("dtype must be of type FeastType")
        return v

    def __eq__(self, other):
        if type(self) is not type(other):
            return False

        if (
            self.name != other.name
            or self.dtype != other.dtype
            or self.description != other.description
            or self.tags != other.tags
            or self.vector_length != other.vector_length
            # or self.vector_index != other.vector_index
            # or self.vector_search_metric != other.vector_search_metric
        ):
            return False

        # Compare default_value - handle None and proto Value comparison
        if self.default_value is None and other.default_value is None:
            pass  # Both None, equal
        elif self.default_value is None or other.default_value is None:
            return False  # One is None, other is not
        elif self.default_value.SerializeToString() != other.default_value.SerializeToString():
            return False  # Both are Values but different

        return True

    def __hash__(self):
        return hash((self.name, hash(self.dtype)))

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return (
            f"Field(\n"
            f"    name={self.name!r},\n"
            f"    dtype={self.dtype!r},\n"
            f"    description={self.description!r},\n"
            f"    tags={self.tags!r}\n"
            f"    vector_index={self.vector_index!r}\n"
            f"    vector_length={self.vector_length!r}\n"
            f"    vector_search_metric={self.vector_search_metric!r}\n"
            f")"
        )

    def __str__(self):
        return f"Field(name={self.name}, dtype={self.dtype}, tags={self.tags})"

    def to_proto(self) -> FieldProto:
        """Converts a Field object to its protobuf representation."""
        value_type = self.dtype.to_value_type()
        vector_search_metric = self.vector_search_metric or ""
        proto = FieldProto(
            name=self.name,
            value_type=value_type.value,
            description=self.description,
            tags=self.tags,
            vector_index=self.vector_index,
            vector_length=self.vector_length,
            vector_search_metric=vector_search_metric,
        )
        # Add default_value if present (using type: ignore until proto is regenerated)
        if self.default_value is not None:
            proto.default_value.CopyFrom(self.default_value)  # type: ignore[attr-defined]

        return proto

    @classmethod
    def from_proto(cls, field_proto: FieldProto):
        """
        Creates a Field object from a protobuf representation.

        Args:
            field_proto: FieldProto protobuf object
        """
        value_type = ValueType(field_proto.value_type)
        vector_search_metric = getattr(field_proto, "vector_search_metric", "")
        vector_index = getattr(field_proto, "vector_index", False)
        vector_length = getattr(field_proto, "vector_length", 0)
        # Extract default_value if present
        default_value = getattr(field_proto, "default_value", None)
        if default_value is not None and not default_value.WhichOneof("val"):
            # Empty Value proto, treat as None
            default_value = None
        return cls(
            name=field_proto.name,
            dtype=from_value_type(value_type=value_type),
            tags=dict(field_proto.tags),
            description=field_proto.description,
            vector_index=vector_index,
            vector_length=vector_length,
            vector_search_metric=vector_search_metric,
            default_value=default_value,
        )

    @classmethod
    def from_feature(cls, feature: Feature):
        """
        Creates a Field object from a Feature object.

        Args:
            feature: Feature object to convert.
        """
        return cls(
            name=feature.name,
            dtype=from_value_type(feature.dtype),
            description=feature.description,
            tags=feature.labels,
            default_value=feature.default_value,
        )
