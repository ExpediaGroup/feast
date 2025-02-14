import warnings
from typing import Dict, Optional

from typeguard import typechecked

from feast.entity import Entity
from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortKey as SortKeyProto,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortOrder,
)
from feast.value_type import ValueType

warnings.simplefilter("ignore", DeprecationWarning)

# DUMMY_ENTITY is a placeholder entity used in entityless FeatureViews
DUMMY_ENTITY_ID = "__dummy_id"
DUMMY_ENTITY_NAME = "__dummy"
DUMMY_ENTITY = Entity(
    name=DUMMY_ENTITY_NAME,
    join_keys=[DUMMY_ENTITY_ID],
)

ONLINE_STORE_TAG_SUFFIX = "online_store_"


@typechecked
class SortKey:
    """
    A helper class representing a sorting key for a SortedFeatureView.
    """

    name: str
    value_type: ValueType
    default_sort_order: SortOrder.Enum.ValueType
    tags: Dict[str, str]
    description: str

    def __init__(
        self,
        name: str,
        value_type: ValueType,
        default_sort_order: SortOrder.Enum.ValueType = SortOrder.ASC,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
    ):
        self.name = name
        self.value_type = value_type
        self.default_sort_order = default_sort_order
        self.tags = tags or {}
        self.description = description

    def ensure_valid(self):
        """
        Validates that the SortKey has the required fields.
        """
        if not self.name:
            raise ValueError("SortKey must have a non-empty name.")
        if not isinstance(self.value_type, ValueType):
            raise ValueError("SortKey must have a valid value_type of type ValueType.")
        if self.default_sort_order not in (SortOrder.ASC, SortOrder.DESC):
            raise ValueError(
                "SortKey default_sort_order must be either SortOrder.ASC or SortOrder.DESC."
            )

    def to_proto(self) -> SortKeyProto:
        proto = SortKeyProto(
            name=self.name,
            value_type=self.value_type.value,
            default_sort_order=self.default_sort_order,
            description=self.description,
        )
        proto.tags.update(self.tags)
        return proto

    @classmethod
    def from_proto(cls, proto: SortKeyProto) -> "SortKey":
        return cls(
            name=proto.name,
            value_type=ValueType(proto.value_type),
            default_sort_order=proto.default_sort_order,
            tags=dict(proto.tags),
            description=proto.description,
        )
