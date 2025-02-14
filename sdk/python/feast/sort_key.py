import warnings
from typing import Dict, Optional

from typeguard import typechecked

from feast.entity import Entity
from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortKey as SortKeyProto,
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
    default_sort_order: int  # Using the integer values from the SortOrder enum
    tags: Dict[str, str]
    description: str

    def __init__(
            self,
            name: str,
            value_type: ValueType,
            default_sort_order: int = SortOrder.ASC,
            tags: Optional[Dict[str, str]] = None,
            description: str = "",
    ):
        self.name = name
        self.value_type = value_type
        self.default_sort_order = default_sort_order
        self.tags = tags or {}
        self.description = description

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
        # Assuming ValueType.from_proto exists.
        vt = ValueType.from_proto(proto.value_type)
        return cls(
            name=proto.name,
            value_type=vt,
            default_sort_order=proto.default_sort_order,
            tags=dict(proto.tags),
            description=proto.description,
        )