# Regression fixture for EAPC-22316 follow-up: a SortedFeatureView whose
# UNIX_TIMESTAMP sort key is requested as a feature must retain millisecond
# precision for rows that differ by less than one second (Go read-side fix in
# InterfaceToProtoValue, go/types/typeconversion.go).

from datetime import timedelta

from feast import Entity, Field, FileSource, SortedFeatureView
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.sort_key import SortKey
from feast.types import String, UnixTimestamp
from feast.value_type import ValueType

tags = {"team": "Feast"}
owner = "test@test.com"

sub_second_entity: Entity = Entity(
    name="sub_second_entity",
    join_keys=["sub_second_entity_id"],
    value_type=ValueType.STRING,
    tags=tags,
    owner=owner,
)

sub_second_source: FileSource = FileSource(
    path="sub_second_data.parquet", timestamp_field="event_timestamp"
)

sub_second_sort_key_view: SortedFeatureView = SortedFeatureView(
    name="sub_second_sort_key_view",
    entities=[sub_second_entity],
    ttl=timedelta(days=0),
    source=sub_second_source,
    tags=tags,
    description="Regression fixture: sort key rows <1s apart for the same entity",
    owner=owner,
    online=True,
    sort_keys=[
        SortKey(
            name="event_timestamp",
            value_type=ValueType.UNIX_TIMESTAMP,
            default_sort_order=SortOrder.DESC,
        )
    ],
    schema=[
        Field(name="value", dtype=String),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
)
