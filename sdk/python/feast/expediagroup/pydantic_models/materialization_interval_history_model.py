"""
Pydantic Model for MaterializationIntervalHistoryEntry

Copyright 2023 Expedia Group
"""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict
from typing_extensions import Self

from feast.protos.feast.core.FeatureView_pb2 import (
    MaterializationIntervalHistoryEntry as MaterializationIntervalHistoryEntryProto,
)


class MaterializationIntervalHistoryEntryModel(BaseModel):
    """
    Pydantic model of a single entry in a feature view's full, uncapped
    materialization-interval history. Unlike most sibling models, there is
    no intermediate domain object for this type (it's a plain historical
    record, not a first-class Feast object) -- this converts directly
    to/from the proto message.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    feature_view_name: str
    project: str
    start_time: datetime
    end_time: datetime
    recorded_at: datetime

    def to_proto(self) -> MaterializationIntervalHistoryEntryProto:
        """
        Converts this model to its protobuf representation.
        """
        entry = MaterializationIntervalHistoryEntryProto(
            feature_view_name=self.feature_view_name,
            project=self.project,
        )
        entry.start_time.FromDatetime(self.start_time)
        entry.end_time.FromDatetime(self.end_time)
        entry.recorded_at.FromDatetime(self.recorded_at)
        return entry

    @classmethod
    def from_proto(
        cls,
        proto: MaterializationIntervalHistoryEntryProto,
    ) -> Self:  # type: ignore
        """
        Converts a MaterializationIntervalHistoryEntry proto to its pydantic
        model representation.
        """
        return cls(
            feature_view_name=proto.feature_view_name,
            project=proto.project,
            start_time=proto.start_time.ToDatetime().replace(tzinfo=timezone.utc),
            end_time=proto.end_time.ToDatetime().replace(tzinfo=timezone.utc),
            recorded_at=proto.recorded_at.ToDatetime().replace(tzinfo=timezone.utc),
        )
