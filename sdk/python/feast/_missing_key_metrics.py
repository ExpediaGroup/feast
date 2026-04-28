import logging
from collections import Counter
from typing import List

from feast.protos.feast.serving.ServingService_pb2 import FieldStatus

logger = logging.getLogger(__name__)


class LookupMetricsAggregator:
    def __init__(
        self,
        project: str,
        online_store_type: str,
        service: str,
        env: str,
        metrics_client,
    ):
        self.project = project
        self.online_store_type = online_store_type
        self.service = service or "unknown_service"
        self.env = env or "unknown_env"
        self.metrics_client = metrics_client
        self.not_found: Counter = Counter()
        self.null_or_expired: Counter = Counter()

    def record(self, feature_id: str, status: int) -> None:
        if status == FieldStatus.NOT_FOUND:
            self.not_found[feature_id] += 1
        elif status in (FieldStatus.NULL_VALUE, FieldStatus.OUTSIDE_MAX_AGE):
            self.null_or_expired[feature_id] += 1

    def emit(self) -> None:
        if self.metrics_client is None:
            return

        base_tags: List[str] = [
            f"project:{self.project}",
            f"online_store_type:{self.online_store_type}",
            f"service:{self.service}",
            f"env:{self.env}",
        ]

        for feat, cnt in self.not_found.items():
            if cnt:
                self.metrics_client.increment(
                    "feast.feature_server.feature_lookup_not_found",
                    cnt,
                    tags=base_tags + [f"feature:{feat}"],
                )

        for feat, cnt in self.null_or_expired.items():
            if cnt:
                self.metrics_client.increment(
                    "feast.feature_server.feature_lookup_null_or_expired",
                    cnt,
                    tags=base_tags + [f"feature:{feat}"],
                )
