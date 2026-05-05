import logging
import os
import random
from collections import Counter
from typing import List

from feast.protos.feast.serving.ServingService_pb2 import FieldStatus

logger = logging.getLogger(__name__)


def _extract_feature_view(feature_name: str) -> str:
    """Extract feature view name from full feature name.

    Feature names follow the format: feature_view__feature_name
    Example: "hotel_fv__price" -> "hotel_fv"

    Args:
        feature_name: Full feature name with feature view prefix

    Returns:
        Feature view name, or "unknown" if format doesn't match
    """
    if "__" in feature_name:
        return feature_name.split("__", 1)[0]
    return "unknown"


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

        # Read sampling rate from environment (default: 1.0 = no sampling)
        sample_rate_str = os.getenv("FEAST_METRICS_SAMPLE_RATE", "1.0")
        try:
            self.sample_rate = float(sample_rate_str)
            # Validate: must be between 0 and 1
            if not 0 < self.sample_rate <= 1.0:
                logger.warning(
                    f"Invalid FEAST_METRICS_SAMPLE_RATE={sample_rate_str}, using 1.0"
                )
                self.sample_rate = 1.0
        except ValueError:
            logger.warning(
                f"Invalid FEAST_METRICS_SAMPLE_RATE={sample_rate_str}, using 1.0"
            )
            self.sample_rate = 1.0

    def record(self, feature_id: str, status: int) -> None:
        if status == FieldStatus.NOT_FOUND:
            self.not_found[feature_id] += 1
        elif status in (FieldStatus.NULL_VALUE, FieldStatus.OUTSIDE_MAX_AGE):
            self.null_or_expired[feature_id] += 1

    def emit(self) -> None:
        if self.metrics_client is None:
            return

        # Probabilistic sampling: skip this request's metrics based on sample_rate
        if self.sample_rate < 1.0 and random.random() > self.sample_rate:
            return

        # Calculate multiplier to preserve statistical accuracy
        # If sample_rate=0.1, we only emit 10% of the time, so multiply counts by 10
        multiplier = 1.0 / self.sample_rate

        base_tags: List[str] = [
            f"project:{self.project}",
            f"online_store_type:{self.online_store_type}",
            f"service:{self.service}",
            f"env:{self.env}",
        ]

        for feat, cnt in self.not_found.items():
            if cnt:
                # Adjust count to preserve accuracy when sampling
                adjusted_count = int(cnt * multiplier)
                self.metrics_client.increment(
                    "feast.feature_server.feature_lookup_not_found",
                    adjusted_count,
                    tags=base_tags
                    + [
                        f"feature:{feat}",
                        f"feature_view:{_extract_feature_view(feat)}",
                    ],
                )

        for feat, cnt in self.null_or_expired.items():
            if cnt:
                adjusted_count = int(cnt * multiplier)
                self.metrics_client.increment(
                    "feast.feature_server.feature_lookup_null_or_expired",
                    adjusted_count,
                    tags=base_tags
                    + [
                        f"feature:{feat}",
                        f"feature_view:{_extract_feature_view(feat)}",
                    ],
                )
