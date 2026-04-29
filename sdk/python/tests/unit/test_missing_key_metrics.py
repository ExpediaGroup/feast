import os
from unittest.mock import MagicMock

from feast._missing_key_metrics import LookupMetricsAggregator, _extract_feature_view
from feast.metrics_client import (
    NoOpStatsdClient,
    get_metrics_client,
    set_metrics_client,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureList,
    FieldStatus,
    GetOnlineFeaturesResponse,
    GetOnlineFeaturesResponseMetadata,
)


class FakeMetricsClient:
    def __init__(self):
        self.calls = []

    def increment(self, metric, value=1, tags=None):
        self.calls.append({"metric": metric, "value": value, "tags": tags or []})


class TestLookupMetricsAggregator:
    def test_not_found_only(self):
        fake = FakeMetricsClient()
        agg = LookupMetricsAggregator("proj", "redis", "svc", "test", fake)

        agg.record("user_fv__age", FieldStatus.NOT_FOUND)
        agg.record("user_fv__age", FieldStatus.NOT_FOUND)
        agg.record("user_fv__age", FieldStatus.NOT_FOUND)
        agg.emit()

        assert len(fake.calls) == 1
        assert (
            fake.calls[0]["metric"] == "feast.feature_server.feature_lookup_not_found"
        )
        assert fake.calls[0]["value"] == 3
        assert "feature:user_fv__age" in fake.calls[0]["tags"]

    def test_null_or_expired(self):
        fake = FakeMetricsClient()
        agg = LookupMetricsAggregator("proj", "redis", "svc", "test", fake)

        agg.record("order_fv__amt", FieldStatus.NULL_VALUE)
        agg.record("order_fv__amt", FieldStatus.OUTSIDE_MAX_AGE)
        agg.emit()

        assert len(fake.calls) == 1
        assert (
            fake.calls[0]["metric"]
            == "feast.feature_server.feature_lookup_null_or_expired"
        )
        assert fake.calls[0]["value"] == 2

    def test_mixed_statuses(self):
        fake = FakeMetricsClient()
        agg = LookupMetricsAggregator("proj", "redis", "svc", "test", fake)

        agg.record("fv_a__f1", FieldStatus.PRESENT)
        agg.record("fv_a__f1", FieldStatus.NOT_FOUND)
        agg.record("fv_b__f2", FieldStatus.NULL_VALUE)
        agg.record("fv_b__f2", FieldStatus.PRESENT)
        agg.record("fv_b__f2", FieldStatus.OUTSIDE_MAX_AGE)
        agg.emit()

        assert len(fake.calls) == 2
        metrics_by_feature = {}
        for call in fake.calls:
            feature_tag = [t for t in call["tags"] if t.startswith("feature:")]
            key = (call["metric"], feature_tag[0] if feature_tag else "")
            metrics_by_feature[key] = call["value"]

        assert (
            metrics_by_feature[
                (
                    "feast.feature_server.feature_lookup_not_found",
                    "feature:fv_a__f1",
                )
            ]
            == 1
        )
        assert (
            metrics_by_feature[
                (
                    "feast.feature_server.feature_lookup_null_or_expired",
                    "feature:fv_b__f2",
                )
            ]
            == 2
        )

    def test_all_present(self):
        fake = FakeMetricsClient()
        agg = LookupMetricsAggregator("proj", "redis", "svc", "test", fake)

        agg.record("fv__f1", FieldStatus.PRESENT)
        agg.record("fv__f1", FieldStatus.PRESENT)
        agg.emit()

        assert len(fake.calls) == 0

    def test_none_client(self):
        agg = LookupMetricsAggregator("proj", "redis", "svc", "test", None)
        agg.record("fv__f1", FieldStatus.NOT_FOUND)
        agg.emit()

    def test_tags(self):
        fake = FakeMetricsClient()
        agg = LookupMetricsAggregator("mlpfs", "eg-valkey", "ranking-fs", "dw", fake)

        agg.record("hotel_fv__price", FieldStatus.NOT_FOUND)
        agg.emit()

        tags = fake.calls[0]["tags"]
        assert "project:mlpfs" in tags
        assert "online_store_type:eg-valkey" in tags
        assert "service:ranking-fs" in tags
        assert "env:dw" in tags
        assert "feature:hotel_fv__price" in tags
        assert "feature_view:hotel_fv" in tags


class TestMetricsClientRegistry:
    def test_default_is_noop(self):
        set_metrics_client(NoOpStatsdClient())
        client = get_metrics_client()
        assert isinstance(client, NoOpStatsdClient)

    def test_set_and_get(self):
        fake = FakeMetricsClient()
        set_metrics_client(fake)
        assert get_metrics_client() is fake
        set_metrics_client(NoOpStatsdClient())


class TestFeatureViewExtraction:
    def test_standard_format(self):
        assert _extract_feature_view("hotel_fv__price") == "hotel_fv"
        assert _extract_feature_view("user_fv__age") == "user_fv"
        assert (
            _extract_feature_view("ranking_signals_fv__score") == "ranking_signals_fv"
        )

    def test_multiple_underscores_in_feature_name(self):
        # Only split on first __ occurrence
        assert _extract_feature_view("hotel_fv__review_score_avg") == "hotel_fv"

    def test_no_double_underscore(self):
        # Fallback to "unknown" for non-standard format
        assert _extract_feature_view("age") == "unknown"
        assert _extract_feature_view("hotel_fv:price") == "unknown"

    def test_empty_string(self):
        assert _extract_feature_view("") == "unknown"


class TestIsMissingKeyMetricsEnabled:
    def test_disabled_by_default(self):
        os.environ.pop("ENABLE_MISSING_KEY_METRICS", None)
        from feast.infra.online_stores.online_store import (
            _is_missing_key_metrics_enabled,
        )

        assert _is_missing_key_metrics_enabled() is False

    def test_enabled(self):
        os.environ["ENABLE_MISSING_KEY_METRICS"] = "true"
        from feast.infra.online_stores.online_store import (
            _is_missing_key_metrics_enabled,
        )

        try:
            assert _is_missing_key_metrics_enabled() is True
        finally:
            os.environ.pop("ENABLE_MISSING_KEY_METRICS", None)


class TestEmitMissingKeyMetricsIntegration:
    def test_with_proto_response(self):
        from feast.infra.online_stores.online_store import _emit_missing_key_metrics

        fake = FakeMetricsClient()
        set_metrics_client(fake)

        response = GetOnlineFeaturesResponse()
        response.metadata.CopyFrom(
            GetOnlineFeaturesResponseMetadata(
                feature_names=FeatureList(val=["fv_a__f1", "fv_a__f2"])
            )
        )
        fv1 = response.results.add()
        fv1.statuses.extend([FieldStatus.PRESENT, FieldStatus.NOT_FOUND])
        fv2 = response.results.add()
        fv2.statuses.extend([FieldStatus.NOT_FOUND, FieldStatus.NOT_FOUND])

        config = MagicMock()
        config.online_store.type = "redis"

        _emit_missing_key_metrics(config, "test_project", response)

        assert len(fake.calls) == 2
        calls_by_feature = {
            [t for t in c["tags"] if t.startswith("feature:")][0]: c for c in fake.calls
        }
        assert calls_by_feature["feature:fv_a__f1"]["value"] == 1
        assert calls_by_feature["feature:fv_a__f2"]["value"] == 2

        set_metrics_client(NoOpStatsdClient())
