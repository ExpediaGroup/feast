# Copyright 2026 The Feast Authors
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

"""Unit tests for the EG Feast -> Metadata Bus OpenLineage emitter.

Scope (metadata-bus-user-guide PR #21, EAPC-22333): ``feast apply`` emits exactly
one ``DatasetEvent`` per FeatureView -- no source/entity/service nodes, no
``RunEvent`` edges. Standard concerns ride on standard OpenLineage facets; the
Feast residue rides on ``feast_*`` custom facets. Stream-backed views are
discriminated by ``datasetType`` and keep both upstreams.
"""

from datetime import timedelta

import pytest

pytest.importorskip("openlineage.client")

from openlineage.client.event_v2 import DatasetEvent, RunEvent  # noqa: E402

from feast import (  # noqa: E402
    Entity,
    FeatureService,
    FeatureView,
    Field,
)
from feast.data_format import AvroFormat  # noqa: E402
from feast.data_source import KafkaSource  # noqa: E402
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (  # noqa: E402
    SparkSource,
)
from feast.openlineage.client import FeastOpenLineageClient  # noqa: E402
from feast.openlineage.config import OpenLineageConfig  # noqa: E402
from feast.openlineage.emitter import FeastOpenLineageEmitter  # noqa: E402
from feast.types import Float32, Int64  # noqa: E402
from feast.value_type import ValueType  # noqa: E402

PROJECT = "hcom_feast_store"
FV_DATASET = "hcom_feast_store.hotel_price_features"


@pytest.fixture
def captured(monkeypatch):
    events = []
    monkeypatch.setenv("REGISTRY_ENV", "dw")
    monkeypatch.delenv("CONTROL_PLANE_ENVIRONMENT", raising=False)
    # Region defaults to the MLP us-east-1 constant; pin it deterministically.
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setattr(
        FeastOpenLineageClient,
        "emit",
        lambda self, event: (events.append(event), True)[1],
    )
    return events


def _emitter(**overrides):
    kwargs = dict(
        enabled=True,
        transport_type="http",
        transport_url="http://localhost:9999",
    )
    kwargs.update(overrides)
    emitter = FeastOpenLineageEmitter(OpenLineageConfig(**kwargs))
    assert emitter.is_enabled
    return emitter


def _batch_feature_view():
    source = SparkSource(
        name="hotel_price_batch_source",
        table="prod_offline_feature_store.feature_store_hotel_price",
        timestamp_field="event_timestamp",
    )
    hotel = Entity(name="hotel_id", join_keys=["hotel_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="hotel_price_features",
        entities=[hotel],
        ttl=timedelta(days=1),
        source=source,
        schema=[
            Field(name="base_price", dtype=Float32, description="nightly base price"),
            Field(name="price_bucket", dtype=Int64),
        ],
        online=True,
        description="Hotel price signals for ranking and forecasting",
        owner="mlp-feature-team",
        # The EG mandatory tags (application/team/owner/product/costCenter), the
        # governed eg-data-product tag, and a custom tag -- to exercise re-keying
        # + duplicate-tag dropping (EAPC-22333).
        tags={
            "domain": "hotels",
            "application": "hotel-price-app",
            "team": "mlp-feature-team-dl",
            "owner": "mlp-owner@expediagroup.com",
            "product": "unified-feature-store",
            "eg-data-product": "commerce-attribute-store",
            "costCenter": "12345",
        },
    )
    return source, hotel, fv


def _stream_feature_view():
    batch = SparkSource(
        name="example_stream_batch_source",
        table="prod_offline_feature_store.example_streaming",
        timestamp_field="published_timestamp",
    )
    kafka = KafkaSource(
        name="example_stream_kafka_source",
        timestamp_field="published_timestamp",
        message_format=AvroFormat("{}"),
        kafka_bootstrap_servers="broker:9092",
        topic="urn:egsp:consumer:data:mlpfs_stream_example:1:consumer:aws_us_east_1",
        batch_source=batch,
        watermark_delay_threshold=timedelta(minutes=5),
        field_mapping={"event_header.published_datetime_utc": "published_timestamp"},
    )
    hotel = Entity(name="hotel_id", join_keys=["hotel_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="example_streaming_view",
        entities=[hotel],
        ttl=timedelta(days=1),
        source=kafka,
        schema=[Field(name="click_count", dtype=Int64)],
        online=True,
    )
    return fv


# --------------------------------------------------------------------- batch


def test_batch_feature_view_dataset_event(captured):
    _, _, fv = _batch_feature_view()
    _emitter().emit_apply([fv], PROJECT)

    dataset_events = [e for e in captured if isinstance(e, DatasetEvent)]
    assert len(dataset_events) == 1
    event = dataset_events[0]
    assert event.dataset.namespace == "mlp://mlpfs-dw"
    assert event.dataset.name == FV_DATASET

    facets = event.dataset.facets
    assert facets["lifecycleStateChange"].lifecycleStateChange == "CREATE"
    assert facets["documentation"].description.startswith("Hotel price signals")
    assert facets["ownership"].owners[0].name == "mlp-feature-team"
    assert facets["ownership"].owners[0].type == "maintainer"

    tags = {t.key: t.value for t in facets["tags"].tags}
    assert tags["domain"] == "hotels"  # custom tag preserved verbatim
    assert tags["costCenter"] == "12345"  # non-duplicate mandatory tag preserved
    assert tags["ttl_seconds"] == "86400"
    # `application` is re-keyed onto the governed eg-application-name tag; the
    # view's own eg-data-product rides through as-is (the relay resolves it to a
    # data-mesh domain + owners) and is *not* taken from the MLP `product` tag.
    assert tags["eg-application-name"] == "hotel-price-app"
    assert tags["eg-data-product"] == "commerce-attribute-store"
    # EG mandatory tags dropped from the tags facet to avoid duplicate info (EGDL
    # feedback): `application` is re-keyed above, `product` is not a data-mesh
    # product name so it is dropped outright, team/owner ride on the ownership
    # facet.
    assert "application" not in tags
    assert "product" not in tags
    assert "team" not in tags
    assert "owner" not in tags

    field_names = {f.name for f in facets["schema"].fields}
    assert {"hotel_id", "base_price", "price_bucket"} <= field_names
    by_field = {f.name: f for f in facets["schema"].fields}
    assert by_field["base_price"].type == "Float32"
    assert by_field["base_price"].description == "nightly base price"
    # A feature with no description emits an empty string, never a null/absent
    # field; the OL client's default empty nested `fields` list is pinned to None
    # so it drops out of the serialized event (EGDL feedback, EAPC-22333).
    assert by_field["price_bucket"].description == ""
    assert all(f.fields is None for f in facets["schema"].fields)

    assert facets["dataSource"].name == "hotel_price_batch_source"
    assert (
        facets["dataSource"].uri
        == "egdl://data-dw.us-east-1/prod_offline_feature_store.feature_store_hotel_price"
    )
    assert facets["datasetType"].datasetType == "TABLE"
    assert facets["datasetType"].subType == "BATCH_SPARK"

    ffv = facets["feast_featureView"]
    assert ffv.entities == ["hotel_id"]
    assert ffv.online_enabled is True
    assert ffv.timestamp_field == "event_timestamp"
    # EG divergence: generic OL DatasetFacet schema, not feast.dev/spec.
    assert ffv._schemaURL.endswith("#/$defs/DatasetFacet")

    # No RunEvents, and trimmed custom facet drops standardized fields.
    assert not [e for e in captured if isinstance(e, RunEvent)]
    assert not hasattr(ffv, "features")
    assert not hasattr(ffv, "ttl_seconds")


def test_aws_region_env_overrides_default(captured, monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    _, _, fv = _batch_feature_view()
    _emitter().emit_apply([fv], PROJECT)

    event = next(e for e in captured if isinstance(e, DatasetEvent))
    # No config option: region comes from AWS_REGION, else the us-east-1 default.
    assert event.dataset.facets["dataSource"].uri == (
        "egdl://data-dw.us-west-2/prod_offline_feature_store.feature_store_hotel_price"
    )


def test_entity_name_differs_from_join_key(captured):
    # Entity name != join_key (e.g. name ``flyte_sample_key`` / join_key ``long_id``).
    # ``long_id`` in the schema is moved into entity_columns by FeatureView.__init__,
    # reproducing the post-inference shape that used to emit a phantom, type-less
    # column for the entity *name*.
    source = SparkSource(
        name="flyte_sample_source",
        table="data_corp_offline_feature_store_dev.feature_store_flyte_sample_table",
        timestamp_field="event_timestamp",
    )
    key = Entity(
        name="flyte_sample_key", join_keys=["long_id"], value_type=ValueType.INT64
    )
    fv = FeatureView(
        name="flyte_fv_3",
        entities=[key],
        ttl=timedelta(days=25),
        source=source,
        schema=[
            Field(name="long_id", dtype=Int64),
            Field(name="double_feature_1", dtype=Float32),
        ],
        online=True,
        offline=False,
    )
    assert [c.name for c in fv.entity_columns] == ["long_id"]  # guard the fixture

    _emitter().emit_apply([fv], PROJECT)
    facets = next(e for e in captured if isinstance(e, DatasetEvent)).dataset.facets

    by_field = {f.name: f for f in facets["schema"].fields}
    # The real join-key column is present and typed...
    assert by_field["long_id"].type == "Int64"
    # ...and no phantom column for the entity *name* is emitted.
    assert "flyte_sample_key" not in by_field

    ffv = facets["feast_featureView"]
    # entities carries the join-key column so it lines up with schema.fields.
    assert ffv.entities == ["long_id"]
    assert set(ffv.entities) <= set(by_field)
    assert ffv.offline_enabled is False


def _view_tagged(tags):
    source = SparkSource(name="src", table="db.tbl", timestamp_field="event_timestamp")
    hotel = Entity(name="hotel_id", join_keys=["hotel_id"], value_type=ValueType.INT64)
    return FeatureView(
        name="no_product_view",
        entities=[hotel],
        ttl=timedelta(days=1),
        source=source,
        schema=[Field(name="base_price", dtype=Float32)],
        online=True,
        tags=tags,
    )


def _emitted_tags(captured):
    facets = next(e for e in captured if isinstance(e, DatasetEvent)).dataset.facets
    return {t.key: t.value for t in facets["tags"].tags}


def test_default_data_product_fallback(captured):
    # A view with no eg-data-product tag falls back to the platform default so it
    # still resolves to a domain/owners instead of landing unowned (mirrors MRS
    # model-repository). The MLP `product` tag is *not* a data-mesh product name,
    # so it is neither used as the fallback source nor emitted.
    fv = _view_tagged({"application": "some-app", "product": "unified-feature-store"})

    _emitter().emit_apply([fv], PROJECT)
    tags = _emitted_tags(captured)
    assert tags["eg-data-product"] == "mlp-feature-registry"
    assert "product" not in tags


def test_blank_data_product_tag_falls_back(captured):
    # A present-but-blank eg-data-product tag is not a valid product value: fall
    # back rather than emit the blank through.
    fv = _view_tagged({"eg-data-product": "   "})

    _emitter().emit_apply([fv], PROJECT)
    assert _emitted_tags(captured)["eg-data-product"] == "mlp-feature-registry"


def test_configured_data_product_is_emitted(captured):
    # The fallback is whatever the config carries -- not hardcoded in the mapper.
    # In-process only: it is deliberately not a feature_store.yaml knob, since the
    # per-object eg-data-product tag is the supported override.
    fv = _view_tagged({"application": "some-app"})

    _emitter(default_data_product="feature-store").emit_apply([fv], PROJECT)
    assert _emitted_tags(captured)["eg-data-product"] == "feature-store"


def test_no_default_data_product_omits_tag(captured):
    # With the fallback nulled out and no tag on the view, eg-data-product is
    # omitted entirely -- never a placeholder that would strand the view.
    fv = _view_tagged({"application": "some-app"})

    _emitter(default_data_product=None).emit_apply([fv], PROJECT)
    assert "eg-data-product" not in _emitted_tags(captured)


# -------------------------------------------------------------------- stream


def test_stream_feature_view_dataset_event(captured):
    fv = _stream_feature_view()
    _emitter().emit_apply([fv], PROJECT)

    event = next(e for e in captured if isinstance(e, DatasetEvent))
    facets = event.dataset.facets

    assert facets["datasetType"].datasetType == "STREAM"
    assert facets["datasetType"].subType == "STREAM_KAFKA"

    # The stream rides on the standard dataSource facet (URN passed through).
    assert facets["dataSource"].name == "example_stream_kafka_source"
    assert facets["dataSource"].uri.startswith("urn:egsp:consumer:")

    stream = facets["feast_streamSource"]
    assert stream.topic.startswith("urn:egsp:consumer:")
    assert stream.message_format == "AvroFormat"
    assert stream.watermark_delay_seconds == 300
    assert stream.field_mapping == {
        "event_header.published_datetime_utc": "published_timestamp"
    }

    # The mandatory batch source is preserved on its own facet, not dropped.
    batch = facets["feast_batchSource"]
    assert batch.name == "example_stream_batch_source"
    assert batch.source_type == "BATCH_SPARK"
    assert (
        batch.uri
        == "egdl://data-dw.us-east-1/prod_offline_feature_store.example_streaming"
    )


# --------------------------------------------------------------------- scope


def test_only_feature_views_are_emitted(captured):
    source, hotel, fv = _batch_feature_view()
    fs = FeatureService(name="hcom_feature_service", features=[fv])
    # Sources, entities and services are passed but must not produce events.
    _emitter().emit_apply([source, hotel, fv, fs], PROJECT)

    dataset_events = [e for e in captured if isinstance(e, DatasetEvent)]
    assert {e.dataset.name for e in dataset_events} == {FV_DATASET}
    assert not [e for e in captured if isinstance(e, RunEvent)]


# --------------------------------------------------------------------- config


def test_disabled_when_env_unresolved(monkeypatch):
    monkeypatch.delenv("REGISTRY_ENV", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_ENVIRONMENT", raising=False)
    cfg = OpenLineageConfig(
        enabled=True, transport_type="http", transport_url="http://localhost:9999"
    )
    emitter = FeastOpenLineageEmitter(cfg)
    assert emitter.is_enabled is False


def test_environment_override_beats_env_var(monkeypatch):
    monkeypatch.setenv("REGISTRY_ENV", "test")
    cfg = OpenLineageConfig(enabled=True, environment="corp")
    assert cfg.namespace == "mlp://mlpfs-corp"
