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

Asserts the event shapes documented in metadata-bus-user-guide PR #21
(EAPC-22333): DatasetEvents register nodes, RunEvents draw edges.
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
    FileSource,
)
from feast.openlineage.client import FeastOpenLineageClient  # noqa: E402
from feast.openlineage.config import OpenLineageConfig  # noqa: E402
from feast.openlineage.emitter import FeastOpenLineageEmitter  # noqa: E402
from feast.types import Float32, Int64  # noqa: E402
from feast.value_type import ValueType  # noqa: E402

PROJECT = "hcom_feast_store"
PROJECT_META = {
    "provider": "aws",
    "online_store_type": "valkey",
    "offline_store_type": "iceberg",
    "registry_type": "http",
}


def _build_objects():
    source = FileSource(
        name="hotel_price_batch_source",
        path="s3://eg-feature-store/hotel_price/",
        timestamp_field="event_timestamp",
    )
    hotel = Entity(name="hotel_id", join_keys=["hotel_id"], value_type=ValueType.INT64)
    fv = FeatureView(
        name="hotel_price_features",
        entities=[hotel],
        ttl=timedelta(days=1),
        source=source,
        schema=[
            Field(name="base_price", dtype=Float32),
            Field(name="price_bucket", dtype=Int64),
        ],
        online=True,
    )
    fs = FeatureService(name="hcom_feature_service", features=[fv])
    return [source, hotel, fv, fs], fv, fs


@pytest.fixture
def captured(monkeypatch):
    events = []
    monkeypatch.setenv("REGISTRY_ENV", "dw")
    monkeypatch.delenv("CONTROL_PLANE_ENVIRONMENT", raising=False)
    monkeypatch.setattr(
        FeastOpenLineageClient,
        "emit",
        lambda self, event: (events.append(event), True)[1],
    )
    return events


def _emit(captured):
    cfg = OpenLineageConfig(
        enabled=True, transport_type="http", transport_url="http://localhost:9999"
    )
    emitter = FeastOpenLineageEmitter(cfg)
    assert emitter.is_enabled
    objects, fv, fs = _build_objects()
    emitter.emit_apply(objects, PROJECT, PROJECT_META)
    return captured, fv, fs


def test_node_registration_dataset_events(captured):
    events, _, _ = _emit(captured)
    dataset_events = [e for e in events if isinstance(e, DatasetEvent)]

    # One DatasetEvent per batch source, entity, feature view, feature service.
    names = {e.dataset.name for e in dataset_events}
    assert names == {
        "hcom_feast_store.hotel_price_batch_source",
        "hcom_feast_store.hotel_id",
        "hcom_feast_store.hotel_price_features",
        "hcom_feast_store.hcom_feature_service",
    }
    # All share the env-derived namespace.
    assert {e.dataset.namespace for e in dataset_events} == {"mlp://mlpfs-dw"}

    by_name = {e.dataset.name: e for e in dataset_events}

    # Feature view node carries lifecycleStateChange + feast_featureView.
    fv_facets = by_name["hcom_feast_store.hotel_price_features"].dataset.facets
    assert fv_facets["lifecycleStateChange"].lifecycleStateChange == "CREATE"
    assert fv_facets["feast_featureView"].name == "hotel_price_features"
    assert fv_facets["feast_featureView"].ttl_seconds == 86400
    assert fv_facets["feast_featureView"].entities == ["hotel_id"]
    # EG divergence: generic OL DatasetFacet schema, not feast.dev/spec.
    assert fv_facets["feast_featureView"]._schemaURL.endswith("#/$defs/DatasetFacet")

    # Batch source node carries both standard dataSource + feast_dataSource.
    src_facets = by_name["hcom_feast_store.hotel_price_batch_source"].dataset.facets
    assert src_facets["dataSource"].uri == "s3://eg-feature-store/hotel_price/"
    assert src_facets["feast_dataSource"].timestamp_field == "event_timestamp"

    # Entity node.
    ent_facets = by_name["hcom_feast_store.hotel_id"].dataset.facets
    assert ent_facets["feast_entity"].join_keys == ["hotel_id"]


def test_lineage_edge_run_events(captured):
    events, _, _ = _emit(captured)
    run_events = [e for e in events if isinstance(e, RunEvent)]
    by_job = {e.job.name: e for e in run_events}

    # One edge per feature view and per feature service, project-qualified.
    assert set(by_job) == {
        "feature_view_hcom_feast_store.hotel_price_features",
        "feature_service_hcom_feast_store.hcom_feature_service",
    }

    fv_edge = by_job["feature_view_hcom_feast_store.hotel_price_features"]
    assert (
        str(fv_edge.eventType) == "RunState.COMPLETE"
        or fv_edge.eventType.name == "COMPLETE"
    )
    assert fv_edge.job.namespace == "mlp://mlpfs-dw"

    # Inputs = batch source + entity, by identity only (no facets).
    input_names = {i.name for i in fv_edge.inputs}
    assert input_names == {
        "hcom_feast_store.hotel_price_batch_source",
        "hcom_feast_store.hotel_id",
    }
    assert all(not getattr(i, "facets", {}) for i in fv_edge.inputs)
    # Output = the feature view, by identity.
    assert [o.name for o in fv_edge.outputs] == [
        "hcom_feast_store.hotel_price_features"
    ]

    # jobType facet: integration FEAST.
    jt = fv_edge.job.facets["jobType"]
    assert jt.integration == "FEAST"
    assert jt.processingType == "BATCH"
    assert jt.jobType == "APPLICATION"

    # feast_project job facet, generic JobFacet schema.
    proj = fv_edge.job.facets["feast_project"]
    assert proj.project_name == "hcom_feast_store"
    assert proj.offline_store_type == "iceberg"
    assert proj._schemaURL.endswith("#/$defs/JobFacet")

    # Feature-service edge: views -> service, jobType only.
    fs_edge = by_job["feature_service_hcom_feast_store.hcom_feature_service"]
    assert [o.name for o in fs_edge.outputs] == [
        "hcom_feast_store.hcom_feature_service"
    ]
    assert {i.name for i in fs_edge.inputs} == {"hcom_feast_store.hotel_price_features"}
    assert "feast_project" not in fs_edge.job.facets


def test_nodes_emitted_before_edges(captured):
    events, _, _ = _emit(captured)
    last_dataset = max(i for i, e in enumerate(events) if isinstance(e, DatasetEvent))
    first_run = min(i for i, e in enumerate(events) if isinstance(e, RunEvent))
    assert last_dataset < first_run


def test_disabled_when_env_unresolved(monkeypatch):
    monkeypatch.delenv("REGISTRY_ENV", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_ENVIRONMENT", raising=False)
    cfg = OpenLineageConfig(
        enabled=True, transport_type="http", transport_url="http://localhost:9999"
    )
    emitter = FeastOpenLineageEmitter(cfg)
    # No environment resolvable -> namespace None -> emission skipped.
    assert emitter.is_enabled is False


def test_environment_override_beats_env_var(monkeypatch):
    monkeypatch.setenv("REGISTRY_ENV", "test")
    cfg = OpenLineageConfig(enabled=True, environment="corp")
    assert cfg.namespace == "mlp://mlpfs-corp"
