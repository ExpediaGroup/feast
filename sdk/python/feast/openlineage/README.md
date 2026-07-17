# Feast → Metadata Bus OpenLineage integration

Emits OpenLineage events to Expedia Group's **Metadata Bus** when `feast apply`
runs, so ML Platform Feature Store assets (feature views, batch sources,
entities, feature services) become discoverable in **OpenMetadata**.

This is the EG fork's adaptation of the upstream `feast-dev/feast` OpenLineage
module. It is scoped to the **`feast apply`** path only — materialization,
feature retrieval, and the upstream OpenLineage *consumer* server are not
included.

## Event model

OpenMetadata requires both endpoints of a lineage edge to exist as catalog
entities before the edge can be drawn, so emission happens in two layers
(**nodes = `DatasetEvent`, edges = `RunEvent`**):

1. **Node registration** — one `DatasetEvent` per batch source, entity, feature
   view, and feature service, each carrying `lifecycleStateChange: CREATE` and
   its descriptive `feast_*` dataset facet.
2. **Lineage edges** — a `RunEvent` (`COMPLETE`) per feature view
   (`feature_view_{project}.{name}`: batch source + entities → view) and per
   feature service (`feature_service_{project}.{name}`: views → service),
   referencing inputs/outputs by identity only.

See `metadata-bus-user-guide` PR #21 (EAPC-22333) for the full contract.

## Naming

| Field | Value |
|---|---|
| Producer | `https://github.com/ExpediaGroup/feast` |
| Namespace | `mlp://mlpfs-{env}` (e.g. `mlp://mlpfs-dw`) |
| Dataset name | `{project}.{asset_name}` (e.g. `hcom_feast_store.hotel_price_features`) |

The `{env}` segment is resolved, in order, from:

1. `openlineage.environment` in `feature_store.yaml` (explicit override)
2. `REGISTRY_ENV` — set by the GitHub Actions `register-features` path
3. `CONTROL_PLANE_ENVIRONMENT` — set by the Flyte feature-registration task

Keying on the same variable that builds the Feast registry host keeps emitted
lineage consistent with the registry the assets are written to. If none resolve,
emission is skipped with a warning (rather than emitting a wrong namespace).

## Configuration (`feature_store.yaml`)

```yaml
openlineage:
  enabled: true                # default: false
  transport_type: http
  transport_url: https://metadata-bus-proxy.rcp.us-east-1.data.test.exp-aws.net
  transport_endpoint: api/v1/lineage   # default
  # environment: dw            # optional override; else REGISTRY_ENV / CONTROL_PLANE_ENVIRONMENT
  # emit_on_apply: true        # default
```

### Metadata Bus proxy endpoints

| Environment | URL |
|---|---|
| Test | `https://metadata-bus-proxy.rcp.us-east-1.data.test.exp-aws.net` |
| Prod (DW) | `https://metadata-bus-proxy.rcp.us-east-1.data.dw.exp-aws.net` |

Endpoint path: `api/v1/lineage`. No authentication required.

## Installation

The OpenLineage client is an optional dependency:

```bash
pip install 'feast[openlineage]'
```

Without it installed, emission is a no-op (a warning is logged). Emission
failures are always non-fatal — they never block `feast apply`.

## Divergences from upstream `feast-dev/feast`

- Emits standalone `DatasetEvent`s to register nodes, then edge `RunEvent`s
  (upstream emits `RunEvent`s only, with facets embedded on run datasets).
- Custom facet `_schemaURL` resolves to the generic OpenLineage
  `#/$defs/DatasetFacet` / `#/$defs/JobFacet` schema (upstream points at
  unhosted `feast.dev/spec/...` URLs that 404).
- Producer is the EG fork; namespace/name follow the `mlp://mlpfs-{env}` /
  `{project}.{asset_name}` convention.
- Job names are project-qualified (`feature_view_{project}.{name}`) since the
  namespace is shared across projects.
