"""Write-time materialization metrics.

A lightweight, in-memory aggregator that accumulates per-feature-view stats while
a materialization run writes to the online store, mirroring the accumulate-then-flush
shape of ``_missing_key_metrics.py``. Unlike that module it does NOT emit to statsd /
an agent — it just accumulates. The materialization job reads the aggregated stats
after the run and flushes one row to the metrics table (a later ticket).

The collector is reached in two ways:

* The compute-engine nodes hold the :class:`ExecutionContext`, so they read/populate
  the aggregator directly.
* The online store's ``online_write_batch`` does NOT receive the ExecutionContext
  (and its signature is deliberately not changed — a ~15-store blast radius). It
  reaches the aggregator through the :data:`_active_aggregator` ``ContextVar`` that the
  output node sets around the write call via :func:`collecting`.

All of this is gated behind the ``ENABLE_MATERIALIZATION_METRICS`` env var; when it is
off, nothing is instantiated and the hooks are no-ops.
"""

import contextlib
import contextvars
import logging
import os
import threading
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}

# The aggregator active for the current materialization write, if any. Set by the
# compute-engine output node so the online store can record its drops without a
# signature change. A ContextVar (not a plain global) keeps concurrent runs in the
# same process isolated and is copied into threads spawned within the context.
_active_aggregator: "contextvars.ContextVar[Optional[MaterializationMetricsAggregator]]" = contextvars.ContextVar(
    "feast_active_materialization_aggregator", default=None
)


def is_materialization_metrics_enabled() -> bool:
    """Whether write-time materialization metrics are enabled (env-gated, off by default)."""
    return os.getenv("ENABLE_MATERIALIZATION_METRICS", "").strip().lower() in _TRUTHY


def get_active_aggregator() -> "Optional[MaterializationMetricsAggregator]":
    """Return the aggregator active for the current write, if any."""
    return _active_aggregator.get()


@contextlib.contextmanager
def collecting(
    aggregator: "Optional[MaterializationMetricsAggregator]",
) -> Iterator[None]:
    """Bind ``aggregator`` as the active collector for the duration of the block.

    Passing ``None`` is a no-op (leaves any outer aggregator untouched-as-None),
    so callers can wrap a write path unconditionally.
    """
    token = _active_aggregator.set(aggregator)
    try:
        yield
    finally:
        _active_aggregator.reset(token)


# --- Layer-1 -> job bridge --------------------------------------------------
# The compute engine can't write the metrics row itself (it doesn't know the run
# lifecycle/identity/provenance -- that's the materialization job's job). So when a
# feature view finishes writing, the engine stashes the collector's stats here, and
# the job drains them after `store.materialize(...)` returns, merges its own Layer-2
# facts, and flushes one row. Process-local (the driver is a single process), guarded
# by a lock for safety.
_run_results_lock = threading.Lock()
_run_results: "List[Dict[str, Any]]" = []


def record_run_result(stats: Optional[Dict[str, Any]]) -> None:
    """Stash one finished feature view's write-time stats for the job to drain."""
    if not stats:
        return
    with _run_results_lock:
        _run_results.append(dict(stats))


def drain_run_results() -> "List[Dict[str, Any]]":
    """Return and clear all write-time stats stashed since the last drain.

    Called by the materialization job after each `store.materialize(...)`; typically
    one entry per feature view materialized.
    """
    global _run_results
    with _run_results_lock:
        drained = _run_results
        _run_results = []
    return drained


class MaterializationMetricsAggregator:
    """Accumulates write-time stats for a single feature view's materialization run.

    Row-count model (keeps a clean, testable reconciliation invariant):

    * :meth:`record_read` — rows read from the offline source.
    * :meth:`record_written` — rows that reached the output node (i.e. sent to the
      store). Upstream filter/dedup drops already happened, so this is the survivor
      count at the write boundary.
    * :meth:`record_upstream_drop` — rows removed *before* the output node
      (filter, dedup). Recorded as a drop reason only; ``rows_written`` is unaffected
      because those rows never reached the output node.
    * :meth:`record_store_drop` — rows the store itself skipped *after* they were
      counted as written (Cassandra TTL). Decrements ``rows_written`` and records the
      reason.

    Invariant: ``rows_read_offline - rows_written_online == rows_dropped ==
    sum(drop_reasons.values())``.
    """

    def __init__(
        self,
        project: str,
        feature_view: str,
        online_store_type: str,
    ):
        self.project = project
        self.feature_view = feature_view
        self.online_store_type = online_store_type

        self.rows_read_offline: int = 0
        self.rows_written_online: int = 0
        self.drop_reasons: Counter = Counter()

        self.fields_written: List[str] = []
        self.field_null_counts: Counter = Counter()
        self.max_event_timestamp: Optional[datetime] = None

        # Volume. bytes_written accumulates the in-memory Arrow size of each
        # written batch (summed across batches/partitions). distinct_entity_keys
        # is exact on the local engine (computed from the single output batch) and
        # set on the driver from an approx count on Spark (see set_distinct_entity_keys).
        self.bytes_written: int = 0
        self.distinct_entity_keys: int = 0

    # -- row counts ---------------------------------------------------------
    def record_read(self, n: int) -> None:
        self.rows_read_offline += int(n)

    def record_written(self, n: int) -> None:
        self.rows_written_online += int(n)

    def record_upstream_drop(self, reason: str, n: int = 1) -> None:
        if n:
            self.drop_reasons[reason] += int(n)

    def record_store_drop(self, reason: str, n: int = 1) -> None:
        if n:
            self.drop_reasons[reason] += int(n)
            self.rows_written_online -= int(n)

    @property
    def rows_dropped(self) -> int:
        return int(sum(self.drop_reasons.values()))

    # -- freshness ----------------------------------------------------------
    def observe_event_timestamp(self, ts: Optional[datetime]) -> None:
        if ts is None:
            return
        if self.max_event_timestamp is None or ts > self.max_event_timestamp:
            self.max_event_timestamp = ts

    def lag_seconds(self, now: datetime) -> Optional[float]:
        if self.max_event_timestamp is None:
            return None
        return (now - self.max_event_timestamp).total_seconds()

    # -- field coverage / nulls / freshness from an Arrow batch -------------
    def set_distinct_entity_keys(self, n: Optional[int]) -> None:
        """Set the distinct-entity-key count directly.

        Used on the Spark driver, where an exact per-partition count can't be summed
        (keys may repeat across partitions); the driver computes one approximate
        count over the whole DataFrame and sets it here.
        """
        if n is not None:
            self.distinct_entity_keys = int(n)

    def observe_written_batch(
        self,
        table: Any,
        feature_fields: List[str],
        timestamp_column: Optional[str] = None,
        entity_key_columns: Optional[List[str]] = None,
    ) -> None:
        """Record field coverage, per-field null counts, freshness, bytes, and (when
        ``entity_key_columns`` is given) an exact distinct-entity-key count from an
        Arrow table.

        ``feature_fields`` is the set of declared feature columns; only those actually
        present in ``table`` are reported. ``entity_key_columns`` should be passed only
        when this is the whole written batch (the local engine's single output table);
        on Spark it is omitted and the driver sets distinct keys via
        :meth:`set_distinct_entity_keys`. Best-effort: any failure is swallowed so
        metrics never break a materialization.
        """
        try:
            present = set(table.column_names)
            for field in feature_fields:
                if field not in present:
                    continue
                if field not in self.fields_written:
                    self.fields_written.append(field)
                self.field_null_counts[field] += int(table.column(field).null_count)

            if timestamp_column and timestamp_column in present:
                col = table.column(timestamp_column)
                if len(col):
                    import pyarrow.compute as pc

                    max_val = pc.max(col).as_py()
                    self.observe_event_timestamp(max_val)

            # Volume: in-memory Arrow size is a cheap, engine-agnostic proxy for
            # bytes written; sums across batches and Spark partitions.
            self.bytes_written += int(getattr(table, "nbytes", 0) or 0)

            # Exact distinct entity keys, only when the caller passes the key columns
            # (local engine). group_by([]).aggregate([]) yields one row per distinct
            # key combination; max() guards the (non-local) multi-call case.
            if entity_key_columns:
                key_cols = [c for c in entity_key_columns if c in present]
                if key_cols:
                    distinct = (
                        table.select(key_cols).group_by(key_cols).aggregate([]).num_rows
                    )
                    self.distinct_entity_keys = max(
                        self.distinct_entity_keys, int(distinct)
                    )
        except Exception as e:  # pragma: no cover - defensive, metrics never fail a run
            logger.warning(f"materialization metrics: failed to observe batch: {e}")

    # -- export -------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "project": self.project,
            "feature_view": self.feature_view,
            "online_store_type": self.online_store_type,
            "rows_read_offline": self.rows_read_offline,
            "rows_written_online": self.rows_written_online,
            "rows_dropped": self.rows_dropped,
            "drop_reasons": dict(self.drop_reasons),
            "fields_written": list(self.fields_written),
            "field_null_counts": dict(self.field_null_counts),
            "distinct_entity_keys": self.distinct_entity_keys,
            "bytes_written": self.bytes_written,
            "max_event_timestamp": self.max_event_timestamp,
        }

    # -- distributed merge --------------------------------------------------
    def merge_from_dict(self, stats: Optional[Dict[str, Any]]) -> None:
        """Fold an aggregated stats dict into this aggregator.

        Used on the Spark driver to populate the run's collector from the value of
        an accumulator that merged per-partition :meth:`to_dict` outputs across
        executors. The incoming counts are already net (store drops applied
        per partition), so they are summed directly rather than replayed through the
        ``record_*`` methods (which would double-apply the store-drop decrement).
        """
        if not stats:
            return
        self.rows_read_offline += int(stats.get("rows_read_offline", 0) or 0)
        self.rows_written_online += int(stats.get("rows_written_online", 0) or 0)
        self.drop_reasons.update(stats.get("drop_reasons") or {})
        for field in stats.get("fields_written") or []:
            if field not in self.fields_written:
                self.fields_written.append(field)
        self.field_null_counts.update(stats.get("field_null_counts") or {})
        self.bytes_written += int(stats.get("bytes_written", 0) or 0)
        # Distinct keys can't be summed across partitions (keys repeat); take the
        # max, so a driver-set approximate count isn't clobbered by partition zeros.
        self.distinct_entity_keys = max(
            self.distinct_entity_keys, int(stats.get("distinct_entity_keys", 0) or 0)
        )
        self.observe_event_timestamp(stats.get("max_event_timestamp"))


def merge_stats(
    a: Optional[Dict[str, Any]], b: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Merge two stats dicts (each in :meth:`MaterializationMetricsAggregator.to_dict`
    shape) into one.

    Pure, associative, and commutative, with the empty dict ``{}`` as identity
    (``merge_stats({}, x) == x``), so it can back a Spark ``AccumulatorParam`` whose
    ``zero`` is ``{}`` and whose ``addInPlace`` is this function. Row counts sum,
    ``drop_reasons`` / ``field_null_counts`` Counters add, ``fields_written`` unions
    (order-stable), and ``max_event_timestamp`` takes the later of the two.
    """
    a = a or {}
    b = b or {}

    drop_reasons: Counter = Counter(a.get("drop_reasons") or {})
    drop_reasons.update(b.get("drop_reasons") or {})

    null_counts: Counter = Counter(a.get("field_null_counts") or {})
    null_counts.update(b.get("field_null_counts") or {})

    fields: List[str] = list(a.get("fields_written") or [])
    for field in b.get("fields_written") or []:
        if field not in fields:
            fields.append(field)

    timestamps = [
        t
        for t in (a.get("max_event_timestamp"), b.get("max_event_timestamp"))
        if t is not None
    ]
    max_ts = max(timestamps) if timestamps else None

    def _identity(key: str) -> Any:
        return a.get(key) if a.get(key) is not None else b.get(key)

    return {
        "project": _identity("project"),
        "feature_view": _identity("feature_view"),
        "online_store_type": _identity("online_store_type"),
        "rows_read_offline": int(a.get("rows_read_offline", 0) or 0)
        + int(b.get("rows_read_offline", 0) or 0),
        "rows_written_online": int(a.get("rows_written_online", 0) or 0)
        + int(b.get("rows_written_online", 0) or 0),
        "rows_dropped": int(sum(drop_reasons.values())),
        "drop_reasons": dict(drop_reasons),
        "fields_written": fields,
        "field_null_counts": dict(null_counts),
        "distinct_entity_keys": max(
            int(a.get("distinct_entity_keys", 0) or 0),
            int(b.get("distinct_entity_keys", 0) or 0),
        ),
        "bytes_written": int(a.get("bytes_written", 0) or 0)
        + int(b.get("bytes_written", 0) or 0),
        "max_event_timestamp": max_ts,
    }
