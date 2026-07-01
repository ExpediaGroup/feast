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
    def observe_written_batch(
        self,
        table: Any,
        feature_fields: List[str],
        timestamp_column: Optional[str] = None,
    ) -> None:
        """Record field coverage, per-field null counts, and freshness from an Arrow table.

        ``feature_fields`` is the set of declared feature columns; only those actually
        present in ``table`` are reported. Best-effort: any failure is swallowed so
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
            "max_event_timestamp": self.max_event_timestamp,
        }
