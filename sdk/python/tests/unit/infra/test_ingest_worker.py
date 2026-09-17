"""Exercise ingest workers with real spawned processes and no external stores."""

import json
import multiprocessing
import os
import signal
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import feast.infra.ingest_worker as ingest_worker
from feast.errors import (
    CassandraWriteTimeoutError,
    IngestWorkerFailedError,
    IngestWorkerHungError,
)
from feast.infra.ingest_worker import run_ingest_workers
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig

pytestmark = pytest.mark.timeout(30)


class UnpickleableWorkerError(Exception):
    def __init__(self):
        super().__init__("unpickleable worker failure")
        self.lock = threading.Lock()


class SpawnTestProvider(PassthroughProvider):
    """Importable provider whose worker behavior is selected by test config."""

    def process(self, table, feature_view, join_keys):
        directory = Path(self.repo_config.ingest_test_directory)
        mode = self.repo_config.ingest_test_mode
        if mode == "ignore_sigterm":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        (directory / f"started-{os.getpid()}").write_text(mode)

        if mode == "error":
            raise ValueError("synthetic worker failure")
        if mode == "large_error":
            raise RuntimeError("large worker failure " + "x" * 10_000)
        if mode == "large_write_deadline":
            raise CassandraWriteTimeoutError("overloaded " + "x" * 10_000)
        if mode == "unpickleable_error":
            raise UnpickleableWorkerError()
        if mode in {"exit", "exit_success"}:
            os._exit(23 if mode == "exit" else 0)
        if mode in {"hang", "ignore_sigterm"}:
            threading.Event().wait()

        (directory / f"result-{table}.json").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "start_method": multiprocessing.get_start_method(),
                    "workers": os.environ.get("NUM_PROCESSES"),
                    "feature_view": feature_view.name,
                    "join_keys": join_keys,
                }
            )
        )


@pytest.fixture
def worker_config(tmp_path):
    return RepoConfig(
        project="worker_test",
        registry=str(tmp_path / "registry.db"),
        provider=f"{__name__}.SpawnTestProvider",
        ingest_test_directory=str(tmp_path),
        ingest_test_mode="success",
    )


@pytest.fixture(autouse=True)
def reap_test_children():
    """Keep failed cleanup assertions from leaking test workers."""
    existing = {process.pid for process in multiprocessing.active_children()}
    yield existing
    children = [
        process
        for process in multiprocessing.active_children()
        if process.pid not in existing
    ]
    for process in children:
        process.kill()
    for process in children:
        process.join(timeout=5)


def _run(config, *, chunks=2, timeout=20):
    feature_view = SimpleNamespace(name="worker_feature_view")
    run_ingest_workers(
        repo_config=config,
        feature_view=feature_view,
        chunks_to_parallelize=[
            (index, feature_view, {"entity": 4}) for index in range(chunks)
        ],
        num_processes=chunks,
        timeout_seconds=timeout,
    )


def _assert_children_reaped(existing):
    assert {process.pid for process in multiprocessing.active_children()}.issubset(
        existing
    )


def test_spawned_workers_complete_all_chunks(
    worker_config, tmp_path, reap_test_children
):
    _run(worker_config)

    results = [
        json.loads(path.read_text()) for path in sorted(tmp_path.glob("result-*.json"))
    ]
    assert len(results) == 2
    assert len({result["pid"] for result in results}) == 2
    for result in results:
        assert result["pid"] != os.getpid()
        assert result["start_method"] == "spawn"
        assert result["workers"] == "2"
        assert result["feature_view"] == "worker_feature_view"
        assert result["join_keys"] == {"entity": 4}
    _assert_children_reaped(reap_test_children)


def test_worker_exception_propagates_and_reaps_children(
    worker_config, reap_test_children
):
    worker_config.ingest_test_mode = "error"

    with pytest.raises(ValueError, match="synthetic worker failure"):
        _run(worker_config)

    _assert_children_reaped(reap_test_children)


def test_sentinel_ready_before_exit_status_does_not_fail_successful_worker(
    worker_config, tmp_path, reap_test_children, monkeypatch
):
    context = multiprocessing.get_context("spawn")

    class DelayedExitStatusProcess:
        """Expose a real child's sentinel before making its status available."""

        def __init__(self, process):
            self.process = process
            self.positive_joins = 0

        def __getattr__(self, name):
            return getattr(self.process, name)

        @property
        def exitcode(self):
            if self.positive_joins < 2:
                return None
            return self.process.exitcode

        def join(self, timeout=None):
            self.process.join(timeout=timeout)
            if timeout is not None and timeout > 0:
                self.positive_joins += 1

    class DelayedExitStatusContext:
        def Pipe(self, **kwargs):
            return context.Pipe(**kwargs)

        def Process(self, **kwargs):
            return DelayedExitStatusProcess(context.Process(**kwargs))

    monkeypatch.setattr(
        ingest_worker.multiprocessing,
        "get_context",
        lambda method: DelayedExitStatusContext(),
    )

    _run(worker_config, chunks=1)

    assert (tmp_path / "result-0.json").exists()
    _assert_children_reaped(reap_test_children)


def test_oversized_write_deadline_preserves_nontransient_type(
    worker_config, reap_test_children
):
    from feast.infra.contrib.spark_kafka_processor import _is_transient_error

    worker_config.ingest_test_mode = "large_write_deadline"

    with pytest.raises(CassandraWriteTimeoutError) as error:
        _run(worker_config, chunks=1)

    assert "overloaded" in str(error.value)
    assert not _is_transient_error(error.value)
    _assert_children_reaped(reap_test_children)


@pytest.mark.parametrize("mode", ["large_error", "unpickleable_error"])
def test_error_reporting_does_not_hang_on_large_or_unpickleable_errors(
    worker_config, reap_test_children, mode
):
    worker_config.ingest_test_mode = mode

    with pytest.raises(IngestWorkerFailedError):
        _run(worker_config)

    _assert_children_reaped(reap_test_children)


@pytest.mark.parametrize("mode", ["exit", "exit_success"])
def test_abrupt_worker_exit_is_reported(worker_config, reap_test_children, mode):
    worker_config.ingest_test_mode = mode

    with pytest.raises(IngestWorkerFailedError):
        _run(worker_config)

    _assert_children_reaped(reap_test_children)


def test_failed_process_start_reaps_already_started_worker(
    worker_config, reap_test_children, monkeypatch
):
    context = multiprocessing.get_context("spawn")

    class FailingStartContext:
        def __init__(self):
            self.created = 0

        def Pipe(self, **kwargs):
            return context.Pipe(**kwargs)

        def Process(self, **kwargs):
            self.created += 1
            process = context.Process(**kwargs)
            if self.created == 2:

                def fail_to_start():
                    raise RuntimeError("synthetic process start failure")

                process.start = fail_to_start
            return process

    failing_context = FailingStartContext()
    monkeypatch.setattr(
        ingest_worker.multiprocessing, "get_context", lambda method: failing_context
    )
    worker_config.ingest_test_mode = "hang"

    with pytest.raises(RuntimeError, match="synthetic process start failure"):
        _run(worker_config)

    assert failing_context.created == 2
    _assert_children_reaped(reap_test_children)


@pytest.mark.parametrize(
    "mode",
    [
        "hang",
        pytest.param(
            "ignore_sigterm",
            marks=pytest.mark.skipif(os.name != "posix", reason="POSIX signals"),
        ),
    ],
)
def test_blocked_workers_are_terminated_and_reaped(
    worker_config, tmp_path, reap_test_children, mode
):
    worker_config.ingest_test_mode = mode
    started = time.monotonic()

    with pytest.raises(IngestWorkerHungError):
        _run(worker_config, timeout=5)

    assert time.monotonic() - started < 12
    # Ensure the deadline exercised a running worker, including its signal
    # handler, rather than merely interrupting process startup.
    assert list(tmp_path.glob("started-*"))
    _assert_children_reaped(reap_test_children)
