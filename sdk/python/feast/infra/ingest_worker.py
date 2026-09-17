"""Optional ingest processes with fresh clients and bounded worker cleanup."""

import logging
import multiprocessing
import os
import pickle
import time
from multiprocessing.connection import Connection, wait
from multiprocessing.process import BaseProcess
from typing import Any, List, Tuple, cast

from feast.errors import (
    CassandraWriteTimeoutError,
    IngestWorkerFailedError,
    IngestWorkerHungError,
)
from feast.repo_config import RepoConfig

logger = logging.getLogger(__name__)

# A worker reports only status, not data. Keep the single frame smaller than
# the minimum POSIX pipe capacity so it can exit before the parent reads it.
_MAX_RESULT_BYTES = 480
_CLEANUP_GRACE_SECONDS = 2.0


def _error_payload(error: Exception) -> bytes:
    """Preserve small pickle-safe exceptions; log full details in the worker."""
    try:
        payload = pickle.dumps(error)
        # Some driver errors pickle successfully but cannot be reconstructed.
        restored = pickle.loads(payload)
        if len(payload) <= _MAX_RESULT_BYTES and isinstance(restored, Exception):
            return payload
    except Exception:
        pass
    description = f"{type(error).__name__}: {error}"
    # Escape non-ASCII characters to make the byte bound independent of Unicode.
    description = description.encode("ascii", errors="backslashreplace").decode()[:200]
    message = f"{description} (see ingest worker logs for full error)"
    if isinstance(error, CassandraWriteTimeoutError):
        return pickle.dumps(CassandraWriteTimeoutError(message))
    if isinstance(error, IngestWorkerHungError):
        return pickle.dumps(
            IngestWorkerHungError(
                error.feature_view_name[:80], error.num_processes, error.timeout_seconds
            )
        )
    return pickle.dumps(IngestWorkerFailedError(message))


def _write_chunk(
    config: RepoConfig,
    chunk: Tuple[Any, Any, Any],
    num_processes: int,
    result: Connection,
) -> None:
    # Import after spawn; clients, locks and background threads belong to this
    # interpreter only. Never serialize a bound method from the live provider.
    from feast.infra.passthrough_provider import PassthroughProvider
    from feast.infra.provider import get_provider

    os.environ["NUM_PROCESSES"] = str(num_processes)
    try:
        provider = get_provider(config)
        if not isinstance(provider, PassthroughProvider):
            raise TypeError("Parallel ingest requires a PassthroughProvider")
        provider.process(*chunk)
    except Exception as exc:
        logger.exception("Ingest worker failed")
        result.send_bytes(_error_payload(exc))
    else:
        result.send_bytes(pickle.dumps(None))
    finally:
        result.close()


def _cleanup_workers(processes: List[BaseProcess]) -> None:
    """Terminate together, then escalate without an unbounded Pool.join()."""
    started = [process for process in processes if process.pid is not None]
    for process in started:
        if process.is_alive():
            process.terminate()
    deadline = time.monotonic() + _CLEANUP_GRACE_SECONDS
    for process in started:
        process.join(timeout=max(0.0, deadline - time.monotonic()))
    for process in started:
        if process.is_alive():
            process.kill()
    deadline = time.monotonic() + _CLEANUP_GRACE_SECONDS
    for process in started:
        process.join(timeout=max(0.0, deadline - time.monotonic()))
        if process.is_alive():
            # An OS-level uninterruptible process cannot be fixed by waiting
            # forever here. Surface the PID for the job supervisor/operator.
            logger.error(
                "Ingest worker pid=%s survived terminate and kill", process.pid
            )
        else:
            process.close()


def run_ingest_workers(
    repo_config: RepoConfig,
    feature_view: Any,
    chunks_to_parallelize: List[Tuple[Any, Any, Any]],
    num_processes: int,
    timeout_seconds: float,
) -> None:
    """Wait for spawned writers, including child startup and client teardown.

    The caller validates a finite positive timeout. The deadline is checked
    between process starts and while waiting; Python's process.start() and
    serialization themselves cannot be interrupted by this function.
    """
    context = multiprocessing.get_context("spawn")
    processes: List[BaseProcess] = []
    readers: List[Connection] = []
    deadline = time.monotonic() + timeout_seconds

    def expired() -> IngestWorkerHungError:
        return IngestWorkerHungError(feature_view.name, num_processes, timeout_seconds)

    try:
        for chunk in chunks_to_parallelize:
            if time.monotonic() >= deadline:
                raise expired()
            reader, writer = context.Pipe(duplex=False)
            readers.append(reader)
            try:
                process: BaseProcess = context.Process(
                    target=_write_chunk,
                    args=(repo_config, chunk, num_processes, writer),
                    daemon=True,
                )
                processes.append(process)
                process.start()
            finally:
                # Each spawned child owns the only remaining write handle for
                # its pipe. A crashed child therefore produces EOF, not a hang.
                writer.close()

        pending = {
            process.sentinel: (process, reader)
            for process, reader in zip(processes, readers)
        }
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise expired()
            completed = wait(list(pending), timeout=remaining)
            if not completed:
                raise expired()
            for sentinel in completed:
                process, reader = pending.pop(cast(int, sentinel))
                # On POSIX the sentinel pipe can close just before waitpid can
                # reap the child. A zero-time join can still leave exitcode=None.
                while process.exitcode is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise expired()
                    process.join(timeout=min(0.05, remaining))
                    if process.exitcode is None:
                        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
                if process.exitcode != 0:
                    raise IngestWorkerFailedError(
                        f"Ingest worker pid={process.pid} for '{feature_view.name}' "
                        f"exited with code {process.exitcode}; the batch was not completed"
                    )
                # Read only once the child exited: partial frames cannot leave
                # recv waiting for a living but stuck writer.
                try:
                    result = pickle.loads(
                        reader.recv_bytes(maxlength=_MAX_RESULT_BYTES)
                    )
                except (EOFError, OSError, pickle.UnpicklingError) as exc:
                    raise IngestWorkerFailedError(
                        f"Ingest worker pid={process.pid} for '{feature_view.name}' "
                        "exited without a valid result"
                    ) from exc
                if isinstance(result, Exception):
                    raise result
                if result is not None:
                    raise IngestWorkerFailedError("Invalid ingest worker result")
    finally:
        try:
            _cleanup_workers(processes)
        finally:
            for reader in readers:
                reader.close()
