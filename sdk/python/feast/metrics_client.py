from typing import List, Optional, Protocol, runtime_checkable


@runtime_checkable
class StatsdClient(Protocol):
    def increment(
        self, metric: str, value: int = 1, tags: Optional[List[str]] = None
    ) -> None: ...


class NoOpStatsdClient:
    def increment(
        self, metric: str, value: int = 1, tags: Optional[List[str]] = None
    ) -> None:
        pass


_global_client: StatsdClient = NoOpStatsdClient()


def set_metrics_client(client: StatsdClient) -> None:
    """Register a StatsD-compatible metrics client for Feast SDK.

    Call once at process startup. Example with Datadog:
        from datadog import DogStatsd
        from feast.metrics_client import set_metrics_client
        set_metrics_client(DogStatsd(host="localhost", port=8125))
    """
    global _global_client
    _global_client = client


def get_metrics_client() -> StatsdClient:
    return _global_client
