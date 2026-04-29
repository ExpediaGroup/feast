def datadog_client(**kwargs):
    """Create a Datadog DogStatsd client. Requires feast[metrics] extra."""
    from datadog import DogStatsd

    return DogStatsd(**kwargs)
