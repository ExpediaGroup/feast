from importlib.metadata import PackageNotFoundError, version

# This repo publishes its own SDK distribution as "eg-feast" (see setup.py), not
# "feast", so the installed package name differs from upstream OSS Feast. Check
# both, upstream name first for compatibility with non-fork installs.
_SDK_DISTRIBUTION_NAMES = ("feast", "eg-feast")


def get_version():
    """Returns version information of the Feast Python Package."""
    for candidate in _SDK_DISTRIBUTION_NAMES:
        try:
            return version(candidate)
        except PackageNotFoundError:
            continue
    return "unknown"


def get_installed_version(package_name: str) -> str:
    """Returns the installed version of an arbitrary package, or "not installed"
    if it isn't present. Useful for opportunistically reporting the version of a
    wrapper/consumer package (e.g. a materialization job image) without requiring
    it to be installed for Feast to function."""
    try:
        return version(package_name)
    except PackageNotFoundError:
        return "not installed"
