"""Feature server reports: models, checks, key and merge rules.

A running feature server reports the URLs it can be called at. Registries
store one record per distinct URL list in a project, as project metadata
(``feast_metadata``) under the ``feature_server:`` key prefix. Everything
here is pure (no database): both the gRPC registry server and the REST
registry call the same registry methods, which use these rules, so checks
and storage are identical everywhere.

Checks stay generic on purpose (well-formed URLs, size limits); naming,
environment and DNS checks belong to readers such as Feature Hub.
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urlsplit

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import BaseModel, ConfigDict, Field, field_validator

from feast.errors import (
    FeatureServerReportError,
    FeatureServerReportInvalid,
    FeatureServerReportLimitExceeded,
)
from feast.metrics_client import get_metrics_client
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)

FEATURE_SERVER_KEY_PREFIX = "feature_server:"
_KEY_HASH_LENGTH = 16

ALLOWED_URL_SCHEMES = frozenset({"http", "https", "grpc", "grpcs"})
MAX_URL_LENGTH = 2048
MAX_URLS = 10
MAX_LABELS = 20
MAX_LABEL_KEY_LENGTH = 63
MAX_LABEL_VALUE_LENGTH = 256
MAX_VERSION_LENGTH = 128
MAX_SCOPE_LENGTH = 32
MAX_REGISTRY_LENGTH = 32
MAX_REPORT_BYTES = 16 * 1024
MAX_RECORDS_PER_PROJECT = 50

THROTTLE_WINDOW = timedelta(hours=1)
VERSION_RETENTION = timedelta(hours=48)
RECORD_RETENTION = timedelta(days=30)

OUTCOME_ACCEPTED = "accepted"
OUTCOME_UNCHANGED = "unchanged"
OUTCOME_REJECTED = "rejected"

METRIC_NAME = "feast.registry.feature_server_report"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class FeatureServerUrl(BaseModel):
    model_config = ConfigDict(extra="ignore")

    url: str
    scope: str = ""


class FeatureServerReport(BaseModel):
    """What a feature server reports about itself (report contract 1).

    Unknown fields are ignored so the format can grow; ``contract`` marks
    the version. Shape only: call :func:`validate_report` for the rules.
    """

    model_config = ConfigDict(extra="ignore")

    contract: int = 1
    urls: List[FeatureServerUrl] = Field(default_factory=list)
    version: str = ""
    registry: str = ""
    labels: Dict[str, str] = Field(default_factory=dict)


class FeatureServerRecord(BaseModel):
    """One stored record: one per distinct URL list in a project.

    Datetimes are timezone-aware UTC. ``versions`` maps each version seen
    in the last 48 hours to when it was last seen.
    """

    model_config = ConfigDict(extra="ignore")

    key: str
    urls: List[FeatureServerUrl]
    labels: Dict[str, str]
    versions: Dict[str, datetime]
    registry: str
    first_seen: datetime
    last_seen: datetime
    verified: bool = False
    identity: Optional[str] = None

    @field_validator("first_seen", "last_seen")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return _as_utc(value)

    @field_validator("versions")
    @classmethod
    def _utc_versions(cls, value: Dict[str, datetime]) -> Dict[str, datetime]:
        return {version: _as_utc(seen) for version, seen in value.items()}


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------


def _check_url(url: str) -> None:
    if len(url) > MAX_URL_LENGTH:
        raise FeatureServerReportInvalid(
            f"URL is longer than {MAX_URL_LENGTH} characters", reason="url_too_long"
        )
    if not url or any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in url):
        raise FeatureServerReportInvalid(
            f"URL {url!r} is empty or contains whitespace or control characters",
            reason="invalid_url",
        )
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as e:
        raise FeatureServerReportInvalid(
            f"URL {url!r} is not well-formed: {e}", reason="invalid_url"
        ) from e
    if parts.scheme not in ALLOWED_URL_SCHEMES:
        raise FeatureServerReportInvalid(
            f"URL {url!r} must use one of the schemes "
            f"{', '.join(sorted(ALLOWED_URL_SCHEMES))}",
            reason="invalid_url_scheme",
        )
    if "@" in parts.netloc:
        raise FeatureServerReportInvalid(
            f"URL {url!r} must not contain a user name or password",
            reason="url_has_credentials",
        )
    if not parts.hostname:
        raise FeatureServerReportInvalid(
            f"URL {url!r} must be absolute with a host", reason="url_missing_host"
        )
    if port is not None and not 1 <= port <= 65535:
        raise FeatureServerReportInvalid(
            f"URL {url!r} has an invalid port", reason="invalid_url_port"
        )


def validate_report(report: FeatureServerReport) -> None:
    """Apply the registry's generic checks to a report.

    Raises:
        FeatureServerReportInvalid: malformed URL, empty label key, contract < 1.
        FeatureServerReportLimitExceeded: a count, length or size limit is exceeded.
    """
    if report.contract < 1:
        raise FeatureServerReportInvalid(
            f"contract must be 1 or higher, got {report.contract}",
            reason="invalid_contract",
        )
    if len(report.urls) > MAX_URLS:
        raise FeatureServerReportLimitExceeded(
            f"A report may carry at most {MAX_URLS} URLs, got {len(report.urls)}",
            reason="too_many_urls",
        )
    if len(report.labels) > MAX_LABELS:
        raise FeatureServerReportLimitExceeded(
            f"A report may carry at most {MAX_LABELS} labels, got {len(report.labels)}",
            reason="too_many_labels",
        )
    if len(report.version) > MAX_VERSION_LENGTH:
        raise FeatureServerReportLimitExceeded(
            f"version is longer than {MAX_VERSION_LENGTH} characters",
            reason="version_too_long",
        )
    if len(report.registry) > MAX_REGISTRY_LENGTH:
        raise FeatureServerReportLimitExceeded(
            f"registry is longer than {MAX_REGISTRY_LENGTH} characters",
            reason="registry_too_long",
        )
    for label_key, label_value in report.labels.items():
        if not label_key:
            raise FeatureServerReportInvalid(
                "label keys must not be empty", reason="invalid_label_key"
            )
        if len(label_key) > MAX_LABEL_KEY_LENGTH:
            raise FeatureServerReportLimitExceeded(
                f"label key {label_key[:MAX_LABEL_KEY_LENGTH]!r}... is longer than "
                f"{MAX_LABEL_KEY_LENGTH} characters",
                reason="label_key_too_long",
            )
        if len(label_value) > MAX_LABEL_VALUE_LENGTH:
            raise FeatureServerReportLimitExceeded(
                f"label {label_key!r} value is longer than "
                f"{MAX_LABEL_VALUE_LENGTH} characters",
                reason="label_value_too_long",
            )
    for entry in report.urls:
        if len(entry.scope) > MAX_SCOPE_LENGTH:
            raise FeatureServerReportLimitExceeded(
                f"URL scope is longer than {MAX_SCOPE_LENGTH} characters",
                reason="scope_too_long",
            )
        _check_url(entry.url)
    size = len(report.model_dump_json().encode("utf-8"))
    if size > MAX_REPORT_BYTES:
        raise FeatureServerReportLimitExceeded(
            f"The report is {size} bytes as JSON; the limit is {MAX_REPORT_BYTES}",
            reason="report_too_large",
        )


def check_reporter_identity(
    project: str, report: FeatureServerReport, identity: Optional[str]
) -> None:
    """Hook for tying a verified identity to the app it reports for.

    Called by the registry for verified reports only. A no-op today.

    TODO(EAPC-23485, open question 8): once we know which token claim
    identifies the app, require the reporter's workload to match
    ``report.labels["app"]`` and the project's registered client_id, and
    raise FeatureServerReportInvalid (or an auth error) when it does not.
    """
    return None


# --------------------------------------------------------------------------
# Key, merge, cleanup
# --------------------------------------------------------------------------


def feature_server_key(report: FeatureServerReport) -> str:
    """Stable record key: ``feature_server:`` + 16 hex chars (31 chars).

    The hash is SHA-256 of the compact JSON of the sorted URL strings, so
    every pod of one deployment lands on one record whatever order it lists
    its URLs in. When ``urls`` is empty, the sorted ``[key, value]`` label
    pairs are hashed instead, so location-less servers of different apps do
    not collapse into one record.
    """
    material: object
    if report.urls:
        material = sorted(entry.url for entry in report.urls)
    else:
        material = sorted([k, v] for k, v in report.labels.items())
    digest = hashlib.sha256(
        json.dumps(material, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"{FEATURE_SERVER_KEY_PREFIX}{digest[:_KEY_HASH_LENGTH]}"


def _url_set(urls: List[FeatureServerUrl]) -> List[tuple]:
    return sorted((entry.url, entry.scope) for entry in urls)


def merge_report(
    existing: Optional[FeatureServerRecord],
    report: FeatureServerReport,
    now: datetime,
    verified: bool = False,
    identity: Optional[str] = None,
) -> Optional[FeatureServerRecord]:
    """Fold a report into the stored record.

    Returns ``None`` when nothing needs writing: the stored record has the
    same URLs, labels, registry and verified/identity, and this version was
    seen less than an hour ago. Otherwise returns the record to store, with
    versions not seen for more than 48 hours dropped.
    """
    now = _as_utc(now)
    identity = identity if verified else None
    if existing is not None:
        seen = existing.versions.get(report.version)
        if (
            seen is not None
            and now - seen < THROTTLE_WINDOW
            and _url_set(existing.urls) == _url_set(report.urls)
            and existing.labels == report.labels
            and existing.registry == report.registry
            and existing.verified == verified
            and existing.identity == identity
        ):
            return None
    versions = {
        version: seen
        for version, seen in (existing.versions if existing else {}).items()
        if now - seen <= VERSION_RETENTION
    }
    versions[report.version] = now
    return FeatureServerRecord(
        key=existing.key if existing else feature_server_key(report),
        urls=[entry.model_copy() for entry in report.urls],
        labels=dict(report.labels),
        versions=versions,
        registry=report.registry,
        first_seen=existing.first_seen if existing else now,
        last_seen=now,
        verified=verified,
        identity=identity,
    )


def is_record_expired(record: FeatureServerRecord, now: datetime) -> bool:
    """True when the record was last seen more than 30 days before ``now``."""
    return _as_utc(now) - record.last_seen > RECORD_RETENTION


# --------------------------------------------------------------------------
# Storage JSON
# --------------------------------------------------------------------------


def record_to_json(record: FeatureServerRecord) -> str:
    """The ``feast_metadata.metadata_value`` for a record (the key is the row key)."""
    return record.model_dump_json(exclude={"key"})


def record_from_json(key: str, value: str) -> FeatureServerRecord:
    data = json.loads(value)
    data["key"] = key
    return FeatureServerRecord.model_validate(data)


# --------------------------------------------------------------------------
# Proto converters
# --------------------------------------------------------------------------


def _timestamp(value: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(_as_utc(value))
    return ts


def _datetime(ts: Timestamp) -> datetime:
    # ToDatetime() returns naive UTC.
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def report_from_proto(
    proto: RegistryServer_pb2.FeatureServerReport,
) -> FeatureServerReport:
    return FeatureServerReport(
        contract=proto.contract,
        urls=[FeatureServerUrl(url=u.url, scope=u.scope) for u in proto.urls],
        version=proto.version,
        registry=proto.registry,
        labels=dict(proto.labels),
    )


def report_to_proto(
    report: FeatureServerReport,
) -> RegistryServer_pb2.FeatureServerReport:
    return RegistryServer_pb2.FeatureServerReport(
        contract=report.contract,
        urls=[
            RegistryServer_pb2.FeatureServerUrl(url=u.url, scope=u.scope)
            for u in report.urls
        ],
        version=report.version,
        registry=report.registry,
        labels=report.labels,
    )


def record_to_proto(
    record: FeatureServerRecord,
) -> RegistryServer_pb2.FeatureServerRecord:
    proto = RegistryServer_pb2.FeatureServerRecord(
        key=record.key,
        urls=[
            RegistryServer_pb2.FeatureServerUrl(url=u.url, scope=u.scope)
            for u in record.urls
        ],
        labels=record.labels,
        registry=record.registry,
        verified=record.verified,
        identity=record.identity or "",
    )
    for version, seen in record.versions.items():
        proto.versions[version].CopyFrom(_timestamp(seen))
    proto.first_seen.CopyFrom(_timestamp(record.first_seen))
    proto.last_seen.CopyFrom(_timestamp(record.last_seen))
    return proto


def record_from_proto(
    proto: RegistryServer_pb2.FeatureServerRecord,
) -> FeatureServerRecord:
    return FeatureServerRecord(
        key=proto.key,
        urls=[FeatureServerUrl(url=u.url, scope=u.scope) for u in proto.urls],
        labels=dict(proto.labels),
        versions={v: _datetime(ts) for v, ts in proto.versions.items()},
        registry=proto.registry,
        first_seen=_datetime(proto.first_seen),
        last_seen=_datetime(proto.last_seen),
        verified=proto.verified,
        identity=proto.identity or None,
    )


# --------------------------------------------------------------------------
# Outcome log line and metric
# --------------------------------------------------------------------------


def log_report_outcome(
    outcome: str,
    project: str,
    report: Optional[FeatureServerReport],
    identity: Optional[str] = None,
    reason: str = "-",
) -> None:
    """One structured log line and one counter per report outcome.

    Reports are not audited objects: this line and the metric are the
    record of who reported what.
    """
    app = (report.labels.get("app") if report else None) or "-"
    line = (
        f"feature_server_report outcome={outcome} reason={reason} "
        f"project={project} app={app} identity={identity or '-'}"
    )
    if outcome == OUTCOME_REJECTED:
        logger.warning(line)
    else:
        logger.info(line)
    try:
        get_metrics_client().increment(
            METRIC_NAME,
            tags=[f"outcome:{outcome}", f"reason:{reason}", f"app:{app}"],
        )
    except Exception:  # metrics must never fail a report
        logger.debug("feature_server_report metric failed", exc_info=True)


def rejection_reason(error: Exception) -> str:
    if isinstance(error, FeatureServerReportError):
        return error.reason
    return type(error).__name__
