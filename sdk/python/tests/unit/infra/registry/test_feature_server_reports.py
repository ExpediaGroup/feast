# Copyright 2021 The Feast Authors
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
"""Pure feature server report rules: checks, key, merge, converters."""

import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import grpc
import pytest

from feast.errors import (
    FeastError,
    FeatureServerReportInvalid,
    FeatureServerReportLimitExceeded,
    FeatureServerReportsDisabled,
    FeatureServerReportUnavailable,
)
from feast.infra.registry import feature_server_reports as fsr
from feast.infra.registry.feature_server_reports import (
    FeatureServerRecord,
    FeatureServerReport,
    FeatureServerUrl,
)
from feast.protos.feast.registry import RegistryServer_pb2

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def _report(**overrides) -> FeatureServerReport:
    values = dict(
        contract=1,
        urls=[
            FeatureServerUrl(url="grpcs://app.example.com:443", scope="external"),
            FeatureServerUrl(
                url="grpc://app.ns.svc.cluster.local:6566", scope="internal"
            ),
        ],
        version="v1",
        registry="grpc",
        labels={"app": "app", "environment": "test"},
    )
    values.update(overrides)
    return FeatureServerReport(**values)


class TestModels:
    def test_defaults(self):
        report = FeatureServerReport()
        assert report.contract == 1
        assert report.urls == []
        assert report.version == ""
        assert report.registry == ""
        assert report.labels == {}

    def test_unknown_fields_ignored(self):
        report = FeatureServerReport.model_validate(
            {
                "contract": 2,
                "urls": [{"url": "https://h", "scope": "x", "method": "POST"}],
                "future_field": {"anything": 1},
            }
        )
        assert report.contract == 2
        assert report.urls[0].url == "https://h"
        assert not hasattr(report, "future_field")

    def test_record_datetimes_are_utc(self):
        naive = datetime(2026, 1, 1, 10, 0)
        plus_two = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
        record = FeatureServerRecord(
            key="feature_server:0123456789abcdef",
            urls=[],
            labels={},
            versions={"v": naive},
            registry="",
            first_seen=naive,
            last_seen=plus_two,
        )
        assert record.first_seen.tzinfo == timezone.utc
        assert record.last_seen == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        assert record.last_seen.utcoffset() == timedelta(0)
        assert record.versions["v"].tzinfo == timezone.utc
        assert record.verified is False
        assert record.identity is None


class TestValidation:
    def test_valid_report_passes(self):
        fsr.validate_report(_report())

    def test_empty_report_passes(self):
        fsr.validate_report(FeatureServerReport())

    @pytest.mark.parametrize(
        "url",
        [
            "http://host",
            "https://host.example.com/path?q=1",
            "grpc://host:6566",
            "grpcs://host.example.com:443",
            "HTTPS://Host.Example.com",
            "http://[::1]:8080",
            "http://10.0.0.1:1",
            "http://host:65535",
        ],
    )
    def test_accepted_urls(self, url):
        fsr.validate_report(_report(urls=[FeatureServerUrl(url=url)]))

    @pytest.mark.parametrize(
        "url,reason",
        [
            ("", "invalid_url"),
            ("host.example.com", "invalid_url_scheme"),
            ("/relative/path", "invalid_url_scheme"),
            ("ftp://host", "invalid_url_scheme"),
            ("file:///etc/passwd", "invalid_url_scheme"),
            ("ws://host", "invalid_url_scheme"),
            ("https://", "url_missing_host"),
            ("https:///path", "url_missing_host"),
            ("https://:443", "url_missing_host"),
            ("https://user@host", "url_has_credentials"),
            ("https://user:secret@host", "url_has_credentials"),
            ("https://:secret@host", "url_has_credentials"),
            ("https://@host", "url_has_credentials"),
            ("https://host:notaport", "invalid_url"),
            ("https://host:70000", "invalid_url"),
            ("https://host:0", "invalid_url_port"),
            ("https://ho st", "invalid_url"),
            ("https://host/\npath", "invalid_url"),
            (" https://host", "invalid_url"),
            ("https://[::1", "invalid_url"),
        ],
    )
    def test_rejected_urls(self, url, reason):
        with pytest.raises(FeatureServerReportInvalid) as exc:
            fsr.validate_report(_report(urls=[FeatureServerUrl(url=url)]))
        assert exc.value.reason == reason

    def test_url_length_limit(self):
        base = "https://h.example.com/"
        ok = base + "a" * (fsr.MAX_URL_LENGTH - len(base))
        fsr.validate_report(_report(urls=[FeatureServerUrl(url=ok)]))
        with pytest.raises(FeatureServerReportInvalid) as exc:
            fsr.validate_report(_report(urls=[FeatureServerUrl(url=ok + "a")]))
        assert exc.value.reason == "url_too_long"

    def test_contract_must_be_at_least_one(self):
        fsr.validate_report(_report(contract=7))
        for contract in (0, -1):
            with pytest.raises(FeatureServerReportInvalid) as exc:
                fsr.validate_report(_report(contract=contract))
            assert exc.value.reason == "invalid_contract"

    def test_url_count_limit(self):
        urls = [FeatureServerUrl(url=f"https://h{i}.example.com") for i in range(10)]
        fsr.validate_report(_report(urls=urls))
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(
                _report(urls=urls + [FeatureServerUrl(url="https://h10.example.com")])
            )
        assert exc.value.reason == "too_many_urls"

    def test_label_count_limit(self):
        labels = {f"k{i}": "v" for i in range(20)}
        fsr.validate_report(_report(labels=labels))
        labels["k20"] = "v"
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(_report(labels=labels))
        assert exc.value.reason == "too_many_labels"

    def test_label_key_length_limit(self):
        fsr.validate_report(_report(labels={"k" * 63: "v"}))
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(_report(labels={"k" * 64: "v"}))
        assert exc.value.reason == "label_key_too_long"

    def test_empty_label_key_is_invalid(self):
        with pytest.raises(FeatureServerReportInvalid) as exc:
            fsr.validate_report(_report(labels={"": "v"}))
        assert exc.value.reason == "invalid_label_key"

    def test_label_value_length_limit(self):
        fsr.validate_report(_report(labels={"k": "v" * 256}))
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(_report(labels={"k": "v" * 257}))
        assert exc.value.reason == "label_value_too_long"

    def test_version_length_limit(self):
        fsr.validate_report(_report(version="v" * 128))
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(_report(version="v" * 129))
        assert exc.value.reason == "version_too_long"

    def test_scope_length_limit(self):
        fsr.validate_report(
            _report(urls=[FeatureServerUrl(url="https://h", scope="s" * 32)])
        )
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(
                _report(urls=[FeatureServerUrl(url="https://h", scope="s" * 33)])
            )
        assert exc.value.reason == "scope_too_long"

    def test_registry_length_limit(self):
        fsr.validate_report(_report(registry="r" * 32))
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(_report(registry="r" * 33))
        assert exc.value.reason == "registry_too_long"

    def test_serialized_size_limit(self):
        # Every field within its own limit, the whole report over 16 KB.
        url = "https://h.example.com/" + "a" * 1900
        urls = [FeatureServerUrl(url=url, scope=str(i)) for i in range(9)]
        report = _report(urls=urls)
        assert len(report.model_dump_json()) > fsr.MAX_REPORT_BYTES
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(report)
        assert exc.value.reason == "report_too_large"

    def test_serialized_size_counts_utf8_bytes(self):
        labels = {f"k{i}": "é" * 256 for i in range(20)}
        urls = [FeatureServerUrl(url=f"https://h{i}/" + "é" * 350) for i in range(10)]
        report = _report(labels=labels, urls=urls)
        as_json = report.model_dump_json()
        assert len(as_json) <= fsr.MAX_REPORT_BYTES
        assert len(as_json.encode("utf-8")) > fsr.MAX_REPORT_BYTES
        with pytest.raises(FeatureServerReportLimitExceeded) as exc:
            fsr.validate_report(report)
        assert exc.value.reason == "report_too_large"


class TestErrors:
    @pytest.mark.parametrize(
        "error,grpc_code,http_code",
        [
            (FeatureServerReportInvalid("x"), grpc.StatusCode.INVALID_ARGUMENT, 422),
            (
                FeatureServerReportLimitExceeded("x"),
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                422,
            ),
            (FeatureServerReportsDisabled(), grpc.StatusCode.UNAVAILABLE, 503),
            (FeatureServerReportUnavailable("x"), grpc.StatusCode.UNAVAILABLE, 503),
        ],
    )
    def test_status_codes(self, error, grpc_code, http_code):
        assert isinstance(error, FeastError)
        assert error.grpc_status_code() == grpc_code
        assert error.http_status_code() == http_code

    def test_round_trip_through_error_detail_keeps_message_and_default_reason(self):
        original = FeatureServerReportInvalid("bad url", reason="invalid_url")
        rebuilt = FeastError.from_error_detail(original.to_error_detail())
        assert isinstance(rebuilt, FeatureServerReportInvalid)
        assert str(rebuilt) == "bad url"
        assert rebuilt.reason == "invalid"


class TestKey:
    def test_format(self):
        key = fsr.feature_server_key(_report())
        assert key.startswith("feature_server:")
        assert len(key) == 31
        assert all(c in "0123456789abcdef" for c in key[len("feature_server:") :])

    def test_stable_value(self):
        # Pinned: changing the formula would split every stored record.
        report = _report(
            urls=[FeatureServerUrl(url="https://a"), FeatureServerUrl(url="https://b")]
        )
        import hashlib

        expected = hashlib.sha256(b'["https://a","https://b"]').hexdigest()[:16]
        assert fsr.feature_server_key(report) == f"feature_server:{expected}"

    def test_independent_of_url_order_scope_version_labels_registry(self):
        a = _report()
        b = _report(
            urls=list(reversed(a.urls)),
            version="v2",
            labels={"app": "other"},
            registry="http",
        )
        b.urls[0].scope = "changed"
        assert fsr.feature_server_key(a) == fsr.feature_server_key(b)

    def test_different_urls_give_different_keys(self):
        a = _report(urls=[FeatureServerUrl(url="https://a")])
        b = _report(urls=[FeatureServerUrl(url="https://b")])
        assert fsr.feature_server_key(a) != fsr.feature_server_key(b)

    def test_empty_urls_hash_sorted_labels(self):
        a = _report(urls=[], labels={"app": "one", "instance": "x"})
        a_reordered = _report(urls=[], labels={"instance": "x", "app": "one"})
        b = _report(urls=[], labels={"app": "two", "instance": "x"})
        assert fsr.feature_server_key(a) == fsr.feature_server_key(a_reordered)
        assert fsr.feature_server_key(a) != fsr.feature_server_key(b)
        assert len(fsr.feature_server_key(a)) == 31

    def test_empty_urls_key_differs_from_url_keys(self):
        no_urls = _report(urls=[], labels={})
        assert fsr.feature_server_key(no_urls) != fsr.feature_server_key(_report())


class TestMerge:
    def test_new_record(self):
        record = fsr.merge_report(None, _report(), NOW)
        assert record is not None
        assert record.key == fsr.feature_server_key(_report())
        assert record.first_seen == NOW
        assert record.last_seen == NOW
        assert record.versions == {"v1": NOW}
        assert record.labels == {"app": "app", "environment": "test"}
        assert record.registry == "grpc"
        assert [u.url for u in record.urls] == [u.url for u in _report().urls]
        assert record.verified is False
        assert record.identity is None

    def test_unverified_drops_identity(self):
        record = fsr.merge_report(None, _report(), NOW, verified=False, identity="x")
        assert record.identity is None
        record = fsr.merge_report(None, _report(), NOW, verified=True, identity="x")
        assert record.verified is True
        assert record.identity == "x"

    def test_throttled_same_version_within_hour(self):
        existing = fsr.merge_report(None, _report(), NOW)
        later = NOW + timedelta(minutes=59)
        assert fsr.merge_report(existing, _report(), later) is None

    def test_throttle_ignores_url_order(self):
        existing = fsr.merge_report(None, _report(), NOW)
        reordered = _report(urls=list(reversed(_report().urls)))
        assert fsr.merge_report(existing, reordered, NOW + timedelta(minutes=1)) is None

    def test_written_after_an_hour(self):
        existing = fsr.merge_report(None, _report(), NOW)
        later = NOW + timedelta(hours=1)
        merged = fsr.merge_report(existing, _report(), later)
        assert merged is not None
        assert merged.first_seen == NOW
        assert merged.last_seen == later
        assert merged.versions == {"v1": later}

    @pytest.mark.parametrize(
        "change",
        [
            {"version": "v2"},
            {"labels": {"app": "app"}},
            {"registry": "http"},
            {
                "urls": [
                    FeatureServerUrl(
                        url="grpcs://app.example.com:443", scope="nearest"
                    ),
                    FeatureServerUrl(
                        url="grpc://app.ns.svc.cluster.local:6566", scope="internal"
                    ),
                ]
            },
        ],
    )
    def test_change_within_hour_is_written(self, change):
        existing = fsr.merge_report(None, _report(), NOW)
        merged = fsr.merge_report(
            existing, _report(**change), NOW + timedelta(minutes=5)
        )
        assert merged is not None
        assert merged.first_seen == NOW

    def test_verified_change_within_hour_is_written(self):
        existing = fsr.merge_report(None, _report(), NOW)
        soon = NOW + timedelta(minutes=5)
        merged = fsr.merge_report(
            existing, _report(), soon, verified=True, identity="a"
        )
        assert merged is not None and merged.verified and merged.identity == "a"
        assert (
            fsr.merge_report(merged, _report(), soon, verified=True, identity="a")
            is None
        )
        other = fsr.merge_report(merged, _report(), soon, verified=True, identity="b")
        assert other is not None and other.identity == "b"
        downgraded = fsr.merge_report(merged, _report(), soon)
        assert downgraded is not None and downgraded.verified is False

    def test_versions_kept_for_48_hours(self):
        existing = fsr.merge_report(None, _report(version="old"), NOW)
        at_48h = NOW + timedelta(hours=48)
        merged = fsr.merge_report(existing, _report(version="new"), at_48h)
        assert merged.versions == {"old": NOW, "new": at_48h}

    def test_versions_older_than_48_hours_dropped(self):
        existing = fsr.merge_report(None, _report(version="old"), NOW)
        mid = fsr.merge_report(
            existing, _report(version="mid"), NOW + timedelta(hours=24)
        )
        later = NOW + timedelta(hours=48, seconds=1)
        merged = fsr.merge_report(mid, _report(version="new"), later)
        assert set(merged.versions) == {"mid", "new"}
        assert merged.versions["mid"] == NOW + timedelta(hours=24)

    def test_reported_version_refreshed_even_when_old(self):
        existing = fsr.merge_report(None, _report(version="v1"), NOW)
        later = NOW + timedelta(days=3)
        merged = fsr.merge_report(existing, _report(version="v1"), later)
        assert merged.versions == {"v1": later}

    def test_merge_does_not_mutate_inputs(self):
        report = _report()
        existing = fsr.merge_report(None, report, NOW)
        before = existing.model_dump()
        merged = fsr.merge_report(
            existing, _report(version="v2"), NOW + timedelta(hours=2)
        )
        merged.urls[0].scope = "mutated"
        merged.labels["x"] = "y"
        assert existing.model_dump() == before
        assert report.urls[0].scope == "external"
        assert "x" not in report.labels


class TestExpiry:
    def test_30_day_cleanup_boundary(self):
        record = fsr.merge_report(None, _report(), NOW)
        assert not fsr.is_record_expired(record, NOW + timedelta(days=30))
        assert fsr.is_record_expired(record, NOW + timedelta(days=30, seconds=1))


class TestStorageJson:
    def test_round_trip_without_key_in_value(self):
        record = fsr.merge_report(None, _report(), NOW, verified=True, identity="me")
        value = fsr.record_to_json(record)
        data = json.loads(value)
        assert "key" not in data
        assert set(data) == {
            "urls",
            "labels",
            "versions",
            "registry",
            "first_seen",
            "last_seen",
            "verified",
            "identity",
        }
        assert data["versions"]["v1"].startswith("2026-11-04T12:00:00")
        assert fsr.record_from_json(record.key, value) == record


class TestProtoConverters:
    def test_report_round_trip(self):
        report = _report()
        proto = fsr.report_to_proto(report)
        assert isinstance(proto, RegistryServer_pb2.FeatureServerReport)
        assert fsr.report_from_proto(proto) == report

    def test_report_from_empty_proto_has_contract_zero(self):
        report = fsr.report_from_proto(RegistryServer_pb2.FeatureServerReport())
        assert report.contract == 0
        with pytest.raises(FeatureServerReportInvalid):
            fsr.validate_report(report)

    def test_record_round_trip(self):
        existing = fsr.merge_report(None, _report(version="a"), NOW)
        record = fsr.merge_report(
            existing,
            _report(version="b"),
            NOW + timedelta(hours=2, microseconds=5),
            verified=True,
            identity="svc",
        )
        proto = fsr.record_to_proto(record)
        assert proto.key == record.key
        assert proto.identity == "svc"
        assert proto.verified is True
        assert set(proto.versions) == {"a", "b"}
        assert proto.first_seen.ToDatetime(tzinfo=timezone.utc) == NOW
        assert fsr.record_from_proto(proto) == record

    def test_unverified_record_proto_has_empty_identity(self):
        record = fsr.merge_report(None, _report(), NOW)
        proto = fsr.record_to_proto(record)
        assert proto.identity == ""
        assert fsr.record_from_proto(proto).identity is None


class TestOutcomeLog:
    def test_log_line_and_metric(self, caplog):
        client = MagicMock()
        with patch.object(fsr, "get_metrics_client", return_value=client):
            with caplog.at_level(logging.INFO, logger=fsr.logger.name):
                fsr.log_report_outcome(
                    fsr.OUTCOME_ACCEPTED, "proj", _report(), identity="svc"
                )
        assert (
            "feature_server_report outcome=accepted reason=- project=proj app=app "
            "identity=svc"
        ) in caplog.text
        client.increment.assert_called_once_with(
            fsr.METRIC_NAME,
            tags=["outcome:accepted", "reason:-", "app:app"],
        )

    def test_rejection_logged_as_warning_without_app(self, caplog):
        with caplog.at_level(logging.INFO, logger=fsr.logger.name):
            fsr.log_report_outcome(
                fsr.OUTCOME_REJECTED,
                "proj",
                _report(labels={}),
                reason="invalid_url",
            )
        record = caplog.records[-1]
        assert record.levelno == logging.WARNING
        assert record.getMessage() == (
            "feature_server_report outcome=rejected reason=invalid_url "
            "project=proj app=- identity=-"
        )

    def test_metric_failure_does_not_raise(self):
        client = MagicMock()
        client.increment.side_effect = RuntimeError("statsd down")
        with patch.object(fsr, "get_metrics_client", return_value=client):
            fsr.log_report_outcome(fsr.OUTCOME_UNCHANGED, "proj", _report())

    def test_rejection_reason(self):
        assert fsr.rejection_reason(FeatureServerReportInvalid("x", "bad")) == "bad"
        assert fsr.rejection_reason(ValueError("x")) == "ValueError"
