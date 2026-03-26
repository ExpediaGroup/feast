import pytest
from unittest.mock import MagicMock, patch

from feast.infra.contrib.spark_kafka_processor import (
    _is_transient_error,
    _write_with_retry,
    TRANSIENT_ERROR_PATTERNS,
)


class TestIsTransientError:
    """Tests for the _is_transient_error function."""

    def test_writetimeout_in_message(self):
        """WriteTimeout errors should be classified as transient."""
        exc = Exception("WriteTimeout during batch insert")
        assert _is_transient_error(exc) is True

    def test_readtimeout_in_message(self):
        """ReadTimeout errors should be classified as transient."""
        exc = Exception("ReadTimeout while fetching data")
        assert _is_transient_error(exc) is True

    def test_unavailable_in_message(self):
        """Unavailable errors should be classified as transient."""
        exc = Exception("Unavailable: Not enough replicas")
        assert _is_transient_error(exc) is True

    def test_operationtimedout_in_message(self):
        """OperationTimedOut errors should be classified as transient."""
        exc = Exception("OperationTimedOut after 10000ms")
        assert _is_transient_error(exc) is True

    def test_nohostsavailable_in_message(self):
        """NoHostsAvailable errors should be classified as transient."""
        exc = Exception("NoHostsAvailable: Unable to connect to any servers")
        assert _is_transient_error(exc) is True

    def test_connection_refused_in_message(self):
        """Connection refused errors should be classified as transient."""
        exc = Exception("Connection refused to host 192.168.1.1")
        assert _is_transient_error(exc) is True

    def test_connection_reset_in_message(self):
        """Connection reset errors should be classified as transient."""
        exc = Exception("Connection reset by peer")
        assert _is_transient_error(exc) is True

    def test_overloaded_in_message(self):
        """Overloaded errors should be classified as transient."""
        exc = Exception("Server is overloaded, try again later")
        assert _is_transient_error(exc) is True

    def test_transient_error_in_exception_type_name(self):
        """Transient patterns in exception type name should be detected."""

        class WriteTimeoutError(Exception):
            pass

        exc = WriteTimeoutError("Some message")
        assert _is_transient_error(exc) is True

    def test_case_insensitive_matching(self):
        """Error matching should be case-insensitive."""
        exc = Exception("WRITETIMEOUT occurred")
        assert _is_transient_error(exc) is True

        exc2 = Exception("ReadTimeOut happened")
        assert _is_transient_error(exc2) is True

    def test_permanent_error_not_classified_as_transient(self):
        """Permanent errors should not be classified as transient."""
        exc = Exception("Invalid query syntax")
        assert _is_transient_error(exc) is False

    def test_key_error_not_transient(self):
        """KeyError should not be classified as transient."""
        exc = KeyError("missing_column")
        assert _is_transient_error(exc) is False

    def test_value_error_not_transient(self):
        """ValueError should not be classified as transient."""
        exc = ValueError("Invalid value provided")
        assert _is_transient_error(exc) is False

    def test_authentication_error_not_transient(self):
        """Authentication errors should not be classified as transient."""
        exc = Exception("Authentication failed: invalid credentials")
        assert _is_transient_error(exc) is False

    def test_schema_mismatch_not_transient(self):
        """Schema mismatch errors should not be classified as transient."""
        exc = Exception("Schema mismatch: expected 5 columns, got 3")
        assert _is_transient_error(exc) is False


class TestWriteWithRetry:
    """Tests for the _write_with_retry function."""

    def test_successful_write_no_retry(self):
        """Successful write should not trigger retries."""
        mock_write_fn = MagicMock()

        _write_with_retry(
            write_fn=mock_write_fn,
            operation_name="test_write",
            max_retries=3,
        )

        mock_write_fn.assert_called_once()

    def test_transient_error_triggers_retry(self):
        """Transient errors should trigger retries."""
        mock_write_fn = MagicMock()
        # First call raises transient error, second call succeeds
        mock_write_fn.side_effect = [
            Exception("WriteTimeout during insert"),
            None,
        ]

        with patch("feast.infra.contrib.spark_kafka_processor.time.sleep"):
            _write_with_retry(
                write_fn=mock_write_fn,
                operation_name="test_write",
                max_retries=3,
            )

        assert mock_write_fn.call_count == 2

    def test_multiple_transient_errors_retry_until_success(self):
        """Multiple transient errors should keep retrying until success."""
        mock_write_fn = MagicMock()
        # First two calls raise transient errors, third succeeds
        mock_write_fn.side_effect = [
            Exception("WriteTimeout"),
            Exception("ReadTimeout"),
            None,
        ]

        with patch("feast.infra.contrib.spark_kafka_processor.time.sleep"):
            _write_with_retry(
                write_fn=mock_write_fn,
                operation_name="test_write",
                max_retries=3,
            )

        assert mock_write_fn.call_count == 3

    def test_max_retries_exceeded_raises_exception(self):
        """Exceeding max retries should raise the last exception."""
        mock_write_fn = MagicMock()
        # All calls raise transient errors
        mock_write_fn.side_effect = Exception("WriteTimeout")

        with patch("feast.infra.contrib.spark_kafka_processor.time.sleep"):
            with pytest.raises(Exception) as exc_info:
                _write_with_retry(
                    write_fn=mock_write_fn,
                    operation_name="test_write",
                    max_retries=3,
                )

        assert "WriteTimeout" in str(exc_info.value)
        # Initial attempt + 3 retries = 4 total calls
        assert mock_write_fn.call_count == 4

    def test_permanent_error_no_retry(self):
        """Permanent errors should not trigger retries."""
        mock_write_fn = MagicMock()
        mock_write_fn.side_effect = ValueError("Invalid schema")

        with pytest.raises(ValueError) as exc_info:
            _write_with_retry(
                write_fn=mock_write_fn,
                operation_name="test_write",
                max_retries=3,
            )

        assert "Invalid schema" in str(exc_info.value)
        mock_write_fn.assert_called_once()

    def test_exponential_backoff_delay(self):
        """Retries should use exponential backoff delays."""
        mock_write_fn = MagicMock()
        mock_write_fn.side_effect = [
            Exception("WriteTimeout"),
            Exception("WriteTimeout"),
            None,
        ]

        sleep_calls = []

        def track_sleep(delay):
            sleep_calls.append(delay)

        with patch(
            "feast.infra.contrib.spark_kafka_processor.time.sleep",
            side_effect=track_sleep,
        ):
            _write_with_retry(
                write_fn=mock_write_fn,
                operation_name="test_write",
                max_retries=3,
                base_delay=1.0,
                max_delay=30.0,
            )

        # Should have slept twice (before retry 1 and retry 2)
        assert len(sleep_calls) == 2
        # First delay should be ~1.0 (base_delay * 2^0 + jitter)
        assert 1.0 <= sleep_calls[0] <= 1.1
        # Second delay should be ~2.0 (base_delay * 2^1 + jitter)
        assert 2.0 <= sleep_calls[1] <= 2.2

    def test_max_delay_cap(self):
        """Delay should be capped at max_delay."""
        mock_write_fn = MagicMock()
        # Need enough failures to hit the max delay cap
        mock_write_fn.side_effect = [
            Exception("WriteTimeout"),
            Exception("WriteTimeout"),
            Exception("WriteTimeout"),
            None,
        ]

        sleep_calls = []

        def track_sleep(delay):
            sleep_calls.append(delay)

        with patch(
            "feast.infra.contrib.spark_kafka_processor.time.sleep",
            side_effect=track_sleep,
        ):
            _write_with_retry(
                write_fn=mock_write_fn,
                operation_name="test_write",
                max_retries=3,
                base_delay=10.0,
                max_delay=15.0,
            )

        # Third delay would be 10 * 2^2 = 40, but capped at 15
        assert sleep_calls[2] <= 15.0 * 1.1  # Allow for jitter

    def test_zero_retries_fails_immediately(self):
        """With max_retries=0, transient errors should fail immediately."""
        mock_write_fn = MagicMock()
        mock_write_fn.side_effect = Exception("WriteTimeout")

        with pytest.raises(Exception) as exc_info:
            _write_with_retry(
                write_fn=mock_write_fn,
                operation_name="test_write",
                max_retries=0,
            )

        assert "WriteTimeout" in str(exc_info.value)
        mock_write_fn.assert_called_once()


class TestTransientErrorPatterns:
    """Tests to verify the TRANSIENT_ERROR_PATTERNS list."""

    def test_all_patterns_are_lowercase(self):
        """All patterns should be lowercase for case-insensitive matching."""
        for pattern in TRANSIENT_ERROR_PATTERNS:
            assert pattern == pattern.lower(), f"Pattern '{pattern}' is not lowercase"

    def test_expected_patterns_present(self):
        """Verify expected Cassandra/ScyllaDB patterns are present."""
        expected_patterns = [
            "writetimeout",
            "readtimeout",
            "unavailable",
            "operationtimedout",
            "nohostsavailable",
        ]
        for pattern in expected_patterns:
            assert pattern in TRANSIENT_ERROR_PATTERNS, f"Expected pattern '{pattern}' not found"

    def test_generic_timeout_not_present(self):
        """Generic 'timeout' pattern should not be present (too broad)."""
        assert "timeout" not in TRANSIENT_ERROR_PATTERNS
