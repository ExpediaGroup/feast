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
"""
Unit tests for SnowflakeRegistry's materialization-interval-history support.
No live Snowflake instance is available in this environment, so
GetSnowflakeConnection/execute_snowflake_statement are mocked -- these tests
verify the query-construction and idempotency logic, not real Snowflake
behavior.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from feast.infra.registry.snowflake import SnowflakeRegistry, SnowflakeRegistryConfig
from feast.utils import _utc_now


@pytest.fixture
def snowflake_registry():
    """A SnowflakeRegistry instance with __init__ (which opens a real
    connection) bypassed, matching this file's existing mocking style."""
    config = SnowflakeRegistryConfig(
        registry_type="snowflake.registry",
        account="acct",
        user="user",
        password="password",
        role="role",
        warehouse="warehouse",
        database="TEST_DB",
        schema_="TEST_SCHEMA",
        materialization_intervals_max_len=3,
    )
    with patch.object(SnowflakeRegistry, "__init__", lambda self, *a, **k: None):
        registry = SnowflakeRegistry.__new__(SnowflakeRegistry)
    registry.registry_config = config
    registry.registry_path = 'TEST_DB."TEST_SCHEMA"'
    return registry


def _empty_df():
    return pd.DataFrame({"START_TIME": [], "END_TIME": []})


class TestSnowflakeRegistryMaterializationIntervalHistory:
    def test_record_history_inserts_new_intervals(self, snowflake_registry):
        now = _utc_now()
        intervals = [
            (now - timedelta(days=1, hours=1), now - timedelta(days=1)),
            (now - timedelta(hours=1), now),
        ]

        executed = []

        def fake_execute(conn, query):
            executed.append(query)
            result = MagicMock()
            result.fetch_pandas_all.return_value = _empty_df()
            return result

        with (
            patch(
                "feast.infra.registry.snowflake.GetSnowflakeConnection"
            ) as mock_conn_ctx,
            patch(
                "feast.infra.registry.snowflake.execute_snowflake_statement",
                side_effect=fake_execute,
            ),
        ):
            mock_conn_ctx.return_value.__enter__.return_value = MagicMock()
            snowflake_registry._record_materialization_interval_history(
                "driver_stats", "test_project", intervals
            )

        # One SELECT (existing-keys check) + one INSERT.
        assert len(executed) == 2
        assert "SELECT" in executed[0]
        assert "MATERIALIZATION_INTERVAL_HISTORY" in executed[0]
        assert "INSERT INTO" in executed[1]
        assert "driver_stats" in executed[1]
        assert "test_project" in executed[1]

    def test_record_history_skips_already_recorded_intervals(self, snowflake_registry):
        now = _utc_now()
        start, end = now - timedelta(hours=1), now

        existing_df = pd.DataFrame(
            {
                "START_TIME": [start],
                "END_TIME": [end],
            }
        )

        def fake_execute(conn, query):
            result = MagicMock()
            if "SELECT" in query:
                result.fetch_pandas_all.return_value = existing_df
            return result

        with (
            patch(
                "feast.infra.registry.snowflake.GetSnowflakeConnection"
            ) as mock_conn_ctx,
            patch(
                "feast.infra.registry.snowflake.execute_snowflake_statement",
                side_effect=fake_execute,
            ) as mock_execute,
        ):
            mock_conn_ctx.return_value.__enter__.return_value = MagicMock()
            snowflake_registry._record_materialization_interval_history(
                "driver_stats", "test_project", [(start, end)]
            )

        # Only the SELECT ran -- no INSERT, since the interval already exists.
        queries = [call.args[1] for call in mock_execute.call_args_list]
        assert not any("INSERT INTO" in q for q in queries)

    def test_record_history_noop_on_empty_intervals(self, snowflake_registry):
        with patch(
            "feast.infra.registry.snowflake.GetSnowflakeConnection"
        ) as mock_conn_ctx:
            snowflake_registry._record_materialization_interval_history(
                "driver_stats", "test_project", []
            )
            mock_conn_ctx.assert_not_called()

    def test_get_materialization_interval_history_returns_entries(
        self, snowflake_registry
    ):
        now = _utc_now()
        df = pd.DataFrame(
            {
                "START_TIME": [now - timedelta(hours=1)],
                "END_TIME": [now],
                "RECORDED_AT": [now],
            }
        )

        def fake_execute(conn, query):
            result = MagicMock()
            result.fetch_pandas_all.return_value = df
            return result

        with (
            patch(
                "feast.infra.registry.snowflake.GetSnowflakeConnection"
            ) as mock_conn_ctx,
            patch(
                "feast.infra.registry.snowflake.execute_snowflake_statement",
                side_effect=fake_execute,
            ),
        ):
            mock_conn_ctx.return_value.__enter__.return_value = MagicMock()
            entries = snowflake_registry.get_materialization_interval_history(
                "driver_stats", "test_project"
            )

        assert len(entries) == 1
        assert entries[0].feature_view_name == "driver_stats"
        assert entries[0].project == "test_project"
