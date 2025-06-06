from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version

from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaSource,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.redshift_source import RedshiftSource
from feast.infra.offline_stores.snowflake_source import SnowflakeSource

from .batch_feature_view import BatchFeatureView
from .data_source import KafkaSource, KinesisSource, PushSource, RequestSource
from .entity import Entity
from .feature import Feature
from .feature_service import FeatureService
from .feature_store import FeatureStore
from .feature_view import FeatureView
from .field import Field
from .on_demand_feature_view import OnDemandFeatureView
from .project import Project
from .repo_config import RepoConfig
from .sorted_feature_view import SortedFeatureView
from .stream_feature_view import StreamFeatureView
from .value_type import ValueType

try:
    __version__ = _version("feast")
except PackageNotFoundError:
    # package is not installed
    pass

__all__ = [
    "BatchFeatureView",
    "Entity",
    "KafkaSource",
    "KinesisSource",
    "Feature",
    "Field",
    "FeatureService",
    "FeatureStore",
    "FeatureView",
    "SortedFeatureView",
    "OnDemandFeatureView",
    "RepoConfig",
    "StreamFeatureView",
    "ValueType",
    "BigQuerySource",
    "FileSource",
    "RedshiftSource",
    "SnowflakeSource",
    "PushSource",
    "RequestSource",
    "AthenaSource",
    "SparkSource",
    "Project",
]
