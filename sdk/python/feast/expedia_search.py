from typing import List, Optional

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import BoolValue

from feast.feature_view import FeatureView
from feast.project import Project
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ExpediaProjectAndRelatedFeatureViews as ExpediaProjectAndRelatedFeatureViewsProto,
)
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ExpediaSearchFeatureViewsRequest as ExpediaSearchFeatureViewsRequestProto,
)
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ExpediaSearchFeatureViewsResponse as ExpediaSearchFeatureViewsResponseProto,
)
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ExpediaSearchProjectsRequest as ExpediaSearchProjectsRequestProto,
)
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ExpediaSearchProjectsResponse as ExpediaSearchProjectsResponseProto,
)


class ExpediaProjectAndRelatedFeatureViews:
    """
    Container for a Project and its related FeatureViews.

    Attributes:
        project: The Feast Project object.
        feature_views: List of FeatureView objects associated with the project.
    """

    project: Project
    feature_views: List[FeatureView]

    def __init__(self, project: Project, feature_views: List[FeatureView]):
        """
        Creates an ExpediaProjectAndRelatedFeatureViews object.

        Args:
            project: The Feast Project object.
            feature_views: List of FeatureView objects associated with the project.
        """
        self.project = project
        self.feature_views = feature_views

    @classmethod
    def from_proto(cls, proto: ExpediaProjectAndRelatedFeatureViewsProto):
        """
        Creates an ExpediaProjectAndRelatedFeatureViews object from its protobuf representation.

        Args:
            proto: Protobuf representation.

        Returns:
            ExpediaProjectAndRelatedFeatureViews object.
        """
        return cls(
            project=Project.from_proto(proto.project),
            feature_views=[FeatureView.from_proto(fv) for fv in proto.feature_views],
        )

    def to_proto(self) -> ExpediaProjectAndRelatedFeatureViewsProto:
        """
        Converts this object to its protobuf representation.

        Returns:
            ExpediaProjectAndRelatedFeatureViewsProto protobuf.
        """
        proto = ExpediaProjectAndRelatedFeatureViewsProto()
        proto.project.CopyFrom(self.project.to_proto())
        proto.feature_views.extend([fv.to_proto() for fv in self.feature_views])
        return proto


class ExpediaSearchFeatureViewsRequest:
    """
    Request object for searching FeatureViews.

    Attributes:
        search_text: Text to search for.
        online: Whether the feature view is online.
        application: Application tag.
        team: Team tag.
        created_at: Creation timestamp.
        updated_at: Last updated timestamp.
        page_size: Number of results per page.
        page_index: Page index for pagination.
    """

    search_text: str
    online: Optional[bool]
    application: str
    team: str
    created_at: Optional[Timestamp]
    updated_at: Optional[Timestamp]
    page_size: int
    page_index: int

    def __init__(
        self,
        search_text: str = "",
        online: Optional[bool] = None,
        application: str = "",
        team: str = "",
        created_at: Optional[Timestamp] = None,
        updated_at: Optional[Timestamp] = None,
        page_size: int = 10,
        page_index: int = 0,
    ):
        """
        Creates an ExpediaSearchFeatureViewsRequest object.

        Args:
            search_text: Text to search for.
            online: Whether the feature view is online.
            application: Application tag.
            team: Team tag.
            created_at: Creation timestamp.
            updated_at: Last updated timestamp.
            page_size: Number of results per page.
            page_index: Page index for pagination.
        """
        self.search_text = search_text
        self.online = online
        self.application = application
        self.team = team
        self.created_at = created_at
        self.updated_at = updated_at
        self.page_size = page_size
        self.page_index = page_index

    @classmethod
    def from_proto(cls, proto: ExpediaSearchFeatureViewsRequestProto):
        """
        Creates an ExpediaSearchFeatureViewsRequest object from its protobuf representation.

        Args:
            proto: Protobuf representation.

        Returns:
            ExpediaSearchFeatureViewsRequest object.
        """
        online = proto.online.value if proto.HasField("online") else None
        created_at = proto.created_at if proto.HasField("created_at") else None
        updated_at = proto.updated_at if proto.HasField("updated_at") else None
        return cls(
            search_text=proto.search_text,
            online=online,
            application=proto.application,
            team=proto.team,
            created_at=created_at,
            updated_at=updated_at,
            page_size=proto.page_size,
            page_index=proto.page_index,
        )

    def to_proto(self) -> ExpediaSearchFeatureViewsRequestProto:
        """
        Converts this object to its protobuf representation.

        Returns:
            ExpediaSearchFeatureViewsRequestProto protobuf.
        """
        proto = ExpediaSearchFeatureViewsRequestProto()
        proto.search_text = self.search_text
        proto.application = self.application
        proto.team = self.team
        proto.page_size = self.page_size
        proto.page_index = self.page_index
        if self.online is not None:
            proto.online.CopyFrom(BoolValue(value=self.online))
        if self.created_at is not None:
            proto.created_at.CopyFrom(self.created_at)
        if self.updated_at is not None:
            proto.updated_at.CopyFrom(self.updated_at)
        return proto


class ExpediaSearchFeatureViewsResponse:
    """
    Response object for searching FeatureViews.

    Attributes:
        feature_views: List of FeatureView objects.
        total_feature_views: Total number of feature views found.
        total_page_indices: Total number of pages.
    """

    feature_views: List[FeatureView]
    total_feature_views: int
    total_page_indices: int

    def __init__(
        self,
        feature_views: List[FeatureView],
        total_feature_views: int,
        total_page_indices: int,
    ):
        """
        Creates an ExpediaSearchFeatureViewsResponse object.

        Args:
            feature_views: List of FeatureView objects.
            total_feature_views: Total number of feature views found.
            total_page_indices: Total number of pages.
        """
        self.feature_views = feature_views
        self.total_feature_views = total_feature_views
        self.total_page_indices = total_page_indices

    @classmethod
    def from_proto(cls, proto: ExpediaSearchFeatureViewsResponseProto):
        """
        Creates an ExpediaSearchFeatureViewsResponse object from its protobuf representation.

        Args:
            proto: Protobuf representation.

        Returns:
            ExpediaSearchFeatureViewsResponse object.
        """
        return cls(
            feature_views=[FeatureView.from_proto(fv) for fv in proto.feature_views],
            total_feature_views=proto.total_feature_views,
            total_page_indices=proto.total_page_indices,
        )

    def to_proto(self) -> ExpediaSearchFeatureViewsResponseProto:
        """
        Converts this object to its protobuf representation.

        Returns:
            ExpediaSearchFeatureViewsResponseProto protobuf.
        """
        proto = ExpediaSearchFeatureViewsResponseProto()
        proto.feature_views.extend([fv.to_proto() for fv in self.feature_views])
        proto.total_feature_views = self.total_feature_views
        proto.total_page_indices = self.total_page_indices
        return proto


class ExpediaSearchProjectsRequest:
    """
    Request object for searching Projects.

    Attributes:
        search_text: Text to search for.
        updated_at: Last updated timestamp.
        page_size: Number of results per page.
        page_index: Page index for pagination.
    """

    search_text: str
    updated_at: Optional[Timestamp]
    page_size: int
    page_index: int

    def __init__(
        self,
        search_text: str = "",
        updated_at: Optional[Timestamp] = None,
        page_size: int = 10,
        page_index: int = 0,
    ):
        """
        Creates an ExpediaSearchProjectsRequest object.

        Args:
            search_text: Text to search for.
            updated_at: Last updated timestamp.
            page_size: Number of results per page.
            page_index: Page index for pagination.
        """
        self.search_text = search_text
        self.updated_at = updated_at
        self.page_size = page_size
        self.page_index = page_index

    @classmethod
    def from_proto(cls, proto: ExpediaSearchProjectsRequestProto):
        """
        Creates an ExpediaSearchProjectsRequest object from its protobuf representation.

        Args:
            proto: Protobuf representation.

        Returns:
            ExpediaSearchProjectsRequest object.
        """
        updated_at = proto.updated_at if proto.HasField("updated_at") else None
        return cls(
            search_text=proto.search_text,
            updated_at=updated_at,
            page_size=proto.page_size,
            page_index=proto.page_index,
        )

    def to_proto(self) -> ExpediaSearchProjectsRequestProto:
        """
        Converts this object to its protobuf representation.

        Returns:
            ExpediaSearchProjectsRequestProto protobuf.
        """
        proto = ExpediaSearchProjectsRequestProto()
        proto.search_text = self.search_text
        proto.page_size = self.page_size
        proto.page_index = self.page_index
        if self.updated_at is not None:
            proto.updated_at.CopyFrom(self.updated_at)
        return proto


class ExpediaSearchProjectsResponse:
    """
    Response object for searching Projects.

    Attributes:
        projects_and_related_feature_views: List of ExpediaProjectAndRelatedFeatureViews objects.
        total_projects: Total number of projects found.
        total_page_indices: Total number of pages.
    """

    projects_and_related_feature_views: List[ExpediaProjectAndRelatedFeatureViews]
    total_projects: int
    total_page_indices: int

    def __init__(
        self,
        projects_and_related_feature_views: List[ExpediaProjectAndRelatedFeatureViews],
        total_projects: int,
        total_page_indices: int,
    ):
        """
        Creates an ExpediaSearchProjectsResponse object.

        Args:
            projects_and_related_feature_views: List of ExpediaProjectAndRelatedFeatureViews objects.
            total_projects: Total number of projects found.
            total_page_indices: Total number of pages.
        """
        self.projects_and_related_feature_views = projects_and_related_feature_views
        self.total_projects = total_projects
        self.total_page_indices = total_page_indices

    @classmethod
    def from_proto(cls, proto: ExpediaSearchProjectsResponseProto):
        """
        Creates an ExpediaSearchProjectsResponse object from its protobuf representation.

        Args:
            proto: Protobuf representation.

        Returns:
            ExpediaSearchProjectsResponse object.
        """
        return cls(
            projects_and_related_feature_views=[
                ExpediaProjectAndRelatedFeatureViews.from_proto(p)
                for p in proto.projects_and_related_feature_views
            ],
            total_projects=proto.total_projects,
            total_page_indices=proto.total_page_indices,
        )

    def to_proto(self) -> ExpediaSearchProjectsResponseProto:
        """
        Converts this object to its protobuf representation.

        Returns:
            ExpediaSearchProjectsResponseProto protobuf.
        """
        proto = ExpediaSearchProjectsResponseProto()
        proto.projects_and_related_feature_views.extend(
            [p.to_proto() for p in self.projects_and_related_feature_views]
        )
        proto.total_projects = self.total_projects
        proto.total_page_indices = self.total_page_indices
        return proto
