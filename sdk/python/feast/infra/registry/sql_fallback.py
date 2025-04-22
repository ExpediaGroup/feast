import logging
from pathlib import Path
from typing import Callable, List, Optional, cast

from pydantic import StrictStr

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    OnDemandFeatureView,
    SortedFeatureView,
    StreamFeatureView,
)
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    OnDemandFeatureViewNotFoundException,
    PermissionObjectNotFoundException,
    SavedDatasetNotFound,
    SortedFeatureViewNotFoundException,
    ValidationReferenceNotFound,
)
from feast.infra.registry import proto_registry_utils
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.permissions.permission import Permission
from feast.saved_dataset import SavedDataset, ValidationReference

logger = logging.getLogger(__name__)


class SqlFallbackRegistryConfig(SqlRegistryConfig):
    registry_type: StrictStr = "sql-fallback"
    """ str: Provider name or a class name that implements Registry."""


def _obj_to_proto_with_project_name(obj, project: str):
    proto = obj.to_proto()
    if "spec" in proto.DESCRIPTOR.fields_by_name:
        proto.spec.project = project
    else:
        proto.project = project
    return proto


def _find_and_delete_cache_object(
    registry_proto_field,
    name: str,
    project: str,
):
    for item in registry_proto_field:
        if "spec" in item.DESCRIPTOR.fields_by_name:
            if item.spec.name == name and item.spec.project == project:
                registry_proto_field.remove(item)
                break
        else:
            if item.name == name and item.project == project:
                registry_proto_field.remove(item)
                break


class SqlFallbackRegistry(SqlRegistry):
    def __init__(
        self,
        registry_config,
        project: str,
        repo_path: Optional[Path],
    ):
        assert registry_config is not None and isinstance(
            registry_config, SqlFallbackRegistryConfig
        ), "SqlFallbackRegistry needs a valid registry_config"

        super().__init__(registry_config, project, repo_path)

    def _get_and_cache_object(
        self,
        registry_proto_field,
        get_fn: Callable,
        name: str,
        project: str,
        not_found_exception: Optional[Callable],
        override_cache: bool = False,
    ):
        obj = get_fn(name, project)
        if obj is None:
            if not_found_exception:
                raise not_found_exception(name, project)
            return None

        if project in self.cache_exempt_projects:
            return obj

        if override_cache:
            _find_and_delete_cache_object(registry_proto_field, name, project)
        registry_proto_field.append(_obj_to_proto_with_project_name(obj, project))
        return obj

    def _get_and_cache_objects(
        self,
        registry_proto_field_name: str,
        get_fn: Callable,
        project: str,
        tags: Optional[dict[str, str]],
    ):
        objects = get_fn(project, tags)
        if len(objects) == 0:
            return []
        elif project in self.cache_exempt_projects:
            return objects
        # Clear and reset the cache with the new list of objects
        protos = [_obj_to_proto_with_project_name(obj, project) for obj in objects]
        self.cached_registry_proto.ClearField(registry_proto_field_name)  # type: ignore[arg-type]
        self.cached_registry_proto.__getattribute__(registry_proto_field_name).extend(
            protos
        )
        return objects

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_data_source(
                    self.cached_registry_proto, name, project
                )
            except DataSourceObjectNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.data_sources,
                    self._get_data_source,
                    name,
                    project,
                    DataSourceObjectNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.data_sources,
            self._get_data_source,
            name,
            project,
            DataSourceObjectNotFoundException,
            override_cache=True,
        )

    def list_data_sources(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_data_sources(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "data_sources", self._list_data_sources, project, tags
                )
            else:
                return result
        return self._get_and_cache_objects(
            "data_sources", self._list_data_sources, project, tags
        )

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_entity(
                    self.cached_registry_proto, name, project
                )
            except EntityNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.entities,
                    self._get_entity,
                    name,
                    project,
                    EntityNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.entities,
            self._get_entity,
            name,
            project,
            EntityNotFoundException,
            override_cache=True,
        )

    def list_entities(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_entities(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "entities", self._list_entities, project, tags
                )
            else:
                return result
        return self._get_and_cache_objects(
            "entities", self._list_entities, project, tags
        )

    def get_any_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> BaseFeatureView:
        override_cache = True
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_any_feature_view(
                    self.cached_registry_proto, name, project
                )
            except FeatureViewNotFoundException:
                override_cache = False
        # if allow_cache=False or failed to find any feature view in the cache, fetch from the registry
        feature_view = self._get_any_feature_view(name, project)
        if feature_view is None:
            raise FeatureViewNotFoundException(
                f"Feature view {name} not found in project {project}"
            )
        if isinstance(feature_view, SortedFeatureView):
            if override_cache:
                _find_and_delete_cache_object(
                    self.cached_registry_proto.sorted_feature_views, name, project
                )
            self.cached_registry_proto.sorted_feature_views.append(feature_view)
        elif isinstance(feature_view, StreamFeatureView):
            if override_cache:
                _find_and_delete_cache_object(
                    self.cached_registry_proto.stream_feature_views, name, project
                )
            self.cached_registry_proto.stream_feature_views.append(feature_view)
        elif isinstance(feature_view, OnDemandFeatureView):
            if override_cache:
                _find_and_delete_cache_object(
                    self.cached_registry_proto.on_demand_feature_views,
                    name,
                    project,
                )
            self.cached_registry_proto.on_demand_feature_views.append(feature_view)
        else:
            if override_cache:
                _find_and_delete_cache_object(
                    self.cached_registry_proto.feature_views, name, project
                )
            self.cached_registry_proto.feature_views.append(feature_view)
        return feature_view

    def list_all_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[BaseFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_all_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) > 0:
                return result
        # If allow_cache=False or failed to find any feature views in the cache, fetch from the registry
        feature_views = self._get_and_cache_objects(
            "feature_views", self._list_feature_views, project, tags
        )
        on_demand_feature_views = self._get_and_cache_objects(
            "on_demand_feature_views", self._list_on_demand_feature_views, project, tags
        )
        stream_feature_views = self._get_and_cache_objects(
            "stream_feature_views", self._list_stream_feature_views, project, tags
        )
        sorted_feature_views = self._get_and_cache_objects(
            "sorted_feature_views", self._list_sorted_feature_views, project, tags
        )
        return (
            cast(list[BaseFeatureView], feature_views)
            + cast(list[BaseFeatureView], on_demand_feature_views)
            + cast(list[BaseFeatureView], stream_feature_views)
            + cast(list[BaseFeatureView], sorted_feature_views)
        )

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_feature_view(
                    self.cached_registry_proto, name, project
                )
            except FeatureViewNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.feature_views,
                    self._get_feature_view,
                    name,
                    project,
                    FeatureViewNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.feature_views,
            self._get_feature_view,
            name,
            project,
            FeatureViewNotFoundException,
            override_cache=True,
        )

    def list_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "feature_views", self._list_feature_views, project, tags
                )
            else:
                return result
        return self._get_and_cache_objects(
            "feature_views", self._list_feature_views, project, tags
        )

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_on_demand_feature_view(
                    self.cached_registry_proto, name, project
                )
            except OnDemandFeatureViewNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.on_demand_feature_views,
                    self._get_on_demand_feature_view,
                    name,
                    project,
                    OnDemandFeatureViewNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.on_demand_feature_views,
            self._get_on_demand_feature_view,
            name,
            project,
            OnDemandFeatureViewNotFoundException,
            override_cache=True,
        )

    def list_on_demand_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_on_demand_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "on_demand_feature_views",
                    self._list_on_demand_feature_views,
                    project,
                    tags,
                )
            else:
                return result
        return self._get_and_cache_objects(
            "on_demand_feature_views", self._list_on_demand_feature_views, project, tags
        )

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_stream_feature_view(
                    self.cached_registry_proto, name, project
                )
            except FeatureViewNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.stream_feature_views,
                    self._get_stream_feature_view,
                    name,
                    project,
                    FeatureViewNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.stream_feature_views,
            self._get_stream_feature_view,
            name,
            project,
            FeatureViewNotFoundException,
            override_cache=True,
        )

    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_stream_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "stream_feature_views",
                    self._list_stream_feature_views,
                    project,
                    tags,
                )
            else:
                return result
        return self._get_and_cache_objects(
            "stream_feature_views", self._list_stream_feature_views, project, tags
        )

    def get_sorted_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SortedFeatureView:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_sorted_feature_view(
                    self.cached_registry_proto, name, project
                )
            except SortedFeatureViewNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.sorted_feature_views,
                    self._get_sorted_feature_view,
                    name,
                    project,
                    SortedFeatureViewNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.sorted_feature_views,
            self._get_sorted_feature_view,
            name,
            project,
            SortedFeatureViewNotFoundException,
            override_cache=True,
        )

    def list_sorted_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SortedFeatureView]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_sorted_feature_views(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "sorted_feature_views",
                    self._list_sorted_feature_views,
                    project,
                    tags,
                )
            else:
                return result
        return self._get_and_cache_objects(
            "sorted_feature_views", self._list_sorted_feature_views, project, tags
        )

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_feature_service(
                    self.cached_registry_proto, name, project
                )
            except FeatureServiceNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.feature_services,
                    self._get_feature_service,
                    name,
                    project,
                    FeatureServiceNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.feature_services,
            self._get_feature_service,
            name,
            project,
            FeatureServiceNotFoundException,
            override_cache=True,
        )

    def list_feature_services(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_feature_services(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "feature_services", self._list_feature_services, project, tags
                )
            else:
                return result
        return self._get_and_cache_objects(
            "feature_services", self._list_feature_services, project, tags
        )

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_saved_dataset(
                    self.cached_registry_proto, name, project
                )
            except SavedDatasetNotFound:
                return self._get_and_cache_object(
                    self.cached_registry_proto.saved_datasets,
                    self._get_saved_dataset,
                    name,
                    project,
                    SavedDatasetNotFound,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.saved_datasets,
            self._get_saved_dataset,
            name,
            project,
            SavedDatasetNotFound,
            override_cache=True,
        )

    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SavedDataset]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_saved_datasets(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "saved_datasets", self._list_saved_datasets, project, tags
                )
            else:
                return result
        return self._get_and_cache_objects(
            "saved_datasets", self._list_saved_datasets, project, tags
        )

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_validation_reference(
                    self.cached_registry_proto, name, project
                )
            except ValidationReferenceNotFound:
                return self._get_and_cache_object(
                    self.cached_registry_proto.validation_references,
                    self._get_validation_reference,
                    name,
                    project,
                    ValidationReferenceNotFound,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.validation_references,
            self._get_validation_reference,
            name,
            project,
            ValidationReferenceNotFound,
            override_cache=True,
        )

    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_validation_references(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "validation_references",
                    self._list_validation_references,
                    project,
                    tags,
                )
            else:
                return result
        return self._get_and_cache_objects(
            "validation_references", self._list_validation_references, project, tags
        )

    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        if allow_cache:
            try:
                self._refresh_cached_registry_if_necessary()
                return proto_registry_utils.get_permission(
                    self.cached_registry_proto, name, project
                )
            except PermissionObjectNotFoundException:
                return self._get_and_cache_object(
                    self.cached_registry_proto.permissions,
                    self._get_permission,
                    name,
                    project,
                    PermissionObjectNotFoundException,
                )
        return self._get_and_cache_object(
            self.cached_registry_proto.permissions,
            self._get_permission,
            name,
            project,
            PermissionObjectNotFoundException,
            override_cache=True,
        )

    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        if allow_cache:
            self._refresh_cached_registry_if_necessary()
            result = proto_registry_utils.list_permissions(
                self.cached_registry_proto, project, tags
            )
            if len(result) == 0:
                return self._get_and_cache_objects(
                    "permissions", self._list_permissions, project, tags
                )
            else:
                return result
        return self._get_and_cache_objects(
            "permissions", self._list_permissions, project, tags
        )
