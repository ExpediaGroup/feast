import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from pydantic import StrictStr
from sqlalchemy import Table

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    OnDemandFeatureView,
    Project,
    SortedFeatureView,
    StreamFeatureView,
)
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeastObjectNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    OnDemandFeatureViewNotFoundException,
    PermissionObjectNotFoundException,
    ProjectObjectNotFoundException,
    SavedDatasetNotFound,
    SortedFeatureViewNotFoundException,
    ValidationReferenceNotFound,
)
from feast.infra.registry.sql import (
    SqlRegistry,
    SqlRegistryConfig,
    data_sources,
    entities,
    feature_services,
    feature_views,
    on_demand_feature_views,
    permissions,
    saved_datasets,
    sorted_feature_views,
    stream_feature_views,
    validation_references,
)
from feast.permissions.permission import Permission
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.utils import _utc_now

logger = logging.getLogger(__name__)


class SqlFallbackRegistryConfig(SqlRegistryConfig):
    registry_type: StrictStr = "sql-fallback"
    """ str: Provider name or a class name that implements Registry."""


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

        self.cached_project_map: Dict[str, Tuple[Project, datetime]] = {}
        self.cached_data_source_map: Dict[
            str, Dict[str, Tuple[DataSource, datetime]]
        ] = {}
        self.cached_entity_map: Dict[str, Dict[str, Tuple[Entity, datetime]]] = {}
        self.cached_feature_service_map: Dict[
            str, Dict[str, Tuple[FeatureService, datetime]]
        ] = {}
        self.cached_feature_view_map: Dict[
            str, Dict[str, Tuple[FeatureView, datetime]]
        ] = {}
        self.cached_on_demand_feature_view_map: Dict[
            str, Dict[str, Tuple[OnDemandFeatureView, datetime]]
        ] = {}
        self.cached_permission_map: Dict[
            str, Dict[str, Tuple[Permission, datetime]]
        ] = {}
        self.cached_saved_dataset_map: Dict[
            str, Dict[str, Tuple[SavedDataset, datetime]]
        ] = {}
        self.cached_sorted_feature_view_map: Dict[
            str, Dict[str, Tuple[SortedFeatureView, datetime]]
        ] = {}
        self.cached_stream_feature_view_map: Dict[
            str, Dict[str, Tuple[StreamFeatureView, datetime]]
        ] = {}
        self.cached_validation_reference_map: Dict[
            str, Dict[str, Tuple[ValidationReference, datetime]]
        ] = {}

        self.cache_process_list = [
            (self.cached_data_source_map, self._get_data_source, data_sources.name),
            (self.cached_entity_map, self._get_entity, entities.name),
            (
                self.cached_feature_service_map,
                self._get_feature_service,
                feature_services.name,
            ),
            (self.cached_feature_view_map, self._get_feature_view, feature_views.name),
            (
                self.cached_on_demand_feature_view_map,
                self._get_on_demand_feature_view,
                on_demand_feature_views.name,
            ),
            (
                self.cached_sorted_feature_view_map,
                self._get_sorted_feature_view,
                sorted_feature_views.name,
            ),
            (
                self.cached_stream_feature_view_map,
                self._get_stream_feature_view,
                stream_feature_views.name,
            ),
            (
                self.cached_saved_dataset_map,
                self._get_saved_dataset,
                saved_datasets.name,
            ),
            (
                self.cached_validation_reference_map,
                self._get_validation_reference,
                validation_references.name,
            ),
            (self.cached_permission_map, self._get_permission, permissions.name),
        ]

        super().__init__(registry_config, project, repo_path)

    def proto(self) -> RegistryProto:
        # proto() is called during the refresh cycle, this implementation only refreshes cached items
        projects_refreshed = 0
        for project_name, project_ttl in self.cached_project_map.items():
            if (
                project_name in self.cached_project_map
                and self.cached_project_map[project_name][1] <= _utc_now()  # type: ignore
            ):
                try:
                    project_obj = self._get_project(project_name)
                    if project_obj:
                        self.cached_project_map[project_name] = (
                            project_obj,
                            _utc_now() + self.cached_registry_proto_ttl,
                        )
                    else:
                        del self.cached_project_map[project_name]
                except ProjectObjectNotFoundException:
                    del self.cached_project_map[project_name]
                finally:
                    projects_refreshed += 1
        logger.info(f"Refreshed {projects_refreshed} projects in cache")

        def process_expiration(cache_map, get_fn, cache_name):
            obj_refreshed = 0
            for project, items in cache_map.items():
                for name, (obj, ttl) in items.items():
                    if ttl <= _utc_now():
                        try:
                            obj = get_fn(name, project)
                            cache_map[project][name] = (
                                obj,
                                _utc_now() + self.cached_registry_proto_ttl,
                            )
                        except FeastObjectNotFoundException:
                            del cache_map[project][name]
                        finally:
                            obj_refreshed += 1
            logger.info(f"Refreshed {obj_refreshed} objects in {cache_name} cache")

        if self.thread_pool_executor_worker_count == 0:
            logger.info("Starting timer for single threaded self.proto()")
            start = time.time()
            for cache_map, get_fn, cache_name in self.cache_process_list:
                process_expiration(cache_map, get_fn, cache_name)
            logger.info(
                f"Finished processing cache expiration and refresh in {time.time() - start} seconds"
            )
        else:
            try:
                logger.info("Starting timer for multi threaded self.proto()")
                start = time.time()

                with ThreadPoolExecutor(
                    max_workers=min(
                        self.thread_pool_executor_worker_count,
                        len(self.cache_process_list),
                    )
                ) as executor:
                    executor.map(
                        lambda args: process_expiration(*args), self.cache_process_list
                    )

                logger.info(
                    f"Multi threaded self.proto() took {time.time() - start} seconds to process cache expiration and refresh"
                )
            except RuntimeError as e:
                logger.error(
                    f"Resetting cache due to error during multi-threaded cache processing: {e}"
                )
                for cache_map, _, _ in self.cache_process_list:
                    cache_map.clear()  # type: ignore
        return self.cached_registry_proto

    def _cache_obj_with_ttl(
        self,
        cache_map: Dict[str, Dict[str, Tuple[Any, datetime]]],
        name: str,
        project: str,
        obj: Any,
    ):
        if project not in cache_map:
            cache_map[project] = {}

        ttl = _utc_now() + self.cached_registry_proto_ttl
        cache_map[project][name] = (obj, ttl)

    def _get_and_cache_object(
        self,
        cache_map: Dict[str, Dict[str, Tuple[Any, datetime]]],
        get_fn: Callable,
        name: str,
        project: str,
        not_found_exception: Optional[Callable],
    ) -> Any:
        obj = get_fn(name, project)
        if obj is None:
            if not_found_exception:
                if project in cache_map and name in cache_map[project]:
                    del cache_map[project][name]
                raise not_found_exception(name, project)
            return None

        if project in self.cache_exempt_projects:
            return obj

        self._cache_obj_with_ttl(cache_map, name, project, obj)
        return obj

    def _delete_object(
        self,
        table: Table,
        name: str,
        project: str,
        id_field_name: str,
        not_found_exception: Optional[Callable],
    ):
        deleted_rows = super()._delete_object(
            table, name, project, id_field_name, not_found_exception
        )
        cache_map: Union[Dict[str, Dict[str, Tuple[Any, datetime]]] | None] = None
        for cm, _, cache_name in self.cache_process_list:
            if cache_name == table.name:
                cache_map = cm  # type: ignore
                break
        if (
            cache_map is not None
            and project in cache_map
            and name in cache_map[project]
        ):
            del cache_map[project][name]
        return deleted_rows

    def get_project(
        self,
        name: str,
        allow_cache: bool = False,
    ) -> Project:
        if allow_cache:
            if name in self.cached_project_map:
                return self.cached_project_map[name][0]

        project = self._get_project(name)
        if project is None:
            raise ProjectObjectNotFoundException(name)

        if name in self.cache_exempt_projects:
            return project

        ttl = _utc_now() + self.cached_registry_proto_ttl

        self.cached_project_map[name] = (project, ttl)
        for cache_map, _, _ in self.cache_process_list:
            if name not in cache_map:  # type: ignore
                cache_map[name] = {}  # type: ignore
        return project

    def delete_project(self, name: str, commit: bool = True):
        super().delete_project(name, commit)
        if commit:
            # Clear the cache for the deleted project
            if name in self.cached_project_map:
                del self.cached_project_map[name]
            for cache_map, _, _ in self.cache_process_list:
                if name in cache_map:  # type: ignore
                    del cache_map[name]  # type: ignore

    def get_any_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> BaseFeatureView:
        if allow_cache:
            if (
                project in self.cached_feature_view_map
                and name in self.cached_feature_view_map[project]
            ):
                return self.cached_feature_view_map[project][name][0]
            if (
                project in self.cached_on_demand_feature_view_map
                and name in self.cached_on_demand_feature_view_map[project]
            ):
                return self.cached_on_demand_feature_view_map[project][name][0]
            if (
                project in self.cached_sorted_feature_view_map
                and name in self.cached_sorted_feature_view_map[project]
            ):
                return self.cached_sorted_feature_view_map[project][name][0]
            if (
                project in self.cached_stream_feature_view_map
                and name in self.cached_stream_feature_view_map[project]
            ):
                return self.cached_stream_feature_view_map[project][name][0]

        # if allow_cache=False or failed to find any feature view in the cache, fetch from the registry
        feature_view = self._get_any_feature_view(name, project)
        if feature_view is None:
            raise FeatureViewNotFoundException(
                f"Feature view {name} not found in project {project}"
            )

        if project in self.cache_exempt_projects:
            return feature_view

        if isinstance(feature_view, SortedFeatureView):
            self._cache_obj_with_ttl(
                self.cached_feature_view_map, name, project, feature_view
            )
        elif isinstance(feature_view, StreamFeatureView):
            self._cache_obj_with_ttl(
                self.cached_stream_feature_view_map, name, project, feature_view
            )
        elif isinstance(feature_view, OnDemandFeatureView):
            self._cache_obj_with_ttl(
                self.cached_on_demand_feature_view_map, name, project, feature_view
            )
        else:
            self._cache_obj_with_ttl(
                self.cached_feature_view_map, name, project, feature_view
            )
        return feature_view

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        if allow_cache:
            if (
                project in self.cached_data_source_map
                and name in self.cached_data_source_map[project]
            ):
                return self.cached_data_source_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_data_source_map,
            self._get_data_source,
            name,
            project,
            DataSourceObjectNotFoundException,
        )

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        if allow_cache:
            if (
                project in self.cached_entity_map
                and name in self.cached_entity_map[project]
            ):
                return self.cached_entity_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_entity_map,
            self._get_entity,
            name,
            project,
            EntityNotFoundException,
        )

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        if allow_cache:
            if (
                project in self.cached_feature_service_map
                and name in self.cached_feature_service_map[project]
            ):
                return self.cached_feature_service_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_feature_service_map,
            self._get_feature_service,
            name,
            project,
            FeatureServiceNotFoundException,
        )

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        if allow_cache:
            if (
                project in self.cached_feature_view_map
                and name in self.cached_feature_view_map[project]
            ):
                return self.cached_feature_view_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_feature_view_map,
            self._get_feature_view,
            name,
            project,
            FeatureViewNotFoundException,
        )

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            if (
                project in self.cached_on_demand_feature_view_map
                and name in self.cached_on_demand_feature_view_map[project]
            ):
                return self.cached_on_demand_feature_view_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_on_demand_feature_view_map,
            self._get_on_demand_feature_view,
            name,
            project,
            OnDemandFeatureViewNotFoundException,
        )

    def get_sorted_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SortedFeatureView:
        if allow_cache:
            if (
                project in self.cached_sorted_feature_view_map
                and name in self.cached_sorted_feature_view_map[project]
            ):
                return self.cached_sorted_feature_view_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_sorted_feature_view_map,
            self._get_sorted_feature_view,
            name,
            project,
            SortedFeatureViewNotFoundException,
        )

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
        if allow_cache:
            if (
                project in self.cached_stream_feature_view_map
                and name in self.cached_stream_feature_view_map[project]
            ):
                return self.cached_stream_feature_view_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_stream_feature_view_map,
            self._get_stream_feature_view,
            name,
            project,
            FeatureViewNotFoundException,
        )

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        if allow_cache:
            if (
                project in self.cached_saved_dataset_map
                and name in self.cached_saved_dataset_map[project]
            ):
                return self.cached_saved_dataset_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_saved_dataset_map,
            self._get_saved_dataset,
            name,
            project,
            SavedDatasetNotFound,
        )

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        if allow_cache:
            if (
                project in self.cached_validation_reference_map
                and name in self.cached_validation_reference_map[project]
            ):
                return self.cached_validation_reference_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_validation_reference_map,
            self._get_validation_reference,
            name,
            project,
            ValidationReferenceNotFound,
        )

    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        if allow_cache:
            if (
                project in self.cached_permission_map
                and name in self.cached_permission_map[project]
            ):
                return self.cached_permission_map[project][name][0]

        return self._get_and_cache_object(
            self.cached_permission_map,
            self._get_permission,
            name,
            project,
            PermissionObjectNotFoundException,
        )

    def list_data_sources(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        return self._list_data_sources(project, tags)

    def list_entities(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        return self._list_entities(project, tags)

    def list_all_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[BaseFeatureView]:
        fvs = self._list_feature_views(project, tags)
        od_fvs = self._list_on_demand_feature_views(project, tags)
        stream_fvs = self._list_stream_feature_views(project, tags)
        sorted_fvs = self._list_sorted_feature_views(project, tags)
        return (
            cast(list[BaseFeatureView], fvs)
            + cast(list[BaseFeatureView], od_fvs)
            + cast(list[BaseFeatureView], stream_fvs)
            + cast(list[BaseFeatureView], sorted_fvs)
        )

    def list_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        return self._list_feature_views(project, tags)

    def list_on_demand_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        return self._list_on_demand_feature_views(project, tags)

    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        return self._list_stream_feature_views(project, tags)

    def list_sorted_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SortedFeatureView]:
        return self._list_sorted_feature_views(project, tags)

    def list_feature_services(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        return self._list_feature_services(project, tags)

    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SavedDataset]:
        return self._list_saved_datasets(project, tags)

    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        return self._list_validation_references(project, tags)

    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        return self._list_permissions(project, tags)
