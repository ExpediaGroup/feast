import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

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


class SqlFallbackCacheMap:
    def __init__(
        self,
        name: str,
        get_fn: Callable,
    ):
        self.lock = Lock()
        self.name = name
        self.get_fn = get_fn
        self.cache_map: Dict[str, Dict[str, Tuple[Any, datetime]]] = {}

    def get(self, project: str, name: str) -> Optional[Any]:
        with self.lock:
            if project in self.cache_map and name in self.cache_map[project]:
                return self.cache_map[project][name][0]
            else:
                return None

    def get_and_cache_object(
        self,
        project: str,
        name: str,
        ttl_offset: timedelta,
        not_found_exception: Optional[Callable],
    ) -> Optional[Any]:
        obj = self.get_fn(name, project)
        if obj is None:
            if not_found_exception:
                self.delete(project, name)
                raise not_found_exception(name, project)
            return None

        self.set(project, name, obj, _utc_now() + ttl_offset)
        return obj

    def set(self, project: str, name: str, obj: Any, ttl: datetime):
        with self.lock:
            if project not in self.cache_map:
                self.cache_map[project] = {}
            self.cache_map[project][name] = (obj, ttl)

    def delete(self, project: str, name: str):
        with self.lock:
            if project in self.cache_map and name in self.cache_map[project]:
                del self.cache_map[project][name]

    def expire(self, ttl_offset: timedelta):
        obj_refreshed = 0
        for project, items in self.cache_map.items():
            for name, (obj, ttl) in items.items():
                if ttl <= _utc_now():
                    try:
                        obj = self.get_fn(name, project)
                        self.set(project, name, obj, _utc_now() + ttl_offset)
                    except FeastObjectNotFoundException:
                        self.delete(project, name)
                    finally:
                        obj_refreshed += 1
        logger.info(f"Refreshed {obj_refreshed} objects in {self.name} cache")

    def clear_project(self, project: str):
        with self.lock:
            if project in self.cache_map:
                del self.cache_map[project]

    def clear(self):
        with self.lock:
            self.cache_map.clear()


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

        self.cached_projects: Dict[str, Tuple[Project, datetime]] = {}
        self.cached_project_lock = Lock()

        self.cached_data_sources = SqlFallbackCacheMap(
            data_sources.name, self._get_data_source
        )
        self.cached_entities = SqlFallbackCacheMap(entities.name, self._get_entity)
        self.cached_feature_services = SqlFallbackCacheMap(
            feature_services.name, self._get_feature_service
        )
        self.cached_feature_views = SqlFallbackCacheMap(
            feature_views.name, self._get_feature_view
        )
        self.cached_on_demand_feature_views = SqlFallbackCacheMap(
            on_demand_feature_views.name, self._get_on_demand_feature_view
        )
        self.cached_permissions = SqlFallbackCacheMap(
            permissions.name, self._get_permission
        )
        self.cached_saved_datasets = SqlFallbackCacheMap(
            saved_datasets.name, self._get_saved_dataset
        )
        self.cached_sorted_feature_views = SqlFallbackCacheMap(
            sorted_feature_views.name, self._get_sorted_feature_view
        )
        self.cached_stream_feature_views = SqlFallbackCacheMap(
            stream_feature_views.name, self._get_stream_feature_view
        )
        self.cached_validation_references = SqlFallbackCacheMap(
            validation_references.name, self._get_validation_reference
        )

        self.cache_process_list = [
            self.cached_data_sources,
            self.cached_entities,
            self.cached_feature_services,
            self.cached_feature_views,
            self.cached_on_demand_feature_views,
            self.cached_sorted_feature_views,
            self.cached_stream_feature_views,
            self.cached_saved_datasets,
            self.cached_validation_references,
            self.cached_permissions,
        ]

        super().__init__(registry_config, project, repo_path)

    def proto(self) -> RegistryProto:
        # proto() is called during the refresh cycle, this implementation only refreshes cached items
        projects_refreshed = 0
        for project_name, project_ttl in self.cached_projects.items():
            if (
                project_name in self.cached_projects
                and self.cached_projects[project_name][1] <= _utc_now()  # type: ignore
            ):
                try:
                    project_obj = self._get_project(project_name)
                    if project_obj:
                        self.cached_projects[project_name] = (
                            project_obj,
                            _utc_now() + self.cached_registry_proto_ttl,
                        )
                    else:
                        del self.cached_projects[project_name]
                except ProjectObjectNotFoundException:
                    del self.cached_projects[project_name]
                finally:
                    projects_refreshed += 1
        logger.info(f"Refreshed {projects_refreshed} projects in cache")

        if self.thread_pool_executor_worker_count == 0:
            logger.info("Starting timer for single threaded self.proto()")
            start = time.time()
            for cache_map in self.cache_process_list:
                cache_map.expire(self.cached_registry_proto_ttl)
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
                    ttl_offset = self.cached_registry_proto_ttl
                    executor.map(
                        lambda cache: cache.expire(ttl_offset), self.cache_process_list
                    )

                logger.info(
                    f"Multi threaded self.proto() took {time.time() - start} seconds to process cache expiration and refresh"
                )
            except RuntimeError as e:
                logger.error(
                    f"Resetting cache due to error during multi-threaded cache processing: {e}"
                )
                for cache_map in self.cache_process_list:
                    cache_map.clear()  # type: ignore
        return self.cached_registry_proto

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
        cache_map: Optional[SqlFallbackCacheMap] = None
        for cache in self.cache_process_list:
            if cache.name == table.name:
                cache_map = cache  # type: ignore
                break
        if cache_map is not None:
            cache_map.delete(project, name)
        return deleted_rows

    def get_project(
        self,
        name: str,
        allow_cache: bool = False,
    ) -> Project:
        if allow_cache:
            with self.cached_project_lock:
                if name in self.cached_projects:
                    return self.cached_projects[name][0]

        project = self._get_project(name)
        if project is None:
            raise ProjectObjectNotFoundException(name)

        if name in self.cache_exempt_projects:
            return project

        ttl = _utc_now() + self.cached_registry_proto_ttl

        with self.cached_project_lock:
            self.cached_projects[name] = (project, ttl)
        return project

    def delete_project(self, name: str, commit: bool = True):
        super().delete_project(name, commit)
        if commit:
            # Clear the cache for the deleted project
            with self.cached_project_lock:
                if name in self.cached_projects:
                    del self.cached_projects[name]
                for cache_map, _, _ in self.cache_process_list:
                    if name in cache_map:  # type: ignore
                        del cache_map[name]  # type: ignore

    def get_any_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> BaseFeatureView:
        if allow_cache:
            fv = self.cached_feature_views.get(name, project)
            if fv:
                return fv
            odfv = self.cached_on_demand_feature_views[project][name][0]
            if odfv:
                return odfv
            sorted_fv = self.cached_sorted_feature_views[project][name][0]
            if sorted_fv:
                return sorted_fv
            stream_fv = self.cached_stream_feature_views[project][name][0]
            if stream_fv:
                return stream_fv

        # if allow_cache=False or failed to find any feature view in the cache, fetch from the registry
        feature_view = self._get_any_feature_view(name, project)
        if feature_view is None:
            raise FeatureViewNotFoundException(
                f"Feature view {name} not found in project {project}"
            )

        if project in self.cache_exempt_projects:
            return feature_view

        if isinstance(feature_view, SortedFeatureView):
            self.cached_feature_views.set(
                project, name, feature_view, _utc_now() + self.cached_registry_proto_ttl
            )
        elif isinstance(feature_view, StreamFeatureView):
            self.cached_stream_feature_views.set(
                project, name, feature_view, _utc_now() + self.cached_registry_proto_ttl
            )
        elif isinstance(feature_view, OnDemandFeatureView):
            self.cached_on_demand_feature_views.set(
                project, name, feature_view, _utc_now() + self.cached_registry_proto_ttl
            )
        else:
            self.cached_feature_views.set(
                project, name, feature_view, _utc_now() + self.cached_registry_proto_ttl
            )
        return feature_view

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        if allow_cache:
            data_source = self.cached_data_sources.get(name, project)
            if data_source:
                return data_source

        if project in self.cache_exempt_projects:
            return self._get_data_source(name, project)

        return self.cached_data_sources.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            DataSourceObjectNotFoundException,
        )

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        if allow_cache:
            entity = self.cached_entities.get(project, name)
            if entity:
                return entity

        if project in self.cache_exempt_projects:
            return self._get_entity(name, project)

        return self.cached_entities.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            EntityNotFoundException,
        )

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        if allow_cache:
            feature_service = self.cached_feature_services.get(name, project)
            if feature_service:
                return feature_service

        if project in self.cache_exempt_projects:
            return self._get_feature_service(name, project)

        return self.cached_feature_services.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            FeatureServiceNotFoundException,
        )

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        if allow_cache:
            feature_view = self.cached_feature_views.get(name, project)
            if feature_view:
                return feature_view

        if project in self.cache_exempt_projects:
            return self._get_feature_view(name, project)

        return self.cached_feature_views.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            FeatureViewNotFoundException,
        )

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        if allow_cache:
            od_feature_view = self.cached_on_demand_feature_views.get(name, project)
            if od_feature_view:
                return od_feature_view

        if project in self.cache_exempt_projects:
            return self._get_on_demand_feature_view(name, project)

        return self.cached_on_demand_feature_views.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            OnDemandFeatureViewNotFoundException,
        )

    def get_sorted_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SortedFeatureView:
        if allow_cache:
            sorted_feature_view = self.cached_sorted_feature_views.get(name, project)
            if sorted_feature_view:
                return sorted_feature_view

        if project in self.cache_exempt_projects:
            return self._get_sorted_feature_view(name, project)

        return self.cached_sorted_feature_views.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            SortedFeatureViewNotFoundException,
        )

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
        if allow_cache:
            stream_feature_view = self.cached_stream_feature_views.get(name, project)
            if stream_feature_view:
                return stream_feature_view

        if project in self.cache_exempt_projects:
            return self._get_stream_feature_view(name, project)

        return self.cached_stream_feature_views.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            FeatureViewNotFoundException,
        )

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        if allow_cache:
            saved_dataset = self.cached_saved_datasets.get(name, project)
            if saved_dataset:
                return saved_dataset

        if project in self.cache_exempt_projects:
            return self._get_saved_dataset(name, project)

        return self.cached_saved_datasets.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            SavedDatasetNotFound,
        )

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        if allow_cache:
            validation_reference = self.cached_validation_references.get(name, project)
            if validation_reference:
                return validation_reference

        if project in self.cache_exempt_projects:
            return self._get_validation_reference(name, project)

        return self.cached_validation_references.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
            ValidationReferenceNotFound,
        )

    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        if allow_cache:
            permission = self.cached_permissions.get(name, project)
            if permission:
                return permission

        if project in self.cache_exempt_projects:
            return self._get_permission(name, project)

        return self.cached_permissions.get_and_cache_object(
            project,
            name,
            self.cached_registry_proto_ttl,
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
