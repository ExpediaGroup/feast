import asyncio
import logging
import uuid
from contextvars import Token
from typing import Any, Callable, Optional, Tuple

import grpc
from starlette.authentication import AuthenticationError

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager

logger = logging.getLogger(__name__)

_REGISTRY_SERVICE = "/feast.registry.RegistryServer"

# Write RPCs that require a Bearer JWT when write_auth_only is enabled.
# Keep this as one allowlist so later tickets only shrink the unauthenticated set.
WRITE_RPC_METHODS = frozenset(
    f"{_REGISTRY_SERVICE}/{name}"
    for name in (
        "ApplyEntity",
        "DeleteEntity",
        "ApplyDataSource",
        "DeleteDataSource",
        "ApplyFeatureView",
        "DeleteFeatureView",
        "ApplyFeatureService",
        "DeleteFeatureService",
        "ApplySavedDataset",
        "DeleteSavedDataset",
        "ApplyValidationReference",
        "DeleteValidationReference",
        "ApplyPermission",
        "DeletePermission",
        "ApplyProject",
        "DeleteProject",
        "ApplyMaterialization",
        "UpdateInfra",
        "Commit",
    )
)


# "Report write" RPCs: a running feature server reports its own URLs. These
# write project metadata, never feature definitions, so they are deliberately
# NOT in WRITE_RPC_METHODS: auth_config.feature_server_report_auth governs
# them (off -> optional -> required) under WritePathAuthInterceptor. Moving a
# method between the two sets must be a deliberate change (a test pins it).
REPORT_RPC_METHODS = frozenset({f"{_REGISTRY_SERVICE}/ReportFeatureServer"})

FEATURE_SERVER_REPORT_AUTH_SETTINGS = ("off", "optional", "required")


def is_write_rpc(method: Optional[str]) -> bool:
    return bool(method) and method in WRITE_RPC_METHODS


def is_report_rpc(method: Optional[str]) -> bool:
    return bool(method) and method in REPORT_RPC_METHODS


class AuthInterceptor(grpc.ServerInterceptor):
    """
    Authenticate every RPC. JWT parse, set_current_user, and request_id
    minting run on the ThreadPoolExecutor worker via handler wrap, not in
    intercept_service.
    """

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        if get_security_manager() is None:
            return handler
        metadata = dict(handler_call_details.invocation_metadata or ())
        return _wrap_authenticated_handler(handler, metadata)


class WritePathAuthInterceptor(grpc.ServerInterceptor):
    """
    Authenticate write RPCs only. JWT parse, set_current_user, and request_id
    minting run on the ThreadPoolExecutor worker via handler wrap, not in
    intercept_service.

    Non-write methods (Get/List/Health/reflection/Refresh/Expedia search) pass
    through with no token and no RBAC.

    REPORT_RPC_METHODS follow feature_server_report_auth: "off" passes through
    with no token parsed; "optional" authenticates when an authorization
    header is present and continues unverified (with a warning) when it is
    invalid; "required" authenticates like a write.
    """

    def __init__(self, feature_server_report_auth: str = "off"):
        if feature_server_report_auth not in FEATURE_SERVER_REPORT_AUTH_SETTINGS:
            raise ValueError(
                "feature_server_report_auth must be one of "
                f"{FEATURE_SERVER_REPORT_AUTH_SETTINGS}, got "
                f"{feature_server_report_auth!r}"
            )
        self.feature_server_report_auth = feature_server_report_auth

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        method = handler_call_details.method
        if is_report_rpc(method):
            if self.feature_server_report_auth == "off":
                return handler
            metadata = dict(handler_call_details.invocation_metadata or ())
            if self.feature_server_report_auth == "optional":
                return _wrap_optionally_authenticated_handler(handler, metadata)
            return _wrap_authenticated_handler(handler, metadata)

        if not is_write_rpc(method):
            return handler

        metadata = dict(handler_call_details.invocation_metadata or ())
        return _wrap_authenticated_handler(handler, metadata)


def _has_authorization_header(metadata: dict) -> bool:
    return any(str(header).lower() == "authorization" for header in metadata)


def _wrap_optionally_authenticated_handler(handler, metadata: dict):
    if not handler.unary_unary:
        # Report RPCs are unary; anything else gets the strict wrapper.
        return _wrap_authenticated_handler(handler, metadata)
    return grpc.unary_unary_rpc_method_handler(
        lambda req, ctx: _optionally_authenticate_and_call(
            handler.unary_unary, req, ctx, metadata
        ),
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


def _optionally_authenticate_and_call(
    behavior: Callable[..., Any], request, context, metadata: dict
):
    """Set the current user when a valid token comes with the call; otherwise
    run the handler with no user (an unverified report)."""
    sm = get_security_manager()
    user_token: Optional[Token] = None
    request_id_token: Optional[Token] = None
    try:
        if _has_authorization_header(metadata):
            try:
                user_token, request_id_token = _set_user_from_metadata(metadata)
            except Exception as e:  # noqa: BLE001 - optional mode never aborts
                # Any failure (bad scheme, malformed header, undecodable token)
                # leaves the report unverified. Log only the exception type:
                # its message can contain the raw Authorization header.
                logger.warning(
                    "Ignoring an unusable token on a feature server report; "
                    "continuing unverified (%s)",
                    type(e).__name__,
                )
        return behavior(request, context)
    finally:
        if sm is not None:
            sm.reset_request_context(user_token, request_id_token)


def _wrap_authenticated_handler(handler, metadata: dict):
    if handler.unary_unary:
        return grpc.unary_unary_rpc_method_handler(
            lambda req, ctx: _authenticate_and_call(
                handler.unary_unary, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    if handler.unary_stream:
        return grpc.unary_stream_rpc_method_handler(
            lambda req, ctx: _authenticate_and_stream(
                handler.unary_stream, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    if handler.stream_unary:
        return grpc.stream_unary_rpc_method_handler(
            lambda req, ctx: _authenticate_and_call(
                handler.stream_unary, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    if handler.stream_stream:
        return grpc.stream_stream_rpc_method_handler(
            lambda req, ctx: _authenticate_and_stream(
                handler.stream_stream, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    return handler


def _authenticate_and_call(
    behavior: Callable[..., Any], request, context, metadata: dict
):
    sm = get_security_manager()
    user_token: Optional[Token] = None
    request_id_token: Optional[Token] = None
    try:
        try:
            user_token, request_id_token = _set_user_from_metadata(metadata)
        except AuthenticationError as e:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, str(e))
        return behavior(request, context)
    finally:
        if sm is not None:
            sm.reset_request_context(user_token, request_id_token)


def _authenticate_and_stream(
    behavior: Callable[..., Any], request, context, metadata: dict
):
    sm = get_security_manager()
    user_token: Optional[Token] = None
    request_id_token: Optional[Token] = None
    try:
        try:
            user_token, request_id_token = _set_user_from_metadata(metadata)
        except AuthenticationError as e:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, str(e))
        yield from behavior(request, context)
    finally:
        if sm is not None:
            sm.reset_request_context(user_token, request_id_token)


def _set_user_from_metadata(
    metadata: dict,
) -> Tuple[Optional[Token], Optional[Token]]:
    auth_manager = get_auth_manager()
    access_token = auth_manager.token_extractor.extract_access_token(metadata=metadata)
    logger.debug(f"Fetching user details for token of length: {len(access_token)}")

    parser = auth_manager.token_parser
    sync_parse = getattr(parser, "user_details_from_access_token_sync", None)
    if callable(sync_parse):
        current_user = sync_parse(access_token)
    else:
        current_user = asyncio.run(parser.user_details_from_access_token(access_token))

    logger.debug(f"User is: {current_user}")
    sm = get_security_manager()
    if sm is None:
        return None, None
    user_token = sm.set_current_user(current_user)
    request_id_token = sm.set_request_id(str(uuid.uuid4()))
    return user_token, request_id_token
