# --------------------------------------------------------------------
# Extends OIDC client auth model with an optional `token` field.
# Works on Pydantic v2-only.
#
# Accepted credential sets (exactly **one** of):
#   1 pre-issued `token`
#   2 `client_secret`            (client-credentials flow)
#   3 `username` + `password` + `client_secret`  (ROPG)
# --------------------------------------------------------------------
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import ConfigDict, field_validator, model_validator

from feast.repo_config import FeastConfigBaseModel


class AuthConfig(FeastConfigBaseModel):
    type: Literal["oidc", "kubernetes", "no_auth"] = "no_auth"
    # Opt-in write-path gRPC auth. Default false so existing no_auth / stock OIDC
    # servers are unchanged. extra=forbid requires this field to be declared.
    write_auth_only: bool = False
    # Auth for the "report write" category (ReportFeatureServer, see
    # REPORT_RPC_METHODS in permissions/server/grpc.py). Reports are project
    # metadata, not feature definitions, so they have their own setting:
    #   off      - no token parsed; reports are stored unverified.
    #   optional - a valid token marks the report verified; a missing or
    #              invalid token is logged and the report stays unverified.
    #   required - a valid token is required (UNAUTHENTICATED otherwise).
    # Applies with write_auth_only; refused at registry startup with no_auth.
    feature_server_report_auth: Literal["off", "optional", "required"] = "off"

    @field_validator("feature_server_report_auth", mode="before")
    @classmethod
    def _yaml_bool_report_auth(cls, value: Any) -> Any:
        # feature_store.yaml is parsed as YAML 1.1, where an unquoted `off`
        # (also `no`, `false`) loads as False and `on`/`yes` as True.
        if value is False:
            return "off"
        if value is True:
            raise ValueError(
                "feature_server_report_auth must be 'off', 'optional' or "
                "'required'; got a boolean true (an unquoted on/yes/true in "
                "YAML), which is ambiguous. Quote the value, e.g. "
                'feature_server_report_auth: "optional".'
            )
        return value


class OidcAuthConfig(AuthConfig):
    auth_discovery_url: str
    client_id: str
    issuer: Optional[str] = None
    audience: Optional[str] = None


class OidcClientAuthConfig(OidcAuthConfig):
    # any **one** of the four fields below is sufficient
    username: Optional[str] = None
    password: Optional[str] = None
    client_secret: Optional[str] = None
    token: Optional[str] = None  # pre-issued `token`

    @model_validator(mode="after")
    def _validate_credentials(cls, values):
        """Enforce exactly one valid credential set."""
        d = values.__dict__ if hasattr(values, "__dict__") else values

        has_user_pass = bool(d.get("username")) and bool(d.get("password"))
        has_secret = bool(d.get("client_secret"))
        has_token = bool(d.get("token"))

        # 1 static token
        if has_token and not (has_user_pass or has_secret):
            return values

        # 2 client_credentials
        if has_secret and not has_user_pass and not has_token:
            return values

        # 3 ROPG
        if has_user_pass and has_secret and not has_token:
            return values

        raise ValueError(
            "Invalid OIDC client auth combination: "
            "provide either\n"
            "  • token\n"
            "  • client_secret (without username/password)\n"
            "  • username + password + client_secret"
        )


class NoAuthConfig(AuthConfig):
    pass


class KubernetesAuthConfig(AuthConfig):
    # Optional user token for users (not service accounts)
    user_token: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
