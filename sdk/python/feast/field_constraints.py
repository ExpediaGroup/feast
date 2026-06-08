"""
Declarative data quality constraints for a SparkSource's columns.

Customers attach a `Dict[str, FieldConstraints]` to a `SparkSource` keyed by
column name. The constraints are stored on the source's proto, round-tripped
through the registry, and consumed at write time.

Only fields the customer sets are enforced. Unset fields are not validated.
"""

import re
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto

# Both messages are nested under the `DataSource` proto message in
# DataSource.proto (alongside SparkOptions, KafkaOptions, etc.). Alias them
# at module load time so the rest of this file reads cleanly.
FieldConstraintsProto = DataSourceProto.FieldConstraints
ImputationProto = DataSourceProto.Imputation


_IMPUTATION_STRATEGIES = ("default", "mean", "median")


class Imputation(BaseModel):
    """How to fill null values in a column before validation runs."""

    model_config = ConfigDict(extra="forbid")
    strategy: str
    # Required when strategy == "default"; ignored otherwise.
    default_value: Optional[Union[float, int, str, bool]] = None

    @field_validator("strategy")
    @classmethod
    def _strategy_known(cls, v: str) -> str:
        if v not in _IMPUTATION_STRATEGIES:
            raise ValueError(
                f"imputation.strategy must be one of {list(_IMPUTATION_STRATEGIES)}, "
                f"got {v!r}"
            )
        return v

    @model_validator(mode="after")
    def _check_default_value(self) -> "Imputation":
        if self.strategy == "default" and self.default_value is None:
            raise ValueError(
                "imputation.default_value is required when strategy='default'"
            )
        if self.strategy in ("mean", "median") and self.default_value is not None:
            raise ValueError(
                "imputation.default_value is only valid with strategy='default'; "
                f"got strategy='{self.strategy}'"
            )
        return self

    def to_proto(self) -> ImputationProto:
        strategy_enum = {
            "default": ImputationProto.DEFAULT,
            "mean": ImputationProto.MEAN,
            "median": ImputationProto.MEDIAN,
        }[self.strategy]

        kwargs = {"strategy": strategy_enum}
        if self.strategy == "default":
            # bool must be checked before int since `isinstance(True, int)` is True.
            if isinstance(self.default_value, bool):
                kwargs["default_bool"] = self.default_value
            elif isinstance(self.default_value, int):
                kwargs["default_long"] = self.default_value
            elif isinstance(self.default_value, float):
                kwargs["default_double"] = self.default_value
            elif isinstance(self.default_value, str):
                kwargs["default_string"] = self.default_value
            else:
                raise ValueError(
                    f"unsupported default_value type: {type(self.default_value).__name__}"
                )
        return ImputationProto(**kwargs)

    @classmethod
    def from_proto(cls, proto: ImputationProto) -> "Imputation":
        strategy_name = {
            ImputationProto.DEFAULT: "default",
            ImputationProto.MEAN: "mean",
            ImputationProto.MEDIAN: "median",
        }.get(proto.strategy)
        if strategy_name is None:
            raise ValueError(f"unknown imputation strategy proto value: {proto.strategy}")

        default_value: Optional[Union[float, int, str, bool]] = None
        if strategy_name == "default":
            which = proto.WhichOneof("default_value")
            if which == "default_bool":
                default_value = proto.default_bool
            elif which == "default_long":
                default_value = proto.default_long
            elif which == "default_double":
                default_value = proto.default_double
            elif which == "default_string":
                default_value = proto.default_string

        return cls(strategy=strategy_name, default_value=default_value)


class FieldConstraints(BaseModel):
    """
    Per-column data quality rules. Attached to a `SparkSource` as
    `field_constraints: Dict[str, FieldConstraints]`, keyed by column name.

    Only the fields the customer sets are enforced; unset fields are not
    validated.
    """

    model_config = ConfigDict(extra="forbid")

    nullable: Optional[bool] = None
    max_null_pct: Optional[float] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    # Minimum row-level compliance for value/range/regex/allowed-values
    # checks. Default 1.0 (strict) — every non-null row must satisfy the
    # predicate. Set below 1.0 when the underlying data is known-noisy
    # (e.g. floating-point ratios that drift past `[0, 1]` by ULP-scale
    # rounding error).
    min_compliance: Optional[float] = None
    allowed_values: Optional[List[str]] = None
    regex: Optional[str] = None
    unique: Optional[bool] = None
    custom: Optional[Dict[str, str]] = None
    imputation: Optional[Imputation] = None

    @field_validator("max_null_pct")
    @classmethod
    def _max_null_pct_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not 0.0 <= v <= 1.0:
            raise ValueError(f"max_null_pct must be in [0, 1], got {v}")
        return v

    @field_validator("min_compliance")
    @classmethod
    def _min_compliance_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not 0.0 <= v <= 1.0:
            raise ValueError(f"min_compliance must be in [0, 1], got {v}")
        return v

    @field_validator("regex")
    @classmethod
    def _regex_compiles(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                re.compile(v)
            except re.error as e:
                raise ValueError(f"regex does not compile: {e}") from e
        return v

    @field_validator("allowed_values")
    @classmethod
    def _allowed_values_nonempty(
        cls, v: Optional[List[str]]
    ) -> Optional[List[str]]:
        if v is not None and len(v) == 0:
            raise ValueError("allowed_values must not be empty if set")
        return v

    @model_validator(mode="after")
    def _cross_field(self) -> "FieldConstraints":
        if self.nullable is False and (self.max_null_pct or 0) > 0:
            raise ValueError("nullable=False contradicts max_null_pct > 0")
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ValueError(
                f"min_value ({self.min_value}) > max_value ({self.max_value})"
            )
        return self

    def to_proto(self) -> FieldConstraintsProto:
        kwargs = {}
        if self.nullable is not None:
            kwargs["nullable"] = self.nullable
        if self.max_null_pct is not None:
            kwargs["max_null_pct"] = self.max_null_pct
        if self.min_value is not None:
            kwargs["min_value"] = self.min_value
        if self.max_value is not None:
            kwargs["max_value"] = self.max_value
        if self.min_compliance is not None:
            kwargs["min_compliance"] = self.min_compliance
        if self.allowed_values is not None:
            kwargs["allowed_values"] = list(self.allowed_values)
        if self.regex is not None:
            kwargs["regex"] = self.regex
        if self.unique is not None:
            kwargs["unique"] = self.unique
        if self.custom is not None:
            kwargs["custom"] = dict(self.custom)
        if self.imputation is not None:
            kwargs["imputation"] = self.imputation.to_proto()
        return FieldConstraintsProto(**kwargs)

    @classmethod
    def from_proto(cls, proto: FieldConstraintsProto) -> "FieldConstraints":
        kwargs = {}
        if proto.HasField("nullable"):
            kwargs["nullable"] = proto.nullable
        if proto.HasField("max_null_pct"):
            kwargs["max_null_pct"] = proto.max_null_pct
        if proto.HasField("min_value"):
            kwargs["min_value"] = proto.min_value
        if proto.HasField("max_value"):
            kwargs["max_value"] = proto.max_value
        if proto.HasField("min_compliance"):
            kwargs["min_compliance"] = proto.min_compliance
        if len(proto.allowed_values) > 0:
            kwargs["allowed_values"] = list(proto.allowed_values)
        if proto.HasField("regex"):
            kwargs["regex"] = proto.regex
        if proto.HasField("unique"):
            kwargs["unique"] = proto.unique
        if len(proto.custom) > 0:
            kwargs["custom"] = dict(proto.custom)
        if proto.HasField("imputation"):
            kwargs["imputation"] = Imputation.from_proto(proto.imputation)
        return cls(**kwargs)
