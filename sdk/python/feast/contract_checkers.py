import logging
from typing import Generic, List, Tuple, Type, TypeVar

from feast import FeatureView, SortedFeatureView

logger = logging.getLogger(__name__)

# Generic type variable for contract checker
T = TypeVar("T")


class ContractChecker(Generic[T]):
    @classmethod
    def check(
        cls: Type["ContractChecker[T]"],
        updated: T,
        current: T,
    ) -> Tuple[bool, List[str]]:
        """
        Run all checks and return (ok, reasons).
        """
        checker = cls(updated, current)
        ok = checker._run_all_checks()
        return ok, checker.reasons

    def __init__(self, updated: T, current: T) -> None:
        self.updated = updated
        self.current = current
        self.reasons: List[str] = []

    def _run_all_checks(self) -> bool:
        """
        Must be implemented by subclasses.
        """
        raise NotImplementedError("_run_all_checks must be implemented by subclasses")


class FeatureViewContractChecker(ContractChecker[FeatureView]):
    """
    Guard-rail for updates to an existing FeatureView.
    """

    def _run_all_checks(self) -> bool:
        self.reasons.clear()
        self._check_entities()
        self._check_fields()
        self._check_ttl()
        return not self.reasons

    def _check_entities(self) -> None:
        if set(self.updated.entities) != set(self.current.entities):
            self.reasons.append("entity definitions cannot change")

    def _check_fields(self) -> None:
        old_fields = {f.name: f.dtype for f in self.current.schema}
        new_fields = {f.name: f.dtype for f in self.updated.schema}

        removed = old_fields.keys() - new_fields.keys()
        for fname in sorted(removed):
            if fname in set(self.current.entities):
                self.reasons.append(
                    f"feature '{fname}' removed from FeatureView '{self.updated.name}' is an entity key and cannot be removed"
                )
            else:
                logger.warning(
                    "Feature '%s' removed from FeatureView '%s'.",
                    fname,
                    self.updated.name,
                )

        for fname, old_dtype in old_fields.items():
            if fname in new_fields and new_fields[fname] != old_dtype:
                self.reasons.append(
                    f"feature '{fname}' type changed ({old_dtype} to {new_fields[fname]}) not allowed"
                )

    def _check_ttl(self) -> None:
        cur_ttl, upd_ttl = self.current.ttl, self.updated.ttl
        if cur_ttl and upd_ttl:
            if upd_ttl < cur_ttl:
                self.reasons.append(
                    "ttl may stay the same or increase, but not decrease"
                )
        elif cur_ttl and not upd_ttl:
            self.reasons.append("ttl cannot be removed once defined")


class SortedFeatureViewContractChecker(
    FeatureViewContractChecker, ContractChecker[SortedFeatureView]
):
    """
    Extends FeatureViewContractChecker with sorted-specific checks
    """

    def _run_all_checks(self) -> bool:
        self.reasons.clear()
        fv_checker = FeatureViewContractChecker(self.updated, self.current)
        fv_checker._check_entities()
        fv_checker._check_ttl()
        self.reasons.extend(fv_checker.reasons)
        self._check_fields()
        self._check_sort_keys()
        return not self.reasons

    def _check_fields(self) -> None:
        old_fields = {f.name: f.dtype for f in self.current.schema}
        new_fields = {f.name: f.dtype for f in self.updated.schema}

        removed = old_fields.keys() - new_fields.keys()
        entity_keys = set(self.current.entities)
        sort_key_names = {key.name for key in self.current.sort_keys}  # type: ignore[attr-defined]  # type: ignore[attr-defined]

        for fname in sorted(removed):
            if fname in entity_keys:
                self.reasons.append(
                    f"feature '{fname}' removed from SortedFeatureView '{self.updated.name}' "
                    "is an entity key and cannot be removed"
                )
            elif fname in sort_key_names:
                self.reasons.append(
                    f"feature '{fname}' removed from SortedFeatureView '{self.updated.name}' "
                    "is a sort key and cannot be removed"
                )
            else:
                logger.warning(
                    "Feature '%s' removed from SortedFeatureView '%s'.",
                    fname,
                    self.updated.name,
                )

        for fname, old_dtype in old_fields.items():
            if fname in new_fields and new_fields[fname] != old_dtype:
                self.reasons.append(
                    f"feature '{fname}' type changed ({old_dtype} → {new_fields[fname]}) not allowed"
                )

    def _check_sort_keys(self) -> None:
        updated_keys = getattr(self.updated, "sort_keys", [])  # type: ignore[attr-defined]
        current_keys = getattr(self.current, "sort_keys", [])  # type: ignore[attr-defined]
        if len(updated_keys) != len(current_keys):
            self.reasons.append("number of sort keys cannot change")
            return
        for uk, ck in zip(updated_keys, current_keys):
            if uk != ck:
                self.reasons.append(
                    f"sort key change detected (old: {ck.name}, new: {uk.name})"
                )
