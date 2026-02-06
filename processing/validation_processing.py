"""
Validation processing stage using BaseStage.
Validates records from any source and yields only valid rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Type

from base import BaseConfig, BaseStage


@dataclass
class ValidationProcessingConfig(BaseConfig):
    required_fields: List[str] = field(default_factory=list)
    type_map: Mapping[str, Type[Any]] = field(default_factory=dict)
    allow_extra_fields: bool = True
    max_errors: Optional[int] = None


class ValidationProcessing(BaseStage):
    def __init__(self, cfg: ValidationProcessingConfig) -> None:
        # Initialize the base stage and store validation config.
        super().__init__(cfg)
        self.cfg = cfg

    def validate(self, records: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        # Validate records and yield only those that pass.
        self.logger.info("validation_start ts=%s", self.now_utc())
        error_count = 0
        valid_count = 0
        try:
            for record in records:
                ok, reason = self._is_valid(record)
                if ok:
                    valid_count += 1
                    yield record
                else:
                    error_count += 1
                    self.logger.error("validation_error reason=%s record=%s", reason, record)
                    if self.cfg.max_errors is not None and error_count >= self.cfg.max_errors:
                        raise ValueError("Max validation errors reached")
        finally:
            self.logger.info(
                "validation_end valid=%s errors=%s ts=%s",
                valid_count,
                error_count,
                self.now_utc(),
            )

    def _is_valid(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        # Apply required field and type checks to a single record.
        for field in self.cfg.required_fields:
            if field not in record:
                return False, f"missing_field:{field}"
        for field, typ in self.cfg.type_map.items():
            if field in record and record[field] is not None and not isinstance(record[field], typ):
                return False, f"type_mismatch:{field}"
        if not self.cfg.allow_extra_fields:
            for key in record.keys():
                if key not in self.cfg.required_fields and key not in self.cfg.type_map:
                    return False, f"extra_field:{key}"
        return True, "ok"
