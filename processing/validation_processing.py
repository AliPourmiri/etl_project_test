"""
Simple validation stage using ETlBase.
Includes schema validation, data quality checks, and basic transformations.
"""
from typing import Any, Dict, Tuple, Type
import json

from base import ETLBase

class ValidationProcessing:
    # Fixed schema and types for simplicity.
    REQUIRED_FIELDS = ["id", "amount", "currency"]
    TYPE_MAP: Dict[str, Type[Any]] = {"id": int, "amount": float, "currency": str}
    MAX_ERRORS = 5
    # Simple referential set for demonstration.
    VALID_CURRENCIES = {"USD", "EUR", "GBP"}

    def __init__(log, cxn):      # this allows to have logging and database connection for referential check
        self.log = log
        self.dbCxn = cxn

    def validate(self, records: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        # Validate a list of records, apply transformations, and return valid rows.
        self.log.info("validation_start ts=%s", self.now_utc())
        error_count = 0
        valid_rows: list[Dict[str, Any]] = []
        seen_ids: set[Any] = set()
        seen_records: set[str] = set()
        try:
            for record in records:
                ok, reason = self._is_valid(record, seen_ids, seen_records)
                if ok:
                    transformed = self._transform(record)
                    valid_rows.append(transformed)
                else:
                    error_count += 1
                    self.log.error("validation_error reason=%s record=%s", reason, record)
                    if error_count >= self.MAX_ERRORS:
                        raise ValueError("Max validation errors reached")
        finally:
            self.log.info(
                "validation_end valid=%s errors=%s ts=%s",
                len(valid_rows),
                error_count,
                self.now_utc(),
            )
        return valid_rows

    def _is_valid(
        self,
        record: Dict[str, Any],
        seen_ids: set[Any],
        seen_records: set[str],
    ) -> Tuple[bool, str]:
        # Apply required field, type checks, duplicates, missing values, and referential checks.
        for field in self.REQUIRED_FIELDS:
            if field not in record:
                return False, f"missing_field:{field}"
            if record[field] in (None, "", []):
                return False, f"missing_value:{field}"
        for field, typ in self.TYPE_MAP.items():
            if field in record and record[field] is not None and not isinstance(record[field], typ):
                return False, f"type_mismatch:{field}"
        rec_fingerprint = json.dumps(record, sort_keys=True, default=str)
        if rec_fingerprint in seen_records:
            return False, "duplicate:record"
        seen_records.add(rec_fingerprint)
        if record.get("id") in seen_ids:
            return False, "duplicate:id"
        seen_ids.add(record.get("id"))
        if record.get("currency") not in self.VALID_CURRENCIES:
            return False, "referential:currency"
        return True, "ok"

    def _transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # Basic normalization and enrichment.
        out = dict(record)
        out["currency"] = str(out.get("currency", "")).upper()
        out["amount"] = float(out.get("amount", 0.0))
        out["amount_usd"] = out["amount"]  # placeholder for FX conversion
        return out
