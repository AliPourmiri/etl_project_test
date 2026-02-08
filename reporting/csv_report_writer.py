"""
CSV report writer.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List
import csv


class CsvReportWriter:
    def write(self, path: str, rows: Iterable[Dict[str, Any]]) -> int:
        data = list(rows)
        if not data:
            return 0
        fieldnames = self._columns(data)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow({k: row.get(k) for k in fieldnames})
        return len(data)

    def _columns(self, data: List[Dict[str, Any]]) -> List[str]:
        cols: List[str] = []
        seen = set()
        for row in data:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    cols.append(key)
        return cols
