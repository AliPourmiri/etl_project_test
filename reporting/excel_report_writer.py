"""
Excel report writer.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


class ExcelReportWriter:
    def write(self, path: str, rows: Iterable[Dict[str, Any]]) -> int:
        try:
            from openpyxl import Workbook  # type: ignore
            from openpyxl.styles import Font  # type: ignore
        except Exception as exc:
            raise RuntimeError("openpyxl is required for Excel reports") from exc

        data = list(rows)
        if not data:
            return 0
        fieldnames = self._columns(data)
        wb = Workbook()
        ws = wb.active
        ws.title = "Report"
        ws.append(fieldnames)
        for row in data:
            ws.append([row.get(k) for k in fieldnames])
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for i, col in enumerate(fieldnames, start=1):
            max_len = len(str(col))
            for row in data:
                val = row.get(col, "")
                max_len = max(max_len, len(str(val)))
            ws.column_dimensions[chr(64 + i)].width = min(max_len + 2, 40)
        wb.save(path)
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
