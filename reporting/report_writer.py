"""
Report writer that outputs CSV, Excel, or PDF based on file extension.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List
import csv
import os


@dataclass
class ReportWriterConfig:
    path: str


class ReportWriter:
    def __init__(self, cfg: ReportWriterConfig) -> None:
        self.cfg = cfg

    def write(self, rows: Iterable[Dict[str, Any]]) -> int:
        ext = self._ext()
        if ext == ".csv":
            return self._write_csv(rows)
        if ext in {".xlsx", ".xlsm"}:
            return self._write_excel(rows)
        if ext == ".pdf":
            return self._write_pdf(rows)
        raise ValueError(f"Unsupported report extension: {ext}")

    def _ext(self) -> str:
        return os.path.splitext(self.cfg.path.lower())[1]

    def _materialize(self, rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(rows)

    def _write_csv(self, rows: Iterable[Dict[str, Any]]) -> int:
        data = self._materialize(rows)
        if not data:
            return 0
        fieldnames = list(data[0].keys())
        with open(self.cfg.path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        return len(data)

    def _write_excel(self, rows: Iterable[Dict[str, Any]]) -> int:
        try:
            from openpyxl import Workbook  # type: ignore
        except Exception as exc:
            raise RuntimeError("openpyxl is required for Excel reports") from exc

        data = self._materialize(rows)
        if not data:
            return 0
        fieldnames = list(data[0].keys())
        wb = Workbook()
        ws = wb.active
        ws.title = "Report"
        ws.append(fieldnames)
        for row in data:
            ws.append([row.get(k) for k in fieldnames])
        wb.save(self.cfg.path)
        return len(data)

    def _write_pdf(self, rows: Iterable[Dict[str, Any]]) -> int:
        try:
            from reportlab.lib.pagesizes import letter  # type: ignore
            from reportlab.lib.units import inch  # type: ignore
            from reportlab.pdfgen import canvas  # type: ignore
        except Exception as exc:
            raise RuntimeError("reportlab is required for PDF reports") from exc

        data = self._materialize(rows)
        if not data:
            return 0

        fieldnames = list(data[0].keys())
        c = canvas.Canvas(self.cfg.path, pagesize=letter)
        width, height = letter
        x = 0.75 * inch
        y = height - 0.75 * inch
        line_height = 12

        c.setFont("Helvetica-Bold", 11)
        c.drawString(x, y, "Finance Report")
        y -= line_height * 2

        c.setFont("Helvetica", 9)
        header = " | ".join(fieldnames)
        c.drawString(x, y, header[:120])
        y -= line_height

        for row in data:
            line = " | ".join(str(row.get(k, "")) for k in fieldnames)
            if y < 0.75 * inch:
                c.showPage()
                y = height - 0.75 * inch
                c.setFont("Helvetica", 9)
            c.drawString(x, y, line[:120])
            y -= line_height

        c.save()
        return len(data)
