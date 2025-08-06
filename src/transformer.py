import json
import logging
import re
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import xlsxwriter

from src.fetcher import LabFolderFetcher
from src.importer import Importer


# — set up a logs directory alongside your project root —
ROOT_DIR = Path(__file__).resolve().parent.parent
LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# — configure a file handler for transformer logs —
TRANS_LOG_FILE = LOG_DIR / "transformer.log"
file_handler = logging.FileHandler(str(TRANS_LOG_FILE), mode="a")
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
))

# — get the Transformer logger and attach the handler —
transformer_logger = logging.getLogger("Transformer")
transformer_logger.setLevel(logging.INFO)
transformer_logger.addHandler(file_handler)

# (Optionally also echo to console:)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
transformer_logger.addHandler(console_handler)


class Transformer:
    def __init__ (self, entries: List[Dict[str, Any]],
                  fetcher: LabFolderFetcher, importer: Importer,
                  logger: logging.Logger = None, ) -> None:
        self._entries = pd.DataFrame(entries)
        self._fetcher = fetcher
        self._importer = importer
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def transform_experiment_data (self) -> Dict[Any, List[Dict[str, Any]]]:
        experiment_data: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for _, row in self._entries.iterrows():
            record = {
                "name"                 : f"{row['author'].get('first_name')} {row['author'].get('last_name')}",
                "entry_creation_date"  : row["creation_date"],
                "elements"             : row["elements"],
                "entry_number"         : row["entry_number"],
                "entry_id"             : row["id"],
                "last_editor_name"     : f"{row['last_editor'].get('first_name')} {row['last_editor'].get('last_name')}",
                "tags"                 : row["tags"],
                "entry_title"          : row["title"],
                "last_edited"          : row["version_date"],
                "project_creation_date": row["project"].get("creation_date"),
                "labfolder_project_id" : row["project"].get("id"),
                "number_of_entries"    : row["project"].get(
                    "number_of_entries"),
                "project_title"        : row["project"].get("title"),
                "project_owner"        : f"{row['author'].get('first_name')} {row['author'].get('last_name')}",
                "Labfolder_ID"         : row["project"].get("id"),
                }
            experiment_data[row["project_id"]].append(record)
        return experiment_data

    def transform_projects_content (self, project: List[Dict[str, Any]],
                                    max_entries: int = None,
                                    category: int = 38) -> List[str]:
        title, tags = self.collect_title_and_tags(project)
        exp_id = self._importer.create_experiment(title, tags)
        entry_htmls: List[str] = []

        for idx, entry in enumerate(project, start=1):
            if max_entries and idx > max_entries:
                break
            entry_htmls.append(self.build_entry_html(entry, exp_id))

        entry_htmls.append(self.build_footer_html(project[0]))
        full_body = "".join(entry_htmls)
        extra_fields = self.build_extra_fields(project[0])
        self._importer.patch_experiment(exp_id, full_body, category, uid=1130,
                                        extra_fields=extra_fields)
        return entry_htmls

    def collect_title_and_tags (self, project: List[Dict[str, Any]]) -> Tuple[
        str, List[str]]:
        title = project[0].get("project_title", "")
        tags: List[str] = []
        for entry in project:
            tags.extend(entry.get("tags", np.array([])))
        return title, tags

    def build_entry_html (self, entry: Dict[str, Any], exp_id: str) -> str:
        entry_tags = entry.get("tags", [])
        formatted_tags = " ". join(f"§{tag}" for tag in entry_tags)
        header = (
            f"\n----Entry {entry['entry_number']} of {entry['number_of_entries']}----<br>"
            f"<strong>Entry: {entry['entry_title']} (labfolder id: {entry['entry_id']})</strong><br>"
            f"<strong>Tags:</strong> {formatted_tags}</strong><br>"
            )
        blocks: List[str] = []

        for element in entry.get("elements", []):
            if not element:
                continue
            typ = element.get("type")

            if typ == "TABLE":
                metadata = self._fetcher.fetch_table(element)
                if metadata:
                    try:
                        excel_files = self._export_table_to_excel(metadata)
                        for sheet_name, xlsx_path in excel_files:
                            self._importer.upload_file(exp_id, xlsx_path)
                            blocks.append(
                                f"<p>[Attached TABLE sheet '{sheet_name}': {xlsx_path.name}]</p>")
                    except Exception as e:
                        self.logger.error(
                            "TABLE Excel conversion/upload failed for %s: %s",
                            metadata.get("id"), e, )
                        blocks.append(
                            f"<p>[Failed to convert/upload TABLE to Excel: {metadata.get('id')}]</p>")
                else:
                    blocks.append("<p>[Empty or invalid TABLE]</p>")

            elif typ == "WELL_PLATE":
                metadata = self._fetcher.fetch_well_plate(element)
                if metadata:
                    try:
                        excel_files = self._export_well_plate_to_excel(
                            metadata)
                        if excel_files:
                            for sheet_name, xlsx_path in excel_files:
                                self._importer.upload_file(exp_id, xlsx_path)
                                blocks.append(
                                    f"<p>[Attached WELL_PLATE sheet '{sheet_name}': {xlsx_path.name}]</p>")
                        else:
                            blocks.append(
                                "<p>[No data to convert for WELL_PLATE]</p>")
                    except Exception as e:
                        self.logger.error(
                            "WELL_PLATE Excel conversion/upload failed for %s: %s",
                            metadata.get("id"), e, )
                        blocks.append(
                            f"<p>[Failed to convert/upload WELL_PLATE to Excel: {metadata.get('id')}]</p>")
                else:
                    blocks.append("<p>[Empty or invalid WELL_PLATE]</p>")

            elif typ == "TEXT":
                try:
                    text = self._fetcher.fetch_text(element)
                    blocks.append(f"<pre>{text}</pre>")
                except Exception as e:
                    self.logger.error("TEXT fetch failed for %s: %s",
                                      element.get("id"), e)
                    blocks.append(
                        f"<p>[Failed to fetch TEXT: {element.get('id')}]</p>")

            elif typ == "FILE":
                path = self._fetcher.fetch_file(element)
                if path:
                    try:
                        self._importer.upload_file(exp_id, path)
                        blocks.append(f"<p>[Attached FILE: {path.name}]</p>")
                    except Exception as e:
                        self.logger.error("FILE upload failed for %s: %s",
                                          path.name, e)
                        blocks.append(
                            f"<p>[Failed to attach FILE: {element.get('id')}]</p>")

            elif typ == "IMAGE":
                path = self._fetcher.fetch_image(element)
                if path:
                    try:
                        self._importer.upload_file(exp_id, path)
                        blocks.append(f"<p>[Attached IMAGE: {path.name}]</p>")
                    except Exception as e:
                        self.logger.error("IMAGE upload failed for %s: %s",
                                          path.name, e)
                        blocks.append(
                            f"<p>[Failed to attach IMAGE: {element.get('id')}]</p>")

            elif typ == "DATA":
                try:
                    data = self._fetcher.fetch_data(element)
                    rows = [(f"<tr><td>{d.get('title')}</td>"
                             f"<td>{d.get('value')}</td>"
                             f"<td>{d.get('unit')}</td></tr>") for d in
                            data.get("data_elements", [])]
                    table_html = (
                            "<table><tr><th>Title</th><th>Value</th><th>Unit</th></tr>" + "".join(
                        rows) + "</table>")
                    blocks.append(table_html)
                except Exception as e:
                    self.logger.error("DATA fetch failed for %s: %s",
                                      element.get("id"), e)
                    blocks.append(
                        f"<p>[Failed to fetch DATA: {element.get('id')}]</p>")

            else:
                self.logger.warning("Skipping element %s of type %s",
                                    element.get("id"), typ)
                blocks.append(f"<p>[Skipped element: {element.get('id')}]</p>")

        dt = datetime.strptime(entry["entry_creation_date"],
                               "%Y-%m-%dT%H:%M:%S.%f%z")
        created = f"Created: {dt.date().isoformat()}<br>"
        body_html = ("\n".join(blocks) + "<br>") if blocks else ""
        return header + body_html + created + "<hr><hr>"

    def _export_table_to_excel (self, metadata: Dict[str, Any]) -> List[
        Tuple[str, Path]]:
        excel_files: List[Tuple[str, Path]] = []
        content = metadata.get("content")
        if not content or not isinstance(content, dict):
            return excel_files

        # use content['sheets'] or fallback to a top-level metadata['sheets']
        sheets = content.get('sheets') or metadata.get('sheets') or {}
        for sheet_name, sheet in sheets.items():
            # If sheet is a dict (SpreadJS), reconstruct a DataFrame from the sparse dataTable
            if isinstance(sheet, dict):
                row_count = sheet.get('rowCount') or 0
                col_count = sheet.get('columnCount') or 0
                data_table = {}
                if isinstance(sheet.get('data'), dict):
                    data_table = sheet.get('data', {}).get('dataTable',
                                                           {}) or {}
                rows = []
                for i in range(int(row_count)):
                    row_table = data_table.get(str(i), {}) or {}
                    row_data = []
                    for j in range(int(col_count)):
                        cell = row_table.get(str(j), {}) or {}
                        value = cell.get('value') if isinstance(cell,
                                                                dict) else cell
                        if isinstance(value, (dict, list)):
                            value = json.dumps(value)
                        row_data.append(value)
                    rows.append(row_data)
                df = pd.DataFrame(rows)
            # If sheet is a string, treat it as a CSV
            elif isinstance(sheet, str):
                from csv import Sniffer
                from io import StringIO
                text = sheet.strip()
                delimiter = ','
                try:
                    delimiter = Sniffer().sniff(text.splitlines()[0]).delimiter
                except Exception:
                    pass
                df = pd.read_csv(StringIO(text), sep=delimiter, header=None)
            else:
                self.logger.warning("Skipping sheet %s: unsupported type %r",
                                    sheet_name, type(sheet))
                continue

            # Write DataFrame to a deterministic Excel file in /tmp
            tmp_dir = Path(tempfile.gettempdir())
            xlsx_path = tmp_dir / f"{sheet_name}.xlsx"
            if xlsx_path.exists():
                xlsx_path.unlink()
            with pd.ExcelWriter(str(xlsx_path), engine='xlsxwriter') as writer:
                safe_name = sheet_name[:31] or "sheet1"
                df.to_excel(writer, sheet_name=safe_name, index=False)
            excel_files.append((sheet_name, xlsx_path))

        return excel_files

    def _export_well_plate_to_excel (self, metadata: Dict[str, Any]) -> List[
        Tuple[str, Path]]:
        """
        Convert a WELL_PLATE element into one or more Excel files.

        If the well plate uses the same SpreadJS structure as a TABLE,
        this just delegates to _export_table_to_excel.  Otherwise, it
        assumes the `content` is a plain text table (CSV-like) and writes
        it to a single sheet.
        """
        excel_files: List[Tuple[str, Path]] = []
        content = metadata.get("content")

        # Case 1: SpreadJS-like content (dict with sheets) – reuse table export
        if isinstance(content, dict) and (
                content.get("sheets") or metadata.get("sheets")):
            return self._export_table_to_excel(metadata)

        # Case 2: CSV-like string
        if isinstance(content, str) and content.strip():
            from csv import Sniffer
            from io import StringIO

            text = content.strip()
            delimiter = ","
            try:
                delimiter = Sniffer().sniff(text.splitlines()[0]).delimiter
            except Exception:
                pass
            df = pd.read_csv(StringIO(text), sep=delimiter, header=None)

            tmp_dir = Path(tempfile.gettempdir())
            xlsx_path = tmp_dir / "well_plate.xlsx"
            if xlsx_path.exists():
                xlsx_path.unlink()

            with pd.ExcelWriter(str(xlsx_path), engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name="well_plate", index=False)

            excel_files.append(("well_plate", xlsx_path))

        return excel_files

    def build_extra_fields (self, first_entry: Dict[str, Any]) -> Dict[
        str, Any]:
        return {
            "Project Owner"        : first_entry.get("project_owner"),
            "Project creation date": first_entry.get("project_creation_date"),
            "Labfolder ID"         : first_entry.get("Labfolder_ID"),
            }

    def build_footer_html (self, first_entry: Dict[str, Any]) -> str:
        return ('<div style="text-align: right; margin-top: 20px;">'
                '<h5 style="margin:0 0 4px 0;">Labfolder Info</h5>'
                f"Project created: {first_entry.get('project_creation_date')}<br>"
                f"Labfolder project id: {first_entry.get('labfolder_project_id')}<br>"
                f"Author: {first_entry.get('project_owner')}<br>"
                f"Last edited: {first_entry.get('last_edited')}<br>"
                "</div>")
