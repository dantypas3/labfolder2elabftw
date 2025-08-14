import json
import logging
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# If flat structure, change these to: from importer import Importer / from fetcher import LabFolderFetcher
from ..elabftw import Importer
from ..labfolder import LabFolderFetcher

ROOT_DIR = Path(__file__).resolve().parent
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

TRANS_LOG_FILE = LOG_DIR / "transformer.log"
file_handler = logging.FileHandler(str(TRANS_LOG_FILE), mode="a")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))

transformer_logger = logging.getLogger("Transformer")
transformer_logger.setLevel(logging.INFO)
transformer_logger.addHandler(file_handler)
transformer_logger.addHandler(logging.StreamHandler())


class Transformer:
    def __init__ (self, entries: List[Dict[str, Any]],
                  fetcher: LabFolderFetcher,
                  importer: Importer,
                  isa_ids_list: Optional[Path] = None,
                  namelist: Optional[Path] = None,
                  logger: Optional[logging.Logger] = None) -> None:

        self._entries = pd.DataFrame(entries)
        self._namelist = namelist
        self._isa_ids_list = isa_ids_list
        self._fetcher = fetcher
        self._importer = importer
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    # ---------- grouping ----------
    def _build_experiment_data (self, df: pd.DataFrame) -> Dict[Any, List[Dict[str, Any]]]:
        experiment_data: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for _, row in df.iterrows():
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
                "number_of_entries"    : row["project"].get("number_of_entries"),
                "project_title"        : row["project"].get("title"),
                "project_owner"        : f"{row['author'].get('first_name')} {row['author'].get('last_name')}",
                "Labfolder_ID"         : row["project"].get("id"),
            }
            experiment_data[row["project_id"]].append(record)
        return experiment_data

    def transform_experiment_data (self) -> Dict[Any, List[Dict[str, Any]]]:
        return self._build_experiment_data(self._entries)

    def transform_experiment_data_filtered (self, first_names: List[str]) -> Dict[Any, List[Dict[str, Any]]]:
        allowed = {n.strip().lower() for n in first_names if isinstance(n, str) and n.strip()}
        if not allowed:
            return self._build_experiment_data(self._entries)

        def match (author: Any) -> bool:
            if not isinstance(author, dict):
                return False
            fn = str(author.get("first_name", "")).strip().lower()
            return fn in allowed

        filtered = self._entries[self._entries["author"].apply(match)]
        if filtered.empty:
            self.logger.info("No entries matched first names: %s", first_names)
        return self._build_experiment_data(filtered)

    # ---------- transform / create experiment ----------
    def transform_projects_content (self, project: List[Dict[str, Any]],
                                    max_entries: Optional[int] = None,
                                    category: int = 83,
                                    xhtml_root: Optional[Path] = None) -> List[str]:
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
        self._importer.patch_experiment(exp_id, full_body, category,
                                        extra_fields=extra_fields)

        isa_id = extra_fields.get("ISA-Study")
        if isa_id:
            try:
                self._importer.link_resource(exp_id, str(isa_id))
            except Exception as e:
                self.logger.error("Failed to link ISA-Study %s to experiment %s: %s", isa_id, exp_id, e)

        # Attach XHTML project index + all XLSX from cached export
        try:
            self._attach_xhtml_artifacts_for_project(exp_id, project, xhtml_root)
        except Exception as e:
            self.logger.error("Failed to attach XHTML artifacts: %s", e)

        # Attach Project PDF (create → wait → download → upload), with caching
        try:
            self._attach_project_pdf(exp_id, project)
        except Exception as e:
            self.logger.error("Failed to attach Project PDF: %s", e)

        return entry_htmls

    # ---------- helpers: building body ----------
    def collect_title_and_tags (self, project: List[Dict[str, Any]]) -> Tuple[str, List[str]]:
        title = project[0].get("project_title", "")
        tags: List[str] = []
        for entry in project:
            tags.extend(entry.get("tags", np.array([])))
        return title, tags

    def build_entry_html (self, entry: Dict[str, Any], exp_id: str) -> str:
        entry_tags = entry.get("tags", [])
        formatted_tags = " ".join(f"§{tag}" for tag in entry_tags)
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
                blocks.append("<p>[TABLE not uploaded yet]</p>")
                # metadata = self._fetcher.fetch_table(element)
                # if metadata:
                #     try:
                #         for sheet_name, xlsx_path in self._export_table_to_excel(metadata):
                #             self._importer.upload_file(exp_id, xlsx_path)
                #             blocks.append(f"<p>[Attached TABLE sheet '{sheet_name}': {xlsx_path.name}]</p>")
                #     except Exception as e:
                #         self.logger.error("TABLE Excel conversion/upload failed for %s: %s",
                #                           metadata.get("id"), e)
                #         blocks.append(f"<p>[Failed to convert/upload TABLE to Excel: {metadata.get('id')}]</p>")
                # else:
                #     blocks.append("<p>[Empty or invalid TABLE]</p>")

            elif typ == "WELL_PLATE":
                blocks.append("<p>[WELL_PLATE not uploaded yet]</p>")
                # metadata = self._fetcher.fetch_well_plate(element)
                # if metadata:
                #     try:
                #         excel_files = self._export_well_plate_to_excel(metadata)
                #         if excel_files:
                #             for sheet_name, xlsx_path in excel_files:
                #                 self._importer.upload_file(exp_id, xlsx_path)
                #                 blocks.append(f"<p>[Attached WELL_PLATE sheet '{sheet_name}': {xlsx_path.name}]</p>")
                #         else:
                #             blocks.append("<p>[No data to convert for WELL_PLATE]</p>")
                #     except Exception as e:
                #         self.logger.error("WELL_PLATE Excel conversion/upload failed for %s: %s",
                #                           metadata.get("id"), e)
                #         blocks.append(f"<p>[Failed to convert/upload WELL_PLATE to Excel: {metadata.get('id')}]</p>")
                # else:
                #     blocks.append("<p>[Empty or invalid WELL_PLATE]</p>")

            elif typ == "TEXT":
                try:
                    text = self._fetcher.fetch_text(element)
                    blocks.append(f"<pre>{text}</pre>")
                except Exception as e:
                    self.logger.error("TEXT fetch failed for %s: %s", element.get("id"), e)
                    blocks.append(f"<p>[Failed to fetch TEXT: {element.get('id')}]</p>")

            elif typ == "FILE":
                path = self._fetcher.fetch_file(element)
                if path:
                    try:
                        self._importer.upload_file(exp_id, path)
                        blocks.append(f"<p>[Attached FILE: {path.name}]</p>")
                    except Exception as e:
                        self.logger.error("FILE upload failed for %s: %s", path.name, e)
                        blocks.append(f"<p>[Failed to attach FILE: {element.get('id')}]</p>")

            elif typ == "IMAGE":
                path = self._fetcher.fetch_image(element)
                if path:
                    try:
                        self._importer.upload_file(exp_id, path)
                        blocks.append(f"<p>[Attached IMAGE: {path.name}]</p>")
                    except Exception as e:
                        self.logger.error("IMAGE upload failed for %s: %s", path.name, e)
                        blocks.append(f"<p>[Failed to attach IMAGE: {element.get('id')}]</p>")

            elif typ == "DATA":
                try:
                    data = self._fetcher.fetch_data(element)
                    rows = [(f"<tr><td>{d.get('title')}</td>"
                             f"<td>{d.get('value')}</td>"
                             f"<td>{d.get('unit')}</td></tr>") for d in data.get("data_elements", [])]
                    table_html = "<table><tr><th>Title</th><th>Value</th><th>Unit</th></tr>" + "".join(rows) + "</table>"
                    blocks.append(table_html)
                except Exception as e:
                    self.logger.error("DATA fetch failed for %s: %s", element.get("id"), e)
                    blocks.append(f"<p>[Failed to fetch DATA: {element.get('id')}]</p>")

            else:
                self.logger.warning("Skipping element %s of type %s", element.get("id"), typ)
                blocks.append(f"<p>[Skipped element: {element.get('id')}]</p>")

        dt = datetime.strptime(entry["entry_creation_date"], "%Y-%m-%dT%H:%M:%S.%f%z")
        created = f"Created: {dt.date().isoformat()}<br>"
        body_html = ("\n".join(blocks) + "<br>") if blocks else ""
        return header + body_html + created + "<hr><hr>"

    def build_footer_html (self, first_entry: Dict[str, Any]) -> str:
        return ('<div style="text-align: right; margin-top: 20px;">'
                '<h5 style="margin:0 0 4px 0;">Labfolder Info</h5>'
                f"Project created: {first_entry.get('project_creation_date')}<br>"
                f"Labfolder project id: {first_entry.get('labfolder_project_id')}<br>"
                f"Author: {first_entry.get('project_owner')}<br>"
                f"Last edited: {first_entry.get('last_edited')}<br>"
                "</div>")


    # ---------- extra metadata ----------
    def match_isa_id (self, first_entry: Dict[str, Any]):
        if not self._isa_ids_list:
            return None
        try:
            user_df = pd.read_csv(self._isa_ids_list)
        except Exception:
            return None
        if user_df.empty:
            self.logger.error("No user mapping found")
            return None
        entry_name = str(first_entry.get("project_owner", "")).strip().lower()
        for _, row in user_df.iterrows():
            csv_name = str(row["User"]).strip().lower()
            if csv_name == entry_name:
                return row["Resource ID"]
        self.logger.warning("No Resource ID found for %s", entry_name)
        return None

    def match_user_id (self, first_entry: Dict[str, Any]):
        if not self._namelist:
            return 847
        try:
            user_df = pd.read_csv(self._namelist)
        except Exception:
            return 847
        if user_df.empty:
            self.logger.error("No user mapping found")
            return 847
        entry_name = str(first_entry.get("project_owner", "")).strip().lower()
        for _, row in user_df.iterrows():
            csv_name = str(f"{row['First Name']} {row['Last Name']}").strip().lower()
            if csv_name == entry_name:
                return row["User ID"]
        self.logger.warning("No User ID found for %s", entry_name)
        return 847

    def build_extra_fields (self, first_entry: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Project Owner"        : first_entry.get("project_owner"),
            "Project creation date": first_entry.get("project_creation_date"),
            "Labfolder Project ID" : first_entry.get("Labfolder_ID"),
            "ISA-Study"            : str(self.match_isa_id(first_entry)),
        }

    # ---------- XHTML attachment logic (project-based) ----------
    def _attach_xhtml_artifacts_for_project(self, exp_id: str, project: List[Dict[str, Any]], xhtml_root: Optional[Path]) -> None:
        """
        Given the cached XHTML export root (folder with subfolders 'projects', 'templates', ...),
        find the project's folder and attach:
          - the project's 'index.html' (if present)
          - all '*.xlsx' files under that project folder (recursively)
        The folder naming follows your example, e.g.:
            projects/
              Group projects_ AG Brügger_3182/
                92321_Example project/
              Group projects_ Lipidomics_3187/
                132999_Test2020/
        We identify the project folder by matching its name prefix with the Labfolder project id.
        """
        if not xhtml_root or not Path(xhtml_root).exists():
            return

        project_id = str(project[0].get("labfolder_project_id") or project[0].get("Labfolder_ID") or "").strip()
        if not project_id:
            self.logger.warning("Cannot attach XHTML: missing Labfolder project id")
            return

        projects_root = Path(xhtml_root) / "projects"
        if not projects_root.exists():
            self.logger.warning("XHTML projects folder not found at %s", projects_root)
            return

        matches: List[Path] = []
        for grp in projects_root.iterdir():
            if not grp.is_dir():
                continue
            for candidate in grp.iterdir():
                if candidate.is_dir() and candidate.name.startswith(f"{project_id}_"):
                    matches.append(candidate)

        if not matches:
            self.logger.info("No XHTML project folder matched id %s under %s", project_id, projects_root)
            return

        matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        project_folder = matches[0]
        self.logger.info("Attaching XHTML artifacts from: %s", project_folder)

        index_html = project_folder / "index.html"
        if index_html.exists():
            try:
                self._importer.upload_file(exp_id, index_html)
                self.logger.info("Attached XHTML index: %s", index_html.name)
            except Exception as e:
                self.logger.warning("Failed to attach %s: %s", index_html, e)

        for xlsx in project_folder.rglob("*.xlsx"):
            try:
                self._importer.upload_file(exp_id, xlsx)
                self.logger.info("Attached XLSX: %s", xlsx.relative_to(project_folder))
            except Exception as e:
                self.logger.warning("Failed to attach %s: %s", xlsx, e)

    # ---------- Project PDF attachment ----------
    def _attach_project_pdf(self, exp_id: str, project: List[Dict[str, Any]]) -> None:
        """
        Create a Labfolder PDF export for this project (preserve layout),
        wait for completion, download into exports/pdf/, then upload to eLabFTW.
        Reuses a cached PDF file if already present.
        """
        project_id = str(project[0].get("labfolder_project_id") or project[0].get("Labfolder_ID") or "").strip()
        project_title = str(project[0].get("project_title") or f"project_{project_id}").strip()
        if not project_id:
            self.logger.warning("Skipping PDF export: missing Labfolder project id")
            return

        pdf_cache_dir = Path("exports/pdf").resolve()
        pdf_cache_dir.mkdir(parents=True, exist_ok=True)

        # If we already have a cached PDF for this project, attach it and return.
        existing = sorted(pdf_cache_dir.glob(f"{project_id}_*.pdf"))
        if existing:
            pdf_path = existing[-1]
            try:
                self._importer.upload_file(exp_id, pdf_path)
                self.logger.info("Attached cached Project PDF: %s", pdf_path.name)
                return
            except Exception as e:
                self.logger.warning("Failed to attach cached PDF %s (will try re-export): %s", pdf_path, e)

        # Build a safe filename for the export (Labfolder uses this as download filename)
        safe_title = "".join(ch if (ch.isalnum() or ch in "-_ .") else "_" for ch in project_title).strip()
        if not safe_title:
            safe_title = f"project_{project_id}"
        requested_filename = f"{project_id}_{safe_title}.pdf"
        dest_pdf = pdf_cache_dir / requested_filename

        # Create + wait + download
        self.logger.info("Creating Project PDF export for project %s…", project_id)
        export_id = self._fetcher.create_pdf_export(
            project_ids=[project_id],
            download_filename=requested_filename,
            preserve_entry_layout=True,
            include_hidden_items=False,
        )
        self._fetcher.wait_for_pdf_export(export_id)
        # Use the actual filename if API changed it (optional but nice)
        try:
            info = self._fetcher.get_pdf_export(export_id)
            final_name = info.get("download_filename") or requested_filename
            dest_pdf = dest_pdf.with_name(final_name)
        except Exception:
            pass

        self._fetcher.download_pdf_export(export_id, dest_pdf)

        # Upload to eLabFTW
        try:
            self._importer.upload_file(exp_id, dest_pdf)
            self.logger.info("Attached Project PDF: %s", dest_pdf.name)
        except Exception as e:
            self.logger.error("Failed to upload Project PDF %s: %s", dest_pdf, e)
