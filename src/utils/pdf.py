"""
pdf_exporter.py
================

This module adds support for exporting entire Labfolder projects as PDF
documents via the Labfolder v2 API.  It leverages the existing
authentication and session management in :class:`~src.labfolder.fetcher.LabFolderFetcher`
to initiate an export, poll for completion and download the final file.

The main entry point is :class:`PdfExporter`, which can be instantiated
with a :class:`~src.labfolder.fetcher.LabFolderFetcher` and provides
the :meth:`export_project_pdf` convenience method.
"""

from __future__ import annotations

import json
import logging
import time
import tempfile
from pathlib import Path
from typing import Optional

from ..labfolder import LabFolderFetcher

logger = logging.getLogger(__name__)


class PdfExportError(Exception):
    """Raised when a PDF export fails."""


class PdfExporter:
    """Create and download PDF exports for Labfolder projects.

    Parameters
    ----------
    fetcher : LabFolderFetcher
        An already authenticated fetcher instance used to access the API.
    poll_interval : float, optional
        Number of seconds to wait between status checks.  Defaults to 5 seconds.
    timeout : float, optional
        Maximum number of seconds to wait for an export to complete.  Defaults
        to 300 seconds (5 minutes).
    """

    def __init__(
        self,
        fetcher: LabFolderFetcher,
        poll_interval: float = 5.0,
        timeout: float = 300.0,
    ) -> None:
        self._fetcher = fetcher
        # Access the authenticated session and base URL from the fetcher
        self._session = fetcher._client._session  # pylint: disable=protected-access
        self._base_url = fetcher.base_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout

    def _create_export(self, project_id: str, filename: Optional[str] = None) -> str:
        """Initiate a PDF export for a single project.

        Returns the export ID.
        """
        endpoint = f"{self._base_url}/exports/pdf"
        # Only the project_ids field is set; other fields left empty
        payload = {
            "content": {
                "project_ids": [project_id],
                "entry_ids": [],
                "template_ids": [],
                "group_ids": [],
            },
            "include_hidden_items": False,
            "settings": {"preserve_entry_layout": False},
        }
        if filename:
            # API will append .pdf extension
            payload["download_filename"] = filename
        resp = self._session.post(endpoint, data=json.dumps(payload))
        try:
            resp.raise_for_status()
        except Exception as exc:
            raise PdfExportError(
                f"Failed to create PDF export for project {project_id}: {resp.status_code} {resp.text}"
            ) from exc
        data = resp.json()
        export_id = data.get("id")
        if not export_id:
            raise PdfExportError(f"No export ID returned for project {project_id}")
        logger.debug("Created PDF export %s for project %s", export_id, project_id)
        return export_id

    def _wait_for_finished(self, export_id: str) -> dict:
        """Poll the export status until it is finished or errors out.

        Returns the final export record as a dict.
        """
        deadline = time.time() + self.timeout
        status_endpoint = f"{self._base_url}/exports/pdf/{export_id}"
        last_status = None
        while True:
            resp = self._session.get(status_endpoint)
            try:
                resp.raise_for_status()
            except Exception as exc:
                raise PdfExportError(
                    f"Failed to fetch status for export {export_id}: {resp.status_code} {resp.text}"
                ) from exc
            info = resp.json()
            status = info.get("status")
            if status == "FINISHED":
                return info
            if status in {"ERROR", "REMOVED", "ABORT_PARALLEL"}:
                raise PdfExportError(f"Export {export_id} failed with status {status}")
            last_status = status
            if time.time() >= deadline:
                raise PdfExportError(
                    f"Timed out waiting for export {export_id} to finish; last status: {last_status}"
                )
            time.sleep(self.poll_interval)

    def _download(self, export: dict) -> Path:
        """Download the finished PDF export to a temporary file.

        Returns a Path to the downloaded PDF file.
        """
        download_href = export.get("download_href")
        if not download_href:
            # If download_href is missing, try direct endpoint
            export_id = export.get("id")
            if not export_id:
                raise PdfExportError("Cannot download export: missing id")
            download_url = f"{self._base_url}/exports/pdf/{export_id}/download"
        else:
            download_url = download_href
        # Download to a temporary file
        resp = self._session.get(download_url, stream=True)
        try:
            resp.raise_for_status()
        except Exception as exc:
            raise PdfExportError(
                f"Failed to download PDF export {export.get('id')}: {resp.status_code} {resp.text}"
            ) from exc
        # Determine filename from headers or use export filename
        filename = export.get("download_filename") or f"export_{export.get('id', '')}.pdf"
        # Use system temporary directory
        tmp_dir = Path(tempfile.gettempdir())
        tmp_path = tmp_dir / filename
        with tmp_path.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)
        logger.debug("Downloaded export %s to %s", export.get("id"), tmp_path)
        return tmp_path

    def export_project_pdf(self, project_id: str, filename: Optional[str] = None) -> Path:
        """Create, wait for and download a PDF export for a project.

        Parameters
        ----------
        project_id : str
            Labfolder project identifier.
        filename : str, optional
            Optional base name for the downloaded file (without extension).

        Returns
        -------
        Path
            Path to the downloaded PDF file.  The file is stored in the
            system temporary directory and may be removed by the OS later.

        Raises
        ------
        PdfExportError
            If the export fails or times out.
        """
        export_id = self._create_export(project_id, filename)
        export_info = self._wait_for_finished(export_id)
        pdf_path = self._download(export_info)
        return pdf_path