# src/labfolder/fetcher.py
import logging
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests import HTTPError

try:
    from tqdm import tqdm  # optional; nice progress bars when running in a TTY
except Exception:
    tqdm = None  # fallback silently if tqdm isn't available

from .client import LabfolderClient


# ---------- logging setup ----------
ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "labfolder_fetcher.log"

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    fh = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(fh)


class LabFolderFetcher:
    """
    High-level helper around the Labfolder API:
      - entries & elements fetching
      - PDF/XHTML exports (creation, polling, download)
    """

    def __init__(self, email: str, password: str, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = LabfolderClient(email, password, self.base_url)
        self._client.login()

    # -------------------------------------------------------------------------
    # Low-level HTTP helpers (with transparent re-login on 401)
    # -------------------------------------------------------------------------

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        try:
            return self._client.get(endpoint, params=params)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.info("401 on GET %s — re-authenticating…", endpoint)
                self._client.login()
                return self._client.get(endpoint, params=params)
            raise

    def _post(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = f"{self.base_url}/{endpoint}"
        resp = self._client._session.post(url, json=json_data or {})  # type: ignore[attr-defined]
        if resp.status_code == 401:
            logger.info("401 on POST %s — re-authenticating…", endpoint)
            self._client.login()
            resp = self._client._session.post(url, json=json_data or {})  # type: ignore[attr-defined]
        resp.raise_for_status()
        return resp

    # -------------------------------------------------------------------------
    # Entries & elements
    # -------------------------------------------------------------------------

    def fetch_entries(
        self,
        expand: Optional[List[str]] = None,
        limit: int = 50,
        include_hidden: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch all entries, paging until completion."""
        entries: List[Dict[str, Any]] = []
        offset = 0
        expand_str = ",".join(expand) if expand else None

        while True:
            params: Dict[str, Any] = {
                "limit": limit,
                "offset": offset,
                "include_hidden": include_hidden,
            }
            if expand_str:
                params["expand"] = expand_str

            resp = self._get("entries", params=params)
            batch = resp.json()
            if not isinstance(batch, list):
                raise RuntimeError(f"Unexpected entries format: {batch!r}")

            entries.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        return entries

    def fetch_text(self, element: Dict[str, Any]) -> str:
        resp = self._get(f"elements/text/{element['id']}")
        data = resp.json()
        return data.get("content", "")

    def fetch_file(self, element: Dict[str, Any]) -> Optional[Path]:
        file_id = element.get("id")
        if not file_id:
            logger.error("Invalid FILE element (no id): %r", element)
            return None

        try:
            resp = self._get(f"elements/file/{file_id}/download")
        except HTTPError as e:
            logger.error("Download failed for FILE %s: %s", file_id, e)
            return None

        filename = "file.bin"
        cd = resp.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            filename = cd.split("filename=", 1)[-1].strip().strip('"')

        dest = Path(tempfile.gettempdir()) / filename
        return self._stream_to_file(resp, dest, desc=f"FILE {filename}")

    def fetch_image(self, element: Dict[str, Any]) -> Optional[Path]:
        image_id = element.get("id")
        if not image_id:
            logger.error("Invalid IMAGE element (no id): %r", element)
            return None

        try:
            resp = self._get(f"elements/image/{image_id}/original-data")
        except HTTPError as e:
            logger.error("Download failed for IMAGE %s: %s", image_id, e)
            return None

        filename = "image"
        cd = resp.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            filename = cd.split("filename=", 1)[-1].strip().strip('"')

        dest = Path(tempfile.gettempdir()) / filename
        return self._stream_to_file(resp, dest, desc=f"IMAGE {filename}")

    def fetch_data(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data_id = element.get("id")
        if not data_id:
            logger.error("Invalid DATA element (no id): %r", element)
            return None
        try:
            resp = self._get(f"elements/data/{data_id}")
            return resp.json()
        except HTTPError as e:
            logger.error("Failed to fetch DATA %s: %s", data_id, e)
            return None

    def fetch_table(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        table_id = element.get("id")
        if not table_id:
            logger.error("Invalid TABLE element (no id): %r", element)
            return None
        try:
            resp = self._get(f"elements/table/{table_id}")
            return resp.json()
        except HTTPError as e:
            logger.error("Failed to fetch TABLE %s: %s", table_id, e)
            return None

    def fetch_well_plate(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        plate_id = element.get("id")
        if not plate_id:
            logger.error("Invalid WELL_PLATE element (no id): %r", element)
            return None
        try:
            resp = self._get(f"elements/well-plate/{plate_id}")
            return resp.json()
        except HTTPError as e:
            logger.error("Failed to fetch WELL_PLATE %s: %s", plate_id, e)
            return None

    # -------------------------------------------------------------------------
    # PDF Exports (Labfolder)
    # -------------------------------------------------------------------------

    def create_pdf_export(
        self,
        project_ids: List[str],
        download_filename: str,
        *,
        preserve_entry_layout: bool = True,
        include_hidden_items: bool = False,
    ) -> str:
        """
        Ask Labfolder to create a PDF export for the given projects.
        Returns the export id (we pick the newest export afterwards).
        """
        payload = {
            "download_filename": download_filename,
            "settings": {"preserve_entry_layout": bool(preserve_entry_layout)},
            "content": {
                "project_ids": [str(x) for x in project_ids],
                "entry_ids": [],
                "template_ids": [],
                "group_ids": [],
            },
            "include_hidden_items": bool(include_hidden_items),
        }
        self._post("exports/pdf", json_data=payload)

        exports = self.list_pdf_exports(status="NEW,RUNNING,QUEUED,FINISHED", limit=50)
        if not exports:
            raise RuntimeError("PDF export creation returned no export objects")

        exports.sort(key=lambda e: e.get("creation_date", ""), reverse=True)
        return exports[0]["id"]

    def list_pdf_exports(self, status: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        resp = self._get("exports/pdf", params=params)
        data = resp.json()
        return data if isinstance(data, list) else []

    def get_pdf_export(self, export_id: str) -> Dict[str, Any]:
        resp = self._get(f"exports/pdf/{export_id}")
        return resp.json()

    def wait_for_pdf_export(self, export_id: str, poll_seconds: int = 3, timeout: int = 1800) -> None:
        """
        Poll a PDF export until it is FINISHED or ERROR.
        If it fails, raise with the most useful details Labfolder returns.
        """
        deadline = time.time() + timeout
        last_status = ""
        while time.time() < deadline:
            info = self.get_pdf_export(export_id)
            status = (info.get("status") or "").upper()

            if status != last_status:
                logger.info("PDF export %s status: %s", export_id, status)
                last_status = status

            if status == "FINISHED":
                return
            if status in {"ERROR", "REMOVED", "ABORT_PARALLEL"}:
                # Surface any error-like fields we can find
                details = {k: v for k, v in info.items()
                           if k in ("error", "errorMessage", "message", "statusMessage", "download_filename") and v}
                raise RuntimeError(f"PDF export {export_id} failed with status {status} and details {details}")

            time.sleep(poll_seconds)

        raise TimeoutError(f"Timed out waiting for PDF export {export_id}")

    def download_pdf_export(self, export_id: str, dest_path: Path) -> Path:
        """
        Download a FINISHED PDF export to dest_path.
        """
        url = f"{self.base_url}/exports/pdf/{export_id}/download"
        resp = self._client._session.get(url, stream=True, allow_redirects=True)  # type: ignore[attr-defined]
        if resp.status_code == 401:
            logger.info("401 on PDF download — re-authenticating…")
            self._client.login()
            resp = self._client._session.get(url, stream=True, allow_redirects=True)  # type: ignore[attr-defined]
        resp.raise_for_status()
        return self._stream_to_file(resp, dest_path, desc="Project PDF")

    # -------------------------------------------------------------------------
    # XHTML Exports (Labfolder)
    # -------------------------------------------------------------------------

    def create_xhtml_export(self, *, include_hidden_items: bool = False) -> str:
        payload = {"include_hidden_items": bool(include_hidden_items)}
        self._post("exports/xhtml", json_data=payload)

        exports = self.list_xhtml_exports(status="NEW,RUNNING,QUEUED,FINISHED", limit=50)
        if not exports:
            raise RuntimeError("XHTML export creation returned no export objects")

        exports.sort(key=lambda e: e.get("creation_date", ""), reverse=True)
        return exports[0]["id"]

    def list_xhtml_exports(self, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        exports: List[Dict[str, Any]] = []
        offset = 0

        while True:
            params: Dict[str, Any] = {"limit": limit, "offset": offset}
            if status:
                params["status"] = status
            resp = self._get("exports/xhtml", params=params)
            batch = resp.json()
            if not isinstance(batch, list):
                raise RuntimeError(f"Unexpected XHTML exports format: {batch!r}")
            exports.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        return exports

    def get_xhtml_export(self, export_id: str) -> Dict[str, Any]:
        resp = self._get(f"exports/xhtml/{export_id}")
        return resp.json()

    def wait_for_xhtml_export(self, export_id: str, poll_seconds: int = 10, timeout: int = 7200) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.get_xhtml_export(export_id)
            status = (info.get("status") or "").upper()
            if status == "FINISHED":
                return
            if status in {"ERROR", "REMOVED", "ABORT_PARALLEL"}:
                raise RuntimeError(f"XHTML export {export_id} failed with status {status}")
            time.sleep(poll_seconds)
        raise TimeoutError(f"Timed out waiting for XHTML export {export_id}")

    def download_xhtml_export(self, export_id: str, dest_zip: Path) -> Path:
        url = f"{self.base_url}/exports/xhtml/{export_id}/download"
        resp = self._client._session.get(url, stream=True, allow_redirects=True)  # type: ignore[attr-defined]
        if resp.status_code == 401:
            logger.info("401 on XHTML download — re-authenticating…")
            self._client.login()
            resp = self._client._session.get(url, stream=True, allow_redirects=True)  # type: ignore[attr-defined]
        resp.raise_for_status()

        self._stream_to_file(resp, dest_zip, desc="XHTML (ZIP)")

        # Validate ZIP integrity early
        if not zipfile.is_zipfile(dest_zip):
            ct = resp.headers.get("Content-Type", "")
            size = dest_zip.stat().st_size if dest_zip.exists() else 0
            try:
                dest_zip.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            raise RuntimeError(f"Downloaded XHTML is not a ZIP (content-type={ct!r}, bytes={size}).")

        return dest_zip

    def extract_zip(self, zip_path: Path, out_dir: Path) -> Path:
        """
        Extract a valid ZIP to out_dir.
        """
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError(f"Not a valid ZIP: {zip_path}")
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(out_dir)
        return out_dir

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def _stream_to_file(self, resp: requests.Response, dest_path: Path, *, desc: str) -> Optional[Path]:
        """
        Stream an HTTP response body to disk with an optional progress bar.
        Returns the destination path, or None on error.
        """
        total = None
        try:
            total_hdr = resp.headers.get("Content-Length")
            total = int(total_hdr) if total_hdr else None
        except Exception:
            total = None

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if tqdm and hasattr(tqdm, "__call__"):
                # Show progress bar only when stdout is a TTY
                import sys as _sys
                use_bar = _sys.stdout.isatty()
            else:
                use_bar = False
        except Exception:
            use_bar = False

        try:
            if use_bar:
                bar = tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024, desc=desc)  # type: ignore[misc]
                try:
                    with dest_path.open("wb") as f:
                        for chunk in resp.iter_content(chunk_size=1024 * 1024):
                            if not chunk:
                                continue
                            f.write(chunk)
                            bar.update(len(chunk))
                finally:
                    bar.close()
            else:
                with dest_path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return dest_path
        except (OSError, requests.RequestException) as e:
            logger.error("Failed to write %s: %s", dest_path, e)
            try:
                dest_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            return None
