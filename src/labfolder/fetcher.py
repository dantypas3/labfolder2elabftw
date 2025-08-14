import hashlib
import json
import logging
import mimetypes
import tempfile
import uuid
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests import HTTPError

import sys
from typing import Any, Dict, List, Optional, Tuple
try:
    from tqdm import tqdm
except Exception:  # tqdm optional fallback
    tqdm = None


# If you are not using packages, change to: from client import LabfolderClient
from .client import LabfolderClient


ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / 'labfolder_fetcher.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(str(LOG_FILE))]
)
logger = logging.getLogger(__name__)


class LabFolderFetcher:
    """
    Wraps the Labfolder client to fetch entries and files.
    Also exposes PDF/XHTML export helpers with robust download & ZIP validation.
    """

    def __init__(self, email: str, password: str, base_url: str) -> None:
        self.email = email
        self.password = password
        self.base_url = base_url.rstrip('/')
        self._client = LabfolderClient(email, password, self.base_url)
        self._client.login()

    # ---------------------------
    # Low-level GET/POST helpers
    # ---------------------------
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Authenticated GET with 401 refresh."""
        try:
            return self._client.get(endpoint, params=params)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.info('Token expired, re-logging in')
                self._client.login()
                return self._client.get(endpoint, params=params)
            raise

    def _post(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Authenticated POST with 401 refresh."""
        url = f"{self.base_url}/{endpoint}"
        resp = self._client._session.post(url, json=json_data or {})  # type: ignore[attr-defined]
        if resp.status_code == 401:
            logger.info('401 on POST; re-logging in and retrying')
            self._client.login()
            resp = self._client._session.post(url, json=json_data or {})  # type: ignore[attr-defined]
        resp.raise_for_status()
        return resp

    # ---------------------------
    # Entries / elements
    # ---------------------------
    def fetch_entries(self, expand: Optional[List[str]] = None,
                      limit: int = 50, include_hidden: bool = True) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        offset = 0
        expand_str = ','.join(expand) if expand else None

        while True:
            params: Dict[str, Any] = {
                'limit': limit,
                'offset': offset,
                'include_hidden': include_hidden,
            }
            if expand_str:
                params['expand'] = expand_str
            resp = self._get('entries', params=params)
            batch = resp.json()
            if not isinstance(batch, list):
                raise RuntimeError(f'Unexpected entries format: {batch!r}')
            entries.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return entries

    def fetch_text(self, element: Dict[str, Any]) -> str:
        resp = self._get(f"elements/text/{element['id']}")
        return resp.json().get('content', '')

    def fetch_file (self, element: Dict[str, Any]) -> Optional[Path]:
        file_id = element.get('id')
        if not file_id:
            logger.error('Invalid file element (no id) %r', element)
            return None
        try:
            resp = self._get(f"elements/file/{file_id}/download")
        except HTTPError as e:
            logger.error('Failed to download file %s: %s', file_id, e)
            return None

        content_disp = resp.headers.get('Content-Disposition', '')
        filename = 'file.bin'
        if 'filename=' in content_disp:
            parts = content_disp.split('filename=')
            if len(parts) > 1:
                filename = parts[-1].strip().strip('"')

        temp_path = Path(tempfile.gettempdir()) / filename
        try:
            self._download_with_progress(resp, temp_path,
                                         desc=f"FILE {filename}")
            return temp_path
        except Exception as e:
            logger.error('Error writing file %s: %s', temp_path, e)
            return None

    def fetch_image(self, element: Dict[str, Any]) -> Optional[Path]:
        image_id = element.get('id')
        if not image_id:
            logger.error('Invalid image element (no id) %r', element)
            return None
        try:
            resp = self._get(f"elements/image/{image_id}/original-data")
        except HTTPError as e:
            logger.error('Failed to download image %s: %s', image_id, e)
            return None

        content_disp = resp.headers.get('Content-Disposition', '')
        filename = 'unnamed_image'
        if 'filename=' in content_disp:
            parts = content_disp.split('filename=')
            if len(parts) > 1:
                filename = parts[-1].strip().strip('"')

        tmp_dir = Path(tempfile.gettempdir())
        temp_path = tmp_dir / filename
        try:
            with temp_path.open('wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
        except (OSError, requests.exceptions.RequestException) as e:
            logger.error('Error writing image file %s: %s', temp_path, e)
            return None
        return temp_path

    def fetch_data(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data_id = element.get('id')
        if not data_id:
            logger.error('Invalid data element (no id): %r', element)
            return None
        try:
            resp = self._get(f"elements/data/{data_id}")
            return resp.json()
        except HTTPError as e:
            logger.error('Failed to fetch data element %s: %s', data_id, e)
            return None
        except ValueError as e:
            logger.error('Invalid JSON for data element %s: %s', data_id, e)
            return None

    def fetch_table(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        table_id = element.get('id')
        if not table_id:
            logger.error('Invalid table element (no id): %r', element)
            return None
        try:
            resp = self._get(f"elements/table/{table_id}")
            return resp.json()
        except HTTPError as e:
            logger.error('Failed to fetch table element %s: %s', table_id, e)
            return None
        except ValueError as e:
            logger.error('Invalid JSON for table element %s: %s', table_id, e)
            return None

    def fetch_well_plate(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        plate_id = element.get('id')
        if not plate_id:
            logger.error('Invalid well plate element (no id): %r', element)
            return None
        try:
            resp = self._get(f"elements/well-plate/{plate_id}")
            return resp.json()
        except HTTPError as e:
            logger.error('Failed to fetch well plate %s: %s', plate_id, e)
            return None
        except ValueError as e:
            logger.error('Invalid JSON for well plate %s: %s', plate_id, e)
            return None

    # ---------------------------------------------------------------------
    # PDF export API (optional)
    # ---------------------------------------------------------------------
    def create_pdf_export(
        self,
        project_ids: List[str],
        download_filename: str,
        *,
        preserve_entry_layout: bool = True,
        include_hidden_items: bool = False,
    ) -> str:
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
            raise RuntimeError("PDF export creation returned no exports")
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

    def wait_for_pdf_export(self, export_id: str, poll_seconds: int = 3, timeout: int = 900) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.get_pdf_export(export_id)
            status = (info.get("status") or "").upper()
            if status == "FINISHED":
                return
            if status in {"ERROR", "REMOVED", "ABORT_PARALLEL"}:
                raise RuntimeError(f"PDF export {export_id} failed with status {status}")
            time.sleep(poll_seconds)
        raise TimeoutError(f"Timed out waiting for PDF export {export_id}")

    def download_pdf_export (self, export_id: str, dest_path: Path) -> Path:
        url = f"{self.base_url}/exports/pdf/{export_id}/download"
        resp = self._client._session.get(url, stream=True,
                                         allow_redirects=True)  # type: ignore[attr-defined]
        if resp.status_code == 401:
            logger.info('401 on PDF download; re-logging in and retrying')
            self._client.login()
            resp = self._client._session.get(url, stream=True,
                                             allow_redirects=True)  # type: ignore[attr-defined]
        resp.raise_for_status()
        return self._download_with_progress(resp, dest_path,
                                            desc="Project PDF")

    def wait_for_xhtml_export (self, export_id: str, poll_seconds: int = 10,
                               timeout: int = 7200) -> None:
        deadline = time.time() + timeout
        spinner = None
        if tqdm and sys.stdout.isatty():
            spinner = tqdm(total=None, desc="Preparing XHTML on server",
                           bar_format="{desc}: {elapsed}")
        try:
            last_status = ""
            while time.time() < deadline:
                info = self.get_xhtml_export(export_id)
                status = (info.get("status") or "").upper()
                if spinner:
                    if status != last_status:
                        spinner.set_description_str(
                            f"Preparing XHTML on server [{status}]")
                        last_status = status
                    spinner.update(0)
                if status == "FINISHED":
                    return
                if status in {"ERROR", "REMOVED", "ABORT_PARALLEL"}:
                    raise RuntimeError(
                        f"XHTML export {export_id} failed with status {status}")
                time.sleep(poll_seconds)
            raise TimeoutError(
                f"Timed out waiting for XHTML export {export_id}")
        finally:
            if spinner:
                spinner.close()

    # ---------------------------------------------------------------------
    # XHTML export API (global export by Labfolder design)
    # ---------------------------------------------------------------------
    def create_xhtml_export(self, *, include_hidden_items: bool = False) -> str:
        payload = {"include_hidden_items": bool(include_hidden_items)}
        self._post("exports/xhtml", json_data=payload)
        exports = self.list_xhtml_exports(status="NEW,RUNNING,QUEUED,FINISHED", limit=50)
        if not exports:
            raise RuntimeError("XHTML export creation returned no exports")
        exports.sort(key=lambda e: e.get("creation_date", ""), reverse=True)
        return exports[0]["id"]

    def list_xhtml_exports(self, status: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        resp = self._get("exports/xhtml", params=params)
        data = resp.json()
        return data if isinstance(data, list) else []

    def get_xhtml_export(self, export_id: str) -> Dict[str, Any]:
        resp = self._get(f"exports/xhtml/{export_id}")
        return resp.json()


    def wait_for_xhtml_export (self, export_id: str, poll_seconds: int = 10,
                               timeout: int = 7200) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.get_xhtml_export(export_id)
            status = (info.get("status") or "").upper()
            if status == "FINISHED":
                return
            if status in {"ERROR", "REMOVED", "ABORT_PARALLEL"}:
                raise RuntimeError(
                    f"XHTML export {export_id} failed with status {status}")
            time.sleep(poll_seconds)
        raise TimeoutError(f"Timed out waiting for XHTML export {export_id}")

    def download_xhtml_export (self, export_id: str, dest_zip: Path) -> Path:
        url = f"{self.base_url}/exports/xhtml/{export_id}/download"
        resp = self._client._session.get(url, stream=True,
                                         allow_redirects=True)  # type: ignore[attr-defined]
        if resp.status_code == 401:
            logger.info('401 on XHTML download; re-logging in and retrying')
            self._client.login()
            resp = self._client._session.get(url, stream=True,
                                             allow_redirects=True)  # type: ignore[attr-defined]
        resp.raise_for_status()

        self._download_with_progress(resp, dest_zip, desc="XHTML export (ZIP)")

        # Validate ZIP
        if not zipfile.is_zipfile(dest_zip):
            ct = resp.headers.get('Content-Type', '')
            size = dest_zip.stat().st_size if dest_zip.exists() else 0
            try:
                dest_zip.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
            raise RuntimeError(
                f"Downloaded XHTML is not a ZIP (content-type={ct!r}, bytes={size}).")
        return dest_zip

    def extract_zip(self, zip_path: Path, out_dir: Path) -> Path:
        """
        Extract a verified ZIP. If invalid, raise with a clear message.
        """
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError(f"Not a valid ZIP: {zip_path}")
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(out_dir)
        return out_dir

    def _download_with_progress (self, resp, dest_path: Path,
                                 desc: str) -> Path:
        """
        Stream 'resp' body to 'dest_path' with a tqdm progress bar (if available).
        Works even when Content-Length is unknown.
        """
        total = None
        try:
            total_hdr = resp.headers.get("Content-Length")
            total = int(total_hdr) if total_hdr else None
        except Exception:
            total = None

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if tqdm and sys.stdout.isatty():
            bar = tqdm(total=total, unit="B", unit_scale=True,
                       unit_divisor=1024, desc=desc)
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
            # Fallback: simple streaming without fancy UI
            with dest_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return dest_path


