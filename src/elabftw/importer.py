import json
import mimetypes
import time
import httpx
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable

from ..utils import get_fixed


class Importer:
    """
    Wraps eLabFTW’s “experiments” endpoint to create, patch experiments,
    and upload file attachments. Also provides helpers to resolve item ids.
    """

    # ---------- Experiments CRUD ----------

    def create_experiment(self, title: str, tags: List[str]) -> str:
        resp = get_fixed("experiments").post(data={
            "title": title,
            "tags": tags
        })
        try:
            body = resp.json()
            exp_id = str(body.get("id", "")).strip()
        except ValueError:
            exp_id = ""
        if not exp_id:
            location = resp.headers.get("Location", "") or resp.headers.get("location", "")
            exp_id = location.rstrip("/").split("/")[-1]
        if not exp_id.isdigit():
            raise RuntimeError(f"Could not parse experiment ID: {exp_id!r}")
        return exp_id

    def patch_experiment(
        self,
        exp_id: str,
        body: str,
        category: int,
        uid: Optional[int] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not exp_id.isdigit():
            raise ValueError(f"Invalid experiment ID: {exp_id!r}")

        ep = get_fixed("experiments")

        current = ep.get(endpoint_id=exp_id).json()
        raw_meta = current.get("metadata") or {}
        if isinstance(raw_meta, str):
            try:
                metadata = json.loads(raw_meta)
            except json.JSONDecodeError:
                metadata = {}
        else:
            metadata = raw_meta

        elab_meta = metadata.get("elabftw", {
            "display_main_text": True,
            "extra_fields_groups": []
        })

        groups_def = {
            1: "Labfolder",
            2: "ISA-Study",
        }

        ef_payload: Dict[str, Any] = {}
        if extra_fields:
            for name, value in extra_fields.items():
                # Determine group and type
                if name == "ISA-Study":
                    group_id = 2
                    field_type = "items"
                    field_value = str(value) if value else ""
                elif name == "Project creation date":
                    group_id = 1
                    field_type = "date"
                    field_value = value
                else:
                    group_id = 1
                    field_type = "text"
                    field_value = value or ""
                ef_payload[name] = {
                    "type": field_type,
                    "value": field_value,
                    "group_id": group_id,
                    "description": "",
                }

        # Replace/augment groups in elab_meta
        elab_meta["extra_fields_groups"] = [
            {"id": gid, "name": gname} for gid, gname in groups_def.items()
        ]

        new_meta: Dict[str, Any] = {"elabftw": elab_meta}
        if ef_payload:
            new_meta["extra_fields"] = ef_payload

        payload: Dict[str, Any] = {
            "body": body,
            "category": category,
            "metadata": json.dumps(new_meta),
            "userid": uid,
        }

        ep.patch(endpoint_id=exp_id, data=payload)


    def upload_file(
        self,
        exp_id: str,
        file_path: Path,
        *,
        max_retries: int = 3,
        timeout: float = 120.0,
    ) -> None:
        """Attach a file to an eLabFTW experiment, retrying on network errors."""
        if not exp_id.isdigit():
            raise ValueError(f"Invalid experiment ID for upload: {exp_id!r}")

        # Determine MIME type
        mime_type, _ = mimetypes.guess_type(file_path.as_posix())
        mime_type = mime_type or "application/octet-stream"

        # Open file once outside the retry loop
        with file_path.open("rb") as f:
            files = {"file": (file_path.name, f, mime_type)}

            for attempt in range(1, max_retries + 1):
                try:
                    # Forward `timeout` and a Connection: close header to httpx
                    get_fixed("experiments").post(
                        endpoint_id=exp_id,
                        sub_endpoint_name="uploads",
                        files=files,
                        headers={"Connection": "close"},
                        timeout=timeout,
                    )
                    return  # Success: exit the method
                except httpx.TimeoutException as err:
                    # Timeout: retry unless we’re out of attempts
                    if attempt == max_retries:
                        raise RuntimeError(
                            f"Upload timed out after {max_retries} attempts"
                        ) from err
                    time.sleep(5)
                except httpx.TransportError as err:
                    # Network/SSL/TLS errors: retry similarly
                    if attempt == max_retries:
                        raise RuntimeError(
                            f"Upload failed due to transport error after "
                            f"{max_retries} attempts"
                        ) from err
                    time.sleep(5)

    def link_resource(self, exp_id: str, resource_id: str) -> None:
        if not exp_id.isdigit():
            raise ValueError(f"Invalid experiment ID for linking: {exp_id!r}")
        if not resource_id or not str(resource_id).isdigit():
            raise ValueError(f"Invalid resource ID for linking: {resource_id!r}")

        get_fixed("experiments").post(
            endpoint_id=str(exp_id),
            sub_endpoint_name="items_links",
            sub_endpoint_id=str(resource_id),
            data={"action": "create"},
        )

    # ---------- Items (resources) helpers ----------

    def _search_items(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search items (resources) by query string.
        Uses eLabFTW /api/v2/items with a 'q' / 'search' parameter depending on backend.
        """
        ep = get_fixed("resources")
        # try 'q', then fall back to 'search'
        for key in ("q", "search"):
            try:
                resp = ep.get(params={key: query, "limit": limit})
                data = resp.json()
                if isinstance(data, dict) and "items" in data:
                    return data["items"]  # some instances wrap results
                if isinstance(data, list):
                    return data
            except Exception:
                continue
        # last resort: fetch first page and filter client-side
        try:
            data = ep.get(params={"limit": limit}).json()
            if isinstance(data, list):
                return data
        except Exception:
            pass
        return []

    def _get_item_by_id(self, item_id: int) -> Optional[Dict[str, Any]]:
        try:
            resp = get_fixed("resources").get(endpoint_id=str(item_id))
            obj = resp.json()
            # sanity: must look like an item
            if isinstance(obj, dict) and (str(obj.get("id") or "") == str(item_id)):
                return obj
        except Exception:
            pass
        return None

    def resolve_item_id(
        self,
        code_or_id: Optional[str | int] = None,
        *,
        study_name: Optional[str] = None,
    ) -> Optional[int]:
        """
        Resolve an internal item id from:
          - a numeric 'code_or_id' (try as real id first),
          - else by searching using 'code_or_id' as text,
          - else by 'study_name' (exact/startswith match).
        Returns the internal eLabFTW item id or None.
        """
        # 1) Try direct id
        if code_or_id is not None:
            s = str(code_or_id).strip()
            if s.isdigit():
                iid = int(s)
                if self._get_item_by_id(iid):
                    return iid

        # 2) Search by code_or_id as text
        if code_or_id:
            hits = self._search_items(str(code_or_id))
            iid = self._pick_best_item(hits, wanted_title=study_name, wanted_code=str(code_or_id))
            if iid is not None:
                return iid

        # 3) Search by study_name
        if study_name:
            hits = self._search_items(str(study_name))
            iid = self._pick_best_item(hits, wanted_title=study_name)
            if iid is not None:
                return iid

        return None

    def _pick_best_item(
        self,
        items: Iterable[Dict[str, Any]],
        *,
        wanted_title: Optional[str] = None,
        wanted_code: Optional[str] = None,
    ) -> Optional[int]:
        """
        Heuristic: prefer exact title match; else startswith; else first result.
        Many eLabFTW instances expose 'title' and 'id' on item objects.
        Some also store custom fields; we ignore those for simplicity.
        """
        items = list(items)
        if not items:
            return None

        if wanted_title:
            # exact case-insensitive title match
            for it in items:
                t = str(it.get("title", "")).strip().lower()
                if t == wanted_title.strip().lower():
                    return int(it.get("id"))
            # startswith match
            for it in items:
                t = str(it.get("title", "")).strip().lower()
                if t.startswith(wanted_title.strip().lower()):
                    return int(it.get("id"))

        # fallback: if we searched with a code, prefer any title containing it
        if wanted_code:
            for it in items:
                t = str(it.get("title", "")).lower()
                if wanted_code.lower() in t:
                    return int(it.get("id"))

        # last resort: take the first hit
        try:
            return int(items[0].get("id"))
        except Exception:
            return None


__all__ = ["Importer"]
