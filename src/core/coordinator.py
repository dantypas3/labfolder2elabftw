import json
import logging
import zipfile
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Iterable

import pandas as pd

from ..labfolder.fetcher import LabFolderFetcher
from ..elabftw.importer import Importer
from ..transformer import Transformer


ROOT_DIR = Path(__file__).resolve().parent
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
COORD_LOG = LOG_DIR / "coordinator.log"

coord_logger = logging.getLogger("Coordinator")
coord_logger.setLevel(logging.INFO)
fh = logging.FileHandler(COORD_LOG, mode="a", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
coord_logger.addHandler(fh)
coord_logger.addHandler(logging.StreamHandler())


class Coordinator:
    def __init__(self,
                 username: str,
                 password: str,
                 url: str = "https://eln.labfolder.com/api/v2",
                 authors: Optional[List[str]] = None,
                 entries_parquet: Optional[Path] = None,
                 use_parquet: bool = False,
                 isa_ids: Optional[Path] = None,
                 namelist: Optional[Path] = None,
                 xhtml_cache_dir: Path = Path("exports/xhtml"),
                 restrict_to_xhtml: bool = False) -> None:

        self._client = LabFolderFetcher(username, password, url)
        self._importer = Importer()
        self.logger = coord_logger
        self._authors = [a.strip() for a in (authors or []) if isinstance(a, str) and a.strip()]
        self._entries_parquet: Optional[Path] = entries_parquet
        self._use_parquet: bool = bool(use_parquet)
        self._isa_ids = isa_ids
        self._namelist = namelist
        self._xhtml_cache_dir = xhtml_cache_dir.resolve()
        self._restrict_to_xhtml = bool(restrict_to_xhtml)

    # ---------- parquet cache helpers ----------
    def _json_cols(self, df: pd.DataFrame) -> List[str]:
        json_cols: List[str] = []
        for col in df.columns:
            series = df[col]
            try:
                if series.map(lambda v: isinstance(v, (dict, list))).any():
                    json_cols.append(col)
            except Exception:
                pass
        return json_cols

    def _encode_json_cols(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        encoded = df.copy()
        for c in cols:
            if c in encoded.columns:
                encoded[c] = encoded[c].apply(
                    lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
                )
        return encoded

    def _decode_json_cols(self, df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        decoded = df.copy()
        for c in cols:
            if c in decoded.columns:
                def _maybe_load(v: Any) -> Any:
                    if isinstance(v, str):
                        t = v.strip()
                        if (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]")):
                            try:
                                return json.loads(t)
                            except Exception:
                                return v
                    return v
                decoded[c] = decoded[c].apply(_maybe_load)
        return decoded

    def _save_entries_to_cache(self, entries: List[Dict[str, Any]], path: Path) -> None:
        try:
            df = pd.DataFrame(entries)
            json_cols = self._json_cols(df)
            enc = self._encode_json_cols(df, json_cols)
            path.parent.mkdir(parents=True, exist_ok=True)
            enc.to_parquet(path, index=False)
            meta_path = path.with_suffix(path.suffix + ".meta.json")
            meta_path.write_text(json.dumps({"json_cols": json_cols}, indent=2))
            self.logger.info("Saved %d entries to parquet: %s", len(entries), path)
        except Exception as e:
            self.logger.warning("Parquet save failed (%s). Falling back to JSON.gz cache.", e)
            gz_path = path.with_suffix(".json.gz")
            import gzip
            with gzip.open(gz_path, "wt", encoding="utf-8") as f:
                for rec in entries:
                    f.write(json.dumps(rec))
                    f.write("\n")
            self.logger.info("Saved %d entries to JSON.gz: %s", len(entries), gz_path)

    def _load_entries_from_cache(self, path: Path) -> List[Dict[str, Any]]:
        if path and path.exists() and path.suffix == ".parquet":
            try:
                df = pd.read_parquet(path)
                meta_path = path.with_suffix(path.suffix + ".meta.json")
                json_cols: List[str] = []
                if meta_path.exists():
                    try:
                        meta = json.loads(meta_path.read_text())
                        json_cols = list(meta.get("json_cols") or [])
                    except Exception:
                        json_cols = []
                if json_cols:
                    df = self._decode_json_cols(df, json_cols)
                records = df.to_dict(orient="records")
                self.logger.info("Loaded %d entries from parquet: %s", len(records), path)
                return records
            except Exception as e:
                self.logger.warning("Parquet load failed (%s). Will try JSON.gz fallback.", e)

        import gzip
        gz_candidates: List[Path] = []
        if path and path.suffix == ".json.gz":
            gz_candidates.append(path)
        elif path:
            gz_candidates.append(path.with_suffix(".json.gz"))

        for gz_path in gz_candidates:
            if gz_path.exists():
                entries: List[Dict[str, Any]] = []
                with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entries.append(json.loads(line))
                        except Exception:
                            pass
                self.logger.info("Loaded %d entries from JSON.gz: %s", len(entries), gz_path)
                return entries

        raise FileNotFoundError(f"No usable cache found for {path}")

    # ---------- XHTML cache & reuse ----------
    def _prepare_xhtml_root(self,
                            prefer_local: bool = True,
                            export_id: Optional[str] = None) -> Optional[Path]:
        """
        Return a local folder containing the extracted XHTML export.

        IMPORTANT CHANGE:
        - If restrict_to_xhtml == False: we only reuse an already-extracted local cache
          or a cached ZIP. We DO NOT call the API (no create/list/wait/download),
          so the app will not hang here. If nothing local is found, we return None.
        - If restrict_to_xhtml == True: we keep the original behavior and may talk
          to the API to ensure the XHTML is available.
        """
        cache_dir = self._xhtml_cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)

        def _latest_extracted() -> Optional[Path]:
            patterns = ["labfolder_xhtml_*", "xhtml_*"]
            candidates: List[Path] = []
            for pat in patterns:
                candidates += [p for p in cache_dir.glob(pat) if p.is_dir()]
            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return candidates[0] if candidates else None

        def _latest_zip() -> Optional[tuple[str, Path]]:
            patterns = ["labfolder_xhtml_*.zip", "xhtml_*.zip"]
            zips: List[Path] = []
            for pat in patterns:
                zips += [p for p in cache_dir.glob(pat) if p.is_file()]
            zips.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            if not zips:
                return None
            name = zips[0].stem
            if "labfolder_xhtml_" in name:
                token = name.split("labfolder_xhtml_")[-1]
            elif "xhtml_" in name:
                token = name.split("xhtml_")[-1]
            else:
                token = name
            return (token, zips[0])

        # 1) Prefer already-extracted
        local = _latest_extracted()
        if local:
            self.logger.info("Reusing local XHTML export at: %s", local)
            return local

        # 2) Extract from an existing ZIP if present (validate first)
        z = _latest_zip()
        if z:
            exp_id, zip_path = z
            if not zipfile.is_zipfile(zip_path):
                self.logger.warning("Cached file is not a valid ZIP, removing: %s", zip_path)
                try:
                    zip_path.unlink()
                except Exception:
                    pass
            else:
                out_dir = cache_dir / f"labfolder_xhtml_{exp_id}"
                if not out_dir.exists():
                    self._client.extract_zip(zip_path, out_dir)
                    self.logger.info("Extracted cached ZIP to: %s", out_dir)
                return out_dir

        # From here on, only proceed with network calls if restriction was requested.
        if not self._restrict_to_xhtml:
            self.logger.info(
                "No local XHTML cache found and --only-projects-from-xhtml is not set. "
                "Skipping XHTML preparation."
            )
            return None

        # 3) Use provided export_id once
        if export_id:
            zip_path = cache_dir / f"labfolder_xhtml_{export_id}.zip"
            if not zip_path.exists():
                self.logger.info("Downloading XHTML export %s to %s", export_id, zip_path)
                self._client.download_xhtml_export(export_id, zip_path)
            if not zipfile.is_zipfile(zip_path):
                self.logger.warning("Downloaded file is not a valid ZIP, removing: %s", zip_path)
                try:
                    zip_path.unlink()
                except Exception:
                    pass
                return None
            out_dir = cache_dir / f"labfolder_xhtml_{export_id}"
            if not out_dir.exists():
                self._client.extract_zip(zip_path, out_dir)
            return out_dir

        # 4) Reuse newest FINISHED from API; else create one time
        try:
            finished = self._client.list_xhtml_exports(status="FINISHED", limit=50)
            if finished:
                finished.sort(key=lambda e: e.get("creation_date", ""), reverse=True)
                reuse = finished[0]
                exp_id = reuse["id"]
                zip_path = cache_dir / f"labfolder_xhtml_{exp_id}.zip"
                if not zip_path.exists():
                    self.logger.info("Reusing FINISHED XHTML export %s; downloading once.", exp_id)
                    self._client.download_xhtml_export(exp_id, zip_path)
                if not zipfile.is_zipfile(zip_path):
                    self.logger.warning("Reused download is not a valid ZIP, removing: %s", zip_path)
                    try:
                        zip_path.unlink()
                    except Exception:
                        pass
                    return None
                out_dir = cache_dir / f"labfolder_xhtml_{exp_id}"
                if not out_dir.exists():
                    self._client.extract_zip(zip_path, out_dir)
                return out_dir
        except Exception as e:
            self.logger.warning("Could not reuse FINISHED XHTML export: %s", e)

        try:
            self.logger.info("Creating XHTML export (one-time)…")
            new_id = self._client.create_xhtml_export(include_hidden_items=False)
            self._client.wait_for_xhtml_export(new_id)
            zip_path = cache_dir / f"labfolder_xhtml_{new_id}.zip"
            self._client.download_xhtml_export(new_id, zip_path)
            if not zipfile.is_zipfile(zip_path):
                self.logger.warning("Newly created download is not a valid ZIP, removing: %s", zip_path)
                try:
                    zip_path.unlink()
                except Exception:
                    pass
                return None
            out_dir = cache_dir / f"labfolder_xhtml_{new_id}"
            self._client.extract_zip(zip_path, out_dir)
            return out_dir
        except Exception as e:
            self.logger.warning("Skipping XHTML attachments (reason: %s)", e)
            return None

    # ---------- helpers to handle nested 'projects/' roots ----------
    def _iter_projects_roots(self, xhtml_root: Path) -> Iterable[Path]:
        if not xhtml_root or not xhtml_root.exists():
            return []
        direct = xhtml_root / "projects"
        if direct.is_dir():
            yield direct

        patterns = [
            "*/projects",         # depth 1
            "*/*/projects",       # depth 2
            "*/*/*/projects",     # depth 3
            "*/*/*/*/projects",   # depth 4
            "*/*/*/*/*/projects", # depth 5
        ]

        # recursively find deeper 'projects' dirs
        for pattern in patterns:
            for p in xhtml_root.glob(pattern):
                if p.is_dir():
                    yield p
    def _xhtml_contains_project(self, xhtml_root: Path, project_id: str) -> bool:
        pid = str(project_id)
        try:
            for projects_root in self._iter_projects_roots(xhtml_root):
                for index_html in projects_root.rglob("index.html"):
                    name = index_html.parent.name
                    if (name.startswith(f"{pid}_")
                            or name.endswith(f"_{pid}")
                            or f"_{pid}_" in name
                            or name == pid):
                        return True
            return False
        except Exception:
            return False

    def _ensure_xhtml_for_projects(self, target_pids: Set[str]) -> Optional[Path]:
        """
        NEW behavior summary:
        - If restrict_to_xhtml is False: only return a local XHTML root if present.
          Do not hit the network.
        - If restrict_to_xhtml is True: reuse/download/create as before.
        """
        root = self._prepare_xhtml_root(prefer_local=True, export_id=None)

        if not self._restrict_to_xhtml:
            if root:
                self.logger.info("Using local XHTML at %s for attachments (optional).", root)
            else:
                self.logger.info("No local XHTML found; proceeding without XHTML attachments.")
            return root

        # Only reach here if restrict_to_xhtml is True (enforce presence)
        def missing_from(r: Optional[Path]) -> List[str]:
            if not r or not r.exists():
                return list(target_pids)
            return [pid for pid in target_pids if not self._xhtml_contains_project(r, pid)]

        missing = missing_from(root)
        if missing:
            self.logger.info("Current XHTML cache missing %d project(s): %s",
                             len(missing), ", ".join(missing[:24]) + ("…" if len(missing) > 24 else ""))
            # Refresh once (allowed to call API in this mode)
            new_root = self._prepare_xhtml_root(prefer_local=False, export_id=None)
            if new_root:
                root = new_root
                still_missing = missing_from(root)
                if still_missing:
                    self.logger.warning(
                        "Even the refreshed XHTML export is missing %d project(s). "
                        "They will be skipped because --only-projects-from-xhtml is set.",
                        len(still_missing)
                    )
        return root

    # ---------- main ----------
    def run(self) -> None:
        # 1) Entries source
        if self._use_parquet:
            if not self._entries_parquet:
                raise ValueError("--use-parquet requires --entries-parquet")
            self.logger.info("Loading all entries from cache: %s", self._entries_parquet)
            entries: List[Dict[str, Any]] = self._load_entries_from_cache(self._entries_parquet)
        else:
            self.logger.info("Fetching all entries from Labfolder…")
            entries: List[Dict[str, Any]] = self._client.fetch_entries(
                expand=["author", "project", "last_editor"])
            self.logger.info("Fetched %d entries", len(entries))
            if self._entries_parquet:
                try:
                    self._save_entries_to_cache(entries, self._entries_parquet)
                except Exception as e:
                    self.logger.warning("Failed to save entries cache: %s", e)

        # 2) Build transformer
        transformer = Transformer(
            entries=entries,
            fetcher=self._client,
            importer=self._importer,
            isa_ids_list=self._isa_ids,
            namelist=self._namelist
        )

        # 3) Group by project (optional author filter)
        if self._authors:
            self.logger.info("Filtering by authors (first names): %s", ", ".join(self._authors))
            data_by_project = transformer.transform_experiment_data_filtered(self._authors)
        else:
            self.logger.info("Transforming entries into grouped experiment data…")
            data_by_project = transformer.transform_experiment_data()

        # 4) Ensure XHTML cache (non-blocking when restrict_to_xhtml is False)
        target_pids = {str(pid) for pid in data_by_project.keys()}
        xhtml_root = self._ensure_xhtml_for_projects(target_pids)

        # 5) Optionally restrict to projects present in XHTML
        if self._restrict_to_xhtml:
            if not xhtml_root or not xhtml_root.exists():
                self.logger.error("--only-projects-from-xhtml is set but no XHTML cache is available. Nothing to process.")
                data_by_project = {}
            else:
                kept = {}
                skipped: List[str] = []
                for pid, entries_list in data_by_project.items():
                    if self._xhtml_contains_project(xhtml_root, str(pid)):
                        kept[pid] = entries_list
                    else:
                        skipped.append(str(pid))
                self.logger.info("Restricting to XHTML projects: keeping %d, skipping %d.", len(kept), len(skipped))
                if skipped:
                    self.logger.info("Skipped project IDs (absent in XHTML): %s",
                                     ", ".join(skipped[:24]) + ("…" if len(skipped) > 24 else ""))
                data_by_project = kept

        # 6) Create experiments per project
        for project_id, project_entries in data_by_project.items():
            self.logger.info("Importing project %r with %d entries…", project_id, len(project_entries))
            html_blocks = transformer.transform_projects_content(
                project_entries, category=83, xhtml_root=xhtml_root
            )
            self.logger.info("Finished project %r: %d HTML blocks created", project_id, len(html_blocks))
