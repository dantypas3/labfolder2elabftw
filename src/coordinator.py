from src.fetcher import LabFolderFetcher
from src.transformer import Transformer
from src.importer import Importer
import logging
import json
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
COORD_LOG = LOG_DIR / "coordinator.log"

coord_logger = logging.getLogger("Coordinator")
coord_logger.setLevel(logging.INFO)
fh = logging.FileHandler(COORD_LOG, mode="a")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
coord_logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("Coordinator: %(message)s"))
coord_logger.addHandler(ch)


class Coordinator:

    def __init__(self, username: str, password: str,
                 url: str = "https://labfolder.labforward.app/api/v2",
                 authors: Optional[List[str]] = None,
                 entries_parquet: Optional[Path] = None,
                 use_parquet: bool = False) -> None:
        self._client = LabFolderFetcher(username, password, url)
        self._importer = Importer()
        self.logger = coord_logger
        self._authors = [a.strip() for a in (authors or []) if isinstance(a, str) and a.strip()]

        self._entries_parquet: Optional[Path] = entries_parquet
        self._use_parquet: bool = bool(use_parquet)

    def _json_cols(self, df: pd.DataFrame) -> List[str]:
        """
        Detect columns that contain dict/list values and should be JSON-encoded
        to make the table parquet-compatible.
        """
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
                        v_strip = v.strip()
                        if (v_strip.startswith("{") and v_strip.endswith("}")) or \
                           (v_strip.startswith("[") and v_strip.endswith("]")):
                            try:
                                return json.loads(v_strip)
                            except Exception:
                                return v
                    return v
                decoded[c] = decoded[c].apply(_maybe_load)
        return decoded

    def _save_entries_to_cache(self, entries: List[Dict[str, Any]], path: Path) -> None:
        """
        Try to save entries to parquet with JSON-encoded complex columns.
        If parquet engine or types are unsupported, fall back to .json.gz.
        """
        try:
            df = pd.DataFrame(entries)
            json_cols = self._json_cols(df)
            enc = self._encode_json_cols(df, json_cols)

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
        """
        Load entries from parquet (preferred) or JSON.gz fallback.
        """
        if path.exists() and path.suffix == ".parquet":
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
        if path.suffix == ".json.gz":
            gz_candidates.append(path)
        else:
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

    def run (self) -> None:
        if self._use_parquet:
            if not self._entries_parquet:
                raise ValueError("--use-parquet requires --entries-parquet to be set to a valid file path")
            self.logger.info("Loading all entries from cache: %s", self._entries_parquet)
            entries: List[Dict[str, Any]] = self._load_entries_from_cache(self._entries_parquet)
        else:
            self.logger.info("Fetching all entries from Labfolder…")
            entries: List[Dict[str, Any]] = self._client.fetch_entries(
                expand=["author", "project", "last_editor"]
            )
            self.logger.info("Fetched %d entries", len(entries))
            if self._entries_parquet:
                try:
                    self._save_entries_to_cache(entries, self._entries_parquet)
                except Exception as e:
                    self.logger.warning("Failed to save entries cache: %s", e)

        transformer = Transformer(entries=entries,
                                  fetcher=self._client,
                                  importer=self._importer)

        if self._authors:
            self.logger.info("Filtering by authors (first names): %s", ", ".join(self._authors))
            data_by_project = transformer.transform_experiment_data_filtered(self._authors)
        else:
            self.logger.info("Transforming entries into grouped experiment data…")
            data_by_project = transformer.transform_experiment_data()

        for project_id, project_entries in data_by_project.items():
            self.logger.info(
                "Importing project %r with %d entries…",
                project_id, len(project_entries)
            )
            html_blocks = transformer.transform_projects_content(
                project_entries,
                category=38
            )
            self.logger.info(
                "Finished project %r: %d HTML blocks created",
                project_id, len(html_blocks)
            )

if __name__ == "__main__":
    coord = Coordinator("<USERNAME>", "<PASSWORD>")
    coord.run()