from src.fetcher import LabFolderFetcher
from src.transformer import Transformer
from src.importer import Importer
import logging
from pathlib import Path
from typing import List, Dict, Any


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

    def __init__(self, username, password, url = "https://labfolder.labforward.app/api/v2"):
        self._client = LabFolderFetcher(username, password, url)
        self._importer = Importer()
        self.logger = coord_logger

    def run (self) -> None:
        self.logger.info("Fetching all entries from Labfolder…")
        entries: List[Dict[str, Any]] = self._client.fetch_entries(
            expand=["author", "project", "last_editor"]
            )
        self.logger.info("Fetched %d entries", len(entries))

        transformer = Transformer(entries=entries,
                                  fetcher=self._client,
                                  importer=self._importer)

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
    coord = Coordinator("you@example.com", "XXXX")
    coord.run()