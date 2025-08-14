from __future__ import annotations
from pathlib import Path
import tempfile
from typing import Optional


def export_project_pdf_and_attach(
    fetcher,            # LabFolderFetcher
    importer,           # Importer
    project_id: str | int,
    exp_id: str | int,
    *,
    filename: Optional[str] = None,
    preserve_entry_layout: bool = True,
    include_hidden_items: bool = False,
    retries: int = 1,
    logger=None,
) -> Path:
    """
    Create a Labfolder PDF export for a single project and attach it to the
    given eLabFTW experiment. Works for any project your account can access.
    """
    if filename is None:
        filename = f"labfolder_project_{project_id}.pdf"

    attempt = 0
    last_err = None
    while attempt <= retries:
        try:
            # 1) Create export job
            export_id = fetcher.create_pdf_export(
                [str(project_id)],
                filename,
                preserve_entry_layout=preserve_entry_layout,
                include_hidden_items=include_hidden_items,
            )

            # 2) Poll until FINISHED (raises if ERROR)
            fetcher.wait_for_pdf_export(export_id)

            # 3) Download the PDF
            pdf_path = Path(tempfile.gettempdir()) / filename
            fetcher.download_pdf_export(export_id, pdf_path)

            # 4) Attach to eLab experiment
            importer.upload_file(str(exp_id), pdf_path)

            if logger:
                logger.info("Attached PDF export for project %s: %s", project_id, pdf_path.name)
            return pdf_path

        except Exception as e:
            last_err = e
            if logger:
                logger.error("PDF export attempt %d failed for project %s: %s",
                             attempt + 1, project_id, e)
            attempt += 1
            if attempt > retries:
                raise last_err
