import argparse
import logging
import sys
from pathlib import Path

import http.client as http_client
from .core.coordinator import Coordinator

SENSITIVE_KEYS = {"password", "username"}

def mask_sensitive(ns: argparse.Namespace) -> dict:
    """Return a dict copy of args with sensitive values masked."""
    data = vars(ns).copy()
    for k in list(data.keys()):
        if k in SENSITIVE_KEYS and data[k]:
            data[k] = "****"
    return data


def configure_logging(debug: bool, log_file: Path | None) -> None:
    level = logging.DEBUG if debug else logging.INFO
    handlers = []
    if log_file is not None:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=handlers,
    )

    if debug:
        http_client.HTTPConnection.debuglevel = 1  # type: ignore[attr-defined]
        for noisy in ("urllib3", "requests", "httpx"):
            logging.getLogger(noisy).setLevel(logging.DEBUG)
            logging.getLogger(noisy).propagate = True


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Import Labfolder projects into eLabFTW")
    p.add_argument("-u", "--username", required=True, help="Labfolder username")
    p.add_argument("-p", "--password", required=True, help="Labfolder password")
    p.add_argument("--url", default="https://eln.labfolder.com/api/v2",
                   help="Labfolder API URL")
    p.add_argument("-a", "--author", dest="authors", action="append",
                   help="Author first name to include (repeatable). Example: -a Emma -a James")
    p.add_argument("--entries-parquet", type=Path,
                   help="Path to a parquet file used to cache entries. "
                         "If --use-parquet is set, entries will be read from "
                         "here. Example: -p entries.parquet")
    p.add_argument("--use-parquet", action="store_true",
                   help="Skip fetching from Labfolder and read entries from --entries-parquet.")
    p.add_argument("--isa-ids", type=Path, required=False,
                   help="Path to a CSV file containing ISA-IDs CSV.")
    p.add_argument("--namelist", type=Path, required=False,
                   help="Path to a CSV file for matching labfolder  "
                        "with elab users.")
    p.add_argument("--only-projects-from-xhtml", action="store_true",
                   help=("Process ONLY those projects that exist in the local XHTML export cache. "
                         "Useful when you have exported a subset and want to migrate just that subset."))
    p.add_argument("--debug", action="store_true",
                   help="Enable verbose debug logging (incl. HTTP wire logs).")
    p.add_argument("--log-file", type=Path, default=None,
                   help="Write logs to this file instead of stderr.")
    return p


def main() -> None:
    args = build_parser().parse_args()
    configure_logging(args.debug, args.log_file)

    log = logging.getLogger("cli")
    log.debug("Parsed args (masked): %s", mask_sensitive(args))

    try:
        coord = Coordinator(
            username=args.username,
            password=args.password,
            url=args.url,
            authors=args.authors,
            entries_parquet=args.entries_parquet,
            use_parquet=args.use_parquet,
            isa_ids = args.isa_ids,
            namelist = args.namelist,
            restrict_to_xhtml=args.only_projects_from_xhtml,
        )
        log.debug("Coordinator initialized")
        coord.run()
        log.info("Done.")
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        sys.exit(130)
    except Exception:
        log.exception("Unhandled error during execution")
        sys.exit(1)


if __name__ == "__main__":

    main()
