# python
import argparse
from pathlib import Path
from .coordinator import Coordinator

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Import Labfolder projects into eLabFTW")
    p.add_argument("-u", "--username", required=False, help="Labfolder username")
    p.add_argument("-p", "--password", required=False, help="Labfolder password")
    p.add_argument("--url", default="https://labfolder.labforward.app/api/v2",
                   help="Labfolder API URL")
    p.add_argument("-a", "--author", dest="authors", action="append",
                   help="Author first name to include (repeatable). Example: -a Alexia -a Helena")

    # Caching options
    p.add_argument("--entries-parquet", type=Path,
                   help="Path to a parquet file used to cache entries. "
                        "If --use-parquet is set, entries will be read from here.")
    p.add_argument("--use-parquet", action="store_true",
                   help="Skip fetching from Labfolder and read entries from --entries-parquet.")

    return p

def main() -> None:
    args = build_parser().parse_args()
    coord = Coordinator(username=args.username or "",
                        password=args.password or "",
                        url=args.url,
                        authors=args.authors,
                        entries_parquet=args.entries_parquet,
                        use_parquet=args.use_parquet)
    coord.run()

if __name__ == "__main__":
    main()