import argparse
import sys
import logging

from .coordinator import Coordinator

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(prog="labfolder2elabftw",
        description="Migrate Labfolder entries into eLabFTW.")
    parser.add_argument("-u","--username", required=True, help="Labfolder user")
    parser.add_argument("-p","--password", required=True, help="Labfolder password")
    parser.add_argument("--url", default="https://labfolder.labforward.app/api/v2",
                        help="Base Labfolder API URL")
    args = parser.parse_args()
    try:
        coord = Coordinator(args.username, args.password, args.url)
        coord.run()
    except Exception as e:
        logger.exception("Fatal error in migration")
        sys.exit(1)
