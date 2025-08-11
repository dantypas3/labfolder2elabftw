<h1 align="center">Migration tool from Labfoldek to eLabFTW â€“ SFB 1638 Tools</h1>
<p align="center">
  <img src="https://github.com/user-attachments/assets/e8ce314e-2f66-47af-9d08-b94324646984" alt="SFB1638 Logo" width="200">
</p>

A command-line utility for migrating Labfolder projects into an eLabFTW instance. It fetches entries via the Labfolder API, transforms them into eLabFTW experiments, uploads attachments, and stores cached copies of fetched entries.

## Features

- Command-line interface for configuring Labfolder credentials, API URL, author filtering, caching, and logging.
- Orchestrated workflow that fetches Labfolder entries, caches them as Parquet or JSON, groups them by project, and imports them into eLabFTW.
- Supports element types such as tables, well plates, text blocks, files, images, and generic data attachments.
- Converts tables and well plates to Excel, uploads files and images, and embeds text directly into experiments.
- Interacts with eLabFTWâ€™s experiments endpoint to create experiments, patch metadata/extra fields, and upload attachments.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/dantypas3/labfolder2elabftw.git
   cd labfolder2elabftw
   ```
2. Install dependencies (Python 3.10+ recommended):
   ```bash
   pip install requests pandas numpy xlsxwriter elapi
   ```

## Usage

Run the tool as a module:

```bash
python -m src --username USER --password PASS  \
  --entries-parquet cache.parquet
```

Common options:

- `--author NAME` to process entries whose authorâ€™s first name matches `NAME` (repeatable).
- `--entries-parquet PATH` to cache entries at `PATH`; `--use-parquet` to read from the cache instead of contacting Labfolder.
- `--debug` for verbose logging; `--log-file FILE` to write logs to a file.
- `--url` to override the default Labfolder API URL.

The script groups entries by project, creates corresponding eLabFTW experiments, uploads associated files, and fills metadata such as project owner and Labfolder project ID.

## How it works

The workflow is managed by a `Coordinator` that ties together three components:

1. **LabFolderFetcher** â€“ Communicates with the Labfolder API, handles token refresh, and downloads elements such as tables, well plates, text, files, images, and generic data.
2. **Transformer** â€“ Converts fetched entries into HTML blocks, exports tables/well plates to Excel, uploads attachments, and adds extra fields to experiments.
3. **Importer** â€“ Uses the eLabFTW API to create experiments, patch body/metadata, and upload attachments.

## License

This project is released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.
