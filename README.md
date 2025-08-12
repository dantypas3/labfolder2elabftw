<h1 align="center">Migration tool from Labfolder to eLabFTW — SFB 1638 Tools </h1>
<p align="center">
  <img src="https://github.com/user-attachments/assets/e8ce314e-2f66-47af-9d08-b94324646984" alt="SFB1638 Logo" width="200">
</p>

A command-line utility for migrating Labfolder projects into an eLabFTW instance. It fetches entries via the Labfolder
API, transforms them into eLabFTW experiments, uploads attachments, and stores cached copies of fetched entries.

Labfolder2eLabFTW was developed as part of the INF Project of the **CRC 1638** at the [Heidelberg University Biochemistry Center (BZH)](https://bzh.db-engine.de/).


## Features

- Command-line interface for configuring Labfolder credentials, API URL, author filtering, caching, and logging.
- Orchestrated workflow that fetches Labfolder entries, caches them as Parquet or JSON, groups them by project, and imports them into eLabFTW.
- Supports element types such as tables, well plates, text blocks, files, images, and generic data attachments.
- Converts tables and well plates to Excel, uploads files and images, and embeds text directly into experiments.
- Interacts with eLabFTW’s experiments endpoint to create experiments, patch metadata/extra fields, and upload attachments.

## Installation
    
### Option A: Install package (recommended)
```bash
   pip install git+https://github.com/dantypas3/labfolder2elab.git
   ```

### Option B: From source (development)

   ```bash
   git clone https://github.com/dantypas3/labfolder2elabftw.git
   cd labfolder2elabftw
   conda env create -f environment.yml
   ```

## Usage
This approach avoids creating or downloading any intermediate ELN export files.
Instead, it fetches data directly from the Labfolder API, transforms it, and imports it int
o eLabFTW in one seamless pipeline.
After installation, run the migration tool from the command line:

```bash
labfolder2elab \
  --username USER \
  --password PASS \
  --entries-parquet cache.parquet
```

Common options:

- `--author NAME` — process entries whose author’s first name matches NAME (repeatable).
    ```bash
    labfolder2elab --username USER --password PASS -a Emma -a James
    ```

`--entries-parquet` PATH — cache entries at PATH.

`--use-parquet` — read from the cache instead of contacting Labfolder.

`--isa-ids` FILE.csv — CSV file containing ISA-IDs.

`--namelist` FILE.csv — CSV file mapping Labfolder users to eLab users.

`--debug` — verbose logging including HTTP wire logs.

`--log-file` FILE — write logs to a file instead of stderr.

`--url` URL — override the default Labfolder API URL (https://labfolder.labforward.app/api/v2).

Example with multiple options:
```bash
labfolder2elab \
  --username alice \
  --password secret \
  --author Emma \
  --author James \
  --isa-ids isa-ids.csv \
  --namelist namelist.csv \
  --entries-parquet entries.parquet \
  --debug \
  --log-file migration.log
```

The script groups entries by project, creates corresponding eLabFTW experiments, uploads associated files, and fills metadata such as project owner and Labfolder project ID.

## How it works

The workflow is managed by a `Coordinator` that ties together three components:

1. **LabFolderFetcher** “ Communicates with the Labfolder API, handles token refresh, and downloads elements such as tables, well plates, text, files, images, and generic data.
2. **Transformer** “ Converts fetched entries into HTML blocks, exports tables/well plates to Excel, uploads attachments, and adds extra fields to experiments.
3. **Importer** “ Uses the eLabFTW API to create experiments, patch body/metadata, and upload attachments.

## License

This project is released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.
