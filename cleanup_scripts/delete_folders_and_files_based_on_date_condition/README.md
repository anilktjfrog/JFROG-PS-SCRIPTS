# JFrog Artifactory Cleanup Script

This script helps you identify and delete old build folders/files from a JFrog Artifactory repository, based on configurable rules. It generates a JFrog CLI file spec for deletion and can optionally execute the deletion using the JFrog CLI.

## Features

- Configurable protected paths and time threshold (in days)
- Custom cleanup target paths
- Tabular and summary output
- Generates a JFrog CLI file spec for deletion
- Supports dry-run mode (default: enabled)
- Optionally executes deletion using the JFrog CLI
- Allows selection of date fields (`created`, `modified`, `updated`) for threshold comparison

## New Features

- **Chunk Size Parameter**: Added a `chunk_size` parameter in the YAML configuration to split deletion tasks into smaller chunks.
- **Folder-Based Spec Files**: Spec files are now written to a timestamped folder for better organization.

## Requirements

- Python 3.8+
- JFrog CLI installed and configured (`jf` command in PATH)
- Required Python packages: `pyyaml`, `tabulate`

## Setup: Virtual Environment & Installing Requirements

It is recommended to use a Python virtual environment for isolation:

```sh
# Create and activate a virtual environment (macOS/Linux)
python3 -m venv .venv
source .venv/bin/activate

# Or on Windows:
# python -m venv .venv
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

```sh
python3 jfrog_cleanup_script.py [--config <config.yaml>] [--json <repo_files.json>] [--dry-run] [--date-field <created|modified|updated>] [--repo_name <repo_name>]
```

- `--config`: Path to the YAML config file (default: `jfrog_cleanup_config.yaml`)
- `--dry-run`: Perform a dry run (no data will be deleted). This is enabled by default. Use `--dry-run=false` to disable.
- `--date-field`: Specify which date field to use for threshold comparison (default: `created`)
- `--repo_name`: Specify the name of the repository to fetch all files from (uses JFrog CLI).
- `--json`: Path to the Artifactory repo files JSON (default: `repo_files.json`)

## Example

```sh
python3 jfrog_cleanup_script.py --config jfrog_cleanup_config.yaml --repo_name <local_repo_name> --date-field created
```

## Configuration File Example (`jfrog_cleanup_config.yaml`)

```yaml
protected_paths:
  - build_tools/
  - builds_ns/builds_xs/
  - builds_ns/ns1/
time_threshold_days: 300
cleanup_target_paths:
  - builds_ns/builds_zion/gcov/
chunk_size: 100
```

## Output

- Tabular summary of folders/files eligible for deletion
- File spec JSON files are now written to a timestamped folder (e.g., `spec_files_20250826_123456/`) for better organization.
- (Optional) JFrog CLI deletion execution for each spec file.

## Deleting Folders

The script generates a file spec and, if not in dry-run mode, runs:

```sh
jf rt del --spec folders_to_delete_spec.json
```

## Safety

- By default, the script runs in dry-run mode. To actually delete, run with `--dry-run` set to `False` in the code or modify the script to support `--no-dry-run`.
