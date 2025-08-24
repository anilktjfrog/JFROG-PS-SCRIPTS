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

The `requirements.txt` file should contain:

```
pyyaml
tabulate
```

## Usage

```sh
python3 jfrog_cleanup_script.py [--config <config.yaml>] [--json <repo_files.json>] [--dry-run] [--date-field <created|modified|updated>]
```

- `--config`: Path to the YAML config file (default: `jfrog_cleanup_config.yaml`)
- `--json`: Path to the Artifactory repo files JSON (default: `repo_files.json`)
- `--dry-run`: Perform a dry run (no data will be deleted). This is enabled by default.
- `--date-field`: Specify which date field to use for threshold comparison (default: `created`)

## Example

```sh
python3 jfrog_cleanup_script.py --config jfrog_cleanup_config.yaml --json repo_files.json --date-field modified
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
```

## Output

- Tabular summary of folders/files eligible for deletion
- File spec JSON (`folders_to_delete_spec.json`) for JFrog CLI
- (Optional) JFrog CLI deletion execution

## Deleting Folders

The script generates a file spec and, if not in dry-run mode, runs:

```sh
jf rt del --spec folders_to_delete_spec.json
```

## Safety

- By default, the script runs in dry-run mode. To actually delete, run with `--dry-run` set to `False` in the code or modify the script to support `--no-dry-run`.
