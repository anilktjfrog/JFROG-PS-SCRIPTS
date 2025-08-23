#!/usr/bin/env python3

import subprocess
import json
import yaml
import re
from datetime import datetime, timedelta, UTC
from collections import defaultdict
import os
from tabulate import tabulate
import argparse
import logging
import pathlib
from datetime import datetime

# Define constants
DEFAULT_REPO_FILE = "repo_files.json"
DEFAULT_CONFIG_FILE = "jfrog_cleanup_config.yaml"
DEFAULT_LOG_LEVEL = "INFO"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def load_config(config_file):
    """
    Load YAML configuration from the given file path.
    Args:
        config_file (str): Path to the YAML config file.
    Returns:
        dict: Parsed configuration as a dictionary.
    """
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def load_repo_files(repo_file):
    """
    Load repository file metadata from a JSON file.
    Args:
        repo_file (str): Path to the JSON file.
    Returns:
        list or dict: Parsed JSON data (list of dicts or dict with 'results').
    """
    with open(repo_file, "r") as f:
        data = json.load(f)
        return data


def is_protected(folder, protected_paths):
    """
    Check if a folder path is protected (should not be deleted).
    Args:
        folder (str): Folder path to check.
        protected_paths (list): List of protected path prefixes.
    Returns:
        bool: True if folder is protected, False otherwise.
    """
    return any(folder.startswith(path) for path in protected_paths)


def match_build_folder(folder):
    """
    Determine if a folder matches known build folder patterns.
    Args:
        folder (str): Folder path to check.
    Returns:
        bool: True if folder matches a build pattern, False otherwise.
    """
    if re.search(r"_a$", folder):
        return True
    if re.search(r"_[1-9]{4}$", folder):
        return True
    if re.search(r"_[a-z1-9]{11}$", folder):
        return True
    return False


def print_file_table(logger, title, files):
    """
    Print a table of files eligible for deletion under a given title.
    Args:
        title (str): Title for the table.
        files (list): List of file dictionaries.
    """
    sorted_files = sorted(files, key=lambda f: f["created"])
    headers = [
        "S.No",
        "Full Path",
        "File Name",
        "Created",
        "Size (MB)",
    ]
    table = [
        [
            i + 1,
            os.path.join(f["path"], f["name"]),
            f["name"],
            datetime.strptime(f["created"], DATE_FORMAT).strftime("%Y-%m-%d %H:%M:%S"),
            round(f["size"] / (1024 * 1024), 2),
        ]
        for i, f in enumerate(sorted_files)
    ]
    total_files = len(files)
    total_size_mb = round(sum(f["size"] for f in files) / (1024 * 1024), 2)
    logger.info(f"\nFiles eligible for deletion under: {title}")
    logger.info("\n" + tabulate(table, headers=headers, tablefmt="heavy_grid"))
    logger.info(f"Total files to be deleted: {total_files}")
    logger.info(f"Total space to be freed: {total_size_mb} MB")


def process_cleanup_targets(
    logger=None,
    repo_files={},
    cleanup_target_paths=[],
    protected_paths=[],
    threshold_date=None,
    date_field=None,
    dry_run=False,
):
    """
    Process and print eligible files for custom cleanup target paths.
    Args:
        repo_files (list): List of file metadata dictionaries.
        cleanup_target_paths (list): List of target path prefixes.
        threshold_date (datetime): Date threshold for deletion eligibility.
        date_field (str): Which date field to use (created/modified/updated).
    """
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filespec_dir = pathlib.Path(f"fileSpec_{now_str}")
    filespec_dir.mkdir(parents=True, exist_ok=True)
    logger.info("-" * 120)
    logger.info(
        f"Delete files from paths: {cleanup_target_paths} which are older than {threshold_date}"
    )
    logger.info("-" * 120)
    for target_path in cleanup_target_paths:
        logger.info("=" * 80)
        logger.info(f"Processing target path: {target_path}")
        logger.info("=" * 80)
        if target_path in protected_paths:
            logger.info(f"Skipping protected path: {target_path}")
            continue
        eligible_files = []
        for entry in repo_files["results"]:
            if not isinstance(entry, dict):
                logger.info(f"Skipping invalid entry: {entry}")
                continue
            if entry.get("type") != "file":
                continue
            if entry["path"].startswith(target_path):
                date_value = entry.get(date_field, entry.get("created"))
                created = datetime.strptime(date_value, DATE_FORMAT).replace(tzinfo=UTC)
                if created < threshold_date:
                    eligible_files.append(entry)
        print_file_table(logger, target_path, eligible_files)

        # Write file spec for this target_path if there are eligible files
        if eligible_files:
            files = [
                {"pattern": os.path.join(f["repo"], f["path"], f["name"])}
                for f in eligible_files
            ]
            file_spec = {"files": files}
            spec_filename = (
                filespec_dir
                / f"filespec_{target_path.replace('/', '_')}_{now_str}.json"
            )
            with open(spec_filename, "w") as f:
                json.dump(file_spec, f, indent=2)
            logger.info(f"File spec written: {spec_filename}")
            # Call delete_folders_with_spec to delete files
            if not dry_run:
                delete_folders_with_spec(logger, str(spec_filename), dry_run=False)


def write_file_spec(logger, folders, file_spec_filename="folders_to_delete_spec.json"):
    """
    Write a JFrog CLI file spec JSON for folders to be deleted.
    Args:
        folders (list): List of folder info dictionaries.
        file_spec_filename (str): Output file spec filename.
    Returns:
        str or None: Path to the file spec JSON, or None if no folders.
    """
    if not folders:
        return None
    files = []
    for f in folders:
        # Expecting folder in the format: repo/path/to/folder
        # If folder does not contain repo, user should adjust logic as needed
        files.append({"pattern": f["folder"] + "/**"})
    file_spec = {"files": files}
    with open(file_spec_filename, "w") as f:
        json.dump(file_spec, f, indent=2)
    logger.info(f"File spec written: {file_spec_filename}")
    return file_spec_filename


# --- Execute JFrog CLI delete command using file spec ---
def delete_folders_with_spec(logger, file_spec_filename, dry_run=False):
    """
    Run the JFrog CLI delete command using the generated file spec.
    Args:
        file_spec_filename (str): Path to the file spec JSON.
        dry_run (bool): If True, perform a dry run only.
    """
    if not file_spec_filename:
        logger.info("No file spec to use for deletion.")
        return
    cmd = ["jf", "rt", "del", "--spec", file_spec_filename]
    if dry_run:
        cmd.append("--dry-run")
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        logger.info("JFrog CLI output:")
        logger.info(result.stdout)
        if result.stderr:
            logger.info("JFrog CLI errors:")
            logger.info(result.stderr)
    except Exception as e:
        logger.info(f"Error running JFrog CLI: {e}")


def get_build_folder(path):
    """
    Extract the build folder from a given path using a regex pattern.
    Args:
        path (str): File path.
    Returns:
        str: The build folder path, or the original path if not matched.
    """
    match = re.search(r"(.*?/)?(build_[^/]+_\d+_\d+)(/.*)?$", path)
    if match:
        return path[: path.find(match.group(2)) + len(match.group(2))]
    else:
        return path


def print_table(logger, title, rows):
    """
    Print a table of build folders with summary statistics and reasons.
    Args:
        title (str): Table title.
        rows (list): List of folder info dictionaries.
    """
    if not rows:
        logger.info(f"\n{title}: None")
        return
    # Sort rows by size_MB descending
    sorted_rows = sorted(rows, key=lambda r: r["size_MB"], reverse=True)
    logger.info(f"\n{title}:")
    # Check if all reasons are the same
    reasons = set(r["reason"] for r in sorted_rows)
    show_reason = False
    reason_text = None
    if len(reasons) == 1:
        reason_text = reasons.pop()
        logger.info(f"**{reason_text}**\n")
    else:
        show_reason = True
    headers = [
        "S.No",
        "Folder",
        "File Count",
        "Oldest",
        "Newest",
        "Size (MB)",
        "Oldest File (days diff)",
        "Newest File (days diff)",
    ]
    if show_reason:
        headers.append("Reason")
    table = []
    for i, r in enumerate(sorted_rows):
        row = [
            i + 1,
            r["folder"],
            r["file_count"],
            r["oldest"],
            r["newest"],
            r["size_MB"],
            r["oldest_path"],
            r["newest_path"],
        ]
        if show_reason:
            row.append(r["reason"])
        table.append(row)
    logger.info("\n" + tabulate(table, headers=headers, tablefmt="heavy_grid"))


def main():
    parser = argparse.ArgumentParser(description="JFrog Cleanup Script")
    parser.add_argument(
        "--json",
        dest="repo_file",
        default=DEFAULT_REPO_FILE,
        help="Path to repo_files.json",
    )
    parser.add_argument(
        "--config",
        dest="config_file",
        default=DEFAULT_CONFIG_FILE,
        help="Path to config YAML file",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Perform a dry run of the delete operation (no data will be deleted). Default: True",
    )
    parser.add_argument(
        "--date-field",
        dest="date_field",
        choices=["created", "modified", "updated"],
        default="created",
        help="Which date field to use for age calculation (created/modified/updated). Default: created",
    )
    args = parser.parse_args()

    config = load_config(args.config_file)
    protected_paths = config.get("protected_paths", [])
    time_threshold_days = config.get("time_threshold_days", 730)
    log_level = config.get("log_level", DEFAULT_LOG_LEVEL).upper()
    threshold_date = datetime.now(UTC) - timedelta(days=time_threshold_days)

    # Set up logger
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger("jfrog_cleanup")

    logger.info("Starting JFrog Cleanup Script")
    logger.info(f"Threshold (days): {time_threshold_days}")
    logger.info(f"Protected paths: {protected_paths}")
    logger.info(
        f"Threshold date (UTC): {threshold_date.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )
    logger.info(
        f"Current date (UTC): {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )

    repo_files = load_repo_files(args.repo_file)
    date_field = args.date_field

    # Process custom cleanup target paths if present
    cleanup_target_paths = config.get("cleanup_target_paths", [])
    if cleanup_target_paths:
        process_cleanup_targets(
            logger=logger,
            repo_files=repo_files,
            cleanup_target_paths=cleanup_target_paths,
            protected_paths=protected_paths,
            threshold_date=threshold_date,
            date_field=date_field,
            dry_run=args.dry_run,
        )
    logger.info("\n" + "=" * 120 + "\n")

    # Group files by any folder path that ends with the build folder name, including all subfolders, regardless of depth
    folders = defaultdict(list)

    for entry in repo_files["results"]:
        if entry.get("type") != "file":
            continue
        build_folder = get_build_folder(entry["path"])
        entry["full_file_name"] = os.path.join(entry["path"], entry["name"])
        folders[build_folder].append(entry)

    to_delete = []
    not_selected = []
    logger.info("-" * 120)
    logger.info(f"Deleting folders which match the build folder pattern...")
    logger.info(f"Total build folders found: {len(folders)}")
    logger.info(f"Processing build folders for deletion criteria...")
    for folder, files in folders.items():
        if is_protected(folder + "/", protected_paths):
            continue
        if not match_build_folder(folder):
            continue
        # Get the repo name from the first file in the folder
        repo_name = files[0].get("repo", "")
        folder = os.path.join(repo_name, folder)
        logger.info("=" * 80)
        logger.info(f"Processing folder: {folder}")
        logger.info("=" * 80)
        oldest = None
        newest = None
        oldest_file = None
        newest_file = None
        total_size = 0
        all_older = True
        for f in files:
            date_value = f.get(date_field, f.get("created"))
            created = datetime.strptime(date_value, DATE_FORMAT).replace(tzinfo=UTC)
            if oldest is None or created < oldest:
                oldest = created
                oldest_file = f
            if newest is None or created > newest:
                newest = created
                newest_file = f
            total_size += f["size"]
            if created > threshold_date:
                all_older = False
        # Calculate days difference for oldest and newest
        oldest_days = (threshold_date - oldest).days
        newest_days = (threshold_date - newest).days
        oldest_path = f"({oldest_days}) {oldest_file['name']}"
        newest_path = f"({newest_days}) {newest_file['name']}"
        folder_info = {
            "folder": folder,
            "file_count": len(files),
            "oldest": oldest.strftime("%Y-%m-%d %H:%M:%S"),
            "newest": newest.strftime("%Y-%m-%d %H:%M:%S"),
            "size_MB": round(total_size / (1024 * 1024), 2),
            "oldest_path": oldest_path,
            "newest_path": newest_path,
        }
        if all_older:
            folder_info["reason"] = f"All files older than {time_threshold_days} days."
            to_delete.append(folder_info)
        else:
            folder_info["reason"] = (
                f"Some files are newer than {time_threshold_days} days."
            )
            not_selected.append(folder_info)

    print_table(logger, "Folders to be deleted", to_delete)
    print_table(logger, "Folders NOT selected for deletion", not_selected)
    # Print summary statistics for build folders to be deleted
    if to_delete:
        total_folders = len(to_delete)
        total_files = sum(r["file_count"] for r in to_delete)
        total_size = round(sum(r["size_MB"] for r in to_delete), 2)
        logger.info(f"\nSummary of deletion candidates:")
        logger.info(f"  Build folders to be deleted: {total_folders}")
        logger.info(f"  Total files to be deleted: {total_files}")
        logger.info(f"  Total space to be freed: {total_size} MB")

        # Write file spec for folders to be deleted
        file_spec = write_file_spec(logger, to_delete)
        # If not dry-run
        if not args.dry_run and file_spec:
            delete_folders_with_spec(
                logger,
                file_spec,
                dry_run=False,
            )


if __name__ == "__main__":
    main()
