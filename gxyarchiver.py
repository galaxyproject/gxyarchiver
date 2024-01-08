import json
import os
import tarfile
import time
import uuid
from pathlib import Path

import click
import requests

# API Access/Key
GALAXY_API_URL = os.getenv("GALAXY_API_URL", "http://localhost:8080/api")
GALAXY_API_KEY = os.getenv("GALAXY_API_KEY", "your_api_key")

# Default size in GB for tar files
DEFAULT_TAR_SIZE_GB = float(os.getenv("DEFAULT_TAR_SIZE_GB", 300))

# Default interval in seconds for checking task status
DEFAULT_TASK_CHECK_INTERVAL_SECONDS = 5

DEFAULT_FILE_PATTERN = "**/*"

# Destination for archived files, maps to a destination in Galaxy.
FILESOURCE_DESTINATION = "gxy-archiver"

# Delay between separate history iterations
REQUEST_DELAY = 1

if GALAXY_API_KEY == "your_api_key":
    api_key_option = click.option("--api-key", prompt=True, hide_input=True, help="API key for authentication.")
else:
    api_key_option = click.option("--api-key", default=GALAXY_API_KEY, help="API key for authentication.")

api_url_option = click.option("--api-url", default=GALAXY_API_URL, help="URL of the Galaxy API.")


def get_up_to_date_export_record(api_url, headers, history_id):
    # Get the ID of the 'up-to-date' export record, or empty.
    export_check_url = f"{api_url}/histories/{history_id}/exports"
    export_check_response = requests.get(
        export_check_url,
        headers={**headers, "Accept": "application/vnd.galaxy.task.export+json"},
    )
    export_check_response.raise_for_status()
    exports = export_check_response.json()

    up_to_date_export_id = None
    for export in exports:
        if export.get("up_to_date"):
            up_to_date_export_id = export.get("id")
            break

    return up_to_date_export_id


def get_history_summary(api_url, headers, history_id):
    return requests.get(f"{api_url}/histories/{history_id}?view=summary", headers=headers).json()


def archive_history(api_url, api_key, history_id):
    """
    Invoke the Archive API endpoint to archive a specific history.

    :param api_url: URL of the Galaxy API.
    :param api_key: API key for authentication.
    :param history_id: ID of the history to be archived.
    """

    # set up basic request headers; we tweak this later for specific requests
    request_headers = {"X-API-KEY": api_key}

    click.echo(f"Processing history: {history_id} ")
    history_summary = get_history_summary(api_url, request_headers, history_id)

    if history_summary["archived"] or history_summary["purged"]:
        click.echo(f"\tHistory {history_id} already archived [{history_summary['archived']}] or purged [{history_summary['purged']}], skipping.")
        return True

    # check to see if there already exists an up to date export record. If there
    # is, we don't know what's going on here and we skip it for some other pass.
    # We could explicitly check for the destination being
    # gxfiles://gxy-archiver, then validate we have a record in the manifest and
    # then purge, down the road.

    up_to_date_export_id = get_up_to_date_export_record(api_url, request_headers, history_id)

    if up_to_date_export_id is None:
        # Generate the export record with a request like: http://localhost:8081/api/histories/7b3e34c9cfd75e90/write_store
        # example_payload = {
        #     "target_uri": "gxfiles://historyarchives/2023-11-30T04-03-24.385902_HISTORYID.rocrate.zip",
        #     "model_store_format": "rocrate.zip",
        #     "include_files": true,
        #     "include_deleted": false,
        #     "include_hidden": false,
        # }
        # example_response = {"id": "1d941e24-b1e3-4e1f-bfe8-1973d33e503a", "ignored": false, "name": null, "queue": null}
        # Create the export record
        export_record_url = f"{api_url}/histories/{history_id}/write_store"
        create_export_payload = {
            "target_uri": f"gxfiles://{FILESOURCE_DESTINATION}/{time.strftime('%Y-%m-%dT%H-%M-%S')}_{history_id}.rocrate.zip",
            "model_store_format": "rocrate.zip",
            "include_files": True,
            "include_deleted": True,
            "include_hidden": True,
        }
        export_response = requests.post(export_record_url, json=create_export_payload, headers=request_headers)
        export_response.raise_for_status()  # This will raise an error if the request fails
        export_data = export_response.json()
        task_id = export_data.get("id")
        click.echo(f"\tNew export record creation with task id: {task_id}")

        # Wait for task to finish, checking the returned task repeatedly.
        # http://localhost:8081/api/tasks/1d941e24-b1e3-4e1f-bfe8-1973d33e503a/state
        # Which responds with "SUCCESS" when done.

        # Monitor task status
        task_status_url = f"{api_url}/tasks/{task_id}/state"
        archive_task_complete = False

        task_check_count = 0
        while not archive_task_complete:
            click.echo(
                f"\rMonitoring archive status, attempt: {task_check_count}, total time: { task_check_count * DEFAULT_TASK_CHECK_INTERVAL_SECONDS} seconds.",
                nl=False,
            )
            task_status_response = requests.get(task_status_url, headers=request_headers)
            task_status_response.raise_for_status()
            task_status = task_status_response.text
            click.echo("\r", nl=False)
            # yes, this is a literal string response with quoted "SUCCESS" or "PENDING"
            if task_status == '"SUCCESS"':
                archive_task_complete = True
                click.echo("Archive task complete")
            elif task_status == '"FAILURE"':
                archive_task_complete = True
                click.echo(f"Archive task failed -- investigate task {task_id} for history {history_id}")
                return False
            else:
                # Wait for a few seconds before checking again
                task_check_count += 1
                time.sleep(DEFAULT_TASK_CHECK_INTERVAL_SECONDS)

        # Check to ensure an up to date export record for the given history
        # example_request = "http://localhost:8081/api/histories/66cdc1effa228605/exports"
        # example_response = [
        #     {
        #         "id": "417e33144b294c21",
        #         "ready": true,
        #         "preparing": false,
        #         "up_to_date": true,
        #         "task_uuid": "d116c9d9-fb89-4ecf-8936-329a92b4102b",
        #         "create_time": "2023-11-30T16:05:01.607873",
        #         "export_metadata": {
        #             "request_data": {
        #                 "object_id": "66cdc1effa228605",
        #                 "object_type": "history",
        #                 "user_id": "f2db41e1fa331b3e",
        #                 "payload": {
        #                     "model_store_format": "rocrate.zip",
        #                     "include_files": true,
        #                     "include_deleted": false,
        #                     "include_hidden": false,
        #                     "target_uri": "gxfiles://historyarchives/2023-11-30T11-05-01_66cdc1effa228605_.rocrate.zip",
        #                 },
        #             },
        #             "result_data": {"success": true, "error": null},
        #         },
        #     }
        # ]

        up_to_date_export_id = get_up_to_date_export_record(api_url, request_headers, history_id)

        if up_to_date_export_id is None:
            raise Exception(f"No up-to-date export record found for history ID: {history_id}")

        # Lastly, once archive is exported and verified, make a request to purge.
        # http://localhost:8081/api/histories/7b3e34c9cfd75e90/archive
        # example_payload = {"archive_export_id":"03501d7626bd192f","purge_history":true}
        # example response
        # resp = {
        #     "export_record_data": {
        #         "model_store_format": "rocrate.zip",
        #         "include_files": true,
        #         "include_deleted": false,
        #         "include_hidden": false,
        #         "target_uri": "gxfiles://historyarchives/2023-11-30T05-24-05.091258_testfinal.rocrate.zip",
        #     },
        #     "model_class": "History",
        #     "id": "c903e9d706700fc8",
        #     "name": "ARCHIVE TEST",
        #     "deleted": true,
        #     "purged": true,
        #     "archived": true,
        #     "url": "/api/histories/c903e9d706700fc8",
        #     "published": false,
        #     "count": 3,
        #     "annotation": null,
        #     "tags": [],
        #     "update_time": "2023-11-30T05:25:01.008069",
        #     "preferred_object_store_id": null,
        # }

        # With export record in hand, purge history

        purge_url = f"{api_url}/histories/{history_id}/archive"
        purge_payload = {
            "archive_export_id": up_to_date_export_id,
            "purge_history": True,
        }
        purge_response = requests.post(purge_url, json=purge_payload, headers=request_headers)
        purge_response.raise_for_status()
    else:
        # Long term, reconcile this with verifying this same record exists in
        # storage manifests and proceed with purge.  Right now, we don't want to
        # touch this.
        click.echo(f"Latest export record already exists for history { history_id }, skipping.")


def find_oldest_files(directory, target_size_gb, file_pattern=DEFAULT_FILE_PATTERN):
    """
    Find the oldest files matching a pattern until their total size reaches a target size.

    Parameters:
    directory (str): The directory to traverse.
    target_size_gb (int): The target total size of the oldest files in gigabytes.
    file_pattern (str): The glob pattern to match files. Defaults to "**/*" which matches all files.

    Returns:
    list: A list of file paths
    """
    files_data = []
    total_size = 0

    # Traverse directory and gather file details
    for file in Path(directory).glob(file_pattern):
        if file.is_file():
            file_size = file.stat().st_size
            file_mod_time = file.stat().st_mtime
            files_data.append((str(file), file_size, file_mod_time))

    # Sort files by modification time (oldest first)
    files_data.sort(key=lambda x: x[2])

    # Select oldest files until reaching target size
    selected_files = []
    for file_data in files_data:
        if total_size + file_data[1] > target_size_gb * 1024**3:
            break
        selected_files.append(file_data[0])
        total_size += file_data[1]

    return selected_files


def create_manifest_and_tar(
    directory,
    manifest_path,
    tar_path,
    file_pattern=DEFAULT_FILE_PATTERN,
    required_size_gb=DEFAULT_TAR_SIZE_GB,
    remove_files_after_archive=True,
):
    """
    Create a manifest of the oldest files in a directory and archive them into a tar file.

    Parameters:
    directory (str): The directory to traverse.
    manifest_path (str): The path to write the manifest file.
    tar_path (str): The path to write the tar file.

    Returns:
    None
    """
    oldest_files = find_oldest_files(directory, required_size_gb, file_pattern)

    # Write manifest as JSON.  This could be sequential, but we have timestamps and catalogs.
    # generate a new manifest, named by datetime, and write it to manifest_path
    unique_id = uuid.uuid4()
    manifest_filename = f"{unique_id}_manifest.json"
    tar_filename = f"{unique_id}_gxyarchive.tar"

    manifest_contents = []
    # Parsing filenames and history IDs from paths and constructing the list of
    # dictionaries.  We include the archive uuid as well here, so these can be
    # merged together downstream without additional lookups.

    for path in oldest_files:
        filename = os.path.basename(path)
        history_id = filename.split("_")[-1].split(".")[0]
        manifest_contents.append(
            {
                "filename": filename,
                "history_id": history_id,
                "archive_uuid": str(unique_id),
            }
        )

    # Create manifest
    manifest_file = os.path.join(manifest_path, manifest_filename)
    with open(manifest_file, "w") as f:
        json.dump(manifest_contents, f, indent=4)

    # Create tar archive
    tar_file = os.path.join(tar_path, tar_filename)
    tar_file_part = os.path.join(tar_path, f"_{tar_filename}.part")
    with tarfile.open(tar_file_part, "w:gz") as tar:
        for file in oldest_files:
            tar.add(file)
        tar.add(manifest_file)
    os.rename(tar_file_part, tar_file)

    if remove_files_after_archive:
        # On successfully creating the tar and writing the manifest, remove the
        # files from the source directory.
        for file in oldest_files:
            os.remove(file)


def check_folder_for_archiving(folder_path, required_size_gb, file_pattern=DEFAULT_FILE_PATTERN):
    """
    Check if a folder has sufficient data for archiving.

    :param folder_path: Path to the folder to be checked.
    :param required_size_gb: Required size in GB for files to be ready for archiving.
    :return: True if folder has sufficient data, False otherwise.
    """
    total_size = 0
    for file in Path(folder_path).glob(file_pattern):
        if file.is_file():
            total_size += file.stat().st_size

    # Convert total size from bytes to gigabytes
    total_size_gb = total_size / (1024**3)

    click.echo(f"{total_size_gb=}, {required_size_gb=}")
    return total_size_gb >= required_size_gb


@click.group()
def cli():
    """Galaxy History Archiving CLI."""
    pass


@click.command()
@api_key_option
@api_url_option
@click.option("--inactive-years", default=3, help="Number of years to consider for inactivity.")
def identify(api_url, api_key, inactive_years):
    """
    Identify old data using Galaxy's API.
    """
    # old_histories = identify_old_histories(api_url, api_key, inactive_years)
    click.echo("Not implemented -- use gxadmin or other tools to generate a list for consumption.")


@click.command()
@api_key_option
@api_url_option
@click.option("--history-id", help="ID of the history to be archived.")
@click.option(
    "--history-id-file",
    type=click.Path(exists=True),
    help="Path to a file with a list of history ids to be archived.",
)
def archive(api_url, api_key, history_id, history_id_file):
    """
    Archive a specific history or list of histories in Galaxy.
    """
    if history_id_file:
        with open(history_id_file) as file:
            history_ids = file.readlines()
            with click.progressbar(history_ids) as bar:
                for line in bar:
                    history_id = line.strip()
                    if history_id != "":
                        archive_history(api_url, api_key, history_id)
                        time.sleep(REQUEST_DELAY)
    else:
        if history_id is None:
            raise Exception("Either --history-id or --history-id-file must be provided.")
        archive_history(api_url, api_key, history_id)


@click.command()
@click.option(
    "--folder-path",
    prompt=True,
    type=click.Path(exists=True),
    help="Base path to archive directory.  This should have 'export' and 'bundled' dirs, and will use the root for manifests.",
)
@click.option(
    "--required-size-gb",
    default=DEFAULT_TAR_SIZE_GB,
    help="Required size in GB for files to be ready for archiving.",
)
@click.option(
    "--continual",
    default=False,
    help="If continual is set, will keep bundling until there are not enough files left."
)
def bundle(folder_path, required_size_gb, continual):
    """
    Check if a folder has sufficient data for archiving.
    """

    basearchivedir = os.path.abspath(folder_path)
    archivesource = basearchivedir + "/export"
    archivedest = basearchivedir + "/bundled"

    if continual:
        # Emulate running the check/bundle script while there remain enough files to tar.
        while check_folder_for_archiving(
            archivesource, required_size_gb, "**/*.rocrate.zip"
        ):
            create_manifest_and_tar(
                archivesource, basearchivedir, archivedest, "**/*.rocrate.zip", required_size_gb
            )
    else:
        # Just run once
        if check_folder_for_archiving(
            archivesource, required_size_gb, "**/*.rocrate.zip"
        ):
            create_manifest_and_tar(
                archivesource, basearchivedir, archivedest, "**/*.rocrate.zip", required_size_gb
            )


# Adding commands to the CLI group
cli.add_command(identify)
cli.add_command(archive)
cli.add_command(bundle)

if __name__ == "__main__":
    cli()
