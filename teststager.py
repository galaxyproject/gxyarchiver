import os
import random
import uuid
import click
from datetime import datetime

basearchivedir = os.path.join(os.getcwd(), "archive")
archivesource = basearchivedir + "/export"
archivedest = basearchivedir + "/bundled"


def generate_random_files(directory, number_of_files):
    if not os.path.exists(directory):
        os.makedirs(directory)

    for _ in range(number_of_files):
        file_size = random.randint(
            1 * 1024 * 1024, 4 * 1024 * 1024
        )  # File size between 1 MB and 4 MB
        history_id = uuid.uuid4().hex[:16]  # Generate a random history ID
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S.%f")
        file_name = os.path.join(directory, f"{timestamp}_{history_id}.rocrate.zip")

        with open(file_name, "wb") as file:
            file.write(os.urandom(file_size))

    print(f"{number_of_files} files have been generated in '{directory}'.")


@click.command()
@click.option("--stage", is_flag=True, help="If set, stage fake test files.")
@click.option("--bundle", is_flag=True, help="If set, bundle files and write manifest.")
def main(stage, bundle):
    if stage:
        # Make sure source has 20 random files 1-4MB.
        generate_random_files(archivesource, 20)

    # If we're running 'stage_only', don't actually do the deletion.
    if bundle:
        # Set env var
        TEST_DEFAULT_TAR_SIZE_GB = 0.02
        os.environ["DEFAULT_TAR_SIZE_GB"] = str(TEST_DEFAULT_TAR_SIZE_GB)

        # After, so env var is set.
        from gxyarchiver import check_folder_for_archiving, create_manifest_and_tar

        # Emulate running the check/bundle script while there remain enough files to tar.
        while check_folder_for_archiving(
            archivesource, TEST_DEFAULT_TAR_SIZE_GB, "**/*.rocrate.zip"
        ):
            create_manifest_and_tar(
                archivesource, basearchivedir, archivedest, "**/*.rocrate.zip"
            )


if __name__ == "__main__":
    main()
