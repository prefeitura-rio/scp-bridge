from pathlib import Path
from time import sleep
from typing import Dict

import pendulum
from google.cloud.storage import Client
from loguru import logger
from pydantic import BaseModel
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

# Configuration
WATCH_DIR = "/scp-bridge"
GCS_BUCKET = "rj-escritorio-scp"
STABLE_TIME = 10  # Time in seconds to wait for file to stabilize
TIMEZONE = "America/Sao_Paulo"

# GCS client
storage_client = Client()
bucket = storage_client.bucket(GCS_BUCKET)


class FileStatus(BaseModel):
    size: int
    timestamp: float


class FileHandler(FileSystemEventHandler):
    def __init__(self) -> None:
        super().__init__()
        self._file_status: Dict[Path, FileStatus] = {}

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.check_and_upload(event.src_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self.check_and_upload(event.src_path)

    def check_all_tracked_files(self) -> None:
        for file_path in list(self._file_status.keys()):
            self.check_and_upload(file_path)

    def check_and_upload(self, file_path: str) -> None:
        logger.info(f"Checking file {file_path}")
        try:
            if self.is_file_stable(file_path):
                self.upload_to_gcs(file_path)
            else:
                logger.info(f"  * File {file_path} is not stable yet. Skipping.")
        except FileNotFoundError:
            logger.warning(f"  * File {file_path} was not found. It may have been deleted or moved.")
            self._file_status.pop(Path(file_path), None)

    def is_file_stable(self, file_path: str | Path) -> bool:
        """
        Check if the file is stable by comparing size and timestamp.
        """
        file_path = Path(file_path)
        try:
            file_size = file_path.stat().st_size
            now = pendulum.now(tz=TIMEZONE)
            if file_path not in self._file_status:
                self._file_status[file_path] = FileStatus(
                    size=file_size, timestamp=now.timestamp()
                )
            else:
                last_status = self._file_status[file_path]
                if last_status.size == file_size:
                    last_status_timestamp = pendulum.from_timestamp(
                        last_status.timestamp, tz=TIMEZONE
                    )
                    diff = now.diff(last_status_timestamp).in_seconds()
                    if diff >= STABLE_TIME:
                        return True
                else:
                    self._file_status[file_path] = FileStatus(
                        size=file_size, timestamp=now.timestamp()
                    )
            return False
        except FileNotFoundError:
            logger.warning(f"  * File {file_path} was not found during stability check.")
            return False

    def upload_to_gcs(self, file_path: str | Path) -> None:
        """
        Upload file to Google Cloud Storage.
        """
        file_path = Path(file_path)
        try:
            logger.info(f"Uploading file {file_path} to GCS.")
            # Blob must have the same relative path as the file
            blob = bucket.blob(str(file_path.relative_to(WATCH_DIR)))
            blob.upload_from_filename(str(file_path.absolute()))
            file_path.unlink()
            self._file_status.pop(file_path)
            logger.info(
                f"  * File {file_path} was successfully uploaded to GCS and deleted."
            )
        except Exception as e:
            logger.error(f"  * Error uploading file {file_path} to GCS: {e}")


def scan_existing_files(handler: FileHandler, directory: str) -> None:
    """Scan the directory for existing files and process them."""
    for path in Path(directory).rglob('*'):
        if path.is_file():
            logger.info(f"Processing existing file: {path}")
            handler.check_and_upload(str(path))


if __name__ == "__main__":
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCH_DIR, recursive=True)
    
    # Scan for existing files before starting the observer
    scan_existing_files(event_handler, WATCH_DIR)
    
    observer.start()
    logger.info(f"Watching directory {WATCH_DIR} for changes")
    try:
        while True:
            sleep(1)
            event_handler.check_all_tracked_files()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
