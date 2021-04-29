"""General helper functions, helper classes, and decorators."""

import time
import json
from uuid import uuid4
from pathlib import Path, PurePath
from typing import Any, Dict, Union, Optional

import pendulum

from singer_sdk.exceptions import MaxRecordsLimitException


def read_json_file(path: Union[PurePath, str]) -> Dict[str, Any]:
    """Read json file, thowing an error if missing."""
    if not path:
        raise RuntimeError("Could not open file. Filepath not provided.")

    if not Path(path).exists():
        msg = f"File at '{path}' was not found."
        for template in [f"{path}.template"]:
            if Path(template).exists():
                msg += f"\nFor more info, please see the sample template at: {template}"
        raise FileExistsError(msg)

    return json.loads(Path(path).read_text())


def utc_now() -> pendulum.datetime:
    """Return current time in UTC."""
    return pendulum.utcnow()


def check_max_records_limit(max_records_limit: int, rows_sent: int) -> None:
    """Check that `row_sent` does not exceed `max_records_limit`.

    Raises MaxRecordsLimitException if check fails.
    """
    if (rows_sent >= max_records_limit):
        raise MaxRecordsLimitException(
            "Stream prematurely aborted due to the stream's max record "
            f"limit ({max_records_limit}) being reached."
        )


def get_batch_dir(
    tap_name: Optional[str] = None, stream_name: Optional[str] = None
) -> Path:
    """Returns a directory path suitable for storing stream batch files."""
    batch_dir = Path.home() / Path('.singer-sdk')
    if tap_name:
        # Add a subdir for this tap
        batch_dir = batch_dir / Path(tap_name)
    if stream_name:
        # Add a subdir for this stream
        batch_dir = batch_dir / Path(stream_name)
    # Add a Timestamp to avoid clashes with other runs of the same tap stream.
    # Only subdir in the case when no tap_name or stream_name is passed.
    batch_dir = batch_dir / Path(time.strftime("%Y-%m-%d--%H-%M-%S"))
    # Create dir and any missing parent dirs.
    batch_dir.mkdir(parents=True, exist_ok=True)
    return batch_dir


def get_batch_file(batch_dir: Path, file_index: int) -> Path:
    """Returns a file path suitable for writing batch record bodies to."""
    return batch_dir / Path(f"{str(file_index).zfill(12)}.jsonl")
