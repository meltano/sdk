"""Sample Parquet target stream class, which handles writing streams."""

import csv
from pathlib import Path

from typing import Dict, Iterable, Optional

from tap_base.target_sink_base import TargetSinkBase


class SampleCSVTargetSink(TargetSinkBase):

    target_name = "target-csv"

    # Standard method overrides:

    def get_sink_filepath(self) -> Path:
        return Path(f"./.output/{self.stream_name}.csv")

    def flush_records(
        self, records_to_load: Iterable[Dict], expected_row_count: Optional[int]
    ):
        self.logger.info("Flushing records...")
        outfile_path = self.get_sink_filepath()
        newfile = False
        openmode = "a"
        if not outfile_path.is_file():
            # Write header row
            newfile = True
            openmode = "w"
        with open(outfile_path, openmode, newline="\n", encoding="utf-8") as csvfile:
            writer = csv.writer(
                csvfile, delimiter="\t", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
            )
            for record in records_to_load:
                if newfile:
                    # Write header row
                    writer.writerow(record.keys())
                writer.writerow(record.values())
