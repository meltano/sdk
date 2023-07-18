"""Sample Parquet target stream class, which handles writing streams."""

from __future__ import annotations

import csv
from pathlib import Path

from singer_sdk.sinks import BatchSink


class SampleCSVTargetSink(BatchSink):
    """Sample CSV Target."""

    target_name = "target-csv"

    @property
    def target_folder(self) -> Path:
        """Return target folder."""
        return Path(str(self.config["target_folder"]))

    @property
    def target_filepath(self) -> Path:
        """Return target filepath."""
        return self.target_folder / f"{self.stream_name}.csv"

    def process_batch(self, context: dict) -> None:
        """Write `records_to_drain` out to file."""
        records_to_drain = context["records"]
        self.logger.info("Draining records...")
        records_written = 0
        newfile = False
        openmode = "a"
        outpath = self.target_filepath.absolute()
        if not outpath.is_file():
            self.logger.info("Writing to new file: %s", outpath)
            newfile = True
            openmode = "w"
        with outpath.open(openmode, newline="\n", encoding="utf-8") as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter="\t",
                quotechar='"',
                quoting=csv.QUOTE_NONNUMERIC,
                escapechar="\\",
            )
            for record in records_to_drain:
                if newfile and not records_written:
                    # Write header line if new file
                    writer.writerow(record.keys())
                writer.writerow(record.values())
                records_written += 1
