"""Sample CSV target stream class, which handles writing streams."""

from __future__ import annotations

import csv
from pathlib import Path

from singer_sdk.sinks import BatchContext, BatchSink


class CSVSink(BatchSink):
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

    def load_batch(self, batch: BatchContext) -> None:
        """Write batch records out to CSV file.

        This demonstrates the modern, type-safe batch loading pattern.

        Args:
            batch: The batch context containing records and metadata.
        """
        # Type-safe access to records (IDE autocomplete works!)
        records_to_drain = batch.records

        self.logger.debug("Draining records...")
        records_written = 0
        newfile = False
        openmode = "a"
        outpath = self.target_filepath.absolute()
        if not outpath.is_file():
            self.logger.debug("Writing to new file: %s", outpath)
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

    # Legacy implementation (no longer needed - kept for reference):
    # def process_batch(self, context: dict) -> None:
    #     """Write `records_to_drain` out to file."""
    #     records_to_drain = context["records"]  # Magic key - no type safety!
    #     ... (same logic as above)
