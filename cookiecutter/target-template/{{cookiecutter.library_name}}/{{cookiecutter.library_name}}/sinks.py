"""Sample Parquet target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union

from singer_sdk.sinks import Sink


class {{ cookiecutter.destination_name }}Sink(Sink):
    """{{ cookiecutter.destination_name }} target sink class."""

    DEFAULT_BATCH_SIZE_ROWS = 10000

    def load_record(self, record: dict) -> None:
        """Load a record."""
        # TODO: Load
        # Sample:
        # for record in records_to_drain:
        #     write(record)
        #     count += 1
        # self.tally_record_written(count)

    def drain(self, records_to_drain: Union[List[dict], Any]) -> None:
        """Write any prepped records out and return only once fully written."""
        # TODO: If records are not written or finalized during load_record(),
        #       write them in batch.
        # Sample:
        # for record in records_to_drain:
        #     write(record)
        #     count += 1
        # self.tally_record_written(count)
