"""Custom client handling, including {{ cookiecutter.source_name }}Stream base class."""

from __future__ import annotations

import typing as t

from singer_sdk.streams import Stream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class {{ cookiecutter.source_name }}Stream(Stream):
    """Stream class for {{ cookiecutter.source_name }} streams."""

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """
        # TODO: Write logic to extract data from the upstream source.
        # records = mysource.getall()  # noqa: ERA001
        # for record in records:
        #     yield record.to_dict()  # noqa: ERA001
        errmsg = "The method is not yet implemented (TODO)"
        raise NotImplementedError(errmsg)
