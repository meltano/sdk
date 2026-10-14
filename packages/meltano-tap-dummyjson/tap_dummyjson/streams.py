from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th

from .client import DummyJSONStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.streams.rest import HTTPRequest, PageContext


class Products(DummyJSONStream):
    """Define custom stream."""

    name = "products"
    path = "/products"

    records_jsonpath = "$.products[*]"

    primary_keys = ["id"]

    replication_key = "_sdc_updated_at"

    schema = th.PropertiesList(
        th.Property("_sdc_updated_at", th.DateTimeType),
        th.Property("id", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("category", th.StringType),
        th.Property("price", th.DecimalType),
        th.Property("discountPercentage", th.DecimalType),
        th.Property("rating", th.DecimalType),
        th.Property("stock", th.DecimalType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("brand", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("weight", th.DecimalType),
        th.Property(
            "dimensions",
            th.ObjectType(
                th.Property("width", th.DecimalType),
                th.Property("height", th.DecimalType),
                th.Property("depth", th.DecimalType),
            ),
        ),
        th.Property("warrantyInformation", th.StringType),
        th.Property("shippingInformation", th.StringType),
        th.Property("availabilityStatus", th.StringType),
        th.Property(
            "reviews",
            th.ArrayType(
                th.ObjectType(
                    th.Property("rating", th.DecimalType),
                    th.Property("comment", th.StringType),
                    th.Property("date", th.DateTimeType),
                    th.Property("reviewerName", th.StringType),
                    th.Property("reviewerEmail", th.StringType),
                ),
            ),
        ),
        th.Property("returnPolicy", th.StringType),
        th.Property("minimumOrderQuantity", th.DecimalType),
        th.Property(
            "meta",
            th.ObjectType(
                th.Property("createdAt", th.DateTimeType),
                th.Property("updatedAt", th.DateTimeType),
                th.Property("barcode", th.StringType),
                th.Property("qrCode", th.StringType),
            ),
        ),
        th.Property("images", th.ArrayType(th.StringType)),
        th.Property("thumbnail", th.StringType),
    ).to_dict()

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["_sdc_updated_at"] = row["meta"]["updatedAt"]
        return row

    @override
    def get_http_request(self, page: PageContext[int]) -> HTTPRequest:
        request = super().get_http_request(page=page)
        if modified_after := self.get_starting_timestamp(page.stream_context):
            request.params["modifiedAfter"] = modified_after
        return request
