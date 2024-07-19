from singer_sdk import typing as th

from .client import DummyJSONStream


class Products(DummyJSONStream):
    """Define custom stream."""

    name = "products"
    path = "/products"

    records_jsonpath = "$.products[*]"

    primary_keys = ["id"]

    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("category", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("discountPercentage", th.NumberType),
        th.Property("rating", th.NumberType),
        th.Property("stock", th.NumberType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("brand", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("weight", th.NumberType),
        th.Property(
            "dimensions",
            th.ObjectType(
                th.Property("width", th.NumberType),
                th.Property("height", th.NumberType),
                th.Property("depth", th.NumberType),
            ),
        ),
        th.Property("warrantyInformation", th.StringType),
        th.Property("shippingInformation", th.StringType),
        th.Property("availabilityStatus", th.StringType),
        th.Property(
            "reviews",
            th.ArrayType(
                th.ObjectType(
                    th.Property("rating", th.NumberType),
                    th.Property("comment", th.StringType),
                    th.Property("date", th.DateTimeType),
                    th.Property("reviewerName", th.StringType),
                    th.Property("reviewerEmail", th.StringType),
                )
            ),
        ),
        th.Property("returnPolicy", th.StringType),
        th.Property("minimumOrderQuantity", th.NumberType),
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
