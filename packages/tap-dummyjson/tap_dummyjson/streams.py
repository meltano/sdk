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
                )
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
