from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk import typing as th

from .client import DummyJSONStream

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context, Record


class Products(DummyJSONStream):
    """Products stream."""

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


class Users(DummyJSONStream):
    """Users stream."""

    name = "users"
    path = "/users"

    records_jsonpath = "$.users[*]"

    primary_keys = ("id",)

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("maidenName", th.StringType),
        th.Property("age", th.IntegerType),
        th.Property("gender", th.StringType),
        th.Property("email", th.EmailType),
        th.Property("phone", th.StringType),
        th.Property("username", th.StringType),
        th.Property("password", th.StringType, secret=True),
        th.Property("birthDate", th.StringType),
        th.Property("image", th.StringType),
        th.Property("bloodGroup", th.StringType),
        th.Property("height", th.NumberType),
        th.Property("weight", th.NumberType),
        th.Property("eyeColor", th.StringType),
        th.Property(
            "hair",
            th.ObjectType(
                th.Property("color", th.StringType),
                th.Property("type", th.StringType),
            ),
        ),
        th.Property("ip", th.IPv4Type),
        th.Property(
            "address",
            th.ObjectType(
                th.Property("address", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("stateCode", th.StringType),
                th.Property("postalCode", th.StringType),
                th.Property(
                    "coordinates",
                    th.ObjectType(
                        th.Property("lat", th.NumberType),
                        th.Property("lng", th.NumberType),
                    ),
                ),
                th.Property("country", th.StringType),
            ),
        ),
        th.Property("macAddress", th.StringType),
        th.Property("university", th.StringType),
        th.Property(
            "bank",
            th.ObjectType(
                th.Property("cardExpire", th.StringType),
                th.Property("cardNumber", th.StringType),
                th.Property("cardType", th.StringType),
                th.Property("currency", th.StringType),
                th.Property("iban", th.StringType),
            ),
        ),
        th.Property(
            "company",
            th.ObjectType(
                th.Property("department", th.StringType),
                th.Property("name", th.StringType),
                th.Property("title", th.StringType),
                th.Property(
                    "address",
                    th.ObjectType(
                        th.Property("address", th.StringType),
                        th.Property("city", th.StringType),
                        th.Property("state", th.StringType),
                        th.Property("stateCode", th.StringType),
                        th.Property("postalCode", th.StringType),
                        th.Property(
                            "coordinates",
                            th.ObjectType(
                                th.Property("lat", th.NumberType),
                                th.Property("lng", th.NumberType),
                            ),
                        ),
                        th.Property("country", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property("ein", th.StringType),
        th.Property("ssn", th.StringType),
        th.Property("userAgent", th.StringType),
        th.Property(
            "crypto",
            th.ObjectType(
                th.Property("coin", th.StringType),
                th.Property("wallet", th.StringType),
                th.Property("network", th.StringType),
            ),
        ),
        th.Property("role", th.StringType),
    ).to_dict()

    def generate_child_contexts(
        self,
        record: Record,
        context: Context | None,
    ) -> Iterable[Context | None]:
        """Generate child contexts for child streams.

        Args:
            record: The record to generate a child context for.
            context: The parent context.

        Yields:
            Contexts for the child streams.
        """
        yield {"user_id": record["id"]}


class Carts(DummyJSONStream):
    """Carts stream."""

    name = "carts"
    path = "/users/{user_id}/carts"

    records_jsonpath = "$.carts[*]"

    primary_keys = ("id",)
    parent_stream_type = Users

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property(
            "products",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("title", th.StringType),
                    th.Property("price", th.NumberType),
                    th.Property("quantity", th.IntegerType),
                    th.Property("total", th.NumberType),
                    th.Property("discountPercentage", th.NumberType),
                    th.Property("thumbnail", th.StringType),
                )
            ),
        ),
        th.Property("total", th.NumberType),
        th.Property("discountedTotal", th.NumberType),
        th.Property("userId", th.IntegerType),
        th.Property("totalProducts", th.IntegerType),
        th.Property("totalQuantity", th.IntegerType),
    ).to_dict()
