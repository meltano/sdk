from __future__ import annotations

from unittest.mock import patch

from moto import mock_dynamodb, mock_sts

from singer_sdk.connectors import AWSBoto3Connector


@patch(
    "singer_sdk.connectors.aws_boto3.boto3.Session",
    return_value="mock_session",
)
@mock_dynamodb
def test_get_session_base(patch):
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        },
        "dynamodb",
    )
    session = auth._get_session()
    patch.assert_called_with(
        aws_access_key_id="foo",
        aws_secret_access_key="bar",
        region_name="baz",
    )
    assert session == "mock_session"


@patch(
    "singer_sdk.connectors.aws_boto3.boto3.Session",
    return_value="mock_session",
)
@mock_dynamodb
def test_get_session_w_token(patch):
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_session_token": "abc",
            "aws_default_region": "baz",
        },
        "dynamodb",
    )
    session = auth._get_session()
    patch.assert_called_with(
        aws_access_key_id="foo",
        aws_secret_access_key="bar",
        aws_session_token="abc",
        region_name="baz",
    )
    assert session == "mock_session"


@patch(
    "singer_sdk.connectors.aws_boto3.boto3.Session",
    return_value="mock_session",
)
@mock_dynamodb
def test_get_session_w_profile(patch):
    auth = AWSBoto3Connector(
        {
            "aws_profile": "foo",
        },
        "dynamodb",
    )
    session = auth._get_session()
    patch.assert_called_with(
        profile_name="foo",
    )
    assert session == "mock_session"


@patch(
    "singer_sdk.connectors.aws_boto3.boto3.Session",
    return_value="mock_session",
)
@mock_dynamodb
def test_get_session_implicit(patch):
    auth = AWSBoto3Connector({}, "dynamodb")
    session = auth._get_session()
    patch.assert_called_with()
    assert session == "mock_session"


@mock_dynamodb
@mock_sts
def test_get_session_assume_role():
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
            "aws_assume_role_arn": "arn:aws:iam::123456778910:role/my-role-name",
        },
        "dynamodb",
    )
    auth._get_session()


@mock_dynamodb
def test_get_client():
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        },
        "dynamodb",
    )
    session = auth._get_session()
    auth._get_client(session, "dynamodb")


@mock_dynamodb
def test_get_resource():
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        },
        "dynamodb",
    )
    session = auth._get_session()
    auth._get_resource(session, "dynamodb")


@patch(
    "singer_sdk.connectors.aws_boto3.AWSBoto3Connector._get_client",
    return_value="mock_client",
)
@mock_dynamodb
def test_client_property(patch):
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        },
        "dynamodb",
    )
    assert auth.client == auth._client
    assert auth.client == auth._client
    patch.assert_called_once()


@patch(
    "singer_sdk.connectors.aws_boto3.AWSBoto3Connector._get_resource",
    return_value="mock_resource",
)
@mock_dynamodb
def test_resource_property(patch):
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        },
        "dynamodb",
    )
    assert auth.resource == auth._resource
    assert auth.resource == auth._resource
    patch.assert_called_once()


@patch(
    "singer_sdk.connectors.aws_boto3.boto3.session.Session.resource",
)
@mock_dynamodb
def test_resource_property_endpoint(patch):
    auth = AWSBoto3Connector(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
            "aws_endpoint_url": "http://localhost:8000",
        },
        "dynamodb",
    )
    assert auth.resource == auth._resource
    patch.assert_called_with("dynamodb", endpoint_url="http://localhost:8000")


def test_use_env_vars():
    import os

    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "key_id",
            "AWS_SECRET_ACCESS_KEY": "access_key",
            "AWS_SESSION_TOKEN": "token",
            "AWS_PROFILE": "profile",
            "AWS_DEFAULT_REGION": "region",
        },
    ):
        auth = AWSBoto3Connector(
            {
                "use_aws_env_vars": True,
            },
            "dynamodb",
        )
        assert auth.aws_access_key_id == "key_id"
        assert auth.aws_secret_access_key == "access_key"
        assert auth.aws_session_token == "token"
        assert auth.aws_profile == "profile"
        assert auth.aws_default_region == "region"
