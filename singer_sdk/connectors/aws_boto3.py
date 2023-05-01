from __future__ import annotations

import logging
import os

from singer_sdk import typing as th  # JSON schema typing helpers

try:
    import boto3
except ImportError:
    raise Exception(
        "boto3 is required for this authenticator. "
        "Please install it with `poetry add boto3`.",
    )

AWS_AUTH_CONFIG = th.PropertiesList(
    th.Property(
        "aws_access_key_id",
        th.StringType,
        secret=True,
        description="The access key for your AWS account.",
    ),
    th.Property(
        "aws_secret_access_key",
        th.StringType,
        secret=True,
        description="The secret key for your AWS account.",
    ),
    th.Property(
        "aws_session_token",
        th.StringType,
        secret=True,
        description=(
            "The session key for your AWS account. This is only needed when"
            " you are using temporary credentials."
        ),
    ),
    th.Property(
        "aws_profile",
        th.StringType,
        description=(
            "The AWS credentials profile name to use. The profile must be "
            "configured and accessible."
        ),
    ),
    th.Property(
        "aws_default_region",
        th.StringType,
        description="The default AWS region name (e.g. us-east-1) ",
    ),
    th.Property(
        "aws_endpoint_url",
        th.StringType,
        description="The complete URL to use for the constructed client.",
    ),
    th.Property(
        "aws_assume_role_arn",
        th.StringType,
        description="The role ARN to assume.",
    ),
    th.Property(
        "use_aws_env_vars",
        th.BooleanType,
        default=False,
        description=("Whether to retrieve aws credentials from environment variables."),
    ),
).to_dict()


class AWSBoto3Connector:
    """Base class for AWS boto3-based connectors.

    The connector class serves as a wrapper around boto3 package.

    The functions of the connector are:
    - initializing a client, resource, or session with a simple interface
    - accessing AWS credentials via config, env vars, or profile and supports assuming roles.
    - enables configurable endpoint_url for testing
    """

    def __init__(
        self,
        config: dict,
        service_name: str,
    ) -> None:
        """Initialize the AWSBotoAuthenticator.

        Args:
            config (dict): The config for the connector.
            service_name (str): The name of the AWS service.
        """
        self._service_name = service_name
        self._config = config
        self._client = None
        self._resource = None
        # config for use environment variables
        if config.get("use_aws_env_vars"):
            self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            self.aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
            self.aws_profile = os.environ.get("AWS_PROFILE")
            self.aws_default_region = os.environ.get("AWS_DEFAULT_REGION")
        else:
            self.aws_access_key_id = config.get("aws_access_key_id")
            self.aws_secret_access_key = config.get("aws_secret_access_key")
            self.aws_session_token = config.get("aws_session_token")
            self.aws_profile = config.get("aws_profile")
            self.aws_default_region = config.get("aws_default_region")

        self.aws_endpoint_url = config.get("aws_endpoint_url")
        self.aws_assume_role_arn = config.get("aws_assume_role_arn")

    @property
    def config(self) -> dict:
        """If set, provides access to the tap or target config.

        Returns:
            The settings as a dict.
        """
        return self._config

    @property
    def logger(self) -> logging.Logger:
        """Get logger.

        Returns:
            Plugin logger.
        """
        return logging.getLogger("aws_boto_connector")

    @property
    def client(self) -> boto3.client:
        """Return the boto3 client for the service.

        Returns:
            boto3.client: The boto3 client for the service.
        """
        if self._client:
            return self._client
        else:
            session = self._get_session()
            self._client = self._get_client(session, self._service_name)
            return self._client

    @property
    def resource(self) -> boto3.resource:
        """Return the boto3 resource for the service.

        Returns:
            boto3.resource: The boto3 resource for the service.
        """
        if self._resource:
            return self._resource
        else:
            session = self._get_session()
            self._resource = self._get_resource(session, self._service_name)
            return self._resource

    def _get_session(self) -> boto3.session:
        """Return the boto3 session.

        Returns:
            boto3.session: The boto3 session.

        Raises:
            Exception: If no credentials are provided.
        """
        session = None
        if (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_session_token
            and self.aws_default_region
        ):
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_default_region,
            )
            self.logger.info(
                "Authenticating using access key id, secret access key, and "
                "session token.",
            )
        elif (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_default_region
        ):
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_default_region,
            )
            self.logger.info(
                "Authenticating using access key id and secret access key.",
            )
        elif self.aws_profile:
            session = boto3.Session(profile_name=self.aws_profile)
            self.logger.info("Authenticating using profile.")
        else:
            session = boto3.Session()
            self.logger.info("Authenticating using implicit pre-installed credentials.")

        if self.aws_assume_role_arn:
            session = self._assume_role(session, self.aws_assume_role_arn)
        return session

    def _factory(
        self,
        aws_obj,
        service_name: str,
    ) -> boto3.resource | boto3.client:
        if self.aws_endpoint_url:
            return aws_obj(
                service_name,
                endpoint_url=self.aws_endpoint_url,
            )
        else:
            return aws_obj(
                service_name,
            )

    def _get_resource(
        self,
        session: boto3.session,
        service_name: str,
    ) -> boto3.resource:
        """Return the boto3 resource for the service.

        Args:
            session (boto3.session.Session): The boto3 session.
            service_name (str): The name of the AWS service.

        Returns:
            boto3.resource: The boto3 resource for the service.
        """
        return self._factory(session.resource, service_name)

    def _get_client(
        self,
        session: boto3.session.Session,
        service_name: str,
    ) -> boto3.client:
        """Return the boto3 client for the service.

        Args:
            session (boto3.session.Session): The boto3 session.
            service_name (str): The name of the AWS service.

        Returns:
            boto3.client: The boto3 client for the service.
        """
        return self._factory(session.client, service_name)

    def _assume_role(
        self,
        session: boto3.session.Session,
        role_arn: str,
    ) -> boto3.session.Session:
        # TODO: use for auto refresh https://github.com/benkehoe/aws-assume-role-lib
        sts_client = self._get_client(session, "sts")
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="tap-dynamodb",
        )
        return boto3.Session(
            aws_access_key_id=response["Credentials"]["AccessKeyId"],
            aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
            aws_session_token=response["Credentials"]["SessionToken"],
            region_name=self.aws_default_region,
        )
