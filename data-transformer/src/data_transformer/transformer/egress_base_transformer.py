"""PKH Enterprise data transformer"""

from abc import abstractmethod
from typing import List, TypeVar
from ..utils import config_utils


from .base_transformer import BaseDataTransformer


T = TypeVar("T")
MAX_ATTEMPTS = 3


class EgressBaseDataTransformer(BaseDataTransformer):
    """PKH Egress data transformer"""

    @abstractmethod
    def transform(self) -> List[T]:
        pass

    def send_to_sftp_server(self, filename, s3_bucket_location) -> None:
        """Send data to SFTP server"""
        # Get Connector Id, Role and S3 Bucket Location from SSM parameters
        connector_id = config_utils.get_ssm_parameter(
            f"/services/{self.context.env_name}/{self.context.tenant_code.lower()}-sftp-connector-id"
        )
        is_invocation_role = config_utils.get_ssm_parameter(
            f"/services/{self.context.env_name}/{self.context.tenant_code.lower()}-sftp-connector-invocation-role-arn"
        )
        # This setup constructs the path based on naming standards for S3 bucket
        s3_bucket_location: str = f"{self.context.env_name}-admissions-gateway-{self.context.tenant_code.lower()}-sftp-egress-{self.context.is_account_number}"

        self.logger.debug(f"Connector_id {connector_id}")
        self.logger.debug(f"is_invocation_role {is_invocation_role}")
        self.logger.debug(f"S3 Transfer Location  {s3_bucket_location}")

        config_data = self.context.get_config(
            f"data_transformer.configs.{self.context.env_name}",
            f"{self.context.tenant_code.lower()}-egress-config.json",
        )
        sftp_remote_path = config_data["sftpRemotePath"]

        # Assume the role
        session = config_utils.assume_role(
            is_invocation_role, f"{self.context.tenant_code}-sftp-transfer-session"
        )

        # Trigger AWS SFTP connector transfer from S3 bucket to SFTP server
        transfer = session.client(service_name="transfer")
        response = transfer.start_file_transfer(
            ConnectorId=connector_id,
            SendFilePaths=[f"/{s3_bucket_location}/{filename}"],
            RemoteDirectoryPath=sftp_remote_path,
        )
        transfer_id = response["TransferId"]
        self.logger.info(f"SFTP Transfer ID: {transfer_id}")
        self.logger.info(f"SFTP Connector ID: {connector_id}")
        self.logger.info(f"SFTP Bucket: {s3_bucket_location}")
        self.logger.info(f"SFTP File: {filename}")
        self.logger.info(f"SFTP Remote Path: {sftp_remote_path}")
        self.logger.info(f"SFTP Response: {str(response)}")
