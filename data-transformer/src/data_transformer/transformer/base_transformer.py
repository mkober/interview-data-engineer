"""Base class for transformer"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypeVar, Generic, List

import boto3

from data_transformer.factory.transformation_context import TransformationContext
from data_transformer.utils import config_utils

T = TypeVar("T")


class TransformError(ABC, Generic[T]):
    """Error object for transformers"""

    def __init__(self, failed_object: T, error: Exception, message: str = ""):
        self.failed_object = failed_object
        if message:
            self.error_message = message + " | " + str(error)
        else:
            self.error_message = str(error)

    def __str__(self):
        return f"Transformation Error: {self.error_message}"

    def to_json(self) -> dict[str, str]:
        """Convert to JSON"""
        return {
            # Commented this line to prevent any sensitive information getting logged
            "failed_object": str(self.failed_object),
            "error_message": self.error_message,
        }


class PublishResponse:
    """Publication response for transformers"""

    def __init__(self, publish_status: bool, status_message: str):
        self.publish_status = publish_status
        self.error_message = status_message

    def __str__(self):
        return f" Publish Status: {str(self.publish_status)} Message: {str(self.error_message)}"

    def to_json(self) -> json:
        """Convert to JSON"""
        return {
            f"publish_status: {self.publish_status}",
            f"error_message: {self.error_message}",
        }


# pylint: disable=too-few-public-methods
class TransformationResponse(ABC, Generic[T]):
    """Response object for transformers"""

    def __init__(
        self,
        transformed_objects: T,
        transform_errors: [TransformError],
        total_count: int,
    ):
        self.transformed_objects = transformed_objects
        self.transform_errors = transform_errors
        self.total_count = total_count
        self.failed_count = len(transform_errors)
        self.success_count = len(transformed_objects)

    def __str__(self):
        return (
            f"Total Count:\t{str(self.total_count)}\n"
            f"Success Count:\t{str(self.success_count)}\n"
            f"Failed Count:\t{str(self.failed_count)}"
        )

    def errors_to_json(self) -> str:
        """Convert errors to JSON"""
        if len(self.transform_errors) == 0:
            return ""
        errors = []
        for error in self.transform_errors:
            errors.append(error.to_json())
        return json.dumps(errors)


class BaseDataTransformer(ABC):
    """Base class for transformers"""

    def __init__(self, context: TransformationContext):
        self.context = context
        self.logger = self.context.get_logger()
        self.kinesis_client = boto3.client("kinesis", region_name="us-east-1")

    @abstractmethod
    def transform(self) -> TransformationResponse[T]:
        """Generic transform method"""

    def publish_errors(self, response: TransformationResponse):
        self.logger.info("Checking for transformation errors")
        if response and response.transform_errors:
            self.logger.error(
                f"{len(response.transform_errors)} error records found. Publishing transformation errors"
            )
            self.publish_transformed_error_records(response.transform_errors)
        else:
            self.logger.info("No transformation errors found")

    def publish_transformed_error_records(
        self, error_data_records: list[TransformError]
    ):
        """
        API to publish error records to internal kinesis stream from transformation jobs.
        The unprocessed error records are pushed to the raw layer of the target enterprise store.
        :param error_data_records:
        :return:
        """
        if not error_data_records:
            return

        try:
            internal_kinesis_arn = config_utils.get_ssm_parameter(
                f"/cdk/{self.context.env_name}/dps-data-service-baseline/internal-kinesis-stream-arn"
            )
            self.logger.info(
                f"Publish error records to kinesis stream: {self.context.tenant_code} : {self.context.job_name} | stream arn : {internal_kinesis_arn} "
            )
            for data in error_data_records:
                stream_data: any = {
                    "data_type": "error",
                    "tenant_code": self.context.tenant_code,
                    "detail_type": data.to_json(),
                    "job_name": self.context.job_name,
                    "job_run_id": self.context.correlation_id,
                    "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                data_w_newline = json.dumps(stream_data) + "\n"
                data_w_newline = data_w_newline.encode("utf-8")

                self.kinesis_client.put_record(
                    StreamARN=internal_kinesis_arn,
                    Data=data_w_newline,
                    PartitionKey="error",
                )

                self.logger.info(
                    f"Successfully streamed error data into kinesis Config Name: {data_w_newline}"
                )
        except Exception as exp:
            self.logger.error(
                f"Tenant : {self.context.tenant_code} | {self.context.job_name} |"
                f" Error putting error records into kinesis exception: {str(exp)}"
            )

    def publish(self, success_payload: T) -> List[PublishResponse]:
        """Generic publish method"""
