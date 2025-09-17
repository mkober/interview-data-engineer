"""Transforms PKH application objects to CMS application data file"""

from datetime import datetime
import json
import re
from typing import Optional

import pandas as pd

from ...dataframe.dataframe_loader import DataframeLoader
from ...factory.transformation_context import TransformationContext
from ...transformer.egress_base_transformer import EgressBaseDataTransformer
from ...utils import config_utils
from ..base_transformer import TransformationResponse
from .config import UNIVERSITY_CMS_TRANSFORMATION_CONFIG


def extract_school_code(json_str: str, index: int) -> str | None:
    if not json_str:  # Check for empty/None json_str
        return None
    try:
        parsed = json.loads(json_str)
        if index < len(parsed):
            school_code = parsed[index].get("schoolCode")
            # Ensure school_code is not None before str() and slicing
            return str(school_code)[-4:] if school_code is not None else None
        return None
    except json.JSONDecodeError:
        # Consider if logging is needed/possible here
        return None


class UniversityCMSApplicationTransformer(EgressBaseDataTransformer):
    """PKH Data Model CMS Admission data object transformer"""

    def __init__(
        self,
        applicant_dataframe: [dict],
        application_dataframe: [dict],
        context: TransformationContext,
    ):
        super().__init__(context)
        self.transformation_context = context
        self.logger = context.get_logger()
        self.dataframe_loader = DataframeLoader(context)
        self.application_dataframe = application_dataframe
        if len(application_dataframe) == 0:
            self.dataframe = pd.DataFrame()
            return

        dataframe = application_dataframe.merge(
            applicant_dataframe,
            on="applicantId",
            how="left",
            suffixes=("", "_applicant"),
        )
        dataframe = dataframe[dataframe.get("applicationStatus") == "Submitted"]
        dataframe["time_application"] = dataframe["time"]
        dataframe["time"] = dataframe.apply(
            lambda row: max(
                pd.to_datetime(row["time_application"], utc=True),
                pd.to_datetime(row["time_applicant"], utc=True),
            ),
            axis=1,
        )

        self.dataframe = self.dataframe_loader.filter_dataframe(dataframe)

        self.logger.info(
            f"Number of applications to transform: {len(self.dataframe.index)}"
        )

    def transform(self) -> TransformationResponse[str]:
        if len(self.dataframe) == 0:
            self.logger.info("No data to transform")
            return TransformationResponse(self.dataframe, [], 0)
        transformed_dataframe = self.__passthrough()
        custom_columns = self.__custom_transforms()
        address_columns = self.__transform_addresses()
        education_columns = self.__transform_education_history()

        transformed_dataframe = pd.concat(
            [transformed_dataframe, custom_columns, address_columns, education_columns],
            axis=1,
            join="inner",
        )
        if len(transformed_dataframe.index) == 0:
            self.logger.info("No data to transform")
            return TransformationResponse(
                transformed_dataframe, [], len(transformed_dataframe)
            )
        transformed_dataframe.replace(
            ["TRUE", "True", "true", True], "Yes", inplace=True
        )
        transformed_dataframe.replace(
            ["FALSE", "False", "false", False], "No", inplace=True
        )
        self.logger.info(
            f"Number of completed application transforms: {len(transformed_dataframe.index)}"
        )
        self.logger.info(
            f"ApplicationIds: {list(transformed_dataframe['applicationId'])}"
        )
        return TransformationResponse(
            transformed_dataframe, [], len(transformed_dataframe)
        )

    def publish(self, transformation_response: TransformationResponse) -> None:
        """publish the data"""
        # Publish output to target S3 buckets on integration services account
        transformed_dataframe = transformation_response.transformed_objects
        if len(transformed_dataframe.index) == 0:
            self.logger.info("No data to publish")
            return
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S.%f")
        filename = f"application-{timestamp}.csv"
        self.logger.info(f"Publishing file to curated bucket: {filename}")
        integration_account_staging_bucket = f"{self.context.env_name}-admissions-gateway-university-sftp-egress-{self.transformation_context.is_account_number}"
        (success, message) = config_utils.upload_to_s3_v2(
            transformed_dataframe.to_csv(index=False),
            filename,
            f"{self.context.env_name}-university-cms-curated-data-{self.transformation_context.ds_account_number}",
            "university-cms",
        )
        if success:
            self.logger.info(message)
        else:
            self.logger.error(message)
        self.logger.info(f"Publishing file to staging bucket: {filename}")
        (success, message) = config_utils.upload_to_s3_v2(
            transformed_dataframe.to_csv(index=False),
            filename,
            integration_account_staging_bucket,
        )
        if success:
            self.logger.info(message)
            self.send_to_sftp_server(filename, integration_account_staging_bucket)
        else:
            self.logger.error(message)

    def __passthrough(self) -> pd.DataFrame:
        """copy the fields with no transforms in UNIVERSITY_CMS_TRANSFORMATION_CONFIG"""
        return pd.DataFrame(
            {
                column: self.dataframe.get(column, None)
                for column in UNIVERSITY_CMS_TRANSFORMATION_CONFIG.get("passthrough")
            }
        )

    def __custom_transforms(self):
        """custom transformations for fields"""
        return pd.DataFrame(
            {
                "campuscode_online": "Online",
                "phones_phoneNumber": self.dataframe.get("phones", pd.Series()).apply(
                    self.__get_phone_number
                ),
                "phones_doNotText": "Yes",
                "emails_emailAddress": self.dataframe.get("emails", pd.Series()).apply(
                    self.__get_email
                ),
                "demographics_affiliation": self.dataframe.get(
                    "demographics_affiliations", pd.Series()
                ).apply(self.__check_affiliation),
                "additionalQuestions_signature": self.dataframe.get(
                    "personalData_firstName", pd.Series()
                )
                + " "
                + self.dataframe.get("personalData_lastName", pd.Series()),
                "payment_waived": "Waived",
                # below are passthroughs, but header name is changed due to locked file requirements
                "militaryAffiliation_plannedBenefits": self.dataframe.get(
                    "militaryAffiliation_educationBenefit", pd.Series()
                ),
                "startTerm": self.dataframe.get(
                    "academicPlan_enrollmentTerm", pd.Series()
                ),
                "additionalQuestions_Disciplinary_Notification_Statement__c'": self.dataframe.get(
                    "additionalQuestions_disciplinaryNotificationStatementConfirmation",
                    pd.Series(),
                ),
            }
        )

    def __transform_addresses(self):
        """transform addresses"""
        return pd.DataFrame(
            {
                "Permanent address": self.dataframe.get("addresses", pd.Series()).apply(
                    self.__get_permanent_address
                ),
                "Permanent address - City": self.dataframe.get(
                    "addresses", pd.Series()
                ).apply(lambda x: self.__get_permanent_address_field(x, "city")),
                "Permanent address - State": self.dataframe.get(
                    "addresses", pd.Series()
                ).apply(lambda x: self.__get_permanent_address_field(x, "stateCode")),
                "Permanent address - Zip": self.dataframe.get(
                    "addresses", pd.Series()
                ).apply(lambda x: self.__get_permanent_address_field(x, "zipCode")),
                "Permanent address - Country": self.dataframe.get(
                    "addresses", pd.Series()
                ).apply(lambda x: self.__get_permanent_address_field(x, "country")),
                "Alternate address available": self.dataframe.get(
                    "receiveMailAtDifferentAddress", pd.Series()
                ),
                "Current address - Address": self.dataframe.apply(
                    lambda row: self.__get_current_address(
                        row["addresses"], row["receiveMailAtDifferentAddress"]
                    ),
                    axis=1,
                ),
                "Current address - City": self.dataframe.apply(
                    lambda row: self.__get_current_address_field(
                        row["addresses"], "city", row["receiveMailAtDifferentAddress"]
                    ),
                    axis=1,
                ),
                "Current address - State": self.dataframe.apply(
                    lambda row: self.__get_current_address_field(
                        row["addresses"],
                        "stateCode",
                        row["receiveMailAtDifferentAddress"],
                    ),
                    axis=1,
                ),
                "Current address - Zip": self.dataframe.apply(
                    lambda row: self.__get_current_address_field(
                        row["addresses"],
                        "zipCode",
                        row["receiveMailAtDifferentAddress"],
                    ),
                    axis=1,
                ),
                "Current address - Country": self.dataframe.apply(
                    lambda row: self.__get_current_address_field(
                        row["addresses"],
                        "country",
                        row["receiveMailAtDifferentAddress"],
                    ),
                    axis=1,
                ),
                "Alternate address from date": self.dataframe.apply(
                    lambda row: self.__get_current_address_field(
                        row["addresses"],
                        "addressEffectiveDate",
                        row["receiveMailAtDifferentAddress"],
                    ),
                    axis=1,
                ),
                "Alternate address to date": self.dataframe.apply(
                    lambda row: self.__get_current_address_field(
                        row["addresses"],
                        "addressExpirationDate",
                        row["receiveMailAtDifferentAddress"],
                    ),
                    axis=1,
                ),
            }
        )

    def __transform_education_history(self):
        """transform education history"""
        education_columns = pd.DataFrame()

        MAX_EDU_HIST = 9
        for index in range(MAX_EDU_HIST):
            education_columns[
                f"applicationEducationHistoryRecords_schoolCode{index + 1}"
            ] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(lambda x: extract_school_code(x, index))
            education_columns[
                f"applicationEducationHistoryRecords_educationInstitutionName{index + 1}"
            ] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(
                lambda x: (
                    json.loads(x)[index].get("educationInstitutionName")
                    if index < len(json.loads(x))
                    else None
                )
            )
            education_columns[f"Month_Entered{index + 1}"] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(lambda x: self.__get_month_entered(x, index))
            education_columns[f"Year_Entered{index + 1}"] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(lambda x: self.__get_year_entered(x, index))
            education_columns[f"Month_Departed{index + 1}"] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(lambda x: self.__get_month_departed(x, index))
            education_columns[f"Year_Departed{index + 1}"] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(lambda x: self.__get_year_departed(x, index))
            education_columns[
                f"applicationEducationHistory_degreeEarnedBeforeEnrolling{index + 1}"
            ] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(
                lambda x: (
                    json.loads(x)[index].get("degreeEarnedBeforeEnrolling")
                    if index < len(json.loads(x))
                    else None
                )
            )
            education_columns[
                f"applicationEducationHistory_degreeEarned{index + 1}"
            ] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(
                lambda x: (
                    json.loads(x)[index].get("degreeEarned")
                    if index < len(json.loads(x))
                    else None
                )
            )
            education_columns[
                f"applicationEducationHistoryRecords_major{index + 1}"
            ] = self.dataframe.get(
                "applicationEducationHistoryRecords", pd.Series()
            ).apply(
                lambda x: (
                    json.loads(x)[index].get("major")
                    if index < len(json.loads(x))
                    else None
                )
            )

        return education_columns

    def __format_phone_number(self, raw: str) -> Optional[str]:
        """
        Normalize and format a phone-number string.
        - Remove non-digits
        - Strip leading '1' if 11 digits
        - Format 10 digits as (123) 456-7890
        - Otherwise, return raw input
        """
        if not raw:
            return None
        digits = re.sub(r"\D", "", raw)
        # Strip country code “1”
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        # Unexpected format: return as-is (or return digits if you prefer)
        return raw

    def __get_phone_number(self, phones_list: str) -> str:
        """
        Extract the first MOBILE phone and format it.
        """
        if phones_list:
            try:
                phones_json_list = json.loads(phones_list)
                for phone in phones_json_list:
                    if phone.get("phoneType") == "MOBILE":
                        return self.__format_phone_number(phone.get("phoneNumber", ""))
            except json.JSONDecodeError:
                # self.logger.warning(f"Could not decode phones_list: {phones_list}")
                return None
        return None

    def __get_email(self, emails_list: str):
        if emails_list:
            for email in json.loads(emails_list):
                if email["emailType"] == "HOME":
                    return email["emailAddress"]
        return None

    def __check_affiliation(self, affiliations_list: str):
        if affiliations_list:
            for affiliation in json.loads(affiliations_list):
                if affiliation in ["Phi Theta Kappa"]:
                    return True
        return False

    def __get_permanent_address(self, addresses_list: str):
        if addresses_list:
            for address in json.loads(addresses_list):
                if address["addressType"] == "HOME":
                    return address.get("line1", "") + " " + address.get("line2", "")
        return None

    def __get_permanent_address_field(self, addresses_list: str, field_name):
        if addresses_list:
            for address in json.loads(addresses_list):
                if address["addressType"] == "HOME":
                    return address.get(field_name)
        return None

    def __get_current_address(
        self, addresses_list: str, hasDifferentMailingAddress: str
    ):
        if hasDifferentMailingAddress == "YES" and addresses_list:
            for address in json.loads(addresses_list):
                if address["addressType"] == "OTHER":
                    return address.get("line1", "") + " " + address.get("line2", "")
        return None

    def __get_current_address_field(
        self, addresses_list: str, field_name, hasDifferentMailingAddress: str
    ):
        if hasDifferentMailingAddress == "YES" and addresses_list:
            for address in json.loads(addresses_list):
                if address["addressType"] == "OTHER":
                    return address.get(field_name)
        return None

    def __get_month_entered(self, application_education_histories: str, index):
        if application_education_histories:
            if index < len(json.loads(application_education_histories)):
                start_date = json.loads(application_education_histories)[index].get(
                    "hedStartDate"
                )
                if start_date:
                    return start_date.split("-")[1]
        return None

    def __get_year_entered(self, application_education_histories: str, index):
        if application_education_histories:
            if index < len(json.loads(application_education_histories)):
                start_date = json.loads(application_education_histories)[index].get(
                    "hedStartDate"
                )
                if start_date:
                    return start_date.split("-")[0]
        return None

    def __get_month_departed(self, application_education_histories: str, index):
        if application_education_histories:
            if index < len(json.loads(application_education_histories)):
                end_date = json.loads(application_education_histories)[index].get(
                    "hedEndDate"
                )
                if end_date:
                    return end_date.split("-")[1]
        return None

    def __get_year_departed(self, application_education_histories: str, index):
        if application_education_histories:
            if index < len(json.loads(application_education_histories)):
                end_date = json.loads(application_education_histories)[index].get(
                    "hedEndDate"
                )
                if end_date:
                    return end_date.split("-")[0]
        return None
