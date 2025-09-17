"""Transformer factory"""
from .transformation_context import TransformationContext
from ..transformer.base_transformer import BaseDataTransformer
from ..dataframe.university_cms_dataframe import (
    UniversityCMSInstitutionApplicationDataframe,
)
from ..transformer.pkh_enterprise.university_cms_transcript_transformer import (
    UniversityCMSTranscriptDataTransformer,
)

class DataTransformerFactory:
    """Data transformer factory"""

    @staticmethod
    def get_data_transformer(
        data_transformer_type, transformation_context: TransformationContext
    ) -> BaseDataTransformer:
        """Creates a data tansformer for a given data type

        :param data_transformer_type: Type of data to transform
        :type data_transformer_type: str
        :param transformation_context: Context object
        :type transformation_context: TransformationContext
        :return: Data transformer
        :rtype: object
        """
        logger = transformation_context.get_logger()
        dataframe_loader = DataframeLoader(transformation_context)
x
        if data_transformer_type == "InstitutionApplication":
            if "UNIVERSITY" == transformation_context.tenant_code:
                application_dataframe = UniversityCMSInstitutionApplicationDataframe(
                    dataframe_loader
                )
                return UniversityCMSInstitutionApplicationDataTransformer(
                    application_dataframe, transformation_context
                )
        elif data_transformer_type == "InstitutionTranscript":
            if "UNIVERSITY" == transformation_context.tenant_code:
                application_dataframe = UniversityCMSInstitutionApplicationDataframe(
                    dataframe_loader
                )
                return UniversityCMSTranscriptDataTransformer(
                    application_dataframe, transformation_context
                )
        else:
            logger.error(
                f"Unknown data transformer type: {data_transformer_type} for tenant {transformation_context.tenant_code}"
            )
            logger.info(
                "Data transformer not found returning base data transformer to client"
            )
            return BaseDataTransformer(transformation_context)
