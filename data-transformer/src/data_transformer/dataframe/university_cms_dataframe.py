import pandas as pd
from data_transformer.dataframe.base_dataframe import BaseDataframe
from data_transformer.dataframe.dataframe_loader import DataframeLoader


class UniversityCMSInstitutionApplicationDataframe(BaseDataframe):
    def __init__(self, dataframe_loader: DataframeLoader):
        super().__init__(dataframe_loader)

    def get_cms_application_dataframe(self) -> pd.DataFrame:
        df = self.get_dataframe(
            DataframeLoader.cms_database_path, "institution_application"
        )

        if not df.empty:
            df = self.dataframe_loader.filter_dataframe_by_period(df, True)

        return df

    def get_cms_transcript_dataframe(self) -> pd.DataFrame:
        df = self.get_dataframe(
            DataframeLoader.cms_database_path,
            "transcript_item",
        )

        if not df.empty:
            df = self.dataframe_loader.filter_dataframe_by_period(df, True)

        return df
