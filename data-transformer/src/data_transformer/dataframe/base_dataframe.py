"""Base dataframe"""

from typing import List

import pandas as pd
from flatten_json import flatten, unflatten

from data_transformer.dataframe.dataframe_loader import DataframeLoader


class BaseDataframe:
    """base dataframe class"""

    def __init__(self, dataframe_loader: DataframeLoader):
        self.dataframe_loader = dataframe_loader

    def get_raw_data(self, database_path: str, table_name: str) -> List[dict]:
        return self.dataframe_loader.raw_data_dict(database_path, table_name)

    def get_dataframe(self, database_path: str, table_name: str) -> pd.DataFrame:
        """Get dataframe

        :param database_path: database path
        :type database_path: str
        :param table_name: table name
        :type table_name: str
        :return: pandas dataframe
        :rtype: pd.DataFrame
        """
        return self.dataframe_loader.create_dataframe(database_path, table_name)

    @staticmethod
    def makeflat(jdata: dict) -> dict:
        """Use this method to flatten nested JSON dictionaries.

        By standardizing how we flatten JSON data, we can reduce astonishment
        and ensure consistent translation between Pandas DataFrames and
        Pydantic models.

        :param jdata: JSON dictionary with nested lists of models, e.g. lists of Email or Phone models
        :type jdata: dict
        :return: A flattened JSON dictionary using '.' as separator
        :rtype: dict
        """
        return flatten(jdata, separator=".")

    @staticmethod
    def nested_json_to_df(jdata: dict, index: str = "pkhId") -> pd.DataFrame:
        """Use this method to create a Pandas DataFrame from nested JSON data.

        This method assumes that the data is indexed by pkhId. If it's not, use
        the optional index parameter to override.

        When converting some aggregate models like Applicant to a Pandas
        DataFrame, we get the following error due to nested structures:

        ValueError: "Mixing dicts with non-Series may lead to ambiguous ordering."

        Use this method to create a DataFrame from such data. If a conversion
        back to a Pydantic model is desired, use the flat_df_to_unflat_json
        method, and regroup your lists as necessary.

        :param jdata: JSON dictionary with nested lists of models, e.g. lists of Email or Phone models
        :type jdata: dict
        :param index: The string field name to use for the DataFrame index
        :type index: str
        :return: A Pandas DataFrame composed of flattened JSON data
        :rtype: pd.DataFrame
        """
        return pd.DataFrame(BaseDataframe.makeflat(jdata), index=[index])

    @staticmethod
    def flat_df_to_unflat_json(df: pd.DataFrame) -> dict:
        """Convert a DataFrame that was created using nested_json_to_df back to unflattened JSON.

        With the data returned by this function, you will still need to group
        up the submodels into lists to bring it back to a state that can be
        validated using the Pydantic model_validate() method. Since that will
        require knowledge of the specific submodel names (e.g. 'phones',
        'emails', etc.), this method cannot provide that transformation.

        This is a companion function to the makeflat() and nested_json_to_df()
        methods.
        """
        return unflatten(df.to_dict(orient="records")[0], separator=".")
