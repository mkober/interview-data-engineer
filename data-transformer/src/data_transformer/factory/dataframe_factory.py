from abc import ABC, abstractmethod
import pandas as pd


# Base class for Dataframe loader
class DataframeFactory(ABC):
    @abstractmethod
    def get_dataframe(self, type) -> pd.DataFrame:
        """
        :param processed_table_name: name of the table in the processed data catalog
        :param modified_start_date: modified start date of the table
        :return: dataframe
        """
        if type == "student":
            return self.get_student_dataframe()
