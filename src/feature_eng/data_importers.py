import pandas as pd
from abc import ABC, abstractmethod
from utils.decorators import log_execution_and_errors


class BaseDataImporter(ABC):

    @abstractmethod
    def import_data(self):
        pass

class CSVDataImporter(BaseDataImporter):

    def __init__(self, file_path):
        self.file_path = file_path

    @log_execution_and_errors(logger_name="Import Data", dataframe_attr="data")
    def import_data(self):
        data = pd.read_csv(self.file_path)
        return data