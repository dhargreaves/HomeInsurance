import pandas as pd
from abc import ABC, abstractmethod
from utils.decorators import log_execution_and_errors

class BaseExporter(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def export_data(self):
        pass


class CSVExporter(BaseExporter):

    def __init__(self, write_destination):
        self.write_destination = write_destination

    @log_execution_and_errors(logger_name="Export Data", dataframe_attr=None)
    def export_data(self, data):
        data.to_csv(self.write_destination, index=False)