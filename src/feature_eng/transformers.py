import pandas as pd
from utils.decorators import log_execution_and_errors


class BaseDataTransformer:

    def __init__(self, categorical_variables:list):
        self.categorical_variables = categorical_variables

    def _trim_data(self,data:pd.DataFrame)->pd.DataFrame:
        data = data[~data['COVER_START'].isna()]
        data.drop(['QUOTE_DATE','CAMPAIGN_DESC','i', 'Police'], inplace=True, axis=1)
        return data

    def _convert_data_types(self, data:pd.DataFrame)->pd.DataFrame:
        data[self.categorical_columns] = data[self.categorical_columns].astype('category')
        data = data.apply(lambda col: col.astype('category') if col.dtype == 'object' else col)
        return data
    
    def _feature_eng(self, data):
        data['start_year'] = data['COVER_START'].dt.year
        return data


class TrainingDataTransformer(BaseDataTransformer):

    def _process_target(self, data:pd.DataFrame)->pd.DataFrame:
        data['lapsed'] = data['POL_STATUS'].apply(lambda x: 1 if x == 'Lapsed' else 0)
        data.drop(['POL_STATUS'],axis=1,inplace=True)
        return data
    
    @log_execution_and_errors(logger_name="Transform", dataframe_attr="data")
    def transform(self,data:pd.DataFrame)->pd.DataFrame:
        data = super()._trim_data(data)
        data = super()._convert_data_types(data)
        data = super()._feature_eng(data)
        data = self._process_target(data)
        return data
    
class InferenceDataTransformer(BaseDataTransformer):

    @log_execution_and_errors(logger_name="Transform", dataframe_attr="data")
    def transform(self,data:pd.DataFrame)->pd.DataFrame:
        data = super()._trim_data(data)
        data = super()._convert_data_types(data)
        data = super()._feature_eng(data)
        return data





    

