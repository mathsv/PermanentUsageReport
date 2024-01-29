import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pathlib as plib

class PermanentUsageReport():
    def __init__(self) -> None:
        load_dotenv()
        self.MAX_CHUNKSIZE = int(os.environ.get("SQL_RETURN_MAX_CHUNKSIZE"))
        self.DATABASE_PATH = os.environ.get("DATABASE_PATH") # database_path following sqlalchemy string connection
        self.ENGINE_ECHO = True if os.environ.get("ENGINE_ECHO")=="True" else False
        self.STREAM_RESULTS = True if os.environ.get("STREAM_RESULTS")=="True" else False
        self.SQL_QUERY_PATH = os.environ.get("SQL_QUERY_PATH")
        self.EXPORT_PATH = os.environ.get("EXPORT_PATH")
        plib.Path(self.EXPORT_PATH).mkdir(parents=True, exist_ok=True) # Creates the export path if it does not exist
        self.RESULT_FILENAME = os.environ.get("RESULT_FILENAME")
        
    def retrieveDataFromDB(self):
        __engine = create_engine(self.DATABASE_PATH, echo=self.ENGINE_ECHO) # type: ignore  # echo=True means that sqlalchemy will log sql informations
        __conn = __engine.connect().execution_options(stream_results=self.STREAM_RESULTS) # streams resulting rows

        with open(self.SQL_QUERY_PATH, 'r') as file: # Read only the first line
            __sql_query = file.readline()

        __chunks = list()
        for __chunk in pd.read_sql(f"{__sql_query}", __conn, chunksize=self.MAX_CHUNKSIZE):
            __chunks.append(__chunk)
            print(f"Chunk #{len(__chunks)}")
        print("Data Extraction Completed!")
        print("Initializing Chunk Concatenation")
        self._dataframe = pd.concat(__chunks)
        del __chunks
        print(self._dataframe.head(5))

    def groupByConsecutiveDates(self, date_col='Date', key_col='Newkey', cols_to_group_with_date=[]):
        cols_to_group_with_date.append(date_col)
        self._dataframe[date_col] =  pd.to_datetime(self._dataframe[date_col])
        self._dataframe.drop_duplicates(cols_to_group_with_date, inplace=True)
        self._dataframe.sort_values(by=cols_to_group_with_date, inplace=True, ascending=True)
        self._dataframe[key_col] = self._dataframe[date_col].diff().dt.days.ne(1).cumsum()

    def groupResults(self, group_dict={'Date':['first','last','count']}, groupby_cols=[]):
        '''
        group_dict: Receives column as key and a list of aggregate functions for that column
        groupby_cols: columns to be used by pandas.groupby()
        '''
        self._result = self._dataframe.groupby(groupby_cols, as_index=False).agg(group_dict)

    def exportResults(self, export_type='csv', index=False, header=True,
                      sep=';', encoding='utf_16', decimal=',', compression_method="gzip"):
        match export_type:
            case 'csv':
                self._result.to_csv(
                    plib.Path(f'{self.EXPORT_PATH}\\{self.RESULT_FILENAME}.csv{'.gz' if compression_method == 'gzip' else ''}'),
                    index=index, header=header, sep=sep,
                    encoding=encoding, decimal=decimal, compression=compression_method)
            case 'excel':
                self._result.to_excel(plib.Path(f'{self.EXPORT_PATH}\\{self.RESULT_FILENAME}'),
                                      index=index, header=header)
            case _:
                raise ValueError(f'The export type {export_type} is not supported or does not exist!')


if __name__ == '__main__':
    PermanentUsageReport().retrieveDataFromDB()