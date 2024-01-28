import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

class PermanentUsageReport():
    def __init__(self) -> None:
        load_dotenv()
        self.MAX_CHUNKSIZE = int(os.environ.get("SQL_RETURN_MAX_CHUNKSIZE"))
        self.DATABASE_PATH = os.environ.get("DATABASE_PATH") # database_path following sqlalchemy string connection
        self.ENGINE_ECHO = True if os.environ.get("ENGINE_ECHO")=="True" else False
        self.STREAM_RESULTS = True if os.environ.get("STREAM_RESULTS")=="True" else False
        self.SQL_QUERY_PATH = os.environ.get("SQL_QUERY_PATH")
        
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

    def groupResults(self):
        pass

    def exportResults(self):
        pass


if __name__ == '__main__':
    PermanentUsageReport().retrieveDataFromDB()