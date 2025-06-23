# Databricks notebook source
class DataSource:
    """
    Abstract class
    """

    def __init__(self,path):
        self.path = path
    
    def get_data_frame(self):
        """
        Abstract class,function will be defined in sub-classes
        """
        raise ValueError("Not Implemented")

class CSVDataSource(DataSource):

    def get_data_frame(self):
        return (
            spark.
            read.
            format("csv").
            option("header","true").
            load(self.path)
        )

class ParquetDataSource(DataSource):

    def get_data_frame(self):
        return (
            spark.
            read.
            format("parquet").
            load(self.path)
        )

class DeltaDataSource(DataSource):

    def get_data_frame(self):
        table_name = self.path
        return (
            spark.
            read.
            table(table_name)
        )



# COMMAND ----------

def get_data_source(file_type,file_path):
    if file_type == 'csv':
        return CSVDataSource(file_path)
    elif file_type == 'parquet':
        return ParquetDataSource(file_path)
    elif file_type == 'delta':
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not Implemented for file type: {file_type}")