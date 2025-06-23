# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    abstract class
    """
    def __init__(self):
        pass

    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        """
        Implement the steps for reading or extracting the data
        """
        transactionInputDF = get_data_source(
            file_type="csv",
            file_path="dbfs:/FileStore/Transaction_Updated.csv"
        ).get_data_frame()

        customerInputDF = get_data_source(
            file_type="delta",
            file_path="default.customer_delta_table"
        ).get_data_frame()

        inputDFs = {
            "transactionInputDF" : transactionInputDF,
            "customerInputDF" : customerInputDF
        }
        return inputDFs
    
