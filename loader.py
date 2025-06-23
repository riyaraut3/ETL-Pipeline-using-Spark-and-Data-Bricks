# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self,firstTransformedDF):
        self.firstTransformedDF = firstTransformedDF

    def sink(self):
        pass
class AirpodsAfterIPhoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(sink_type="dbfs",df=self.firstTransformedDF,path="/dbfs/FileStore/tables/",method="overwrite").load_data_frame()

class OnlyAirpodsAndIphone(AbstractLoader):
    def sink(self):
        params = {
            'partitionByColumns':['location']
        }
        get_sink_source(sink_type="dbfs_with_partition",df=self.firstTransformedDF,path="/dbfs/OnlyAirpodsAndIphone/",method="overwrite",params=params).load_data_frame()
        get_sink_source(sink_type="delta",df=self.firstTransformedDF,path="default.onlyAirpodsAndIphone",method="overwrite").load_data_frame()

