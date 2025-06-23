# Databricks notebook source
# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./loader"
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lead,col,broadcast,collect_set,array_contains,size
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("thebigdatashow").getOrCreate()


# COMMAND ----------

class FirstWorkFlow:
    def __init__(self):
        pass

    def runner(self):
        
        #Step 1: Extract all required data from different sources
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Step2: Implement the transformation logic
        # Customers who have bought Airpods after buying the iPhone --> check
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputDFs)

        #Step 3: Load all required data  to different sink
        AirpodsAfterIPhoneLoader().sink(firstTransformedDF)

# COMMAND ----------

class SecondWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought both Airpods and  iPhone
    """
    def __init__(self):
        pass

    def runner(self):
        
        #Step 1: Extract all required data from different sources
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Step2: Implement the transformation logic
        # Customers who have bought only Airpods and  iPhone nothing else
        secondTransformedDF = OnlyAirpodsAndIphoneTransformer().transform(inputDFs)

        #Step 3: Load all required data  to different sink
        OnlyAirpodsAndIphone(secondTransformedDF).sink()



# COMMAND ----------

# MAGIC %md
# MAGIC -> List all the products bought by customers after their initial purchase
# MAGIC -> Determine the avg time delay buying an iphone and buying airpods for each customer
# MAGIC -> Identify the top 3 selling products in each category by total revenue

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self,name):
        self.name = name
    def runner(self):
        if self.name == "firstworkflow":
            return FirstWorkFlow().runner()
        elif self.name == "secondworkflow":
            return SecondWorkFlow().runner()

# name = "firstworkflow"
name = "secondworkflow"

workflowRunner = WorkFlowRunner(name).runner()