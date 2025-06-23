# Databricks notebook source
class Transformer:
    def __init__(self):
        pass

    def transform(self):
        pass

class AirpodsAfterIphoneTransformer(Transformer):
    def transform(self,inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """
        transactionInputDF = inputDFs.get('transactionInputDF')
        transactionInputDF.show()
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformDF = transactionInputDF.withColumn("next_product",lead("product_name").over(windowSpec))
        # print("Airpods after buying iPhone")
        # transformDF.show()

        #print("Filtered df")
        fileteredDF = transformDF.filter((col("product_name")=='iPhone') & (col("next_product")=="AirPods"))
        #fileteredDF.show()
        customerInputDF = inputDFs.get("customerInputDF")

        joinDF = customerInputDF.join(broadcast(fileteredDF),customerInputDF.customer_id==fileteredDF.customer_id,"inner")
        joinDF = joinDF.select(customerInputDF.customer_id,col("customer_name"),col("location"))

        print("Customers who have bought Airpods after buying the iPhone")
        joinDF.show()

        return joinDF

class OnlyAirpodsAndIphoneTransformer(Transformer):
    def transform(self,inputDFs):
        """
        Customers who have bought only Airpods and iPhone nothing else
        """
        transactionInputDF = inputDFs.get('transactionInputDF')
        groupedDF = transactionInputDF.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        # print("grouped df:")
        # groupedDF.show(truncate=False)

        filteredDF = groupedDF.filter(
            (array_contains(col("products"),"iPhone")) &
            (array_contains(col("products"),"AirPods")) &
            (size(col("products")) == 2)
        )
        # print("FileteredDF: ")
        # filteredDF.show()


        customerInputDF = inputDFs.get('customerInputDF')

        joinDF = customerInputDF.join(broadcast(filteredDF),"customer_id")

        joinDF = joinDF.select(col("customer_id"),col("customer_name"),col("location"))
        print("Customers who have bought only Airpods and iPhone nothing else:")
        joinDF.show()

        return joinDF
        