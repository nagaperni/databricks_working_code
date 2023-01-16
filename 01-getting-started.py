# Databricks notebook source
diamonds_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

diamonds_df.show(10)

# COMMAND ----------

from pyspark.sql.functions import avg

results_df = diamonds_df.select("color", "price") \
                .groupBy("color") \
                .agg(avg("price")) \
                .sort("color")

results_df.show()

# COMMAND ----------

display(results_df)
