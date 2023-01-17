# Databricks notebook source
# MAGIC %md 
# MAGIC Learn with Demo 
# MAGIC 
# MAGIC What:
# MAGIC   1. How to load data into Spark
# MAGIC   2. Howe to query data into Spark
# MAGIC 
# MAGIC Methods:
# MAGIC   1. Using SparkSQL
# MAGIC   2. Using DataFrame API

# COMMAND ----------

# MAGIC %md
# MAGIC DF:
# MAGIC   1. is runtime in-memory object
# MAGIC   2. supports schema on read
# MAGIC   3. stores metadata in runtime catalog
# MAGIC   4. does not support SQL
# MAGIC   5. only supports DF API
# MAGIC 
# MAGIC Spark table:
# MAGIC   1. stores schema info in metadata store
# MAGIC   2. tables and metadata are presistent objects and visible across applications
# MAGIC   3. we create tables with a a predefined table schema
# MAGIC   4. table supports sql expressions and does not support API
# MAGIC   
# MAGIC Spark DF:
# MAGIC   1. df stores schema information in runtime catalog
# MAGIC   2. df and catalog are runtime objects and live only during the application runtime. df is visible to your application only
# MAGIC   3. dataframe supports schema on read
# MAGIC   4. df offers APIs and does not support sql expressions
# MAGIC   
# MAGIC **We can convert a table to a df and vice versa

# COMMAND ----------

# MAGIC %md Creating a DF

# COMMAND ----------

fire_df = spark.read \ #This is a sparkSession object, sparkSession is the entry for the Spark API.
          .format("csv") \
          .option("header","true") \
          .option("inferschema","true") \
          .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

fire_df.show(10) #Using show() method to show the df results

# COMMAND ----------

display(fire_df) #Function offered by Databricks for better viewing

# COMMAND ----------

fire_df.createGlobalTempView("fire_service_calls_view") #using createGlobalTempView method of object DataFrame to convert the DF into a View so that we can run SQL Expressions

# COMMAND ----------

# MAGIC %sql select * from global_temp.fire_service_calls_view

# COMMAND ----------


