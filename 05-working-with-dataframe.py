# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

raw_fire_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - column names are case insensitive but its good to have a standardized naming convention. Above df has spaces in name, lets fix that

# COMMAND ----------

remaned_fire_df = raw_fire_df \
    .withColumnRenamed("Call Number", "CallNumber") \
    .withColumnRenamed("Unit ID", "UnitID") \
    .withColumnRenamed("Incident Number", "IncidentNumber") \
    .withColumnRenamed("Call Date", "CallDate") \
    .withColumnRenamed("Watch Date", "WatchDate") \
    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
    .withColumnRenamed("Available DtTm", "AvailableDtTm") \
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")

# COMMAND ----------

display(remaned_fire_df)

# COMMAND ----------

remaned_fire_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - AvailableDtTm is in string format, lets fix that
# MAGIC - Delay round to 2 decimal

# COMMAND ----------

fire_df = remaned_fire_df \
    .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Delay", round("Delay", 2))

# COMMAND ----------

display(fire_df)

# COMMAND ----------

#Caching the DF
fire_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q1. How many distinct types of calls were made to the Fire Department?
# MAGIC 
# MAGIC ``` sql
# MAGIC select count(distinct CallType) as distinct_call_type_count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC ```

# COMMAND ----------

# Method 1 to solve Q1 using the SQL way
fire_df.createOrReplaceTempView("fire_service_calls_views")

# COMMAND ----------

q1_sql_df = spark.sql("""
            select count(distinct calltype) as distinct_call_type_count
            from fire_service_calls_views
            where calltype is not null
            """)

display(q1_sql_df)

# COMMAND ----------

q1_df = fire_df.where("calltype is not null") \
              .select("calltype") \
              .distinct()

print(q1_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q2. What were distinct types of calls made to the Fire Department?
# MAGIC ```sql
# MAGIC select distinct CallType as distinct_call_types
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC ```

# COMMAND ----------

q2_df = fire_df.where("calltype is not null") \
              .select(expr("calltype as distinct_call_type")) \
              .distinct() 

q2_df.show()

# COMMAND ----------

display(q2_df) #This is a Databricks function not an Apache Spark function

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q3. Find out all response for delayed times greater than 5 mins?
# MAGIC ``` sql
# MAGIC select CallNumber, Delay
# MAGIC from fire_service_calls_tbl
# MAGIC where Delay > 5
# MAGIC ```

# COMMAND ----------

q3_df = fire_df.where("delay > 5") \
                .select("callnumber", "delay") \
                .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q4. What were the most common call types?
# MAGIC ```sql
# MAGIC select CallType, count(*) as count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC group by CallType
# MAGIC order by count desc
# MAGIC ```

# COMMAND ----------

q4_df = fire_df.select("CallType") \
    .where("CallType is not null") \
    .groupBy("CallType") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q5. What zip codes accounted for most common calls?
# MAGIC ```sql
# MAGIC select CallType, ZipCode, count(*) as count
# MAGIC from fire_service_calls_tbl
# MAGIC where CallType is not null
# MAGIC group by CallType, Zipcode
# MAGIC order by count desc
# MAGIC ```

# COMMAND ----------

q5_df = fire_df.select("calltype","zipcode") \
  .where("calltype is not null") \
  .groupBy("calltype", "zipcode") \
  .count() \
  .orderBy("count", ascending = False) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q6. What San Francisco neighborhoods are in the zip codes 94102 and 94103
# MAGIC ```sql
# MAGIC select distinct Neighborhood, Zipcode
# MAGIC from fire_service_calls_tbl
# MAGIC where Zipcode== 94102 or Zipcode == 94103
# MAGIC ```

# COMMAND ----------

q6_df = fire_df.select("neighborhood", "zipcode") \
  .where("Zipcode = 94102 or Zipcode = 94103") \
  .distinct() \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q7. What was the sum of all calls, average, min and max of the response times for calls?
# MAGIC ```sql
# MAGIC select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
# MAGIC from fire_service_calls_tbl
# MAGIC ```

# COMMAND ----------

q7_df = fire_df.select(expr("sum(numalarms)"), expr("avg(delay)"), expr("min(delay)"), expr("max(delay)")) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q8. How many distinct years of data is in the CSV file?
# MAGIC ```sql
# MAGIC select distinct year(to_timestamp(CallDate, "MM/dd/yyyy")) as year_num
# MAGIC from fire_service_calls_tbl
# MAGIC order by year_num
# MAGIC ```

# COMMAND ----------

q8_df = fire_df.select(expr("year(to_timestamp(CallDate)) as year_num")) \
  .distinct() \
  .orderBy("year_num") \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q9. What week of the year in 2018 had the most fire calls?
# MAGIC ```sql
# MAGIC select weekofyear(to_timestamp(CallDate)) week_year, count(*) as count
# MAGIC from fire_service_calls_tbl 
# MAGIC where year(to_timestamp(CallDate)) = 2018
# MAGIC group by week_year
# MAGIC order by count desc
# MAGIC ```

# COMMAND ----------

q9_df = fire_df.select(expr("weekofyear(to_timestamp(CallDate)) week_year")) \
  .where("year(to_timestamp(CallDate)) = 2018") \
  .groupBy("week_year") \
  .count() \
  .orderBy("count", ascending = False) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Q10. What neighborhoods in San Francisco had the worst response time in 2018?
# MAGIC ```sql
# MAGIC select Neighborhood, Delay
# MAGIC from fire_service_calls_tbl 
# MAGIC where year(to_timestamp(CallDate)) = 2018
# MAGIC ```

# COMMAND ----------

q10_df = fire_df.select("neighborhood", "delay") \
  .where("year(to_timestamp(CallDate)) = 2018") \
  .show()
