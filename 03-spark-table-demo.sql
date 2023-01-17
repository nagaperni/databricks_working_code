-- Databricks notebook source
create database if not exists demo_db;

-- COMMAND ----------

show databases;

-- COMMAND ----------

use demo_db;

-- COMMAND ----------

create table if not exists demo_db.fire_service_calls_tbl(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) using parquet --Save the data in parquet file format

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl 
values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null) --This is not an optimal way in the world of big data.

-- COMMAND ----------

select * from fire_service_calls_tbl;

-- COMMAND ----------

truncate table fire_service_calls_tbl;

-- COMMAND ----------

--Spark SQL doesnt offer a delete but however Databricks offers and we will learn that in the Databricks module

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl
select * from global_temp.fire_service_calls_view;

-- COMMAND ----------

select * from fire_service_calls_tbl;

-- COMMAND ----------


