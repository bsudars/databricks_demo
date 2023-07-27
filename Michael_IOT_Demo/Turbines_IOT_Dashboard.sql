-- Databricks notebook source
-- DBTITLE 1,Count of Turbines
select count(distinct turbine_id) from db_michael_iot.turbine 
where (len(lat) > 5 or len(long) > 5);

-- COMMAND ----------

-- DBTITLE 1,Count of Turbines By Location
select location,count(distinct turbine_id),cast(`lat` as integer), cast(`long` as integer) from db_michael_iot.turbine 
where (len(lat) > 5 or len(long) > 5)
group by location,cast(`lat` as integer), cast(`long` as integer)
order by location

-- COMMAND ----------

-- DBTITLE 1,Total energy produced & Total revenue
select sum(energy) as Energy_produced, round(sum(energy) * 110 / 1000000,2)  as Revenue_Generated 
from 
db_michael_iot.turbine a
join
db_michael_iot.sensor_bronze b
on a.turbine_id = b.turbine_id
where (len(lat) > 5 or len(long) > 5);

-- COMMAND ----------

-- DBTITLE 1,Top 10 Energy Produced and generated revenue by Location
select a.location, sum(energy) as Energy_produced, round(sum(energy) * 110 / 1000000,2) as Revenue_Generated 
from 
db_michael_iot.turbine a
join
db_michael_iot.sensor_bronze b
on a.turbine_id = b.turbine_id
where (len(lat) > 5 or len(long) > 5)
group by a.location
order by sum(energy) desc limit 10;

-- COMMAND ----------

-- DBTITLE 1,Faulty Turbines by Location
select a.location,count(distinct a.turbine_id) as Faulty_turbines from
db_michael_iot.turbine a
join 
db_michael_iot.historical_turbine_status b 
on a.turbine_id = b.turbine_id
where abnormal_sensor <> 'ok'
group by a.location

-- COMMAND ----------

-- DBTITLE 1,Energy Production by Location Hourly
select a.location,b.hourly_timestamp as Hourly, sum(b.avg_energy) as Energy from
db_michael_iot.turbine a
join 
db_michael_iot.sensor_hourly b 
on a.turbine_id = b.turbine_id
group by a.location,a.turbine_id,b.hourly_timestamp
order by sum(b.avg_energy) desc

