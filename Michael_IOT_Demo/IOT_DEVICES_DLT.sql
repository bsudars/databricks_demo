-- Databricks notebook source
CREATE STREAMING LIVE TABLE iot_devices (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "This JSON data contains geographic information,country name, lat & long"
AS SELECT * FROM cloud_files("/databricks-datasets/iot", "json", map("cloudFiles.inferColumnTypes" , "true"));
