-- Databricks notebook source
CREATE STREAMING LIVE TABLE turbine1
COMMENT "Turbine details, with location, wind turbine model type etc"
AS SELECT * FROM cloud_files("/demos/manufacturing/iot_turbine/turbine", "json", map("cloudFiles.inferColumnTypes" , "true"));
