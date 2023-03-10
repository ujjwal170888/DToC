-- Databricks notebook source
select * from 
dtoc_db.dtoc_customer_churn_pred
where date(prediction_date_time) = current_date

-- COMMAND ----------


