# Databricks notebook source
from pyspark.sql import functions as F 

# COMMAND ----------

display(spark.table('dtoc_db.dtoc_customer_features'))

# COMMAND ----------

trans_cust_card_df = spark.sql('select * from dtoc_db.dtoc_trans_cust_card')
cust_feat = spark.table('dtoc_db.dtoc_customer_features')
final_df = trans_cust_card_df.join(cust_feat.select(['customer_id','churn']),['customer_id'],'left')
display(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

churned_pred_df = spark.table("dtoc_db.dtoc_customer_churn_pred")
display(churned_pred_df)

# COMMAND ----------


