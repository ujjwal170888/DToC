# Databricks notebook source
# MAGIC %md
# MAGIC # Define label for churn

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.functions import col, expr, when
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read tables

# COMMAND ----------

trans_cust_card_df = spark.sql('select * from dtoc_db.dtoc_trans_cust_card')
cust_df= spark.table('dtoc_db.dtoc_customer_master')
#card_df = spark.table('dtoc_db.dtoc_cards_master')

# COMMAND ----------

trans_cust_card_df.display()

# COMMAND ----------

display(cust_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer related features

# COMMAND ----------

# MAGIC %md
# MAGIC ### Age

# COMMAND ----------

customer_df = trans_cust_card_df.withColumn('age_months',round(datediff(current_date(),col("customer_created_date"))/30,0) )

# COMMAND ----------

display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average monthly Transaction value

# COMMAND ----------

#customer_df = customer_df.withColumn('transaction_frequency_avg_per_month',avg())
customer_sum_transac_df = trans_cust_card_df.groupBy("customer_id").agg(sum(col('transaction_amount')).alias('total_transaction_value'))\
.join(customer_df,['customer_id'], 'left')\
.withColumn('avg_mnthly_txn_value',round(col('total_transaction_value')/col('age_months'),2))

# COMMAND ----------

transaction_count_by_cust = customer_sum_transac_df.groupby("customer_id")\
.count()\
.withColumnRenamed('count','total_txn_count')\
.join(customer_sum_transac_df,['customer_id'], 'left')\
.withColumn('avg_mnthly_txn_count',round(col('total_txn_count')/col('age_months'),2))
display(transaction_count_by_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Card Count

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW card_counts AS
# MAGIC select 
# MAGIC tot.*,
# MAGIC IFNULL(act.active_card_count,0) as active_card_count,
# MAGIC IFNULL(inact.inactive_card_count,0) as inactive_card_count,
# MAGIC IFNULL(susp.suspended_card_count,0) as suspended_card_count
# MAGIC 
# MAGIC from
# MAGIC   (select 
# MAGIC   customer_id, 
# MAGIC   count(distinct(card_id)) as total_card_count 
# MAGIC   from dtoc_db.dtoc_trans_cust_card
# MAGIC   group by customer_id ) tot
# MAGIC   
# MAGIC   left join
# MAGIC     (select 
# MAGIC     customer_id, 
# MAGIC     count(distinct(card_id)) as active_card_count 
# MAGIC     from dtoc_db.dtoc_trans_cust_card
# MAGIC     where card_status = 'Active'
# MAGIC     group by customer_id) act
# MAGIC   on tot.customer_id = act.customer_id
# MAGIC   
# MAGIC   left join
# MAGIC     (select 
# MAGIC     customer_id, 
# MAGIC     count(distinct(card_id)) as inactive_card_count 
# MAGIC     from dtoc_db.dtoc_trans_cust_card
# MAGIC     where card_status = 'Inactive'
# MAGIC     group by customer_id) inact
# MAGIC   on tot.customer_id = inact.customer_id
# MAGIC 
# MAGIC   left join
# MAGIC     (select 
# MAGIC     customer_id, 
# MAGIC     count(distinct(card_id)) as suspended_card_count 
# MAGIC     from dtoc_db.dtoc_trans_cust_card
# MAGIC     where card_status = 'Suspended'
# MAGIC     group by customer_id) susp
# MAGIC   on tot.customer_id = susp.customer_id

# COMMAND ----------

card_count_df = spark.table('card_counts')

total_card_count_by_cust =  transaction_count_by_cust\
.join(card_count_df,['customer_id'], 'left')

display(total_card_count_by_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Site count

# COMMAND ----------

cust_site_cnt =  total_card_count_by_cust\
.select(['customer_id','site_id'])\
.distinct()\
.groupby(col('customer_id'))\
.count()\
.withColumnRenamed('count','visiting_site_count')
display(cust_site_cnt)

# COMMAND ----------

cust_site_cnt_with_prev = total_card_count_by_cust.join(cust_site_cnt, ['customer_id'], 'left')
display(cust_site_cnt_with_prev)

# COMMAND ----------

# MAGIC %md
# MAGIC ### product wise transactions counts

# COMMAND ----------

prd_df =  cust_site_cnt_with_prev.select(['customer_id','product_name','product_type','transaction_amount'])
prd_type_txns_cnt = prd_df\
.groupby('customer_id','product_type').count()\
.groupBy("customer_id").pivot("product_type").sum("count")\
.fillna(0)
for colmn in prd_type_txns_cnt.columns:
    if 'customer' not in colmn:
        prd_type_txns_cnt = prd_type_txns_cnt.withColumnRenamed(colmn,colmn+"_txn_cnt")
        
cust_site_cnt_with_prev_v2 =  cust_site_cnt_with_prev.join(prd_type_txns_cnt,['customer_id'],'left')
display(cust_site_cnt_with_prev_v2)

# COMMAND ----------

cols = ['customer_id','registered_country','SIC_code','banding','office_head_quarter','employee_count','net_worth_million_dollar','profit_loss_million_dollar','energy_buying_strength','transportaion_buying_strength','IT_buying_strength','company_stage','legal_structure','customer_created_date','age_months','total_txn_count','total_transaction_value','avg_mnthly_txn_value','avg_mnthly_txn_count','total_card_count','active_card_count','inactive_card_count','suspended_card_count','visiting_site_count','Fee_txn_cnt','Fuel_txn_cnt','Service_txn_cnt']

customer_detailed_df = cust_site_cnt_with_prev_v2.select(*cols).distinct()
display(customer_detailed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Churn Column -- >= 50% of card is inactive

# COMMAND ----------

churn_rule = when( col('inactive_card_count')/col("total_card_count") >=0.5, 'Y').otherwise('N')

customer_detailed_labelled_df  = customer_detailed_df.withColumn('churn',churn_rule)

# COMMAND ----------

display(customer_detailed_labelled_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer importance score

# COMMAND ----------

#customer_detailed_labelled_df = spark.table('dtoc_db.dtoc_customer_features')

# COMMAND ----------

'''
All value transform within range of 0-1
avg(energy_buying_strength
transportaion_buying_strength
IT_buying_strength)

company_stage - stable 2 , growing 3, decline 1

avg_mnthly_txn_value
avg_mnthly_txn_count

age_months

total_card_count
'''

# COMMAND ----------

customer_detailed_labelled_df_score =  customer_detailed_labelled_df\
.withColumn('buy_strength', (f.col('energy_buying_strength') + f.col('transportaion_buying_strength') + f.col('IT_buying_strength'))/3)\
.withColumn('company_stage_score', when(f.col('company_stage') == 'decline', 1)\
           .when(f.col('company_stage') == 'stable', 2)\
           .when(f.col('company_stage') == 'growing', 3)\
           .otherwise(0))\
.select(['customer_id','buy_strength','company_stage_score','avg_mnthly_txn_value','avg_mnthly_txn_count','age_months','total_card_count'])\
.toPandas()

# COMMAND ----------

customer_detailed_labelled_df_score

# COMMAND ----------

from sklearn.preprocessing import StandardScaler, MinMaxScaler
import pandas as pd

features = ['buy_strength','company_stage_score','avg_mnthly_txn_value','avg_mnthly_txn_count','age_months','total_card_count']
#autoscaler = StandardScaler()
#customer_detailed_labelled_df_score[features] = autoscaler.fit_transform(customer_detailed_labelled_df_score[features])

scaler = MinMaxScaler()
# transform data
customer_detailed_labelled_df_score[features] = scaler.fit_transform(customer_detailed_labelled_df_score[features])


# COMMAND ----------

customer_detailed_labelled_df_score

# COMMAND ----------

customer_detailed_labelled_df_score['importance_score'] = customer_detailed_labelled_df_score[features].sum(axis=1).round(2)
customer_detailed_labelled_df_score

# COMMAND ----------

customer_detailed_labelled_df_final = customer_detailed_labelled_df.join(spark.createDataFrame(customer_detailed_labelled_df_score).select(['customer_id','importance_score']),['customer_id'], 'left')
display(customer_detailed_labelled_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write detailed customer profile to DBFS

# COMMAND ----------

customer_detailed_labelled_df_final.write\
  .mode("overwrite")\
  .option("overewriteschema", "true")\
  .option('checkLatestSchemaOnRead','false')\
  .saveAsTable("dtoc_db.dtoc_customer_features")

# COMMAND ----------


