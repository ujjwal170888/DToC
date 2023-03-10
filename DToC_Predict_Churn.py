# Databricks notebook source
# MAGIC %md
# MAGIC # Feed Any User's data and predict if They may Churn

# COMMAND ----------

from pyspark.sql import functions as F 
from pyspark.sql.functions import lit,unix_timestamp
import random
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
import mlflow
import datetime, time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate random customer Data

# COMMAND ----------

non_churn_cust_df = spark.table('dtoc_db.dtoc_customer_features').filter(F.col('churn') == 'N').sample(False, 0.1, seed=0)
non_churn_cust_df.display()

# COMMAND ----------

modify_feature = ['avg_mnthly_txn_value','avg_mnthly_txn_count','active_card_count','inactive_card_count']


# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulating Rule/ Assumed business scenario

# COMMAND ----------

# MAGIC %md
# MAGIC   Rule 1:- In Ukraine Country 
# MAGIC 
# MAGIC       avg_mnthly_txn_value decreased by 50-1000%,
# MAGIC 
# MAGIC       avg_mnthly_txn_count decreased by 50-100%, 
# MAGIC 
# MAGIC   
# MAGIC   Rule 2:- In Germany 
# MAGIC   
# MAGIC       Randomly avg_mnthly_txn_value increased by 10% - 50%  

# COMMAND ----------

def rule_1(df):
    '''
    '''
    try:
        country = 'Ukraine'
        ukraine_cust = df.filter(F.col('registered_country') == country)
        #Randomly sample 30% of the data without replacement
        ukraine_cust_sample = ukraine_cust.sample(False, 0.5, seed=0)
        ukraine_cust_sample_simulated = ukraine_cust_sample\
        .withColumn('avg_mnthly_txn_value_simulated', F.col('avg_mnthly_txn_value')-(F.col('avg_mnthly_txn_value')* (random.choice(np.arange(0.5, 1, 0.05)))))\
        .withColumn('avg_mnthly_txn_count_simulated', F.col('avg_mnthly_txn_value')-(F.col('avg_mnthly_txn_value')* (random.choice(np.arange(0.5, 1, 0.05)))))\
        .withColumn('active_card_count_simulated', F.lit(0))\
        .withColumn('inactive_card_count_simulated', (F.col('total_card_count'))- (F.col('suspended_card_count')))\
        .withColumn('visiting_site_count_simulated', (F.col('visiting_site_count'))-(F.col('visiting_site_count')* (random.choice(np.arange(0.7, 1, 0.1)))))
        return(ukraine_cust_sample_simulated)
    except Exception as e:
        print(e)

'''       
def rule_2(df):

    try:
        country = 'Germany'
        Germany_cust = df.filter(F.col('registered_country') == country)
        #Randomly sample 30% of the data without replacement
        Germany_cust_sample = Germany_cust.sample(False, 0.5, seed=0)
        Germany_cust_sample_simulated = Germany_cust_sample\
        .withColumn('avg_mnthly_txn_value_simulated', F.col('avg_mnthly_txn_value')+(F.col('avg_mnthly_txn_value')* (random.choice(np.arange(0.1, 0.5, 0.05)))))\
        .withColumn('avg_mnthly_txn_count_simulated', F.col('avg_mnthly_txn_value')+(F.col('avg_mnthly_txn_value')* (random.choice(np.arange(0.1, 0.5, 0.05))))) 
        
        return(Germany_cust_sample_simulated)
    except Exception as e:
        print(e)
'''

# COMMAND ----------

rule_1_df = rule_1(non_churn_cust_df)
#display(rule_1_df)
#rule_2_df = rule_2(non_churn_cust_df)
simulated_df = rule_1_df 
simulated_df2 = simulated_df.drop(*['avg_mnthly_txn_value','avg_mnthly_txn_count','active_card_count','inactive_card_count','visiting_site_count','customer_created_date','churn'])
#display(simulated_df2)
for column in simulated_df2.columns:
    simulated_df2 = simulated_df2.withColumnRenamed(column, column.replace('_simulated',''))
#display(simulated_df3)

residual_df = non_churn_cust_df.join(simulated_df2.select(['customer_id']),['customer_id'],'leftanti').drop(*['customer_created_date','churn'])

simulated_residual_df = residual_df.unionByName(simulated_df2)
#simulated_residual_df = simulated_residual_df.drop(*['avg_mnthly_txn_value','avg_mnthly_txn_count','customer_created_date','churn'])
simulated_df_for_model_feed = simulated_residual_df.drop('customer_id','importance_score')




# COMMAND ----------

sim_cols = simulated_df_for_model_feed.columns

# COMMAND ----------

simulated_df_for_model_feed_pd = simulated_df_for_model_feed.toPandas()
cat_cols_filtered = ['registered_country', 'SIC_code', 'banding', 'office_head_quarter',
       'company_stage', 'legal_structure']

simulated_df_for_model_feed_pd_OHE = pd.get_dummies(simulated_df_for_model_feed_pd, columns =cat_cols_filtered)

# COMMAND ----------

#simulated_df_for_model_feed_pd_OHE

# COMMAND ----------

simulated_spark_df=spark.createDataFrame(simulated_df_for_model_feed_pd_OHE) 
for cols in simulated_spark_df.columns:
    simulated_spark_df = simulated_spark_df.withColumnRenamed(cols,cols.replace('.','').replace(' ','_'))
    
model_log_cols = ['total_txn_count',
 'total_transaction_value',
 'avg_mnthly_txn_value',
 'avg_mnthly_txn_count',
 'total_card_count',
 'active_card_count',
 'inactive_card_count',
 'suspended_card_count',
 'visiting_site_count',
 'Fee_txn_cnt',
 'Fuel_txn_cnt',
 'Service_txn_cnt']

for cols in model_log_cols:
    simulated_spark_df = simulated_spark_df.withColumnRenamed(cols,cols+'_log')
    
#display(simulated_spark_df)

# COMMAND ----------

display(simulated_spark_df)

# COMMAND ----------

simulated_spark_df_pd = simulated_spark_df.toPandas()

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/75abebcff4594d908c04758a9499b35d/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')


# COMMAND ----------

# Predict on a Spark DataFrame.
simulated_spark_df_predicted = simulated_spark_df.withColumn('predictions', loaded_model(struct(*map(col, simulated_spark_df.columns))))
display(simulated_spark_df_predicted)

# COMMAND ----------

pred_df = simulated_spark_df_predicted.select(['predictions'])

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# since there is no common column between these two dataframes add row_index so that it can be joined
pred_df=pred_df.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
simulated_residual_df=simulated_residual_df.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))

final_df = simulated_residual_df.join(pred_df, on=["row_index"]).drop("row_index")
display(final_df)

# COMMAND ----------


timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
churned_cust_pred =  final_df.filter(F.col('predictions') == 1)\
                            .withColumn('prediction_date_time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

display(churned_cust_pred)

# COMMAND ----------

churned_cust_pred.write \
  .mode("append") \
  .option("overewriteschema", "true") \
  .saveAsTable("dtoc_db.dtoc_customer_churn_pred")

# COMMAND ----------


