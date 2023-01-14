# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
import random
from datetime import datetime, date
from faker import Faker


# COMMAND ----------

fake = Faker()

#date_fake = datetime.strftime(fake.date_between(start_date='-3y', end_date='today'),'%d/%m/%Y')
print( fake.date_between(start_date='-3y', end_date='today'))

# COMMAND ----------

#CONSTANTS
COUNTRY_LIST= ['Poland','Ukraine','UK','Germany','Netherlands']
BANDING = ['Gold','Silver','Bronze']
SIC_CODES = ['transportaion','agriculture','manufacturing','service','real estate','whole sale trading','delivery','business process','energy']
GROWTH_STAGE = ['decline','stable','growing']
LEGAL = ['Private ltd.','cooperation','government','partnership','NGO','proprietary']

# COMMAND ----------

# MAGIC %md
# MAGIC # create Database

# COMMAND ----------

db_name = "dtoc_db"
#spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare data for dtoc_customer_master table

# COMMAND ----------

def prepare_data_for_dtoc_customer_master():
    '''
    '''
    try:
        column_list = ['customer_id','registered_country','region','account_manager','SIC_code','created_date','valid_upto_date','banding','office_head_quarter','employee_count','net_worth_M$','profit_loss_M$','energy_buying_strength','transportaion_buying_strength','IT_buying_strength','company_stage','legal_structure','insert_timestamp','update_timestamp']
        values = []
        no_of_records = 1000000
        # Create a spark session
        spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
        # Create an empty RDD
        #emp_RDD = spark.sparkContext.emptyRDD()
        # Create an expected schema
        #columns = StructType([])
        # Create an empty RDD with empty schema
        #dtoc_customer_master_df = spark.createDataFrame(data = emp_RDD, schema = columns)
        '''
        columns = ['Identifier', 'Value', 'Extra Discount']
        vals = [(1, 150, 0), (2, 160, 12)]
        df = spark.createDataFrame(vals, columns)
        '''
        for i in range(0,no_of_records):
            record = []
            record.append(str(uuid.uuid4()))
            record.append(random.choice(COUNTRY_LIST))
            record.append('EU')
            record.append(random.choice(range(100,500)))
            record.append(random.choice(SIC_CODES))
            record.append(fake.date_between(start_date='-3y', end_date='today'))
            record.append(fake.date_between(start_date='-2y', end_date='+5y'))
            record.append(random.choice(BANDING))
            record.append(record[1])
            record.append(random.choice(range(100,5000)))
            record.append(random.choice(range(1000,50000)))
            record.append(random.choice(range(-1000,50000)))
            record.append(random.choice(range(1,100)))
            record.append(random.choice(range(1,100)))
            record.append(random.choice(range(1,100)))
            record.append(random.choice(GROWTH_STAGE))
            record.append(random.choice(LEGAL))
            record.append(datetime.now())
            record.append(datetime.now())
            
            rec_tuple= tuple(record)
            #print(rec_tuple)
            values.append(rec_tuple)
            #print('values = ', values)
        dtoc_customer_master_df = spark.createDataFrame(values, column_list)
        return dtoc_customer_master_df
        
    except Exception as e:
        print(e)
    

# COMMAND ----------

#if __name__ == '__main__':
dtoc_customer_master_df = prepare_data_for_dtoc_customer_master()

dtoc_customer_master_df.display()

# COMMAND ----------

'''
dtoc_customer_master_df.write \
  .mode("overwrite") \
  .option("overewriteschema", "true") \
  .saveAsTable("dtoc_db.dtoc_customer_master")
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtoc_db.dtoc_customer_master

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for dtoc_cards

# COMMAND ----------

x = dtoc_customer_master_df.select('customer_id').rdd.flatMap(lambda x: x).collect()
x[0]

# COMMAND ----------

CARD_STATUS = ['Active','Suspended','Inactive']

# COMMAND ----------

def prepare_data_for_dtoc_cards():
    '''
    '''
    try:
        column_list = ['customer_id','card_id','card_status','effective_start_date','effective_end_date','insert_timestamp','update_timestamp']
        values = []
        # Create a spark session
        spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
        customer_ids = dtoc_customer_master_df.select('customer_id').rdd.flatMap(lambda x: x).collect()
        no_of_records = len(customer_ids)
        
        for i in range(0,no_of_records):
            record = []
            record.append(customer_ids[i])
            record.append(str(uuid.uuid4()))
            record.append(random.choice(CARD_status))
            record.append(fake.date_between(start_date='-3y', end_date='today'))
            record.append(fake.date_between(start_date='-2y', end_date='+5y'))
            record.append(random.choice(BANDING))
            record.append(record[1])
            record.append(random.choice(range(100,5000)))
            record.append(random.choice(range(1000,50000)))
            record.append(random.choice(range(-1000,50000)))
            record.append(random.choice(range(1,100)))
            record.append(random.choice(range(1,100)))
            record.append(random.choice(range(1,100)))
            record.append(random.choice(GROWTH_STAGE))
            record.append(random.choice(LEGAL))
            record.append(datetime.now())
            record.append(datetime.now())
            
            rec_tuple= tuple(record)
            #print(rec_tuple)
            values.append(rec_tuple)
            #print('values = ', values)
        dtoc_cards_df = spark.createDataFrame(values, column_list)
        return dtoc_cards_df
        
    except Exception as e:
        print(e)
