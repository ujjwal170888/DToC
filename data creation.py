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

# MAGIC %md
# MAGIC # Constants

# COMMAND ----------

#CONSTANTS
COUNTRY_LIST= ['Poland','Ukraine','UK','Germany','Netherlands']
BANDING = ['Gold','Silver','Bronze']
SIC_CODES = ['transportaion','agriculture','manufacturing','service','real estate','whole sale trading','delivery','business process','energy']
GROWTH_STAGE = ['decline','stable','growing']
LEGAL = ['Private ltd.','cooperation','government','partnership','NGO','proprietary']

#PRODUCTS = ['petrol','Hydrogen','diesel','CNG','lubricant','coolant','food','vehicle servicing','telematics','toll fee']

PRODUCTS = {'Fuel':['petrol','Hydrogen','diesel','CNG'],
            'Fee': ['telematics','toll fee'],
            'Service':['lubricant','coolant','food','vehicle servicing']
           }

SITE_ID = {'Poland':range(1,50),
           'Ukraine':range(51,65),
           'UK':range(66,80),
           'Germany':range(81,105),
           'Netherlands':range(110,138)
          }

PURCHASE_UNIT = {
    'Fuel':'Litre',
    'Fee' : 'Count',
    'Service':'Count'
}

PURCHASE_QTY = {
    'Fuel': range(50,200),
    'Fee' : range(1,2),
    'Service': range(1,10)
}

TRANSACTION_AMOUNT ={
    'Fuel': range(100,500),
    'Fee' : range(20,80),
    'Service': range(100,1000)
}

# COMMAND ----------

random.choice(SITE_ID['Poland'])

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
        no_of_records = 100000
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

dtoc_customer_master_df=spark.sql('select * from dtoc_db.dtoc_customer_master')

# COMMAND ----------

display(dtoc_customer_master_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtoc_db.dtoc_customer_master

# COMMAND ----------

#cid = '6b199e90-6eed-480d-a05b-600300196fda'
#x = dtoc_customer_master_df.filter(dtoc_customer_master_df.customer_id == cid).select('insert_timestamp').rdd.flatMap(lambda x: x).collect()[0]
#x

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for dtoc_cards

# COMMAND ----------

#x = dtoc_customer_master_df.select('customer_id').rdd.flatMap(lambda x: x).collect()
#x[0]

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
        
        for i in range(0,int(no_of_records*2.5)):
            record = []
            record.append(random.choice(customer_ids))
            record.append(str(uuid.uuid4()))
            record.append(random.choice(CARD_STATUS))
            #customer_start_date = dtoc_customer_master_df.filter(dtoc_customer_master_df.customer_id == customer_ids[i]).select('insert_timestamp').rdd.flatMap(lambda x: x).collect()[0]
            record.append('1900-01-01')
            record.append('2099-01-01')
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

# COMMAND ----------

dtoc_cards_df = prepare_data_for_dtoc_cards()
dtoc_cards_df.orderBy("customer_id").display()


# COMMAND ----------

dtoc_cards_df.createOrReplaceTempView('dtoc_cards_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtoc_cards_view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW CARD_UPDATED AS 
# MAGIC select 
# MAGIC card.*,
# MAGIC cust.created_date AS card_start_date,
# MAGIC cust.valid_upto_date as customer_valid_upto_date,
# MAGIC CASE
# MAGIC     WHEN card.card_status = 'Inactive' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, cust.created_date, now())), cust.created_date)
# MAGIC     ELSE ""
# MAGIC END AS card_inactive_date,
# MAGIC CASE
# MAGIC     WHEN card.card_status = 'Suspended' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, cust.created_date, now())), cust.created_date)
# MAGIC     ELSE ""
# MAGIC END AS card_suspend_date,
# MAGIC CASE
# MAGIC     WHEN card.card_status = 'Active' THEN cust.valid_upto_date
# MAGIC     WHEN card.card_status = 'Inactive' THEN "1900-01-01"
# MAGIC     WHEN card.card_status = 'Suspended' THEN cust.valid_upto_date
# MAGIC     ELSE ""
# MAGIC END AS card_valid_upto_date
# MAGIC 
# MAGIC from dtoc_cards_view card
# MAGIC left join dtoc_db.dtoc_customer_master cust
# MAGIC on card.customer_id = cust.customer_id

# COMMAND ----------

dtoc_cards_df_updated =  spark.table('CARD_UPDATED')

# COMMAND ----------

display(dtoc_cards_df_updated)

# COMMAND ----------


dtoc_cards_df_updated.write \
  .mode("overwrite") \
  .option("overewriteschema", "true") \
  .saveAsTable("dtoc_db.dtoc_cards_master")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtoc_db.dtoc_cards_master

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtoc_db.dtoc_cards_master
# MAGIC where card_id ='0c323a57-e813-49d5-9eb9-e94783d13cdb'

# COMMAND ----------

# MAGIC %sql
# MAGIC select card.card_id,
# MAGIC 
# MAGIC CASE
# MAGIC   WHEN card.card_status = 'Active' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, card.card_start_date, now())), card.card_start_date)
# MAGIC   WHEN card.card_status = 'Inctive' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, card.card_start_date, card.card_inactive_date)), card.card_start_date)
# MAGIC   WHEN card.card_status = 'Suspended' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, card.card_start_date, card.card_suspend_date)), card.card_start_date)
# MAGIC END AS transaction_date
# MAGIC from dtoc_db.dtoc_cards_master card

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare data for dtoc_transactions

# COMMAND ----------

dtoc_cards_master_df =  spark.sql('select * from dtoc_db.dtoc_cards_master')

# COMMAND ----------

def prepare_data_for_dtoc_cards_transaction():
    '''
    '''
    try:
        values = []
        # Create a spark session
        spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
        column_list = ['transaction_id','card_id','product_name','product_type','purchase_quantity','unit','transaction_amount','transaction_currency','transaction_country','transaction_city','site_id']
        # later to add columns - 'transaction_timestamp','insert_timestamp','update_timestamp'
        card_ids = dtoc_cards_master_df.select('card_id').rdd.flatMap(lambda x: x).collect()
        no_of_records =  len(card_ids)
        for i in range(0,int(no_of_records)):
        #for i in range(0,1):
            record = []
            record.append(str(uuid.uuid4()))
            record.append(random.choice(card_ids))
            product_type = random.choice(list(PRODUCTS.keys()))
            product_name = random.choice(PRODUCTS[product_type])
            record.append(product_name) 
            record.append(product_type)
            record.append(random.choice(PURCHASE_QTY[product_type]))
            record.append(PURCHASE_UNIT[product_type]) 
            record.append(random.choice(TRANSACTION_AMOUNT[product_type]))
            record.append('EUR')
            country = random.choice(COUNTRY_LIST)
            record.append(country)
            record.append("")
            record.append(random.choice(SITE_ID[country]))
            rec_tuple= tuple(record)
            #print(rec_tuple)
            values.append(rec_tuple)
            #print('values = ', values)
        dtoc_cards_transaction_df = spark.createDataFrame(values, column_list)
        return dtoc_cards_transaction_df
    except Exception as e:
        print(e)

# COMMAND ----------

for i in range(0,5):
    dtoc_cards_transaction_df = prepare_data_for_dtoc_cards_transaction()
    #dtoc_cards_transaction_df.write \
    #.mode("append") \
    #.saveAsTable("dtoc_db.dtoc_cards_transaction")
#dtoc_cards_transaction_df_v2 = prepare_data_for_dtoc_cards_transaction()
#dtoc_cards_transaction_df.orderBy('card_id').display()

# COMMAND ----------

#'''


# COMMAND ----------

#transaction_df_union = dtoc_cards_transaction_df.unionByName(dtoc_cards_transaction_df_v2)
#dtoc_cards_transaction_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtoc_db.dtoc_cards_transaction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add dates to transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW TRANS_DATE AS 
# MAGIC select 
# MAGIC trans.transaction_id,
# MAGIC trans.card_id,
# MAGIC trans.product_name,
# MAGIC trans.product_type,
# MAGIC trans.purchase_quantity,
# MAGIC trans.unit,
# MAGIC trans.transaction_amount,
# MAGIC trans.transaction_currency,
# MAGIC trans.transaction_country,
# MAGIC trans.transaction_city,
# MAGIC trans.site_id,
# MAGIC CASE
# MAGIC   WHEN card.card_status = 'Active' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, card.card_start_date, now())), card.card_start_date)
# MAGIC   WHEN card.card_status = 'Inactive' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, card.card_start_date, card.card_inactive_date)), card.card_start_date)
# MAGIC   WHEN card.card_status = 'Suspended' THEN DATEADD(DAY, RAND()*(1+DATEDIFF(DAY, card.card_start_date, card.card_suspend_date)), card.card_start_date)
# MAGIC END AS transaction_date_time,
# MAGIC now() as insert_timestamp,
# MAGIC now() as update_timestamp
# MAGIC 
# MAGIC from dtoc_db.dtoc_cards_transaction trans
# MAGIC left join dtoc_db.dtoc_cards_master card
# MAGIC on trans.card_id = card.card_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TRANS_DATE

# COMMAND ----------

dtoc_cards_transaction_date_df = spark.table('TRANS_DATE')


'''
dtoc_cards_transaction_date_df.write \
.mode("overwrite") \
.option("overwriteSchema", "true")\
.saveAsTable("dtoc_db.dtoc_cards_transaction")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dtoc_db.dtoc_cards_transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC Distinct trans.transaction_country,
# MAGIC trans.site_id
# MAGIC from dtoc_db.dtoc_cards_transaction trans
# MAGIC 
# MAGIC LEFT JOIN dtoc_db.dtoc_cards_master card
# MAGIC on trans.card_id = card.card_id
# MAGIC 
# MAGIC left join dtoc_db.dtoc_customer_master cust
# MAGIC on card.customer_id =  cust.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC trans.*
# MAGIC from dtoc_db.dtoc_cards_transaction trans
# MAGIC 
# MAGIC LEFT JOIN dtoc_db.dtoc_cards_master card
# MAGIC on trans.card_id = card.card_id
# MAGIC 
# MAGIC 
# MAGIC where card.card_status = 'Inactive'
# MAGIC --and transaction_date_time is not null

# COMMAND ----------

# MAGIC %md
# MAGIC ### dtoc_db.dtoc_trans_cust_card 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW TRANSACT_CUST AS
# MAGIC select 
# MAGIC trans.transaction_id,
# MAGIC trans.card_id,
# MAGIC trans.product_name,
# MAGIC trans.product_type,
# MAGIC trans.purchase_quantity,
# MAGIC trans.unit,
# MAGIC trans.transaction_amount,
# MAGIC trans.transaction_currency,
# MAGIC CASE 
# MAGIC   WHEN trans.transaction_country= 'UK' THEN 'United Kingdom'
# MAGIC   ELSE trans.transaction_country
# MAGIC END AS transaction_country,
# MAGIC trans.transaction_city,
# MAGIC trans.site_id,
# MAGIC trans.transaction_date_time,
# MAGIC card.card_status,
# MAGIC card.card_inactive_date,
# MAGIC card.card_suspend_date,
# MAGIC cust.customer_id,
# MAGIC CASE 
# MAGIC   WHEN cust.registered_country= 'UK' THEN 'United Kingdom'
# MAGIC   ELSE cust.registered_country
# MAGIC END AS registered_country,
# MAGIC cust.region,
# MAGIC cust.account_manager,
# MAGIC cust.SIC_code,
# MAGIC cust.banding,
# MAGIC cust.office_head_quarter,
# MAGIC cust.employee_count,
# MAGIC cust.`net_worth_M$` as net_worth_million_dollar,
# MAGIC cust.`profit_loss_M$` as profit_loss_million_dollar,
# MAGIC cust.energy_buying_strength,
# MAGIC cust.transportaion_buying_strength,
# MAGIC cust.IT_buying_strength,
# MAGIC cust.company_stage,
# MAGIC cust.legal_structure,
# MAGIC cust.created_date as customer_created_date
# MAGIC from dtoc_db.dtoc_cards_transaction trans
# MAGIC LEFT JOIN dtoc_db.dtoc_cards_master card
# MAGIC on trans.card_id = card.card_id
# MAGIC left join dtoc_db.dtoc_customer_master cust
# MAGIC on card.customer_id =  cust.customer_id

# COMMAND ----------

transact_cust_df =  spark.table('TRANSACT_CUST')

# COMMAND ----------

'''
transact_cust_df.write \
  .mode("overwrite") \
  .option("overewriteschema", "true") \
  .saveAsTable("dtoc_db.dtoc_trans_cust_card")
