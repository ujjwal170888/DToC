# Databricks notebook source
# MAGIC %md
# MAGIC # EDA

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

cust_df = spark.table('dtoc_db.dtoc_customer_features')
display(cust_df)

# COMMAND ----------

cust_df_pd = cust_df.toPandas()

# COMMAND ----------

# DBTITLE 1,Dataset INFO
cust_df_pd.info()

# COMMAND ----------

# DBTITLE 1,Unique value counts
cust_df_pd.nunique()

# COMMAND ----------

# DBTITLE 1,Missing Values
cust_df_pd.isna().sum()

# COMMAND ----------

# DBTITLE 1,Data Reduction
cust_df_pd = cust_df_pd.drop(['customer_id',], axis = 1)

# COMMAND ----------

cust_df_pd.describe(include='all').T

# COMMAND ----------

# DBTITLE 1,Separate Cat cols and Nm cols
cat_cols=cust_df_pd.select_dtypes(include=['object']).columns
num_cols = cust_df_pd.select_dtypes(include=np.number).columns.tolist()
print("Categorical Variables:")
print(cat_cols)
print("Numerical Variables:")
print(num_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Univariate Analysis

# COMMAND ----------

# DBTITLE 1,Numerical features
for col in num_cols:
    print(col)
    print('Skew :', round(cust_df_pd[col].skew(), 2))
    plt.figure(figsize = (15, 4))
    plt.subplot(1, 2, 1)
    cust_df_pd[col].hist(grid=False)
    plt.ylabel('count')
    plt.subplot(1, 2, 2)
    sns.boxplot(x=cust_df_pd[col])
    plt.show()

# COMMAND ----------

# DBTITLE 1,Categorical features
for col in cat_cols:
    print(col)
    
    plt.figure(figsize = (15, 4))
    plt.subplot(1, 2, 1)
    sns.countplot(x = col, data = cust_df_pd, color = 'blue', 
              order = cust_df_pd[col].value_counts().index)

    plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### outlier removal

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation for skewed variables

# COMMAND ----------

# Function for log transformation of the column
def log_transform(data,col):
    try:
        for colname in col:
            if (data[colname] == 1.0).all():
                data[colname + '_log'] = np.log(data[colname]+1)
            else:
                data[colname + '_log'] = np.log(data[colname])
        data.replace([np.inf, -np.inf], 0, inplace=True)
    except Exception as e:
        print(e)
    #data.info()

cols_to_log_transform = ['avg_mnthly_txn_count','total_card_count','active_card_count','inactive_card_count','suspended_card_count','visiting_site_count','Fee_txn_cnt','Fuel_txn_cnt','Service_txn_cnt']
#cols_to_log_transform = ['avg_mnthly_txn_count','total_card_count','inactive_card_count','suspended_card_count','visiting_site_count','Fee_txn_cnt','Fuel_txn_cnt','Service_txn_cnt']
log_transform(cust_df_pd,cols_to_log_transform)
#Log transformation of the feature 'Kilometers_Driven'
for col in cols_to_log_transform:
    try:
        plt.figure(figsize = (15, 4))
        plt.subplot(1, 2, 1)
        sns.distplot(cust_df_pd[col+'_log'], axlabel=col+"_log")
        plt.show()
    except Exception as e:
        print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bivariate analysis

# COMMAND ----------

plt.figure(figsize=(13,17))
sns.pairplot(data=cust_df_pd.drop(cols_to_log_transform,axis=1))
plt.show()

# COMMAND ----------

plt.figure(figsize=(20, 20))
sns.heatmap(cust_df_pd.drop(cols_to_log_transform,axis=1).corr(), annot = True, vmin = -1, vmax = 1)
plt.show()

# COMMAND ----------


