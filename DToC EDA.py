# Databricks notebook source
# MAGIC %md
# MAGIC # EDA

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

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

# DBTITLE 1,Five Number Summary
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
skew_kurt_before_processing ={}

for col in num_cols:
    print(col)
    skew_kurt_before_processing[col]={'skew':round(cust_df_pd[col].skew(), 2), 'kurtosis':round(cust_df_pd[col].kurtosis(), 2)}
    print('Skew :', round(cust_df_pd[col].skew(), 2))
    print('Kurtosis :', round(cust_df_pd[col].kurtosis(), 2))
    plt.figure(figsize = (15, 4))
    plt.subplot(1, 2, 1)
    cust_df_pd[col].hist(grid=False)
    plt.ylabel('count')
    plt.subplot(1, 2, 2)
    sns.boxplot(x=cust_df_pd[col])
    plt.show()

# COMMAND ----------

print(skew_kurt_before_processing)

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

def remove_outlier(df, col_list):
    '''
    '''
    try:
        for col in col_list:
            q = df[col].quantile(0.95)
            print(col + "-" + str(q))
            df_filtered  = df[df[col] < q]
        return df_filtered 
    except Exception as e:
        print(e)


# COMMAND ----------

cols_to_remove_outliers = ['total_txn_count',
 'total_transaction_value',
 'avg_mnthly_txn_value',
 'avg_mnthly_txn_count',
 'visiting_site_count',
 'Fee_txn_cnt',
 'Fuel_txn_cnt']

cust_df_pd_removed_outlier = remove_outlier(cust_df_pd,cols_to_remove_outliers)

# COMMAND ----------

cust_df_pd_removed_outlier

# COMMAND ----------

skew_kurt_before_processing

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

cols_to_log_transform = ['total_txn_count','total_transaction_value','avg_mnthly_txn_value','avg_mnthly_txn_count','total_card_count','active_card_count','inactive_card_count','suspended_card_count','visiting_site_count','Fee_txn_cnt','Fuel_txn_cnt','Service_txn_cnt']

log_transform(cust_df_pd_removed_outlier,cols_to_log_transform)

for col in cols_to_log_transform:
    try:
        plt.figure(figsize = (15, 4))
        plt.subplot(1, 2, 1)
        sns.distplot(cust_df_pd_removed_outlier[col+'_log'], axlabel=col+"_log")
        plt.show()
    except Exception as e:
        print(e)
        
cust_df_pd_removed_outlier_lt = cust_df_pd_removed_outlier.drop(cols_to_log_transform, axis=1)

# COMMAND ----------

skew_kurt_before_processing

# COMMAND ----------

cust_df_pd_removed_outlier_lt.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Skew Kurtosis after log transformation

# COMMAND ----------

skew_kurt_after_processing ={}
log_lit = '_log'
log_transformed_cols = [x+log_lit for x in cols_to_log_transform]
for col in log_transformed_cols:
    print(col)
    skew_kurt_after_processing[col]={'skew':round(cust_df_pd_removed_outlier_lt[col].skew(), 2), 'kurtosis':round(cust_df_pd_removed_outlier_lt[col].kurtosis(), 2)}
    print('Skew :', round(cust_df_pd_removed_outlier_lt[col].skew(), 2))
    print('Kurtosis :', round(cust_df_pd_removed_outlier_lt[col].kurtosis(), 2))
    plt.figure(figsize = (15, 4))
    plt.subplot(1, 2, 1)
    cust_df_pd_removed_outlier_lt[col].hist(grid=False)
    plt.ylabel('count')
    plt.subplot(1, 2, 2)
    sns.boxplot(x=cust_df_pd_removed_outlier_lt[col])
    plt.show()

# COMMAND ----------

skew_kurt_after_processing

# COMMAND ----------

[skew_kurt_before_processing[col]['skew'] for col in cols_to_log_transform]

# COMMAND ----------

[skew_kurt_after_processing[col+log_lit]['skew'] for col in cols_to_log_transform]

# COMMAND ----------

import matplotlib.pyplot as plt
X = cols_to_log_transform
Y_before = [skew_kurt_before_processing[col]['skew'] for col in cols_to_log_transform]
Y_after = [skew_kurt_after_processing[col+log_lit]['skew'] for col in cols_to_log_transform]

K_before = [skew_kurt_before_processing[col]['kurtosis'] for col in cols_to_log_transform]
K_after = [skew_kurt_after_processing[col+log_lit]['kurtosis'] for col in cols_to_log_transform]

# Plot lines and/or markers to the Axes.
plt.figure(figsize = (20, 5))
plt.subplot(1, 2, 1)
plt.plot(X, Y_before)
plt.plot(X, Y_after)
# Set the x axis label of the current axis.
plt.xlabel('Columns')
plt.xticks(rotation = 90)
# Set the y axis label of the current axis.
plt.ylabel('Skewness')
# Set a title 
plt.title('Skewness')
plt.legend(["Skew Before", "Skew After"], loc ="upper right")
# Display the figure.

plt.subplot(1, 2, 2)
plt.plot(X, K_before)
plt.plot(X, K_after)
# Set the x axis label of the current axis.
plt.xlabel('Columns')
plt.xticks(rotation = 90)
# Set the y axis label of the current axis.
plt.ylabel('Kurtosis')
# Set a title 
plt.title('Kurtosis')
plt.legend(["Kurtosis Before", "Kurtosis After"], loc ="upper right")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bivariate analysis

# COMMAND ----------

plt.figure(figsize=(20, 20))
sns.heatmap(cust_df_pd_removed_outlier_lt.corr(), annot = True, vmin = -1, vmax = 1)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model training

# COMMAND ----------

from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
from xgboost import XGBClassifier
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix, precision_score, recall_score, f1_score,  accuracy_score

import tensorflow as tf
from tensorflow import keras


# COMMAND ----------

cust_df_pd_removed_outlier_lt.groupby('churn').size().plot(kind='pie', legend=True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### onehot Encoding

# COMMAND ----------

cat_cols_filtered = ['registered_country', 'SIC_code', 'banding', 'office_head_quarter',
       'company_stage', 'legal_structure']


cust_df_pd_removed_outlier_OHE = pd.get_dummies(cust_df_pd_removed_outlier_lt, columns =cat_cols_filtered)

cust_df_pd_removed_outlier_LE = cust_df_pd_removed_outlier_OHE.apply(LabelEncoder().fit_transform)

for column in cust_df_pd_removed_outlier_LE.columns:
    cust_df_pd_removed_outlier_LE.rename(columns = {column:column.replace('.','').replace(' ','_')}, inplace = True)

# COMMAND ----------

cust_df_pd_removed_outlier_LE

# COMMAND ----------

cust_df_pd_removed_outlier_LE.columns

# COMMAND ----------

# remove unwanted columns
cust_df_filtered =cust_df_pd_removed_outlier_LE.drop(['customer_created_date'], axis=1)

target_col = 'churn'
X = cust_df_filtered.loc[:,cust_df_filtered.columns != target_col]
y = cust_df_filtered.loc[:,cust_df_filtered.columns == target_col]

# COMMAND ----------

y= y['churn']
X_train, X_test, Y_train,Y_test =  train_test_split(X,y, test_size=0.3, random_state=100, stratify=y)
sc=StandardScaler()
X_train= sc.fit_transform(X_train)
X_test = sc.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANN Tensorflow

# COMMAND ----------

ann = tf.keras.models.Sequential()
ann.add(tf.keras.layers.Dense(units=6,activation="relu"))
ann.add(tf.keras.layers.Dense(units=6,activation="relu"))
ann.add(tf.keras.layers.Dense(units=6,activation="relu"))
ann.add(tf.keras.layers.Dense(units=1,activation="sigmoid"))
ann.compile(optimizer="adam",loss="binary_crossentropy",metrics=['accuracy'])
history = ann.fit(X_train,Y_train, batch_size=32,epochs = 15, verbose =2)

'''
def model_builder(hp):
    model = tf.keras.models.Sequential()
    model.add(keras.layers.Flatten(input_shape=(28, 28)))

    # Tune the number of units in the first Dense layer
    # Choose an optimal value between 32-512
    hp_units = hp.Int('units', min_value=32, max_value=512, step=32)
    model.add(keras.layers.Dense(units=hp_units, activation='relu'))
    model.add(keras.layers.Dense(10))

    # Tune the learning rate for the optimizer
    # Choose an optimal value from 0.01, 0.001, or 0.0001
    hp_learning_rate = hp.Choice('learning_rate', values=[1e-2, 1e-3, 1e-4])

    model.compile(optimizer=keras.optimizers.Adam(learning_rate=hp_learning_rate),
                loss=keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                metrics=['accuracy'])

    return model
'''


# COMMAND ----------

plt.plot(history.history['accuracy'])
plt.show()

# COMMAND ----------

results= ann.evaluate(X_test, Y_test, batch_size=128)
print("test loss, test acc:", results)

# COMMAND ----------

y_pred= ann.predict(X_test)
y_pred_1_dim_arr = [1 if x[0] > 0.5 else 0 for x in y_pred]

# COMMAND ----------

conf_matrix = confusion_matrix(y_true=Y_test, y_pred=y_pred_1_dim_arr)

fig, ax = plt.subplots(figsize=(5, 5))
ax.matshow(conf_matrix, cmap=plt.cm.Oranges, alpha=0.3)

for i in range(conf_matrix.shape[0]):
    for j in range(conf_matrix.shape[1]):
        ax.text(x=j, y=i,s=conf_matrix[i, j], va='center', ha='center', size='xx-large')
        
plt.xlabel('Predictions', fontsize=18)
plt.ylabel('Actuals', fontsize=18)
plt.title('Confusion Matrix', fontsize=18)

plt.show()

# COMMAND ----------

print('Precision: %.3f' % precision_score(Y_test, y_pred_1_dim_arr))
print('Recall: %.3f' % recall_score(Y_test, y_pred_1_dim_arr))
print('Accuracy: %.3f' % accuracy_score(Y_test, y_pred_1_dim_arr))
print('F1 Score: %.3f' % f1_score(Y_test, y_pred_1_dim_arr))

# COMMAND ----------

from collections import Counter
counter =  Counter(y)
spw = counter[0]/counter[1]
spw

# COMMAND ----------

counter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline for Random Forest and XGBoost

# COMMAND ----------

#pipe_lr = Pipeline([('LR', LogisticRegression(random_state=42))])
#pipe_dt = Pipeline([('DT',DecisionTreeClassifier(random_state=42))])
pipe_rf = Pipeline([('RF',RandomForestClassifier(random_state=42, class_weight='balanced'))])
#pipe_knn = Pipeline([('KNN', KNeighborsClassifier())])
#pipe_svm = Pipeline([('SVM', svm.SVC(random_state=42))])


pipe_xgb = Pipeline([('XGB', XGBClassifier(random_state=42,scale_pos_weight= spw))])


param_range = [ 3, 4, 5]
param_range_fl = [1.0, 0.5, 0.1]
n_estimators = [25,50,75]
learning_rates = [.1,.2,.3]
xgb_sow = [spw]

'''
lr_param_grid = [{'LR__penalty': ['l1', 'l2'],
                   'LR__C': param_range_fl,
                   'LR__solver': ['liblinear']}]
                 
dt_param_grid = [{'DT__criterion': ['gini', 'entropy'],
                   'DT__min_samples_leaf': param_range,
                   'DT__max_depth': param_range,
                   'DT__min_samples_split': param_range[1:]}]
'''
rf_param_grid = [{'RF__min_samples_leaf': param_range,
                   'RF__max_depth': param_range,
                   'RF__min_samples_split': param_range[1:]}]

'''
knn_param_grid = [{'KNN__n_neighbors': param_range,
                   'KNN__weights': ['uniform', 'distance'],
                   'KNN__metric': ['euclidean', 'manhattan']}]

                 
svm_param_grid = [{'SVM__kernel': ['rbf','sigmoid','poly'], 
                    'SVM__C': param_range}]
'''  
xgb_param_grid = [{'XGB__learning_rate': learning_rates,
                    'XGB__max_depth': param_range,
                    'XGB__min_child_weight': param_range[:2],
                    'XGB__subsample': param_range_fl,
                    'XGB__n_estimators': n_estimators,
                    'XGB__scale_pos_weight':xgb_sow}]


'''
lr_grid_search = GridSearchCV(estimator=pipe_lr,
        param_grid=lr_param_grid,
        scoring='accuracy',
        cv=3)
dt_grid_search = GridSearchCV(estimator=pipe_dt,
        param_grid=dt_param_grid,
        scoring='accuracy',
        cv=3)
'''        
rf_grid_search = GridSearchCV(estimator=pipe_rf,
        param_grid=rf_param_grid,
        scoring='roc_auc',
        cv=3)
'''
knn_grid_search = GridSearchCV(estimator=pipe_knn,
        param_grid=knn_param_grid,
        scoring='accuracy',
        cv=3)
        
svm_grid_search = GridSearchCV(estimator=pipe_svm,
        param_grid=svm_param_grid,
        scoring='roc_auc',
        cv=3)
'''       
xgb_grid_search = GridSearchCV(estimator=pipe_xgb,
        param_grid=xgb_param_grid,
        scoring='roc_auc',
        cv=3)


#grids = [lr_grid_search, dt_grid_search, rf_grid_search, knn_grid_search, svm_grid_search, xgb_grid_search]
grids = [rf_grid_search, xgb_grid_search]
for pipe in grids:
    pipe.fit(X_train,Y_train)
'''
grid_dict = {0: 'Logistic Regression', 1: 'Decision Trees', 
             2: 'Random Forest', 3: 'K-Nearest Neighbors', 
             4: 'Support Vector Machines' , 5: 'XGBoost'}
'''

grid_dict = {0: 'Random Forest', 1: 'XGBoost'}

for i, model in enumerate(grids):
    print('{} Test Accuracy: {}'.format(grid_dict[i],
    model.score(X_test,Y_test)))
    print('{} Best Params: {}, Best features : {}'.format(grid_dict[i], model.best_params_, model.best_estimator_))

# COMMAND ----------

for i,clf in enumerate(grids):
    disp = ConfusionMatrixDisplay.from_estimator(
          clf,
          X_test,
          Y_test,
          cmap=plt.cm.Blues
      )
    disp.ax_.set_title(grid_dict[i])

    print(grid_dict[i])
    print(disp.confusion_matrix)

plt.show()

# COMMAND ----------

final_model = XGBClassifier(random_state=42, learning_rate= 0.2, max_depth= 5, min_child_weight= 3, n_estimators= 25, scale_pos_weight= 1.608775728390847, subsample= 0.5)
final_model.fit(X_train, Y_train)

# COMMAND ----------

final_model.score(X_test, Y_test, )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Saved Model

# COMMAND ----------

len(X_test[0])

# COMMAND ----------


