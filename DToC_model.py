# Databricks notebook source
# MAGIC %md
# MAGIC # create Digital twin model for all entities

# COMMAND ----------

import json 

# COMMAND ----------

# MAGIC %md
# MAGIC ## DT model for customers

# COMMAND ----------

cust_feature = spark.table('dtoc_db.dtoc_customer_features')
display(cust_feature)

# COMMAND ----------

# DBTITLE 1,CONSTANTS
customer_dt_model_id = 'dtmi:dtoc:Customer;1' 
card_dt_model_id = 'dtmi:dtoc:Card;1' 
products_dt_model_id = 'dtmi:dtoc:Product;1'
country_dt_model_id = 'dtmi:dtoc:Country;1'

context = "dtmi:dtdl:context;2"

# COMMAND ----------

class customer_digital_twin_model:
    '''
    '''
    def __init__(self):
        pass
    
    def create_customer_model(self):
        '''
        '''
        try:
            cust_model_base_dict = {
                              "@id":customer_dt_model_id,
                              "@type": "Interface",
                              "displayName": "Customer",
                              "@context": context,
                              }
            content_list = []
            content_dict = {}
            property_list = {'customer_id':'string','registered_country':'string','SIC_code':'string','banding':'string','office_head_quarter':'string','legal_structure':'string','customer_created_date':'string',
                            'company_stage':'string','legal_structure':'string'}
            telemetry_list = {'age_months':'float','total_txn_count':'float','total_transaction_value':'float','avg_mnthly_txn_value':'float','avg_mnthly_txn_count':'integer','total_card_count':'integer','active_card_count':'integer','inactive_card_count':'integer','suspended_card_count':'integer','visiting_site_count':'integer','Fee_txn_cnt':'integer','Fuel_txn_cnt':'integer','Service_txn_cnt':'integer','churn':'string','importance_score':'float'}

            relationship_list= {"owns":card_dt_model_id,"belongs_to":country_dt_model_id}

            content_list = content_list + self.get_properties(property_list) + self.get_telemetry(telemetry_list) + self.get_relationship(relationship_list)
            cust_model_base_dict["contents"] = content_list
            
            #json_object = json.dumps(cust_model_base_dict, indent = 4)
            return cust_model_base_dict
        except Exception as e:
            print(e)

    def get_properties(self, property_dict):
        '''
        '''
        try:
            tmp_lst = []
            for prop_key in property_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Property"
                tmp_dict["name"] = prop_key
                tmp_dict["schema"] = property_dict[prop_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)
            
    def get_relationship(self, relationship_dict):
        '''
        '''
        try:
            tmp_lst = []
            for relationship_key in relationship_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Relationship"
                tmp_dict["name"] = relationship_key
                tmp_dict["displayName"] = relationship_key
                tmp_dict["target"] = relationship_dict[relationship_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)
            
    def get_telemetry(self, telemetry_dict):
        '''
        '''
        try:
            tmp_lst = []
            for telemetry_key in telemetry_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Telemetry"
                tmp_dict["name"] = telemetry_key
                tmp_dict["schema"] = telemetry_dict[telemetry_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)

# COMMAND ----------

cutsomer_model = customer_digital_twin_model().create_customer_model()
path ='/dbfs/FileStore/tables/DToC/Model/'
file_name ='customer_model.json'
with open(path+file_name, "w") as fp:
    json.dump(cutsomer_model , fp)
json.dumps(cutsomer_model)

# COMMAND ----------

class card_digital_twin_model:
    '''
    '''
    def __init__(self):
        pass
    
    def create_card_model(self):
        '''
        '''
        try:
            card_model_base_dict = {
                              "@id":card_dt_model_id,
                              "@type": "Interface",
                              "displayName": "Card",
                              "@context": context,
                              }
            content_list = []
            content_dict = {}
            property_list = {'card_id':'string','card_status':'string'}
            relationship_list= {"purchases":products_dt_model_id}
            
            content_list = content_list + self.get_properties(property_list) +self.get_relationship(relationship_list)
            card_model_base_dict["contents"] = content_list
            
            #json_object = json.dumps(cust_model_base_dict, indent = 4)
            return card_model_base_dict
        except Exception as e:
            print(e)

    def get_properties(self, property_dict):
        '''
        '''
        try:
            tmp_lst = []
            for prop_key in property_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Property"
                tmp_dict["name"] = prop_key
                tmp_dict["schema"] = property_dict[prop_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)
            
    def get_relationship(self, relationship_dict):
        '''
        '''
        try:
            tmp_lst = []
            for relationship_key in relationship_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Relationship"
                tmp_dict["name"] = relationship_key
                tmp_dict["displayName"] = relationship_key
                tmp_dict["target"] = relationship_dict[relationship_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)

# COMMAND ----------

card_model = card_digital_twin_model().create_card_model()
path ='/dbfs/FileStore/tables/DToC/Model/'
file_name ='card_model.json'
with open(path+file_name, "w") as fp:
    json.dump(card_model , fp)
json.dumps(card_model)

# COMMAND ----------

class product_digital_twin_model:
    '''
    '''
    def __init__(self):
        pass
    
    def create_product_model(self):
        '''
        '''
        try:
            product_model_base_dict = {
                              "@id":products_dt_model_id,
                              "@type": "Interface",
                              "displayName": "Product",
                              "@context": context,
                              }
            content_list = []
            content_dict = {}
            property_list = {'product_id':'string','product_name':'string', 'product_type':'string','product_unit':'string'}
            telemetry_list = {'product_price':'float'}
            content_list = content_list + self.get_properties(property_list)
            product_model_base_dict["contents"] = content_list
            
            #json_object = json.dumps(cust_model_base_dict, indent = 4)
            return product_model_base_dict
        except Exception as e:
            print(e)

    def get_properties(self, property_dict):
        '''
        '''
        try:
            tmp_lst = []
            for prop_key in property_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Property"
                tmp_dict["name"] = prop_key
                tmp_dict["schema"] = property_dict[prop_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)
            
    def get_telemetry(self, telemetry_dict):
        '''
        '''
        try:
            tmp_lst = []
            for telemetry_key in telemetry_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Telemetry"
                tmp_dict["name"] = telemetry_key
                tmp_dict["schema"] = telemetry_dict[telemetry_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)

# COMMAND ----------

product_model = product_digital_twin_model().create_product_model()
path ='/dbfs/FileStore/tables/DToC/Model/'
file_name ='product_model.json'
with open(path+file_name, "w") as fp:
    json.dump(product_model , fp)
json.dumps(product_model)

# COMMAND ----------

class country_digital_twin_model:
    '''
    '''
    def __init__(self):
        pass
    
    def create_country_model(self):
        '''
        '''
        try:
            card_model_base_dict = {
                              "@id":country_dt_model_id,
                              "@type": "Interface",
                              "displayName": "Coountry",
                              "@context": context,
                              }
            content_list = []
            content_dict = {}
            property_list = {'name':'string','region':'string'}
            
            content_list = content_list + self.get_properties(property_list)  
            card_model_base_dict["contents"] = content_list
            
            #json_object = json.dumps(cust_model_base_dict, indent = 4)
            return card_model_base_dict
        except Exception as e:
            print(e)

    def get_properties(self, property_dict):
        '''
        '''
        try:
            tmp_lst = []
            for prop_key in property_dict:
                tmp_dict = {}
                tmp_dict["@type"] = "Property"
                tmp_dict["name"] = prop_key
                tmp_dict["schema"] = property_dict[prop_key]
                tmp_lst.append(tmp_dict)
            return tmp_lst
        except Exception as e:
            print(e)

# COMMAND ----------

country_model = country_digital_twin_model().create_country_model()
path ='/dbfs/FileStore/tables/DToC/Model/'
file_name ='product_model.json'
with open(path+file_name, "w") as fp:
    json.dump(country_model , fp)
json.dumps(country_model)

# COMMAND ----------


