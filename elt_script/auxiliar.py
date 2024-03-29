import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Estudos\GCP\case_env\Lib\site-packages')

import datetime as dt
from datetime import datetime,date,time
from google.cloud import storage
import logging
import json
import os
import pandas as pd
from pathlib import Path
import pymongo
from tempfile import TemporaryDirectory
import uuid


# [begin] setting up variables

# dotenv_path = Path(r'/workspaces/app/.env')
# load_dotenv(dotenv_path=dotenv_path)

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/elt_script/vendas-de-412318-2cdd56112d35.json"
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/workspaces/app/elt_script/vendas-de-412318-2cdd56112d35.json"
logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("case.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )

# dotenv_path = Path('C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/elt_script/.env')
# load_dotenv(dotenv_path=dotenv_path)

# dotenv_path = Path(r'/workspaces/app/.env')
# load_dotenv(dotenv_path=dotenv_path)

year = dt.date.today().year
month = dt.date.today().month
day = dt.date.today().day
hour = datetime.now().strftime("%H")
current_time = datetime.now().strftime("%H:%M:%S")
tags = {'case':'case',
        'vendas':'vendas'}

# [end] setting up variables

# [begin] Function developed to validate parameters inserted in class methods
def input_check(input, param):
    '''
    input can be:
    ['daily' or 'monthly' or 'hourly'] for the param 'frequency'
    ['landing' or 'processed' or 'consume'] for the param 'datalake'
    ['bronze','silver','gold'] for the param 'data lakehouse'
    ['local','container'] for the param 'source'
    '''
    
    parameter_list = {
        'frequency':['daily','monthly','hourly'],
        'datalake':['landing','processed','consume'],
        'data lakehouse':['bronze','silver','gold'],
        'local_source':['local_csv','local_json','container_csv','container_json']}
    
    if param not in parameter_list.keys():
        logging.error(f"Please insert a valid parameter like *{parameter_list.keys()}")
    else:
        logging.info(f"The param *{param} was set in order to check if the *{input} is a valid input.")
        
    if input not in parameter_list[param]:
        logging.error(" Please type a valid frequency like (daily, hourly ou monthly)")
        raise ValueError("Invalid parameter was passed!!")
    else:
        logging.info(f"Valid parameter *{input} was set!") 
        return None     
# [end] Function developed to validate parameters inserted in class methods        


class frequency():
    
    '''
    This class aims to build folder structure in a standard form, like described bellow:
    
    daily = yyyy/mm/dd/
    monthly = yyyy/mm/
    hourly = yyyy/mm/dd/hh
    
    As a result is expected the avoidance of issues by mistypped values and agility on the folder building process.     
    '''
    
    def __init__(self,frequency=None):   
            
            self.frequency = frequency
    
    def subfolder(self):    
            '''
            The *Define method is for effectively choose a type of folder structure.
            It depends on the variable {pipeline_method} defined in the __init__ method.
            When {pipeline_method} is equal 1 the function uses the value passed inside curly brackets, and when it is equal 0 the function uses the value typed manually.
            '''  
            input_check(self.frequency, 'frequency')
            
            if  self.frequency == 'daily':
                
                return f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/"
                
            elif self.frequency == 'monthly':
                return f"{year}/{str(month).zfill(2)}/"
            elif self.frequency == 'hourly':
                return f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/{str(hour).zfill(2)}/"
            else:
                logging.error("Enter a valid frequency! [daily, monthly, hourly] is allowed.")
                raise TypeError("A invalid value was typed for frequency. Please check if it was mistyped or it is a new frequency that has to be included in valid frequency list")

 
class extract:
    def __init__(self, arquitecture:list,source:str, destination:str, filename:list, context:str, frequency:str ) -> None:
         
         self.arquitecture = arquitecture
         self.source = source
         self.destination = destination
         self.filename = filename
         self.context = context
         self.frequency = frequency
    
    @property  
    def parameters(self):  
        input_check(self.source, self.arquitecture[0])
        input_check(self.destination, self.arquitecture[1])
        
        # setting up variables        
        
        source = {
            'local_source':{
            'local_csv':'C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/sources/csv',
            'local_json':'C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/sources/mongo/',
            'container_csv':'/workspaces/app/sources/csv',
            'container_json':'/workspaces/app/sources/mongo'},
            
            'datalake':{
                'landing':'landing-6638477',
                'processed':'processed-6638477',
                'consume':'consume-6638477'},
                        
            'data lakehouse':{
                'bronze':'bronze-6638477',
                'silver':'silver-6638477',
                'gold':'gold-6638477'}}  
        
        filename = self.filename
        process_id = uuid.uuid4().hex[:16] 
        vars = {
            'source_path':f"{source[self.arquitecture[0]][self.source]}",
            'gcp_source':f"{source[self.arquitecture[0]][self.source]}",
            'gcp_destination':f"{source[self.arquitecture[1]][self.destination]}",
            'filename':filename,
            'subfolder':f"{frequency(self.frequency).subfolder()}{self.context}",
            'process_id':process_id,
            'context':self.context,
            'tags':tags[self.context]       
        }        
        return vars
        
    def csv_to_gcp(self):
        vars = extract(self.arquitecture,self.source,self.destination,self.filename,self.context,self.frequency).parameters

        # reading data and adding some metada fields    

        df = pd.read_csv(f"{vars['source_path']}/{vars['filename']}.csv",sep=';',encoding='utf-8')
        df['loaded_date'] = dt.date.today().day
        df['loaded_time'] = current_time
        df['tags'] = tags['case']
        df['process_id'] = vars['process_id'] 
        df['rows_count'] = len(df)
        df['from'] = self.source
        df['to'] = self.destination
        logging.info("Dataframe criado e metadados inseridos.")
        
        # deletiing some metadata columns from df
        columns=['tags','rows_count','from','to']
        df.drop(columns, inplace=True, axis=1)  
        
        # saving metadata
        # metadata = df[['process_id','loaded_date','loaded_time','process_id','rows_count','from','to']]
        # metadata.to_csv('metadata/metadata.csv',sep=';',encoding='utf-8',index=None)
        
        # uploading to gcp
        # subfolder = vars['subfolder']
        
        storage_client =  storage.Client()
        bucket_name = vars['gcp_destination']
        destination_path = f"{vars['subfolder']}/{vars['filename']}.csv"
        bucket = storage_client.get_bucket(bucket_name)          

        bucket.blob(destination_path).upload_from_string(df.to_csv(), 'text/csv')
        
        logging.info(f"File {vars['filename']} containing {len(df)} rows was stored in {destination_path} bucket! \n The process_id is {vars['process_id']}. Please check the metadata file if you need more information about the pipeline")
        
        return None
    
    
    def mongo_to_df(self):
                
        client = pymongo.MongoClient("mongodb://localhost:27017/vendas")
        
        # Database Name
        db = client["vendas"]

        # Collection Name
        col = db["clientes"]

        df = pd.DataFrame()
        serie = pd.Series()
        id_list = list()
        registro_list = list()
        nome_list = list()
        idade_list = list()
        estado_list = list()
        data_registro_list = list()

        for doc in col.find():
            id_list.append(doc['_id'])
            registro_list.append(doc['registro'])
            nome_list.append(doc['nome'])
            idade_list.append(doc['idade'])
            estado_list.append(doc['estado'])
            data_registro_list.append(doc['data_registro'])

        df = pd.DataFrame(list(zip(id_list, registro_list, nome_list, idade_list, estado_list, data_registro_list)),
                    columns =['_id', 'registro','nome','idade','estado','data_registro'])
        
        return df


    def mongo_to_gcp(self):
        
        logging.info("Mongodb - Extraction has begun!")
        
        client = pymongo.MongoClient("mongodb://localhost:27017/vendas")
        vars = extract(
            self.arquitecture,
            self.source,
            self.destination,
            self.filename,
            self.context,
            self.frequency).parameters
        
        # Database Name
        db = client["vendas"]

        # Collection Name
        col = db["clientes"]

        doc_list = []
        for doc in col.find({},{'_id':False}):
            doc_list.append(doc)
        doc_list = json.dumps(doc_list,indent=2)
        parsed = json.loads(doc_list)
        # print(parsed)
        
        logging.info(f"Mongodb - {len(parsed)} documents was encountered.")    
        storage_client =  storage.Client()
        bucket_name = vars['gcp_destination']
        destination_path = f"{vars['subfolder']}/{vars['filename']}.json"
        bucket = storage_client.get_bucket(bucket_name)        
        bucket.blob(destination_path).upload_from_string(json.dumps(parsed,indent=2), 'json')
        logging.info(f"Mongodb - {len(parsed)} documents was stored in GCS as a json file called {vars['filename']}") 
        return None

    def landing_to_processed(self):
        vars = extract(
            self.arquitecture,
            self.source,
            self.destination,
            self.filename,
            self.context,
            self.frequency).parameters
        
        storage_client =  storage.Client()
        vars['gcp_destination']     

        df = pd.read_csv(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename']}.csv")
        df['loaded_date'] = dt.date.today().day
        df['loaded_time'] = current_time
        df['tags'] = tags['case']
        df['process_id'] = vars['process_id'] 
        df['rows_count'] = len(df)
        df['from'] = self.source
        df['to'] = self.destination
        logging.info("Dataframe criado e metadados inseridos.")   
  
        destination_path = f"{vars['subfolder']}/{vars['filename']}.parquet"
        bucket = storage_client.get_bucket(vars['gcp_destination'])  
           

        bucket.blob(destination_path).upload_from_string(df.to_parquet(), 'parquet')
        
        logging.info(f"File {vars['filename']} containing {len(df)} rows was stored in {destination_path} bucket! \n The process_id is {vars['process_id']}. Please check the metadata file if you need more information about the pipeline")
        
        return None
    
    def landing_to_processed_json(self):
            vars = extract(
            self.arquitecture,
            self.source,
            self.destination,
            self.filename,
            self.context,
            self.frequency).parameters
                
            storage_client =  storage.Client()
            vars['gcp_destination']  
            
            df = pd.read_json(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename']}.json") 
            df['loaded_date'] = dt.date.today().day
            df['loaded_time'] = current_time
            df['tags'] = tags['case']
            df['process_id'] = vars['process_id'] 
            df['rows_count'] = len(df)
            df['from'] = self.source
            df['to'] = self.destination
            
            logging.info("Dataframe criado e metadados inseridos.")   
  
            destination_path = f"{vars['subfolder']}/{vars['filename']}.parquet"
            bucket = storage_client.get_bucket(vars['gcp_destination'])  
            

            bucket.blob(destination_path).upload_from_string(df.to_parquet(index=None), 'parquet')
            
            logging.info(f"File {vars['filename']} containing {len(df)} rows was stored in {destination_path} bucket! \n The process_id is {vars['process_id']}. Please check the metadata file if you need more information about the pipeline")
            
            return None
            
            
            
    def processed_to_consume(self):
        vars = extract(
            self.arquitecture,
            self.source,
            self.destination,
            self.filename,
            self.context,
            self.frequency).parameters
        
        storage_client =  storage.Client()
        vars['gcp_destination']   
        

        alpha_vendas = pd.read_parquet(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename'][0]}.parquet")
        beta_vendas = pd.read_parquet(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename'][1]}.parquet")
        produtos = pd.read_parquet(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename'][2]}.parquet")
        calendario = pd.read_parquet(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename'][3]}.parquet")
        clientes = pd.read_parquet(f"gs://{vars['gcp_source']}/{vars['subfolder']}/{vars['filename'][4]}.parquet")
        
        alpha_vendas['empresa'] = 'Alpha'
        beta_vendas['empresa'] = 'Beta'
        
        
        vendas = pd.concat([alpha_vendas,beta_vendas])
        
        vendas_columns = ['sk_cliente','sk_produto','canal_venda','campanha','data_venda','quantidade','desconto','empresa']
        vendas = vendas[vendas_columns]
        
        

        produtos_columns = ['fk','curso','preco']
        produtos = produtos[produtos_columns]

        calendario_columns = ['Date','nome_dia','Month','Ano']
        calendario = calendario[calendario_columns]

        clientes_columns = ['registro','nome','genero','idade','estado','data_registro']
        clientes = clientes[clientes_columns]

        vendas = vendas.merge(clientes,left_on='sk_cliente',right_on='registro')

        vendas.drop(['registro'], inplace=True, axis=1)

        vendas = vendas.merge(produtos,left_on='sk_produto',right_on='fk')
        vendas.drop(['fk'], inplace=True, axis=1)

        vendas = vendas.merge(calendario,left_on='data_venda',right_on='Date')
        vendas.drop(['Date'], inplace=True, axis=1)     
        
        destination_path = f"{vars['subfolder']}/vendas_obt.parquet"
        bucket = storage_client.get_bucket(vars['gcp_destination'])  
           

        bucket.blob(destination_path).upload_from_string(vendas.to_parquet(), 'parquet')
        
        return None
        
        

            


           






    






  

            


