import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Estudos\GCP\case_env\Lib\site-packages')

import datetime as dt
from datetime import datetime,date,time
from dotenv import load_dotenv
from google.cloud import storage
import logging
import os
import pandas as pd
from pathlib import Path
from tempfile import TemporaryDirectory
import uuid

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="C:/Users/SALA443/Downloads/vendas-de-412318-2cdd56112d35.json"
logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("case.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )

dotenv_path = Path('C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/elt_script/.env')
load_dotenv(dotenv_path=dotenv_path)

# dotenv_path = Path(r'/workspaces/app/.env')
# load_dotenv(dotenv_path=dotenv_path)

year = dt.date.today().year
month = dt.date.today().month
day = dt.date.today().day
hour = datetime.now().strftime("%H")
current_time = datetime.now().strftime("%H:%M:%S")
tags = {'case':'case',
        'vendas':'vendas'}

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
        'source':['local','container']}
    
    if param not in parameter_list.keys():
        logging.error(f"Please insert a valid parameter like *{parameter_list.keys()}")
    else:
        logging.info(f"The param *{param} was set in order to check if the *{input} is a valid input.")
        
    if input not in parameter_list[param]:
        logging.error(" Please type a valid frequency like (daily, hourly ou monthly)")
        raise ValueError("Invalid parameter was passed!!")
    else:
        logging.info(f"Valid parameter *{input} was set!")      
            

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
    
    def key(self):    
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
    def __init__(self, arquitecture:str,source:str, target:str, filename:str, context:str, frequency:str ) -> None:
         
         self.arquitecture = arquitecture
         self.source = source
         self.target = target
         self.filename = filename
         self.context = context
         self.frequency = frequency
         
    def params(self):  
        input_check(self.source, self.arquitecture)
        input_check(self.target, self.arquitecture)
        
        # setting up variables        
        source = {
            'local':'C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/sources/csv',
            'container':'workspaces/opt/sources'}   

        
        bucket = {
            'datalake':{
                'landing':'landing-6638477',
                'processed':'processed-6638477',
                'consume':'processed-6638477'},
                        
            'data lakehouse':{
                'bronze':'bronze-6638477',
                'silver':'silver-6638477',
                'gold':'gold-6638477'}}  
    
        filename = self.filename
        process_id = uuid.uuid4().hex[:16]  
        
        
        # reading data and adding some metada fields
        
        df = pd.read_csv(f"{source['local']}/{filename}",sep=';',encoding='utf-8')
        df['loaded_date'] = dt.date.today().day
        df['loaded_time'] = current_time
        df['tags'] = tags['case']
        df['process_id'] = process_id 
        df['rows_count'] = len(df)
        df['from'] = self.source
        df['to'] = self.target
        logging.info("Dataframe criado e metadados inseridos.")
        
        # deletiing some metadata columns from df
        columns=['tags','rows_count','from','to']
        df.drop(columns, inplace=True, axis=1)
        
  
        
        # saving metadata
        # metadata = df[['process_id','loaded_date','loaded_time','process_id','rows_count','from','to']]
        # metadata.to_csv('metadata/metadata.csv',sep=';',encoding='utf-8',index=None)
        
        # uploading to gcp
        key = frequency(self.frequency).key()
        storage_client =  storage.Client()
        bucket_name = bucket[f'{self.arquitecture}'][f'{self.target}']
        destination_path = f"{key}{self.context}/{filename}"
        bucket = storage_client.get_bucket(bucket_name)        
        bucket.blob(destination_path).upload_from_string(df.to_csv(), 'text/csv')
        logging.info(f"File {self.filename} containing {len(df)} rows was stored in {self.target} bucket! \n The process_id is {process_id}. Please check the metadata file if you need more information about the pipeline")
        
        
        
        

        
        

 


