import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Estudos\GCP\case_env\Lib\site-packages')

import datetime as dt
from datetime import datetime,date,time
from dotenv import load_dotenv
import logging
import pandas as pd
from pathlib import Path
import uuid

logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("dadoseconomicos.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )

dotenv_path = Path(r'C:\Users\SALA443\Desktop\Estudos\GCP\GCP_DataEngineering\elt_script\.env')
load_dotenv(dotenv_path=dotenv_path)

# dotenv_path = Path(r'/workspaces/app/.env')
# load_dotenv(dotenv_path=dotenv_path)

year = dt.date.today().year
month = dt.date.today().month
day = dt.date.today().day
hour = datetime.now().strftime("%H")
current_time = datetime.now().strftime("%H:%M:%S")
sep = {'standard':';','altered':','}
encoding = {'standard':'utf-8','altered':'Windows-1252'}
tags = {'case':'case',
        'vendas':'vendas'}



# def check_index(self):
#         '''This method performs validation for inputed index range for
#         all parameters'''
#         # index = [self.enviroment, self.provider, self.profile, self.layer, self.project]
                 
#         if self.enviroment not in enviroment.values() or self.provider not in provider.values() or self.profile not in profile.values() or self.layer not in layers.values() or self.source not in source.values() or self.target not in target.values() or self.project not in project.values():
#             logging.error("Unexpected value passed as parameter.\nCheck the docstring for path_name().define.")
#             raise Exception(f"Check if the typed parameters:\n [layers, enviroment, project, provider, profile, source, target] matched with the variables defined in the beginning of the objects.py file") 
            
#         else:
#             logging.info(f"All parameters was validated!")     

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
    
    def define(self):    
            '''
            The *Define method is for effectively choose a type of folder structure.
            It depends on the variable {pipeline_method} defined in the __init__ method.
            When {pipeline_method} is equal 1 the function uses the value passed inside curly brackets, and when it is equal 0 the function uses the value typed manually.
            '''  
                
            if  self.frequency == 'daily':
                return f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/"
            elif self.frequency == 'monthly':
                return f"{year}/{str(month).zfill(2)}/"
            elif self.frequency == 'hourly':
                return f"{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/{str(hour).zfill(2)}/"
            else:
                logging.error("Enter a valid frequency! [daily, monthly, hourly] is allowed.")
                raise TypeError("A invalid value was typed for frequency. Please check if it was mistyped or it is a new frequency that has to be included in valid frequency list")

class path():
    def __init__(self, folder:str,key:str,context:str):
         self.folder = folder
         self.key = key
         self.context = context
         
    def mount(self):
        folder = f"{self.folder}/"
        key = f"{frequency(self.key).define()}{self.context}/"
        
        return folder + key
    
    
