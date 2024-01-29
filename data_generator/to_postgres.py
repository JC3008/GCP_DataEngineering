from variaveis import * 
import sys
sys.path.append(sources['virtualenv'])

import pandas as pd
import pyodbc
from sqlalchemy import create_engine

df = pd.read_csv(f"{sources['local_csv']}vendas_alpha.csv")
connection = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=DESKTOP-DD8P4JN;'
    'DATABASE=estudos;'
    'UID=sa;'
    'PWD=jotace007'
)
engine = create_engine('mssql+pyodbc://sa:jotace007@DESKTOP-DD8P4JN/estudos')

df.to_sql('vendas',engine,if_exists='replace',index=None)
