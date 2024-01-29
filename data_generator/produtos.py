from variaveis import *
import sys
sys.path.append(sources['virtualenv'])
import names
import random
import uuid
import json
from datetime import date as dt
import pandas as pd
import numpy as np
from variaveis import * 


fk_presencial = [100,101,102,103,104]
produtos_presencial = ['Java','Python','JavaScript','R','SQL-SERVER','Bootcamp Especialista em dados']
precos_presencial = [3600,2600,2600,3700,4000,15000]

fk_ead = [105,106,107,108,109]
produtos_ead = ['Java','Python','JavaScript','R','SQL-SERVER','Bootcamp Especialista em dados']
precos_ead = [1500,1800,1400,2000,3000,9200]

fk = fk_presencial + fk_ead
produtos = produtos_presencial + produtos_ead
precos = precos_presencial + precos_ead

numero_de_registros = len(fk)

df = pd.DataFrame(list(zip(fk, produtos, precos)),
            columns =['fk', 'curso','preco'])

df.to_csv(f"{sources['local_csv']}produtos.csv",index=None,sep=';',encoding='utf-8')