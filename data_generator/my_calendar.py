# from variaveis import * 
# import sys
# sys.path.append(sources['virtualenv'])
from datetime import *
import pandas as pd


nome_dia = ['Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo']
num_dia = [0,1,2,3,4,5,6]


df_dias = pd.DataFrame(list(zip(nome_dia,num_dia)),columns =['nome_dia', 'num_dia'])

start='2023-08-01' 
end='2024-01-27'
df = pd.DataFrame({"Date": pd.date_range(start, end)})
df["day_name"] = df['Date'].dt.day_name()
df["Numero_do_dia"] = df['Date'].dt.weekday
df["Ano"] = df['Date'].dt.year
df["Month"] = df['Date'].dt.month_name()

df = df.merge(df_dias,left_on='Numero_do_dia', right_on='num_dia')

n = 1
df['fk'] = 0
for d in range(len(df['Date'])):
    df['fk'][d] = n
    n += 1    

df.to_csv(f"{sources['container_csv']}calendario.csv",sep=';',encoding='utf-8',index=None)