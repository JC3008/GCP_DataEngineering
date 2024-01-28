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


numero_de_registros = 100
   
id_venda = 0 
data_vendas = dt.today()
sk_cliente = random.randrange(1, 1000)
sk_produto = random.randrange(100, 109)
canal_venda = ['site','instagram','loja','loja','loja']
campanha = ['24 horas pela metade do preço','compre dois e leve 3','sem campanha','sem campanha','sem campanha']

col_cliente = []
col_produto = []
col_canal_venda = []
col_campanha = []
col_id_venda = []


def salva_arquivo(nome_arquivo:str):
    n = 1
    while len(col_id_venda) <= numero_de_registros:
        col_id_venda.append(n)
        n += 1

    while len(col_cliente) <= numero_de_registros:
        col_cliente.append(random.randrange(1, 100))
        
    while len(col_produto) <= numero_de_registros:
        col_produto.append(random.randrange(100, 109))
        
    while len(col_canal_venda) <= numero_de_registros:
        col_canal_venda.append(canal_venda[random.randrange(0, 4)])
        
    while len(col_campanha) <= numero_de_registros:
        col_campanha.append(campanha[random.randrange(0, 4)])


    df = pd.DataFrame(list(zip(col_id_venda,col_cliente, col_produto,col_canal_venda,col_campanha)),
                columns =['id_venda','sk_cliente', 'sk_produto', 'canal_venda','campanha'])

    df['data_venda'] = dt.today()
    df['quantidade'] = np.where(df['campanha']=='compre dois e leve 3', 2, 1)
    df['desconto'] = np.where(df['campanha']=='24 horas pela metade do preço', 0.5, 0)

    df.to_csv(f"{sources['local_csv']}{nome_arquivo}",sep=';',encoding='utf-8',index=None)
    


arquivos =  ['vendas_alpha.csv','vendas_beta.csv']
for arquivo in arquivos:
    col_cliente = []
    col_produto = []
    col_canal_venda = []
    col_campanha = []
    salva_arquivo(arquivo)