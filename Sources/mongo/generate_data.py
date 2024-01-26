import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Estudos\GCP\case_env\Lib\site-packages')
import names
import random
import uuid
import json
from datetime import date as dt


# criação dos registros
ids = list()
while len(ids) < 101:
    ids.append(uuid.uuid4().hex[:8])
    
# criação das idades
idades = list()
while len(idades) < 101:
    idades.append(random.randint(a=21,b=90))

# criação dos nomes
nomes = list()
while len(nomes) < 101:
    nomes.append(names.get_full_name())

# criação dos estados
lista_estados = ['RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE',
      'AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF']
estados = []
for n in range(101):
    estados.append(lista_estados[random.randint(a=7,b=25)])

# criação das datas de registro
since = list()
for n in range(101):
    data_str = str(dt(random.randrange(2020, 2024),random.randrange(1, 12),random.randrange(1, 28)))
    since.append(data_str)

# criação do dicionario
data = []

for n in range(101):
    data.append(dict(registro = ids[n], nome = nomes[n], idade = idades[n], estado = estados[n], data_registro = since[n]))

# conversão para json
file = json.dumps(data)


with open("C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/sources/mongo/clientes.json", "w") as arquivo:
    arquivo.write(file)
