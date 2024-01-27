import sys
sys.path.append(r'C:\Users\SALA443\Desktop\Estudos\GCP\case_env\Lib\site-packages')
import names
import random
import uuid
import json
from datetime import date as dt
    
ids = []
idades = []
nomes_male = []
genero_male = []
nomes_female = []
genero_female = [] 
lista_estados = ['RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE',
    'AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF']
since = []
numero_de_registros = 1000

# lista_de_variaveis = ['ids','idades','nomes_male','genero_male',
#                       'nomes_female','genero_female','lista_estados','since']

n = 1
while len(ids) < numero_de_registros:
    ids.append(n)
    n+=1
    
# criação das idades

while len(idades) < numero_de_registros:
    idades.append(random.randint(a=21,b=90))

# criação dos nomes

while len(nomes_male) < numero_de_registros / 2:
    nomes_male.append(names.get_full_name(gender='male'))


while len(genero_male) < numero_de_registros / 2:
    genero_male.append("m")


while len(nomes_female) < numero_de_registros / 2:
    nomes_female.append(names.get_full_name(gender='female'))


while len(genero_female) < numero_de_registros / 2:
    genero_female.append("f")
    
nomes = nomes_male+nomes_female
genero = genero_male+genero_female

# criação dos estados

estados = []
for n in range(numero_de_registros):
    estados.append(lista_estados[random.randint(a=7,b=25)])

# criação das datas de registro

for n in range(numero_de_registros):
    data_str = str(dt(random.randrange(2020, 2024),random.randrange(1, 12),random.randrange(1, 28)))
    since.append(data_str)

# criação do dicionario
data = []

for n in range(numero_de_registros):
    data.append(dict(registro = ids[n], nome = nomes[n],genero = genero[n], idade = idades[n], estado = estados[n], data_registro = since[n]))

# conversão para json
file = json.dumps(data)


with open("C:/Users/SALA443/Desktop/Estudos/GCP/GCP_DataEngineering/sources/mongo/clientes.json", "w") as arquivo:
    arquivo.write(file)
