# Resumo do projeto
Nome do projeto no GCP: **Vendas-De**
Este projeto visa a criação de um projeto de engenharia de dados para analisar dados comerciais de duas empresas fictícias.

# Arquitetura
A arquiquetura foi definida como:
* GCP - datalake
* Bigquery - Data Warehouse
* Data Studio - Visualização de dados
* Container - Docker
* Banco de Dados NOSQL - Mongo
* Banco de dados SQL - MySQL
* Scripts - Python

# Estrutura das pastas
Dentro da pasta GCP_DataEngineering temos as pastas:
### elt_script
Esta pasta contém o script Python que carrega os dados para o bucket GCP, bem como carrega metadados[process_id;loaded_date;loaded_time;process_id;rows_count;from;to].
O arquivo auxiliar.py contém as classes utilizadas. <br>
**frequency**: Classe criada para a definição de estrutura de pastas no destino, de forma que podemos escolher salvar: <br>
    daily = yyyy/mm/dd/
    monthly = yyyy/mm/
    hourly = yyyy/mm/dd/hh <br>
**extract**: Classe desenvolvida para padronizar o script de carga de dados no bucket GCP, de forma que apenas seja preciso passar alguns parâmetros na chamada da função.
Exemplo de chamada da função: <br>
![Alt text](imagens/extract.png)














