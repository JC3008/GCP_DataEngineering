from auxiliar import extract

arquivos = ['vendas_alpha','vendas_beta','produtos']
print('Inicio da carga dos arquivos csv')
for arquivo in arquivos:
    print(f'carregando {arquivo}')
    extract(
    arquitecture=['local_source','datalake'],
    source='local_csv',
    destination='landing',
    filename=arquivo,
    context='case',
    frequency='daily').csv_to_gcp()
    
    
    
print('Inicio da carga dos arquivos json')

extract(
arquitecture=['local_source','datalake'],
source='local_json',
destination='landing',
filename='clientes',
context='case',
frequency='daily').mongo_to_gcp()

arquivos = ['vendas_alpha','vendas_beta','produtos']
print('Inicio da carga dos arquivos csv')
for arquivo in arquivos:
    extract(
        arquitecture=['datalake','datalake'],
        source='landing',
        destination='processed',
        filename=arquivo,
        context='case',
        frequency='daily').landing_to_processed()
    
    
    
extract(
arquitecture=['datalake','datalake'],
source='landing',
destination='processed',
filename='clientes',
context='case',
frequency='daily').landing_to_processed_json()



    

