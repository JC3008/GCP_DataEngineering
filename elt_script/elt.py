from auxiliar import extract
import logging
# arquivos = ['vendas_alpha','vendas_beta','produtos','calendario']


logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("case.log", mode='w'),
                  logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )

def carregar_csv(arquivos:list):    

    print('Inicio da carga dos arquivos csv')
    for arquivo in arquivos:
        logging.info(f"carregando o arquivo {arquivo} para o GCS.")
        print(f'carregando {arquivo}')
        extract(
        arquitecture=['local_source','datalake'],
        source='container_csv',
        destination='landing',
        filename=arquivo,
        context='case',
        frequency='daily').csv_to_gcp()
        
def carregar_mongo_para_gcs():   
    
    print('Inicio da carga dos arquivos json')

    extract(
    arquitecture=['local_source','datalake'],
    source='container_json',
    destination='landing',
    filename='clientes',
    context='case',
    frequency='daily').mongo_to_gcp()


def carrega_csv_da_landing_para_processed(arquivos:list):
    
    print('Inicio da carga dos arquivos csv')
    for arquivo in arquivos:
        logging.info(f"carregando o arquivo {arquivo} para o GCS.")
        extract(
            
            arquitecture=['datalake','datalake'],
            source='landing',
            destination='processed',
            filename=arquivo,
            context='case',
            frequency='daily').landing_to_processed()
            
    
def landing_para_processed_json():    
        
    extract(
    arquitecture=['datalake','datalake'],
    source='landing',
    destination='processed',
    filename='clientes',
    context='case',
    frequency='daily').landing_to_processed_json()

def carrega_obt_na_consume():
    
    extract(
            arquitecture=['datalake','datalake'],
            source='processed',
            destination='consume',
            filename=['vendas_alpha','vendas_beta','produtos','calendario','clientes'],
            context='case',
            frequency='daily').processed_to_consume()
    
    

if __name__ == "__main__":
    
    
    carregar_csv(['vendas_alpha','vendas_beta','produtos','calendario'])
    carregar_mongo_para_gcs()
    carrega_csv_da_landing_para_processed(['vendas_alpha','vendas_beta','produtos','calendario'])
    landing_para_processed_json()
    carrega_obt_na_consume()

    
