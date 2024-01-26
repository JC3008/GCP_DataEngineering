from auxiliarcopy import extract

if __name__ == "__main__":
    extract(
    arquitecture=['local_source','datalake'],
    source='local_json',
    destination='landing',
    filename='clientes.json',
    context='case',
    frequency='daily').mongo_to_gcp()
    
    extract(
    arquitecture=['local_source','datalake'],
    source='local_csv',
    destination='landing',
    filename='empresa_alpha.csv',
    context='case',
    frequency='daily').transfer()

