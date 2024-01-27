from auxiliar import extract

if __name__ == "__main__":
    print("here starts the csv elt")
    extract(
    arquitecture=['local_source','datalake'],
    source='local_csv',
    destination='landing',
    filename='empresa_alpha.csv',
    context='case',
    frequency='daily').transfer()
    print("here ends the csv elt")
    
    print("here starts the json elt")
    extract(
    arquitecture=['local_source','datalake'],
    source='local_json',
    destination='landing',
    filename='clientes.json',
    context='case',
    frequency='daily').mongo_to_gcp()
    print("here ends the csv elt")

    

