from auxiliar import extract

extract(
    arquitecture='datalake',
    source='landing',
    target='landing',
    filename='empresa_alpha.csv',
    context='case',
    frequency='daily').params()

