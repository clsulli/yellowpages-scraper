class Config():
    DSN = 'LeadGenerator'
    MSSQL_DATABASE_URI = 'mssql+pyodbc://{}'.format(DSN)