class Config():
    DSN = 'LeadGenerator'
    MSSQL_DATABASE_URI = 'mssql+pyodbc://{}'.format(DSN)
    PROXY_ROTATOR = '209.126.120.13:9500'