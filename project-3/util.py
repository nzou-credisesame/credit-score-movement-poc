import psycopg2




CREDENTIALS = {
                'user' : 'nzou',
                'password' : 'Nzou03355!',
                'host' : 'localhost',
                'port' : '50439',
                'dbname' : 'prod'
              }
              

def create_engine_string():
    engine_string = 'postgresql://%(user)s:%(password)s@%(host)s:%(port)s/%(dbname)s'
    return engine_string % {'user': CREDENTIALS['user'],
                            'password': CREDENTIALS['password'],
                            'host': CREDENTIALS['host'],
                            'port': CREDENTIALS['port'],
                            'dbname': CREDENTIALS['dbname']}

def insert_df_to_redshift(df, table_name, schema, index=False,append=True):
    """Insert a dataframe to a redshift table."""
    engine_string = create_engine_string()
    print ('creating engine...')
    engine = create_engine(engine_string)
    print ('uploading to db')
    if append:
        if_exists = 'append'
    else:
        if_exists = 'fail'
    df.to_sql(table_name, engine, schema=schema, index=index, if_exists=if_exists)
    return 'Upload successful!!'

def redshift_query_to_df(query, CREDENTIALS = CREDENTIALS):
    """Connect to redshift and return a dataframe. Note that this is just for select statements."""
    try:
        with psycopg2.connect(
            host=CREDENTIALS['host'],
            user=CREDENTIALS['user'],
            port=CREDENTIALS['port'],
            password=CREDENTIALS['password'],
            dbname=CREDENTIALS['dbname'],
            connect_timeout=14400) as conn:
            print ('connection established')
            print ('executing query...')
            query_results = pd.read_sql(query, conn)
    except Exception as e:
        print ("Error querying SQL {:s}".format(e))
        raise e
           
    return query_results