import os
import trino


def run_trino_query_dq_check(query) -> bool:
    """Check for non-null response"""
    
    results = execute_trino_query(query)
    
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    
    return all(bool(column) for result in results for column in result)


def execute_trino_query(query):
    """Returns response from Trino"""

    conn = trino.dbapi.connect(
        host=os.environ['DATAEXPERT_TRINO_HOST'],
        port=443,
        user=os.environ['DATAEXPERT_TRINO_USER'],
        http_scheme='https',
        catalog=os.environ['DATAEXPERT_TRINO_CATALOG'],
        auth=trino.auth.BasicAuthentication(os.environ['DATAEXPERT_TRINO_USER'], os.environ['DATAEXPERT_TRINO_PW']),
    )
    print(query)
    cursor = conn.cursor()
    print("Executing query for the first time...")
    cursor.execute(query)
    return cursor.fetchall()
