import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function in order to drop existing tables.
    The list drop_table_queries is imported from sql_queries module.
    
    Input: cursor and connection to database.
    
    How does it work: iterate over each query of list drop_table_queries, then execute it to drop each table.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function in order to create staging as well as target tables.
    The list create_table_queries is imported from sql_queries module.
    
    Input: cursor and connection to database.
    
    How does it work: iterate over each query of list create_table_queries, then execute it to create each table. 
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    When .py file excuted in Command Prompt, this function will be invoked first.
    This file will be executed only if we want to clean existing tables then recreate them again to import new data.
    
    How does it work:
    - first, loading configurations from dwh.cfg file.
    - second, using configurations which just been loaded to connect database in Redshift.
    - once connection established, call helper function to drop existing tables.
    - then call another helper function to create those tables again.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()