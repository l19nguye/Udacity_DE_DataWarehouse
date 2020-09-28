import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, counting_queries


def load_staging_tables(cur, conn):
    """
    This function will execute queries to copy data into table. 
    The list of queries copy_table_queries is imported from sql_queries module.
    
    Input: cursor and connection to database.
    
    How does it work: will iterate over each query of list copy_table_queries, then execute it.
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function will execute queries to insert data into staging as well as target tables.
    The list of queries insert_table_queries is imported from sql_queries module.
    
    Input: cursor and connection to database.
    
    How does it work: will iterate over each query of list copy_table_queries, then execute it.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
def counting_records(cur, conn):
    """
    This function will count the number of record of each table then print out the result.
    
    Input: cursor and connection to database.
    
    How does it work: will iterate over each query of list counting_queries, then execute it.
    """
    
    for query in counting_queries:
        cur.execute(query)
        print("table {} has {} records.".format(query[query.rfind(" "): ], cur.fetchall()[0][0]))


def main():
    """
    When .py file excuted in Command Prompt, this function will be invoked first.
    
    How does it work:
    
    - first, loading configurations from dwh.cfg file.
    - second, using configurations which just been loaded to connect database in Redshift.
    - once connection established, call helper function to load data into staging tables.
    - call helper function insert_tables() to insert data into tables.
    - call helper function counting_records() to print out number of records of each table.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    counting_records(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()