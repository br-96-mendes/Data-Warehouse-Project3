import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Establishes connection with the Redshift and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    cluster_params = [
        config['CLUSTER']['HOST'],
        config['CLUSTER']['DWH_DB'],
        config['CLUSTER']['DWH_DB_USER'],
        config['CLUSTER']['DWH_DB_PASSWORD'],
        config['CLUSTER']['DWH_PORT']
    ]

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*cluster_params))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
