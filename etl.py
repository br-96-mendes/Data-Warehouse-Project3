import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    This function receives cursor and connection to the Redshift and load the json files
    (log and events) into a relational database (two staging databases)

    Args:
        cur: Redshift cursor.
        conn: Redshift connection.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function receives cursor and connection to the Redshift, transforms the
    staging data and load into the relational database.

    Args:
        cur: Redshift cursor.
        conn: Redshift connection.
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function loads the Redshift environment variables, creates a connection and a cursor,
    loads the json data into staging tables, transform the data and loads into the relational
    model.
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
