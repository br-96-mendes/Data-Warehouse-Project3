import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
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
