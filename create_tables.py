import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from iac import *

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    cluster_params = [
        config['CLUSTER']['HOST'],
        config['CLUSTER']['DWH_DB'],
        config['CLUSTER']['DWH_DB_USER'],
        config['CLUSTER']['DWH_DB_PASSWORD']
    ]

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*cluster_params))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    create_redshift_cluster()
    #redshift = create_redshift_cluster(roleArn)
    #redshift = open_tcp_port(redshift)
    #main()
