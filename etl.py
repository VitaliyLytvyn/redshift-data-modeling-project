import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging tables on Redshift.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables to analytics tables on Redshift.
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Entry point.
    """

    # Read the configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Create the connection to a running Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data to Redshift cluster
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()