import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all the tables in Redshift defined in sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tabless in Redshift defined in sql_queries.py
    """
    for query in create_table_queries:
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

    # Delete, if exist, and create anew tables in the Redshift cluster
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()