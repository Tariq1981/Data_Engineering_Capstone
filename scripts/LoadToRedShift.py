import configparser
import psycopg2
from SQLQueries import *

config = configparser.ConfigParser()
config.read('../config/etl.cfg')


def createConCurObjects():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    return (conn,cur)

def createSchema(conn,cur):
    cur.execute(create_schema)
    conn.commit()

def createTables(conn,cur):
    for query in create_tables_list:
        cur.execute(query)
        conn.commit()

def purgeTable(conn,cur,tblName):
    for tblName in config["DWH_TABLES"].keys():
        query = delete_table_sql.format(config["DWH_TABLES"][tblName])
        cur.execute(query)
        conn.commit()

def main():
    conn,cur = createConCurObjects()
    createSchema(conn,cur)
    createTables(conn,cur)
    purgeTable(conn,cur)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()