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

def purgeTables(conn,cur):
    for tblName in config["DWH_TABLES"].keys():
        query = delete_table_sql.format(config["DWH_TABLES"][tblName])
        cur.execute(query)
        conn.commit()

def copyTables(conn,cur):
    for i in range(0,len(src_dl_tables_list)):
        query = copy_table_sql.format(trgt_dw_tables_list[i],config["S3"]["TARGET_BUCKET"],
                                      src_dl_tables_list[i],config["S3"]["IAM_ROLE"])
        cur.execute(query)
        conn.commit()


def main():
    conn,cur = createConCurObjects()
    createSchema(conn,cur)
    createTables(conn,cur)
    purgeTables(conn,cur)
    copyTables(conn,cur)
    conn.close()


if __name__ == "__main__":
    main()