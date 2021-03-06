import configparser
import psycopg2
from SQLQueries import *

config = configparser.ConfigParser()
config.read('../config/etl.cfg')


def createConCurObjects():
    """
        Description: This function creates the connection and cursor objects

        Arguments:
            None

        Returns:
            conn: The connection object
            cur: The cursor object
    """
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    return (conn,cur)

def createSchema(conn,cur):
    """
        Description: This function creates the schema used in the model

        Arguments:
            conn: The connection object
            cur: The cursor object

        Returns:
            None
    """
    cur.execute(create_schema)
    conn.commit()

def createTables(conn,cur):
    """
        Description: This function creates the tables used in the model.

        Arguments:
            conn: The connection object
            cur: The cursor object

        Returns:
            None
    """
    for query in create_tables_list:
        cur.execute(query)
        conn.commit()

def purgeTables(conn,cur):
    """
        Description: This function purges all the tables in the model.

        Arguments:
            conn: The connection object
            cur: The cursor object

        Returns:
            None
    """
    for tblName in config["DWH_TABLES"]:
        query = delete_table_sql.format(config["DWH_SCHEMA"]["MODEL_SCH"],config["DWH_TABLES"][tblName])
        cur.execute(query)
        conn.commit()

def copyTables(conn,cur):
    """
        Description: This function copies the data from S3 to the target table in Redshift

        Arguments:
            conn: The connection object
            cur: The cursor object

        Returns:
            None
    """
    for i in range(0,len(src_dl_tables_list)):
        query = copy_table_sql.format(trgt_dw_tables_list[i],config["S3"]["TARGET_BUCKET"],
                                      src_dl_tables_list[i],config["S3"]["AWS_ACCESS_KEY_ID"],
                                      config["S3"]["AWS_SECRET_ACCESS_KEY"])
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