import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('../config/etl.cfg')


def createConCurObjects():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    return (conn,cur)

def createSchema():

def purgeTable(conn,cur,tblName):
    query = "DELETE FROM "
    cur.execute()

def main():
    conn,cur = createConCurObjects()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()