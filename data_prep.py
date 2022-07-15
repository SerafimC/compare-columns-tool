import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def insert_metric(conn, databasename, schemaname, tablename, rowcount):

    cur = conn.cursor()
    command = "insert into Metrics values('"+databasename+"','"+schemaname+"','"+tablename+"',"+rowcount+");"
    print(command)
    cur.execute(command,)

    return 

def clean_metrics(conn, client_database):

    cur = conn.cursor()
    command = "delete from Metrics where database_name = '"+client_database+"';"
    print(command)
    cur.execute(command,)

    return 

def select_tables(conn):

    tables_list = []

    cur = conn.cursor()
    cur.execute("select * from TablesObjects",)

    rows = cur.fetchall()

    for row in rows:
        tables_list.append([row[0], row[1]])

    return tables_list
        
def main():
    database = "./metrics.sqlite"

    # create a database connection
    conn = create_connection(database)
    with conn:
        select_tables(conn)

    return 0

main()