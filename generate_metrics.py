import pymssql 
import data_prep as data_proc

sqlite = data_proc.create_connection("./metrics.sqlite")
tables_list = data_proc.select_tables(sqlite)

driver = 'ODBC Driver 18 for SQL Server'
DB_SERVER='db_server'
DB_NAME='db_name'
DB_USER='db_user'
DB_PASSWORD='db_password'

conn = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
cursor = conn.cursor(as_dict=True)

client_database = 'cl_db'

with sqlite:
    data_proc.clean_metrics(sqlite, client_database)

for t in tables_list:
    try:
        cursor.execute('select count(*) as cnt from '+client_database+'.'+t[0]+'.'+t[1]+';')
        for row in cursor:
            with sqlite:
                data_proc.insert_metric(sqlite, client_database, t[0], t[1], str(row['cnt']))
    except:
        pass
    finally:
        pass

conn.close()