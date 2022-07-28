import pymssql 
import data_prep as data_proc

sqlite = data_proc.create_connection("./metrics.sqlite")

# ========================== CONNECTION ==========================
driver = 'ODBC Driver 18 for SQL Server'
DB_SERVER='db_server'
DB_NAME='db_name'
DB_USER='db_user'
DB_PASSWORD='db_password'

conn = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
conn2 = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
conn3 = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
cursor = conn.cursor(as_dict=True)
cursor2 = conn2.cursor(as_dict=True)
cursor3 = conn3.cursor(as_dict=True)

# ========================== PARAMETERS ==========================
client_database = 'Opici_LW_Test'
client_database_target = 'Opici_LW_Prod'
skip_columns = "'SystemModstamp', 'LastViewedDate', 'LastReferencedDate', 'LastModifiedDate', 'LastActivityDate','CreatedDate'"
schemaname = 'GVP'
tablename = 'gvp__survey__c'
commit_window = 100

# =========================== FUNCTIONS ===========================
def get_columns_list(c1):
    columns_list = ''

    c1.execute('''select 
                    distinct column_name
                from information_schema.columns
                where table_name = '''+"'"+tablename+"'"+''' and table_schema = '''+"'"+schemaname+"'"+'''
                and column_name not in ('''+skip_columns+''') order by 1''')

    for row in c1:
        columns_list += row['column_name'] + ', '
    
    columns_list = columns_list[:-2]

    return columns_list

# ========================= COMPARE COLUMNS =========================
columns_list = get_columns_list(cursor)

comparison = '''
            select top 10
                t1.table_schema, 
                t1.table_name , 
                t1.ID, 
                t1.row_hash as t1_hash, 
                t2.row_hash as t2_hash
            from dbo.hash_table t1
            inner join dbo.hash_table t2
                on t1.table_schema = t2.table_schema 
                and t1.table_name = t2.table_name 
                and t1.ID = t2.ID 
                and t1.row_hash <> t2.row_hash 
                and t2.database_name = 'Opici_LW_Prod'
            where t1.database_name = 'Opici_LW_Test'
'''

cursor.execute(comparison)

for row in cursor:

    for cn in columns_list.split(","):

        select1 = "select "+cn+" as col from "+client_database+"."+schemaname+"."+tablename+" where ID = '"+row['ID']+"'"
        select2 = "select "+cn+" as col from "+client_database_target+"."+schemaname+"."+tablename+" where ID = '"+row['ID']+"'"

        cursor2.execute(select1)
        cursor3.execute(select2)

        for row_cursor2 in cursor2:
            for row_cursor3 in cursor3:
                if row_cursor2['col'] != row_cursor3['col']:
                    print(cn)

print(' => done')

conn.close()
conn2.close()
conn3.close()