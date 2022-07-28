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
cursor = conn.cursor(as_dict=True)

# ========================== PARAMETERS ==========================
client_database = 'Opici_LW_Prod'
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

# ========================= GENERATE HASHES =========================
columns_list = get_columns_list(cursor)

print(' => Deleting previous hashes')
cursor.execute("delete from dbo.hash_table where database_name = '"+client_database+"' and table_schema = '"+schemaname+"' and table_name  ='"+tablename+"'")
conn.commit()

print(' => Generating hashes for '+client_database+'.'+schemaname+'.'+tablename)
insert_command = '''insert into dbo.hash_table
                select '''+"'"+client_database+"'"+''', '''+"'"+schemaname+"'"+''', '''+"'"+tablename+"'"+''', id, cast(BINARY_CHECKSUM('''+columns_list+''') as varchar) + '|' + cast(checksum('''+columns_list+''') as varchar) row_hash 
                from '''+client_database+'''.'''+schemaname+'''.'''+tablename+'''
'''
cursor.execute(insert_command)
conn.commit()

print(' => done')

conn.close()