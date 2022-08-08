import pymssql 
import json

f = open('config.json')
config = json.load(f)
f.close()

# ========================== CONNECTION ==========================
CF_DBSERVER = config['DB_SERVER']
CF_DBNAME = config['DB_NAME']
CF_DBUSER = config['DB_USER']
CF_DBPASSWORD = config['DB_PASSWORD']

conn = pymssql.connect(CF_DBSERVER, CF_DBUSER, CF_DBPASSWORD, CF_DBNAME)
cursor = conn.cursor(as_dict=True)

# ========================== PARAMETERS ==========================
CF_DATABASE = config['client_database']
CF_DATABASE_TARGET = config['client_database_to_compare']
CF_SKPCOLS = config['skip_columns']
CF_SCHEMA = config['schemaname']
CF_TABLENAME = config['tablename']

# =========================== FUNCTIONS ===========================
def get_columns_list(con_cursor):
    columns_list = ''

    con_cursor.execute('''select 
                    distinct column_name
                from information_schema.columns
                where table_name = '''+"'"+CF_TABLENAME+"'"+''' and table_schema = '''+"'"+CF_SCHEMA+"'"+'''
                and column_name not in ('''+CF_SKPCOLS+''') order by 1''')

    for row in con_cursor:
        columns_list += row['column_name'] + ', '
    
    columns_list = columns_list[:-2]

    return columns_list

# ========================= GENERATE HASHES =========================
def generate_hashes(client_db):
    columns_list = get_columns_list(cursor)

    print(' => Deleting previous hashes')
    cursor.execute("delete from dbo.hash_table where database_name = '"+client_db+"' and table_schema = '"+CF_SCHEMA+"' and table_name  ='"+CF_TABLENAME+"'")
    conn.commit()

    print(' => Generating hashes for '+client_db+'.'+CF_SCHEMA+'.'+CF_TABLENAME)
    insert_command = '''insert into dbo.hash_table
                    select '''+"'"+client_db+"'"+''', '''+"'"+CF_SCHEMA+"'"+''', '''+"'"+CF_TABLENAME+"'"+''', id, cast(BINARY_CHECKSUM('''+columns_list+''') as varchar) + '|' + cast(checksum('''+columns_list+''') as varchar) row_hash 
                    from '''+client_db+'''.'''+CF_SCHEMA+'''.'''+CF_TABLENAME+'''
    '''
    cursor.execute(insert_command)
    conn.commit()

    print(' => done')

generate_hashes(CF_DATABASE)
generate_hashes(CF_DATABASE_TARGET)

conn.close()