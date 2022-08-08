import pymssql 
from load_config import *
from common_functions import *

# ========================== CONNECTION ==========================
conn = pymssql.connect(CF_DBSERVER, CF_DBUSER, CF_DBPASSWORD, CF_DBNAME)
cursor = conn.cursor(as_dict=True)

# ========================= GENERATE HASHES =========================
def generate_hashes(client_db):

    try:
        columns_list = get_columns_list(cursor, CF_TABLENAME, CF_SCHEMA, CF_SKPCOLS)
    except:
        raise Exception("Failed to get the columns list.")

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
