import pymssql 
from load_config import *
from common_functions import *

# ========================== CONNECTION ==========================
conn = pymssql.connect(CF_DBSERVER, CF_DBUSER, CF_DBPASSWORD, CF_DBNAME)
cursor = conn.cursor(as_dict=True)

# ========================= GENERATE HASHES =========================
def generate_hashes(client_db, schemaname, tablename):

    try:
        columns_list = get_columns_list(cursor, tablename, schemaname, CF_SKPCOLS)
    except:
        raise Exception("Failed to get the columns list.")

    # print(' => Deleting previous hashes')
    # cursor.execute("delete from dbo.hash_table where database_name = '"+client_db+"' and table_schema = '"+schemaname+"' and table_name  ='"+tablename+"'")
    # conn.commit()

    isDeletedColumn = 'IsDeleted' if 'IsDeleted' in get_full_columns_list(cursor, tablename, schemaname).split(',') else "'false'"
    LastModifiedDateColumn = 'LastModifiedDate' if 'LastModifiedDate' in get_full_columns_list(cursor, tablename, schemaname).split(',') else "'1900-01-01'"

    print(' => Generating hashes for '+client_db+'.'+schemaname+'.'+tablename)
    insert_command = '''insert into dbo.hash_table
                    select top 100
                        '''+"'"+client_db+"'"+''', 
                        '''+"'"+schemaname+"'"+''', 
                        '''+"'"+tablename+"'"+''', 
                        id, 
                        cast(BINARY_CHECKSUM('''+columns_list+''') as varchar) + '|' + cast(checksum('''+columns_list+''') as varchar) row_hash,
                        '''+isDeletedColumn+''' as isDeleted
                    from '''+client_db+'''.'''+schemaname+'''.'''+tablename+'''
                    where '''+LastModifiedDateColumn+''' < DATEADD(day, -2, cast(getdate() as date))
                    order by id desc
    '''
    cursor.execute(insert_command)
    conn.commit()

print('Running',CF_MODE,'mode')

print(' => Truncating hash table')
cursor.execute("truncate table dbo.hash_table;")
conn.commit()

if CF_MODE == 'full':
    tableslist = get_tables_list(cursor, CF_SCHEMALIST)

    for item in tableslist:
        generate_hashes(CF_DATABASE, item['schemaname'], item['tablename'])
        generate_hashes(CF_DATABASE_TARGET, item['schemaname'], item['tablename'])

elif CF_MODE == 'single':
    generate_hashes(CF_DATABASE, CF_SCHEMA, CF_TABLENAME)
    generate_hashes(CF_DATABASE_TARGET, CF_SCHEMA, CF_TABLENAME)

print(' => done')
conn.close()
