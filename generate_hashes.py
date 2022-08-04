import pymssql 
from dotenv import load_dotenv
import os

load_dotenv('.env')

# ========================== CONNECTION ==========================
driver = 'ODBC Driver 18 for SQL Server'
DB_SERVER=os.environ.get('DB_SERVER')
DB_NAME=os.environ.get('DB_NAME')
DB_USER=os.environ.get('DB_USER')
DB_PASSWORD=os.environ.get('DB_PASSWORD')

conn = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
cursor = conn.cursor(as_dict=True)

# ========================== PARAMETERS ==========================
client_database_test = 'MaisonSinnae_LW_Test'
client_database_prod = 'MaisonSinnae_LW_Prod'
skip_columns = "'SystemModstamp', 'LastViewedDate', 'LastReferencedDate', 'LastModifiedDate', 'LastActivityDate','CreatedDate'"
schema_name = 'GVP'
table_name = 'gvp__Item__c'
commit_window = 100


# =========================== FUNCTIONS ===========================
def get_columns_list(c1):
    columns_list = ''

    c1.execute('''select 
                    distinct column_name
                from information_schema.columns
                where table_name = '''+"'"+table_name+"'"+''' and table_schema = '''+"'"+schema_name+"'"+'''
                and column_name not in ('''+skip_columns+''') order by 1''')

    for row in c1:
        columns_list += row['column_name'] + ', '
    
    columns_list = columns_list[:-2]

    return columns_list

def generate_hashes(database):
    columns_list = get_columns_list(cursor)

    print(' => Deleting previous hashes')
    cursor.execute("delete from dbo.hash_table where database_name = '"+database+"' and table_schema = '"+schema_name+"' and table_name  ='"+table_name+"'")
    conn.commit()

    print(' => Generating hashes for '+database+'.'+schema_name+'.'+table_name)
    hashtable_insert_sql = '''insert into dbo.hash_table
                    select '''+"'"+database+"'"+''', '''+"'"+schema_name+"'"+''', '''+"'"+table_name+"'"+''', id, cast(BINARY_CHECKSUM('''+columns_list+''') as varchar) + '|' + cast(checksum('''+columns_list+''') as varchar) row_hash 
                    from '''+database+'''.'''+schema_name+'''.'''+table_name+'''
    '''
    cursor.execute(hashtable_insert_sql)
    conn.commit()

    print(' => Done.')

    

# ========================= GENERATE HASHES =========================

if __name__ == "__main__":
    generate_hashes(client_database_test)
    generate_hashes(client_database_prod)
    conn.close()