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
def get_columns_list(c1):
    columns_list = ''

    c1.execute('''select 
                    distinct column_name
                from information_schema.columns
                where table_name = '''+"'"+CF_TABLENAME+"'"+''' and table_schema = '''+"'"+CF_SCHEMA+"'"+'''
                and column_name not in ('''+CF_SKPCOLS+''') order by 1''')

    for row in c1:
        columns_list += row['column_name'] + ', '
    
    columns_list = columns_list[:-2]

    return columns_list

# ========================= COMPARE HASHES =========================
columns_list = get_columns_list(cursor)

print(' => Comparing hashes')
hashes_query = '''
            select top 1
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
                and t2.database_name = '''+"'"+CF_DATABASE+"'"+'''
            where t1.database_name = '''+"'"+CF_DATABASE_TARGET+"'"+'''
'''

cursor.execute(hashes_query)

# ========================= IDs TO COMPARE =========================
print(' => Getting IDs to comapre columns')
ids_to_check = ''
for row in cursor:

    ids_to_check += "'"+ row['ID'] + "',"

ids_to_check = ids_to_check[:-1]

problematic_columns = []

compare_query = 'select '

# ========================= COMPARE COLUMNS =========================
print(' => Comparing columns')
for col_name in columns_list.split(','):
    compare_query += 't1.'+col_name.strip() +' as '+ col_name.strip() + 'c1,' + 't2.'+col_name.strip() + ' as ' + col_name.strip() + 'c2,'

compare_query = compare_query[:-1]
compare_query += " from "+CF_DATABASE+"."+CF_SCHEMA+"."+CF_TABLENAME+" t1 "
compare_query += " inner join "+CF_DATABASE_TARGET+"."+CF_SCHEMA+"."+CF_TABLENAME+" t2 "
compare_query += " on t1.ID = t2.ID "
compare_query += " and t1.ID in (" + ids_to_check + ")"

cursor.execute(compare_query)

for row in cursor:

    for col_name in columns_list.split(','):
        if row[col_name.strip()+'c1'] != row[col_name.strip()+'c2']:
            problematic_columns.append(col_name.strip())

print(' => Columns to check:')
print(problematic_columns)
print(' => done')

conn.close()