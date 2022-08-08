import pymssql 
from load_config import *
from common_functions import *

# ========================== CONNECTION ==========================
conn = pymssql.connect(CF_DBSERVER, CF_DBUSER, CF_DBPASSWORD, CF_DBNAME)
cursor = conn.cursor(as_dict=True)

# ========================= COMPARE HASHES =========================
try:
    columns_list = get_columns_list(cursor, CF_TABLENAME, CF_SCHEMA, CF_SKPCOLS)
except:
    raise Exception("Failed to get the columns list.")

print(' => Comparing hashes')
hashes_query = '''
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
                and t2.database_name = '''+"'"+CF_DATABASE+"'"+'''
            where t1.database_name = '''+"'"+CF_DATABASE_TARGET+"'"+'''
'''

cursor.execute(hashes_query)

# ========================= IDs TO COMPARE =========================
print(' => Getting IDs to compare columns')
ids_to_check = ''
for row in cursor:

    ids_to_check += "'"+ row['ID'] + "',"

ids_to_check = ids_to_check[:-1]

if ids_to_check == '':
    raise Exception("No ID to be compared.")

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
            if col_name.strip() not in problematic_columns:
                problematic_columns.append(col_name.strip())

print(' => Columns to check:')
print(problematic_columns)
print(' => done')

conn.close()