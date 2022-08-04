import pymssql 
from dotenv import load_dotenv
from generate_hashes import get_columns_list
import os

load_dotenv('.env')

# ========================== CONNECTION ==========================
driver = 'ODBC Driver 18 for SQL Server'
DB_SERVER=os.environ.get('DB_SERVER')
DB_NAME=os.environ.get('DB_NAME')
DB_USER=os.environ.get('DB_USER')
DB_PASSWORD=os.environ.get('DB_PASSWORD')

conn = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
conn2 = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
conn3 = pymssql.connect(DB_SERVER, DB_USER, DB_PASSWORD, DB_NAME)
cursor = conn.cursor(as_dict=True)
cursor_test = conn2.cursor(as_dict=True)
cursor_prod = conn3.cursor(as_dict=True)

# ========================== PARAMETERS ==========================
client_database_test = 'MaisonSinnae_LW_Test'
client_database_prod = 'MaisonSinnae_LW_Prod'
skip_columns = "'SystemModstamp', 'LastViewedDate', 'LastReferencedDate', 'LastModifiedDate', 'LastActivityDate','CreatedDate'"
schema_name = 'GVP'
table_name = 'gvp__Item__c'
commit_window = 100

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
                and t2.database_name = '''+"'"+client_database_test+"'"+'''
            where t1.database_name = '''+"'"+client_database_prod+"'"+'''
'''

cursor.execute(comparison)

problematic_columns = []

for row in cursor:

    for column in columns_list.split(","):

        select1 = "select "+column+" as col from "+client_database_test+"."+schema_name+"."+table_name+" where ID = '"+row['ID']+"'"
        select2 = "select "+column+" as col from "+client_database_prod+"."+schema_name+"."+table_name+" where ID = '"+row['ID']+"'"

        cursor_test.execute(select1)
        cursor_prod.execute(select2)

        for row_test in cursor_test:
            for row_prod in cursor_prod:
                if row_test['col'] != row_prod['col']:
                    if column not in problematic_columns:
                        problematic_columns.append(column)
                        print(row['ID'] + ' ' + column)


print(problematic_columns)
print(' => done')

conn.close()
conn2.close()
conn3.close()