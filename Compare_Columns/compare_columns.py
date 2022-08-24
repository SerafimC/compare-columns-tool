
# ========================= FUNCTIONS =========================
def compare_hashes(cursor, schemaname, tablename, config, common_functions):
    try:
        columns_list = common_functions.get_columns_list(cursor, tablename, schemaname, config.CF_SKPCOLS)
    except:
        raise Exception("Failed to get the columns list for",schemaname,'.',tablename )

    print(' => Comparing hashes - ',schemaname,'.',tablename)
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
                    and t2.database_name = '''+"'"+config.CF_DATABASE+"'"+'''
                    and t2.isDeleted = 'false'
                where t1.database_name = '''+"'"+config.CF_DATABASE_TARGET+"'"+'''
                and t1.table_schema = '''+"'"+schemaname+"'"+'''
                and t1.table_name = '''+"'"+tablename+"'"+'''
                and t1.isDeleted = 'false'
    '''

    cursor.execute(hashes_query)

    # ========================= IDs TO COMPARE =========================
    print(' => Getting IDs to compare columns -',schemaname,'.',tablename)
    ids_to_check = ''
    for row in cursor:

        ids_to_check += "'"+ row['ID'] + "',"

    ids_to_check = ids_to_check[:-1]

    return ids_to_check

def compare_columns(cursor, schemaname, tablename, ids_list, config, common_functions, log):

    try:
        columns_list = common_functions.get_columns_list(cursor, tablename, schemaname, config.CF_SKPCOLS)
    except:
        raise Exception("Failed to get the columns list for",schemaname,'.',tablename )

    problematic_columns = []
    
    print(' => Comparing columns -',schemaname,'.',tablename)
    compare_query = 'select t1.id,'
    for col_name in columns_list.split(','):
        compare_query += 't1.'+col_name.strip() +' as '+ col_name.strip() + 'c1,' + 't2.'+col_name.strip() + ' as ' + col_name.strip() + 'c2,'

    compare_query = compare_query[:-1]
    compare_query += " from "+config.CF_DATABASE+"."+schemaname+"."+tablename+" t1 "
    compare_query += " inner join "+config.CF_DATABASE_TARGET+"."+schemaname+"."+tablename+" t2 "
    compare_query += " on t1.ID = t2.ID "
    compare_query += " and t1.ID in (" + ids_list + ")"

    cursor.execute(compare_query)

    for row in cursor:

        for col_name in columns_list.split(','):
            if row[col_name.strip()+'c1'] != row[col_name.strip()+'c2']:
                if col_name.strip() not in [item['column'] for item in problematic_columns]:
                    problematic_columns.append({"column":col_name.strip(), "example":row['id']})

    log.write_to_log(config.CF_DATABASE+' - '+schemaname+'.'+tablename)
    print(' => Columns to check: - '+schemaname+'.'+tablename)
    print(*problematic_columns, sep='\n')
    log.write_to_log(problematic_columns)
    log.write_to_log(' ==============')
    print(' => done')

# ========================= EXECUTION =========================
def run(conn, cursor, config, common_functions, log):
    print('Running',config.CF_MODE,'mode')

    if config.CF_MODE == 'full':
        tableslist = common_functions.get_tables_list(cursor, config.CF_SCHEMALIST)

        for item in tableslist:
            ids_to_check = compare_hashes(cursor, item['schemaname'], item['tablename'], config, common_functions)
            if ids_to_check == '':
                print("No ID to be compared -",item['schemaname'],'.',item['tablename'] )
                continue
            compare_columns(cursor, item['schemaname'], item['tablename'], ids_to_check, config, common_functions, log)

    elif config.CF_MODE == 'single':
        ids_to_check = compare_hashes(cursor, config.CF_SCHEMA, config.CF_TABLENAME, config, common_functions)
        if ids_to_check == '':
                print("No ID to be compared -", config.CF_SCHEMA,'.', config.CF_TABLENAME)
        else:
            compare_columns(cursor, config.CF_SCHEMA, config.CF_TABLENAME, ids_to_check, config, common_functions, log)
