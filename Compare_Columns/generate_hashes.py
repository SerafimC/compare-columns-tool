
# ========================= GENERATE HASHES =========================
def generate_hashes(conn, cursor, client_db, schemaname, tablename, skip_columns, common_functions):

    try:
        columns_list = common_functions.get_columns_list(cursor, tablename, schemaname, skip_columns)
    except:
        raise Exception("Failed to get the columns list.")

    # print(' => Deleting previous hashes')
    # cursor.execute("delete from dbo.hash_table where database_name = '"+client_db+"' and table_schema = '"+schemaname+"' and table_name  ='"+tablename+"'")
    # conn.commit()

    isDeletedColumn = 'IsDeleted' if 'IsDeleted' in common_functions.get_full_columns_list(cursor, tablename, schemaname).split(',') else "'false'"
    LastModifiedDateColumn = 'LastModifiedDate' if 'LastModifiedDate' in common_functions.get_full_columns_list(cursor, tablename, schemaname).split(',') else "'1900-01-01'"

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
                    and id is not null
                    order by id desc
    '''
    cursor.execute(insert_command)
    conn.commit()

def run(conn, cursor, config, common_functions):
    print('Running',config.CF_MODE,'mode')

    print(' => Truncating hash table')
    cursor.execute("truncate table dbo.hash_table;")
    conn.commit()

    if config.CF_MODE == 'full':
        tableslist = common_functions.get_tables_list(cursor, config.CF_SCHEMALIST)

        for item in tableslist:
            generate_hashes(conn, cursor, config.CF_DATABASE, item['schemaname'], item['tablename'], config.CF_SKPCOLS, common_functions)
            generate_hashes(conn, cursor,config.CF_DATABASE_TARGET, item['schemaname'], item['tablename'], config.CF_SKPCOLS, common_functions)

    elif config.CF_MODE == 'single':
        generate_hashes(conn, cursor, config.CF_DATABASE, config.CF_SCHEMA, config.CF_TABLENAME, config.CF_SKPCOLS, common_functions)
        generate_hashes(conn, cursor, config.CF_DATABASE_TARGET, config.CF_SCHEMA, config.CF_TABLENAME, config.CF_SKPCOLS, common_functions)

    print(' => done')

