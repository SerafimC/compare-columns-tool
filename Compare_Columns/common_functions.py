def get_columns_list(cursor, tablename, schema, skip_columns):
    columns_list = ''

    cursor.execute('''select 
                    distinct c.column_name
                from information_schema.columns c
                left join information_schema.CONSTRAINT_COLUMN_USAGE pks
                    on c.table_schema = pks.table_schema 
                    and c.table_name = pks.table_name 
                    and c.column_name = pks.column_name 
                where c.table_name = '''+"'"+tablename+"'"+''' and c.table_schema = '''+"'"+schema+"'"+'''
                and c.column_name not in ('''+skip_columns+''') 
                and data_type not in ('ntext', 'datetime', 'date')
                and (pks.column_name is null or pks.column_name = 'id')
                order by 1''')

    for row in cursor:
        columns_list += row['column_name'] + ', '
    
    columns_list = columns_list[:-2]

    return columns_list

def get_tables_list(cursor, schemalist):
    tablelist = []

    cursor.execute('''select distinct table_schema, table_name
                        from information_schema.columns
                        where table_schema in ('''+schemalist+''')
                        and lower(column_name) in ('id') 
                        and lower(table_name) not in ('user', 'productdimview')
                        and lower(table_name) not like 'vw%'
                        order by 1, 2 ''')

    for row in cursor:
        tablelist.append({"schemaname" : row['table_schema'], "tablename" : row['table_name']})

    return tablelist