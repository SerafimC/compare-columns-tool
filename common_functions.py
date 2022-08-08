def get_columns_list(cursor, tablename, schema, skip_columns):
    columns_list = ''

    cursor.execute('''select 
                    distinct column_name
                from information_schema.columns
                where table_name = '''+"'"+tablename+"'"+''' and table_schema = '''+"'"+schema+"'"+'''
                and column_name not in ('''+skip_columns+''') order by 1''')

    for row in cursor:
        columns_list += row['column_name'] + ', '
    
    columns_list = columns_list[:-2]

    return columns_list