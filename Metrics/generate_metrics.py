

def run(sqlite, cursor, data_proc, tables_list, client_database):

    with sqlite:
        data_proc.clean_metrics(sqlite, client_database)

    for t in tables_list:
        try:
            cursor.execute("select count(*) as cnt from "+client_database+"."+t[0]+"."+t[1]+" where isDeleted <> 'true';")
            for row in cursor:
                with sqlite:
                    data_proc.insert_metric(sqlite, client_database, t[0], t[1], str(row['cnt']))
        except:
            pass
        finally:
            pass

