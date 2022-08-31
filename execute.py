import pymssql 
from Config import load_config as lg
from Metrics import data_prep as data_proc, generate_metrics
from Misc import notify_channel as nc, write_log
from Compare_Columns import generate_hashes, common_functions, compare_columns

# ======================== CONNECTION ========================
conn = pymssql.connect(lg.CF_DBSERVER, lg.CF_DBUSER, lg.CF_DBPASSWORD, lg.CF_DBNAME)
cursor = conn.cursor(as_dict=True)

sqlite = data_proc.create_connection("./Metrics/metrics.sqlite")
tables_list = data_proc.select_tables(sqlite)

# ========================= METRICS =========================
def run_metrics():
    generate_metrics.run(sqlite, cursor, data_proc, tables_list, lg.CF_DATABASE)
    generate_metrics.run(sqlite, cursor, data_proc, tables_list, lg.CF_DATABASE_TARGET)

    result = data_proc.compare_metrics(sqlite, lg.CF_DATABASE, lg.CF_DATABASE_TARGET)
    nc.notify_slack(nc.format_metric(result), lg.CF_DATABASE[:-8] + ' || Metrics - row count', '#AA0000')

# ===================== COLUMN COMPARISON =====================
def run_compare():
    generate_hashes.run(conn, cursor, lg, common_functions)
    compare_columns.run(conn, cursor, lg, common_functions, write_log)

run_metrics()
# run_compare()

conn.close()