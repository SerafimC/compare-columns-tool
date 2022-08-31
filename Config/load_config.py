import json

# f = open('./Config/config.json')
# f = open('./Config/config_jaeger.json')
# f = open('./Config/config_european.json')
f = open('./Config/config_maison.json')
config = json.load(f)
f.close()

# ========================== CONNECTION ==========================
CF_DBSERVER = config['DB_SERVER']
CF_DBNAME = config['DB_NAME']
CF_DBUSER = config['DB_USER']
CF_DBPASSWORD = config['DB_PASSWORD']

# ========================== PARAMETERS ==========================
CF_DATABASE = config['client_database']
CF_DATABASE_TARGET = config['client_database_to_compare']
CF_SKPCOLS = config['skip_columns']
CF_SCHEMA = config['schemaname']
CF_TABLENAME = config['tablename']
CF_MODE = config['mode']
CF_SCHEMALIST = config['schemalist']

if CF_MODE != 'full' and CF_MODE != 'single':
    raise Exception('Invalid execution mode. Choose "full" or "single"')