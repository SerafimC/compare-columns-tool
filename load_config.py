import json

f = open('config.json')
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