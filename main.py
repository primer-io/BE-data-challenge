import sqlite3
import json
from utils import hash_join, flatten_dict, log

# WAL records
FILE_PATH = "./wal.json"

def load_data_from_file(path):
    """
    Loads data from file and parses the json string to a dictionary. It assumes the file is in json format.
    """
    log("Loading data from: {}".format(path))
    
    with open(path, "r") as f:
        records = json.loads(f.read())
    
    log("Data loaded. {} records found.".format(str(len(records))))
    return records

# %%
def transform_data(records):
    """
    Process and transform data from a log to key:value structure, splitting rows by database
    """
    log("Processing data...")
    tables = {}
    for r in records:
        change = r['change'][0]
        tablename = change['table']
        
        if tablename not in tables:
            tables[tablename] = {}
            tables[tablename]['table_schema'] = dict(zip(change['columnnames'], change['columntypes']))
            tables[tablename]['json_fields'] = []
            for c, t in tables[tablename]['table_schema'].items():
                if t == 'jsonb':
                    tables[tablename]['json_fields'].append(c)
            tables[tablename]['records'] = []

        entry = dict(zip(change['columnnames'], change['columnvalues']))
        
        # Some fields are json strings that need to be parsed. We parse all columns described as `jsonb`
        for c in tables[tablename]['json_fields']:
            if entry[c]:
                entry[c] = json.loads(entry[c])

        # Flattens the dictionary and add the record to table
        tables[tablename]['records'].append(flatten_dict(entry))
    
    log("Data processed.")
    return tables


def process_metrics(tables):
    """
    Joins tables used to create metrics end table.
    """

    log("Processing metrics table...")
    event_transaction = hash_join(tables['event_v2_data']['records'], tables['transaction']['records'], 'transaction_id')
    et_transaction_request = hash_join(event_transaction, tables['transaction_request']['records'], 'flow_id')
    ett_payment_instrument_token_data = hash_join(et_transaction_request, tables['payment_instrument_token_data']['records'], 'token_id')

    res = list(ett_payment_instrument_token_data)

    log("Metrics table processed. Resulting {} rows.".format(str(len(res))))
    return res

def generate_ddl():
    # ''CREATE TABLE stocks
    #           (date text, trans text, symbol text, qty real, price real)''')
    pass

def generate_dml():
    # "INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)"
    pass


def load_into_database(create_statement, insert_statement):
    con = sqlite3.connect('metrics.db')
    cur = con.cursor()
    
    # Create table if non existent
    cur.execute(create_statement)
    # Insert all rows at once
    cur.execute(insert_statement)
    # Save (commit) the changes
    con.commit()

def main():
    """
    Function with the main execution
    """
    log("Starting execution")
    # load from file =  read the WAL records
    raw = load_data_from_file(FILE_PATH)

    # process data, splitting into tables and converting nested json strings to dict
    tables = transform_data(raw)

    # join tables returning a list of merged rows/events
    rows = process_metrics(tables)

    # generate metric table DDL based on specified columns 
    create_statement = generate_ddl()
    
    # generate insert DML
    insert_statement = generate_dml()

    # insert data to database
    load_into_database(create_statement, insert_statement)
    
    # end
    log("Finished execution")


if __name__ == '__main__':
    main()