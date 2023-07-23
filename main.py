import sqlite3
import json
from utils import hash_join, flatten_dict, log

# WAL records
FILE_PATH = "./wal.json"
METRICS_SCHEMA = {
    "event_v2_data.event_id": "",
    "event_v2_data.flow_id": "",
    "event_v2_data.created_at": "",
    "event_v2_data.transaction_lifecycle_event": "",
    "event_v2_data.error_details.decline_reason": "",
    "event_v2_data.error_details.decline_type": "",
    "transaction_request.vault_options.payment_method": "",
    "transaction.transaction_id": "",
    "transaction.transaction_type": "",
    "transaction.amount": "",
    "transaction.currency_code": "",
    "transaction.processor_merchant_account_id": "",
    "payment_instrument_token_data.three_d_secure_authentication": "",
    "payment_instrument_token_data.payment_instrument_type": "",
    "payment_instrument_token_data.vault_data.customer_id": "",
}


def load_data_from_file(path):
    """
    Loads data from file and parses the json string to a dictionary. It assumes the file is in json format.
    """
    log("Loading data from: {}".format(path))

    with open(path, "r") as f:
        records = json.loads(f.read())

    log("Data loaded. {} records found.".format(str(len(records))))
    return records


def transform_data(records):
    """
    Process and transform data from a log to key:value structure, splitting rows by database
    """
    log("Transforming data...")
    tables = {}
    for r in records:
        change = r["change"][0]
        table_name = change["table"]

        if table_name not in tables:
            tables[table_name] = {}
            tables[table_name]["table_schema"] = dict(
                zip(change["columnnames"], change["columntypes"])
            )
            tables[table_name]["records"] = []

        entry = dict(zip(change["columnnames"], change["columnvalues"]))

        # Some fields are json strings that need to be parsed. We parse all columns described as `jsonb`
        for c, t in tables[table_name]["table_schema"].items():
            if t == "jsonb":
                if entry[c]:
                    entry[c] = json.loads(entry[c])

        # Flattens the dictionary and add the record to table
        tables[table_name]["records"].append(flatten_dict(entry))

    log("Data transformed.")
    return tables


def process_metrics(tables):
    """
    Joins tables used to create metrics end table.
    """

    log("Processing metrics table...")

    event_transaction = hash_join(
        tables["event_v2_data"]["records"],
        tables["transaction"]["records"],
        "transaction_id",
    )
    et_transaction_request = hash_join(
        event_transaction, tables["transaction_request"]["records"], "flow_id"
    )
    ett_payment_instrument_token_data = hash_join(
        et_transaction_request,
        tables["payment_instrument_token_data"]["records"],
        "token_id",
    )

    res = list(ett_payment_instrument_token_data)

    log("Metrics table processed. Resulting {} rows.".format(str(len(res))))
    return res


def generate_ddl(tables, target_name, target_schema):
    """
    Generates DDL (Create table) sql statements
    """

    sql = "CREATE TABLE IF NOT EXISTS {target_name} ({columns});"
    columns_sql = []
    for k in target_schema:
        column_sql = "{} {}"

        s = k.split(".")
        table_name = s[0]
        column_name = s[1]
        nested_field = ""
        if len(s) == 3:
            nested_field = s[2]

        if not nested_field:
            column_type = (
                "character varying(255)"
                if tables[table_name]["table_schema"][column_name] == "jsonb"
                else tables[table_name]["table_schema"][column_name]
            )
        else:
            # There is no information about nested fields data type, so we fallback to varchar
            column_name = nested_field
            column_type = "character varying(255)"

        columns_sql.append(column_sql.format(column_name, column_type))
    return sql.format(target_name=target_name, columns=", ".join(columns_sql))


def generate_dml(rows, target_name, target_schema):
    """
    Generates SQL DML string to insert all rows at once
    """

    statement = "INSERT INTO {target_name} VALUES \n{values}"
    values = []
    for r in rows:
        sql = "({row})"
        row_sql = []

        for k, t in target_schema.items():
            s = k.split(".")
            column_name = ".".join(s[1:])
            if column_name not in r or r[column_name] == None:
                row_sql.append("null")

            else:
                template = "{}"
                if t not in ["integer"]:
                    template = '"{}"'
                row_sql.append(template.format(r[column_name]))

        values.append(sql.format(row=", ".join(row_sql)))

    return statement.format(target_name=target_name, values=",\n".join(values))


def load_into_database(target_name, create_statement, insert_statement):
    """
    Connects to database and runs the DDL and DML statements passed by arguments
    """

    con = sqlite3.connect("metrics.db")
    cur = con.cursor()

    # Create table if non existent
    log("Creating table if non existent")
    cur.execute(create_statement)

    # Trucating the table to avoid duplicated load
    cur.execute("DELETE from {target_name};".format(target_name=target_name))

    # Insert all rows at once
    log("Inserting rows into the database")
    cur.execute(insert_statement)

    log("Rows loaded into the database with success.")
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

    target_name = "metrics"

    # generate metric table DDL based on specified columns
    create_statement = generate_ddl(tables, target_name, METRICS_SCHEMA)

    # generate insert DML
    insert_statement = generate_dml(rows, target_name, METRICS_SCHEMA)

    # insert data to database
    load_into_database(target_name, create_statement, insert_statement)

    # end
    log("Finished execution")


if __name__ == "__main__":
    main()
