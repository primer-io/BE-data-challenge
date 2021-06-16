import sqlite3
import json

con = sqlite3.connect('metrics.db')
cur = con.cursor()

# Create table - this is an example, you should create a table called metrics or similar
# cur.execute('''CREATE TABLE stocks
#               (date text, trans text, symbol text, qty real, price real)''')

# Insert a row of data - again, just for illustration
# cur.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
# con.commit()

# Read the WAL records
with open("./wal.json", "r") as f:
    records = json.loads(f.read())

# Add code here
