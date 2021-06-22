# Data Engineer Challenge

We are going to simulate processing batches of WAL records. The data from these records will be used to display metrics to our customers within the dashboard, but before the data can be used, the records need to be preprocessed, joined together, and then inserted into a database for use by our dashboard (for this task, we are going to use sqlite to keep it simple, but in reality we would be using something like Elasticsearch).

### WAL
A WAL record is created each time there is an insert, update, or delete in the database - we will only be dealing with insert records for this exercise. Each WAL record is represented by a dictionary in Python:
  
```
  ## INSERT
  {
    "change": [
      {
        "kind": "insert",
        "schema": "public",
        "table": "foo",
        "columnnames": ["a", "b", "c"],
        "columntypes": ["integer", "character varying(30)", "timestamp without time zone"],
        "columnvalues": [1, "Backup and Restore", "2018-03-27 12:05:29.914496"]
      }
    ]
  }

```	
- The WAL records are produced by 4 tables called:
	- event_v2_data
	- transaction
	- transaction_request
	- payment_instrument_token_data

## The Task
`wal.json` contains a list of WAL records - in order to generate the dashboard metric, we need to join an `event_v2_data`, `transaction`, `transaction_request`, and `payment_instrument_token_data` record together. Records can be joined like this:

```
  event_v2_data.transaction_id = transaction.transaction_id
  event_v2_data.flow_id = transaction_request.flow_id
  transaction_request.token_id = payment_instrument_token_data.token_id
```

After joining the 4 records together, we only want to keep a subset of the data - the final metric should look like 

```
	{
		"event_v2_data.event_id": "value",
		"event_v2_data.flow_id": "value",
		"event_v2_data.created_at": "value",
		"event_v2_data.transaction_lifecycle_event": "value",
		"event_v2_data.error_details.decline_reason": "value",
		"event_v2_data.error_details.decline_type": "value",
		"transaction_request.vault_options.payment_method": "value",
		"transaction.transaction_id": "value",
		"transaction.transaction_type": "value",
		"transaction.amount": "value",
		"transaction.currency_code": "value",
		"transaction.processor_merchant_account_id": "value",
		"payment_instrument_token_data.three_d_secure_authentication": "value",
		"payment_instrument_token_data.payment_instrument_type": "value",
		"payment_instrument_token_data.vault_data.customer_id": "value"
	}
```

I have included the table name as well as the column to make it easier for you to know where each data point in the metric should come from, but please do not include the table name when inserting into the database (ie the column name for `transaction_request.vault_options.payment_method` should just be `payment_method`). You will need to create a table in sqlite to store the metric - the WAL records should have everything you need to do that including the `type` for each column. 

Once the metric has been created, add the metric into the database. When finished, you should have 529 records in the database. 

Please do the challenge in Python using only the modules contained in standard library - do not use third party libraries such as pandas. `main.py` contains a small amount of code to get you started, but feel free to keep or discard it. 

## What We're Looking For

☑️  Working code

☑️  Clean code

☑️  Good documentation

## Finishing Up 📈

📄 1. Document your findings, how did you go about solving the challenge?

☁️ 2. Push your work to your fork on GitHub 

✉️ 3. Share both the code and your findings with us (please add the Github user primer-api as a reviewer) 🎉 





