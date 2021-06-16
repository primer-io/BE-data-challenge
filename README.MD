# Data Engineer Challenge

We are going to simulate processing batches of WAL records every minute. The WAL records will be used to display metrics to our customer within the dashboard, but before the data can be used, the records need to be preprocessed, joined together, and then inserted into a database (for this task, we are going to use sqlite to keep it simple, but in reality we would be using something like Elasticsearch).

### WAL
A WAL record is created each time there is an insert, update, or delete in the database. Each WAL record is represented by a dictionary in Python:
  
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

  ## UPDATE
	
  {
    "change": [
        {
          "kind": "update",
          "schema": "public",
          "table": "foo",
          "columnnames": ["a", "b", "c"],
          "columntypes": ["integer", "character varying(30)", "timestamp without time zone"],
          "columnvalues": [1, "Backup and Restore", "2021-03-27 12:05:29.914496"],
          "oldkeys": {
          "keynames": ["a", "b", "c"],
          "keytypes": ["integer", "character varying(30)", "timestamp without time zone"],
          "keyvalues": [1, "Backup and Restore", "2018-03-27 12:05:29.914496"]
        }
      }
    ]
	}

  ## DELETE

  {
    "change": [
        {
          "kind": "delete",
          "schema": "public",
          "table": "foo",
          "oldkeys": {
          "keynames": ["a", "b", "c"],
          "keytypes": ["integer", "character varying(30)", "timestamp without time zone"],
          "keyvalues": [1, "Backup and Restore", "2021-03-27 12:05:29.914496"]
        }
      }
    ]
  }
```	
- The WAL records are produced by 4 tables called:
	- event
	- transaction
	- transaction_request
	- payment_instrument_token

## The Task
`wal.json` contains a list of lists with each sublist representing 1 minute of batched data. Each batch of data will contain between 4-24 WAL records (it will always be a multiple of a 4 because it takes 4 records to create our metric). 

In order to generate the dashboard metric, we need to join an `event`, `transaction`, `transaction_request`, and `payment_instrument_token` record together - records can be joined like this:

```
  event.transaction_id = transaction.transaction_id
  event.flow_id = transaction_request.flow_id
  transaction_request.token_id = payment_instrument_token.transaction_id
```

- After joining the 4 records together, we only want to keep a subset of the data - the final metric should look like 
	```
	{
		"event.id": "value",
		"event.flow_id": "value",
		"event.created_at": "value",
		"event.transaction_lifecycle_event": "value",
		"event.error_details.decline_reason": "value",
		"event.error_details.decline_type": "value",
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
I have included the table name as well as the column to make it easier for you to know where each data point in the metric should come from, but pleae do not include the table name when inserting into the database (ie the column name for `transaction_request.vault_options.payment_method` should just be `payment_method`). You will need to create a table in sqlite to store the metric - the WAL records should have everything you need to do that including the `type` for each column. 

Once the metric has been created, update the database based on the `kind` (i.e. on insert then insert a new record, on update you should update the record, and on delete you should delete the record) - you can use `event.id` as the primary key for our metric.

Please do the challenge in Python using only the modules contained in standard library - do not use third party libraries such as pandas. 

## What We're Looking For

☑️  Working code

☑️  Clean code

☑️  Good documentation

## Finishing Up 📈

📄 1. Document your findings, how did you go about solving the challenge?

☁️ 2. Push your work to your fork on GitHub 

✉️ 3. Share both the code and your findings with us 🎉





