# summarizer
Summarize CSV data


## data model

### transaction

Assuming a single currency

1. id (pk, incrementally growing integer)
2. booking_date (datetime, when transaction is entered)
3. value_date (datetime, when transaction happened)
4. amount (float)
5. tr_type (enum, debit or credit)


### transaction_detail_type

1. id
2. label (varchar 255)
3. description (text)


### transaction_detail

1. id (pk, incrementally growing integer)
2. transaction_id (fk, references transaction)
3. type_id (fk, references transaction_type)
4. description (text)