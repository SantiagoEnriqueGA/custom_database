# SEGADB

SEGADB is a simple database library for Python.
This project is primarily educational. It is designed to help understand the workings of a simple database system by building it from scratch. The implementations focus on fundamental concepts first and then on optimizing for speed or robustness, using basic Python data structures and custom transaction handling for specific tasks.

## Features
- **Database Management**: Create, drop, copy, restore, and add constraints to database. See [`Database`](segadb/database.py).
- **Table Operations**: Insert, update, delete, and select records in a table. See [`Table`](segadb/table.py).
- **Transaction Handling**: Support for transactions with commit and rollback functionality. See [`Transaction`](segadb/transaction.py).
- **Indexing**: Create and manage indexes for efficient data retrieval. See [`Index`](segadb/index.py).
- **Storage**: Save and load the database to and from a file. See [`Storage`](segadb/storage.py).
- **Record Management**: Manage individual records with unique IDs and data. See [`Record`](segadb/record.py).
  - Support for vector records
    - `magnitude()`: Calculates the magnitude of the vector.
    - `normalize()`: Normalizes the vector.
    - `dot_product(other_vector)`: Calculates the dot product with another vector.
  - Time series records 
    - `moving_average(window_size)`: Calculates the moving average of the time series.
  - Image records
    - `image_data`: Returns the image data.
    - `image_path`: Returns the file path to the image.
    - `image_size`: Returns the size of the image in bytes.
    - `get_image()`: Converts the image data to a PIL Image object.
  - Text records
    - `word_count()`: Counts the number of words in the text.
    - `to_uppercase()`: Converts the text to uppercase.
    - `to_lowercase()`: Converts the text to lowercase.
## File Structure
The project directory structure is as follows:

- **setup.py**: Setup script for packaging the segadb library. ~TBD~
- **segadb/**: Contains the main database library code.
  - [`__init__.py`](segadb/__init__.py): Initializes the segadb package.
  - [`database.py`](segadb/database.py): Implements the `Database` class for managing tables.
  - [`index.py`](segadb/index.py): Implements the `Index` class for indexing records.
  - [`main.py`](segadb/main.py): Main entry point for the database operations.
  - [`record.py`](segadb/record.py): Implements the `Record` class for individual records.
  - [`storage.py`](segadb/storage.py): Implements the `Storage` class for saving and loading the database.
  - [`table.py`](segadb/table.py): Implements the `Table` class for table operations.
  - [`transaction.py`](segadb/transaction.py): Implements the `Transaction` class for transaction handling.
- **tests/**: Contains unit and performance tests for the database library.
  - [`run_all_tests.py`](tests/run_all_tests.py): Runs all available tests.
  - [`test_database.py`](tests/test_database.py): Unit tests for the `Database` class.
  - [`test_table.py`](tests/test_table.py): Unit tests for the `Table` class.
  - [`test_index.py`](tests/test_index.py): Unit tests for the `Index` class.
  - [`test_record.py`](tests/test_record.py): Unit tests for the `Record` class.
  - [`test_storage.py`](tests/test_storage.py): Unit tests for the `Storage` class.
  - [`test_transaction.py`](tests/test_transaction.py): Unit tests for the `Transaction` class.
  - [`test_utils.py`](tests/test_utils.py): Utility functions for tests.
  - [`test_segadb_performance.py`](tests/test_segadb_performance.py): Performance tests for the segadb package.
  - [`test_examples.py`](tests/test_examples.py): Contains tests for the example scripts.
- **examples/**: Example usages of the segadb library.
  - [example_csv.py](examples/example_csv.py): Demonstrates how to create a table from a CSV file.
  - [example_constraints.py](examples/example_constraints.py): Demonstrates how to add and enforce constraints on table columns.
  - [example_transactions.py](examples/example_transactions.py): Demonstrates how to use transactions for commit and rollback operations.
  - [example_change_ids.py](examples/example_change_ids.py): Demonstrates how to change record IDs, diffrence between IDs and Index.
  - [example_recordTypes.py](examples/example_recordTypes.py): Demonstrates how to use different record types (VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord).
  - [example_queries.py](examples/example_queries.py): Demonstrates how to create tables, add constraints, insert data, perform joins, aggregations, and filtering operations.
  - [example_storage.py](examples/example_storage.py): Demonstrates how to save and load the database, and check constraints.
- **example_storage/**: Example storage and exports from the segadb library.
  - database.segadb: database saved in custom json format
  - database_encrypted.segadb: database saved in custom json format, encrypted with fernet key. (base64-encoded 32-byte key)
  - users.json: User table experted to .json
  - users.csv: User table experted to .csv
  - users.db: User table experted to SQLite database file

## Usage
```python
from segadb import *

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.insert({"name": "John Doe", "email": "john@example.com"})

print("Before Insert:")
users_table.print_table()

# Add a constraint to the table
users_table.add_constraint("email", lambda x: "@" in x)
print("\n--Constraint added to the table: email must contain '@'")

users_table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})    # violates the constraint
users_table.insert({"name": "Jane Doe", "email": "jane@example.com"})       # satisfies the constraint

print("\nAfter Insert:")
users_table.print_table()


# Add constraint for unique email
users_table.add_constraint("email", "UNIQUE")
print("\n--Constraint added to the table: email must be unique")

# Try to insert a record with a duplicate email
users_table.try_insert({"name": "John Doe", "email": "john@example.com"})     # violates the constraint
users_table.try_insert({"name": "James Doe", "email": "james@example.com"})   # satisfies the constraint

print("\nAfter Insert:")
users_table.print_table()
```
```
Before Insert:
Record ID: 1, Data: {'name': 'John Doe', 'email': 'john@example.com'}

--Constraint added to the table: email must contain '@'
Constraint violation on column email for value janeexample.com
Error on insert: Constraint violation on column email for value janeexample.com

After Insert:
Record ID: 1, Data: {'name': 'John Doe', 'email': 'john@example.com'}
Record ID: 2, Data: {'name': 'Jane Doe', 'email': 'jane@example.com'}

--Constraint added to the table: email must be unique
Constraint violation on column email for value john@example.com
Error on insert: Constraint violation on column email for value john@example.com

After Insert:
Record ID: 1, Data: {'name': 'John Doe', 'email': 'john@example.com'}
Record ID: 2, Data: {'name': 'Jane Doe', 'email': 'jane@example.com'}
Record ID: 3, Data: {'name': 'James Doe', 'email': 'james@example.com'}
```

## Tests
To run the tests, use the following command:
```sh
python -m unittest discover -s tests
```
OR
Run the all tests file:
```sh
python run_all_tests.py
```


### Test Files and Cases

- **`test_database.py`**: Unit tests for the `Database` class.
- **`test_table.py`**: Unit tests for the `Table` class.
- **`test_index.py`**: Unit tests for the `Index` class.
- **`test_record.py`**: Unit tests for the `Record` class.
- **`test_storage.py`**: Unit tests for the `Storage` class.
- **`test_transaction.py`**: Unit tests for the `Transaction` class.
- **`test_utils.py`**: Utility functions for tests.
- **`test_segadb_performance.py`**: Performance tests for the segadb package.
- **`test_examples.py`**: Contains tests for the example scripts.
