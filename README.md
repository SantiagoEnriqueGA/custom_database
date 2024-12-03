# SEGADB

segadb is a simple database library for Python.
This project is primarily educational. It is designed to help understand the workings of a simple database system by building it from scratch. The implementations focus on fundamental concepts first and then on optimizing for speed or robustness, using basic Python data structures and custom transaction handling for specific tasks.

## Features
- **Database Management**: Create, drop, and manage tables within a database. See [`Database`](segadb/database.py).
- **Table Operations**: Insert, update, delete, and select records in a table. See [`Table`](segadb/table.py).
- **Transaction Handling**: Support for transactions with commit and rollback functionality. See [`Transaction`](segadb/transaction.py).
- **Indexing**: Create and manage indexes for efficient data retrieval. See [`Index`](segadb/index.py).
- **Storage**: Save and load the database to and from a file. See [`Storage`](segadb/storage.py).
- **Record Management**: Manage individual records with unique IDs and data. See [`Record`](segadb/record.py).

## File Structure
The project directory structure is as follows:

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
  - [`test_segadb.py`](tests/test_segadb.py): Unit tests for the segadb package.
  - [`test_segadb_performance.py`](tests/test_segadb_performance.py): Performance tests for the segadb package.
- **main.py**: Example usage of the segadb library.
- **setup.py**: Setup script for packaging the segadb library.

## Usage
```python
from segadb import Database, Storage, Transaction 

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["id", "name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.insert({"name": "John Doe", "email": "john@example.com"})

print("Before transaction:")
users_table.print_table()

# Start a transaction
transaction = Transaction(db)
transaction.begin()

# Insert a record within a transaction
users_table.insert({"name": "Jane Doe2", "email": "jane@example.com"}, transaction)

print("\nAfter insert transaction:")
users_table.print_table()

# Rollback the transaction
transaction.rollback()
users_table = db.get_table("Users")

print("\nAfter rollback:")
users_table.print_table()

# Save the database to a file
Storage.save(db, "database.json")
```