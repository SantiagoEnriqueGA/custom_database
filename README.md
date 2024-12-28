# SEGADB

SEGADB is a simple database library for Python.
This project is primarily educational. It is designed to help understand the workings of a simple database system by building it from scratch. The implementations focus on fundamental concepts first and then on optimizing for speed or robustness, using basic Python data structures and custom transaction handling for specific tasks.

<!-- Add Links to Other Sections Here! -->
- [Features](#features)
- [Installation](#installation)
- [File Structure](#file-structure)
- [Usage Example](#usage-examples)
- [Scripts](#scripts)
- [Documentation](#documentation)
- [Tests](#tests)

## Features
- **Database Management**: Create, drop, copy, restore, and add constraints to database. See [`Database`](segadb/database.py).
- **Table Operations**: Insert, update, delete, and select records in a table. See [`Table`](segadb/table.py).
- **Transaction Handling**: Support for transactions with commit and rollback functionality. See [`Transaction`](segadb/transaction.py).
- **Indexing**: Create and manage indexes for efficient data retrieval. See [`Index`](segadb/index.py).
- **Storage**: Save and load the database to and from a file. Create and restore from backups. See [`Storage`](segadb/storage.py).
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
- **User Management and Authorization**: Manage users, their roles, and permissions. See [`User`, `UserManager`, and `Authorization`](segadb/users.py).
- **View and Materialized View Management**: Create, retrieve, refresh, and delete views and materialized views. See [`View`](segadb/views.py).

## Installation

To set up the project environment, you can use the provided `environment.yml` file to create a conda environment with all the necessary dependencies.

1. Open a terminal or command prompt.
2. Navigate to the directory where your repository is located.
3. Run the following command to create the conda environment: `conda env create -f environment.yml`  
4. Activate the newly created environment: `conda activate segadb_env`

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
  - [`users.py`](segadb/users.py): Implements the `User`, `UserManager`, and `Authorization` classes for user management and authorization.
  - [`views.py`](segadb/views.py): Implements the `View` and `MaterializedView` classes for view management.
  - ['crypto.py'](segadb/crypto.py): Implements the `CustomFernet` class for encryption and decryption.
- **tests/**: Contains unit and performance tests for the database library.
  - [`run_all_tests.py`](tests/run_all_tests.py): Runs all available tests.
  - [`test_utils.py`](tests/test_utils.py): Utility functions for tests.
  - [`test_database.py`](tests/test_database.py): Unit tests for the `Database` class.
  - [`test_table.py`](tests/test_table.py): Unit tests for the `Table` class.
  - [`test_index.py`](tests/test_index.py): Unit tests for the `Index` class.
  - [`test_record.py`](tests/test_record.py): Unit tests for the `Record` class.
  - [`test_storage.py`](tests/test_storage.py): Unit tests for the `Storage` class.
  - [`test_transaction.py`](tests/test_transaction.py): Unit tests for the `Transaction` class.
  - [`test_users.py`](tests/test_users.py): Unit tests for the `User`, `UserManager`, and `Authorization` classes.
  - [`test_views.py`](tests/test_views.py): Unit tests for the `View` and `MaterializedView` classes.
  - [`test_crypto.py`](tests/test_crypto.py): Unit tests for the `CustomFernet` class.
  - [`test_segadb_performance.py`](tests/test_segadb_performance.py): Performance tests for the segadb package.
  - [`test_examples.py`](tests/test_examples.py): Contains tests for the example scripts.
- **examples/**: Example usages of the segadb library.
  - [example_backupRecovery.py](examples/example_backupRecovery.py): Demonstrates how to create and restore backups.
  - [example_change_ids.py](examples/example_change_ids.py): Demonstrates how to change record IDs, difference between IDs and Index.
  - [example_constraints.py](examples/example_constraints.py): Demonstrates how to add and enforce constraints on table columns.
  - [example_databaseDetails.py](examples/example_databaseDetails.py): Demonstrates how to create tables and manage records.
  - [example_dataExport.py](examples/example_dataExport.py): Demonstrates how to export data to different formats: CSV, JSON, SQLite.
  - [example_dataImports.py](examples/example_dataImports.py): Demonstrates how to import data from a CSV file.
  - [example_foreignKeys.py](examples/example_foreignKeys.py): Demonstrates how to use foreign key constraints.
  - [example_millionRowLoad.py](examples/example_millionRowLoad.py): Demonstrates how to load a table with a million rows using multiprocessing.
  - [example_queries.py](examples/example_queries.py): Demonstrates how to create tables, add constraints, insert data, perform joins, aggregations, and filtering operations.
  - [example_recordTypes.py](examples/example_recordTypes.py): Demonstrates how to use different record types (VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord).
  - [example_storage.py](examples/example_storage.py): Demonstrates how to save and load the database, and check constraints.
  - [example_storageCompression.py](examples/example_storageCompression.py): Demonstrates how to save and load the database with compression.
  - [example_storageCompressionLarge.py](examples/example_storageCompressionLarge.py): Demonstrates how to save and load a large database with compression, using multiprocessing.
  - [example_transactions.py](examples/example_transactions.py): Demonstrates how to use transactions for commit and rollback operations.
  - [example_UsersAuth.py](examples/example_UsersAuth.py): Demonstrates user authentication and authorization.
  - [example_views.py](examples/example_views.py): Demonstrates how to create, retrieve, refresh, and delete views and materialized views.
  - [example_stored_procs.py'](examples/example_stored_procs.py): Demonstrates how to create and use stored procedures.
- **docs/**: Contains the generated documentation for the segadb library.
  - [segadb.database.html](docs/segadb.database.html): Documentation for the `Database` class.
  - [segadb.index.html](docs/segadb.index.html): Documentation for the `Index` class.
  - [segadb.record.html](docs/segadb.record.html): Documentation for the `Record` class.
  - [segadb.storage.html](docs/segadb.storage.html): Documentation for the `Storage` class.
  - [segadb.table.html](docs/segadb.table.html): Documentation for the `Table` class.
  - [segadb.transaction.html](docs/segadb.transaction.html): Documentation for the `Transaction` class.
  - [segadb.users.html](docs/segadb.users.html): Documentation for the `User`, `UserManager`, and `Authorization` classes.
  - [segadb.views.html](docs/segadb.views.html): Documentation for the `View` and `MaterializedView` classes.
  - [segadb.crypto.html](docs/segadb.crypto.html): Documentation for the `CustomFernet` class.
  - [segadb.__init__.html](docs/segadb.__init__.html): Documentation for the `__init__.py` file.
- **scripts/**: PowerShell scripts to help with various tasks.
  - [_run_all_scripts.ps1](scripts/_run_all_scripts.ps1): Runs all PowerShell scripts in the `scripts/` folder sequentially.
  - [todo_comments.ps1](scripts/todo_comments.ps1): Finds and lists all TODO comments in Python files.
  - [count_lines.ps1](scripts/count_lines.ps1): Counts the number of lines in each Python file.
  - [comment_density.ps1](scripts/comment_density.ps1): Calculates the comment density in Python files.
  - [documentation_html.ps1](scripts/documentation_html.ps1): Generates HTML documentation.
  - [documentation_md.ps1](scripts/documentation_md.ps1): Generates markdown documentation.


## Usage Examples

### Backup and Recovery
- [example_backupRecovery.py](examples/example_backupRecovery.py): Demonstrates how to create and restore backups.

### Record Management
- [example_change_ids.py](examples/example_change_ids.py): Demonstrates how to change record IDs, difference between IDs and Index.
- [example_recordTypes.py](examples/example_recordTypes.py): Demonstrates how to use different record types (VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord).

### Constraints and Keys
- [example_constraints.py](examples/example_constraints.py): Demonstrates how to add and enforce constraints on table columns.
- [example_foreignKeys.py](examples/example_foreignKeys.py): Demonstrates how to use foreign key constraints.

### Data Operations
- [example_databaseDetails.py](examples/example_databaseDetails.py): Demonstrates how to create tables and manage records.
- [example_dataExport.py](examples/example_dataExport.py): Demonstrates how to export data to different formats: CSV, JSON, SQLite.
- [example_dataImports.py](examples/example_dataImports.py): Demonstrates how to import data from a CSV file.
- [example_queries.py](examples/example_queries.py): Demonstrates how to create tables, add constraints, insert data, perform joins, aggregations, and filtering operations.

### Performance
- [example_millionRowLoad.py](examples/example_millionRowLoad.py): Demonstrates how to load a table with a million rows using multiprocessing.

### Storage
- [example_storage.py](examples/example_storage.py): Demonstrates how to save and load the database, and check constraints.
- [example_storageCompression.py](examples/example_storageCompression.py): Demonstrates how to save and load the database with compression.
- [example_storageCompressionLarge.py](examples/example_storageCompressionLarge.py): Demonstrates how to save and load a large database with compression, using multiprocessing.

### Transactions
- [example_transactions.py](examples/example_transactions.py): Demonstrates how to use transactions for commit and rollback operations.

### User Management
- [example_UsersAuth.py](examples/example_UsersAuth.py): Demonstrates user authentication and authorization.

### View Management
- [example_views.py](examples/example_views.py): Demonstrates how to create, retrieve, refresh, and delete views and materialized views.

---
#### Here is a simple example of creating a segadb Database and a table with constraints.
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

## Scripts
The following PowerShell scripts are included in the `scripts/` folder to help with various tasks:

- **_run_all_scripts.ps1**: Runs all PowerShell scripts in the `scripts/` folder sequentially.
- **todo_comments.ps1**: Finds and lists all TODO comments in Python files.
- **count_lines.ps1**: Counts the number of lines in each Python file, sorts the files by line count in descending order, and calculates the total number of lines.
- **comment_density.ps1**: Calculates the comment density (percentage of lines that are comments) in Python files.
- **documentation_html.ps1**: Generates HTML documentation for Python files in the `segadb/` folder, and moves the generated HTML files to the `docs/` folder.
- **documentation_md.ps1**: Generates markdown documentation for Python files in the `segadb/` folder.


## Documentation
### HTML Documentation
Pydoc documentation is generated from the PowerShell script `documentation_html.ps1`.  
To see live version: https://santiagoenriquega.github.io/custom_database/segadb  

Self host documentation, run the following command in the terminal: `python -m pydoc -p 8080`  
Then open a web browser and navigate to http://localhost:8080/segadb.html

### Markdown Documentation
Pydoc Markdown is also availible and is generated from the PowerShell script `documentation_md.ps1`.  
The output file is located in [`docs/documentation.md`](docs/documentation.md)

## Tests
To run the tests, use the following command: `python -m unittest discover -s tests`  
Or run the all tests file: `python run_all_tests.py`
### Test Results
The following are the results of running the tests:

```sh
Testing CustomFernet Class
...Testing Database Class
..........................
.Testing file: example_UsersAuth.py
.Testing file: example_backupRecovery.py
.Testing file: example_change_ids.py
.Testing file: example_constraints.py
.Testing file: example_dataExport.py
.Testing file: example_dataImports.py
.Testing file: example_databaseDetails.py
.Testing file: example_foreignKeys.py
.Testing file: example_millionRowLoad.py
.Testing file: example_queries.py
.Testing file: example_recordTypes.py
.Testing file: example_storage.py
.Testing file: example_storageCompression.py
.Testing file: example_storageCompressionLarge.py
.Testing file: example_transactions.py
.Testing file: example_views.py
.Testing Imports
..Testing Index Class
..Testing Record Class
.....Delete performance on 5000 [id, name, email] records: 0.011 seconds.
.Insert performance for 5000 [id, name, email] records: 1.0 seconds.
.Load performance for 5000 records: 0.97 seconds.
.Restore performance for 5000 records: 0.0 seconds.
.Save performance for 5000 records: 0.17 seconds.
.Select performance of 1 out of 5000 [id, name, email] records: 0.003 seconds.
.Update performance on 5000 [id, name, email] records: 0.58 seconds.
.Testing Storage Class
............Testing Table Class
........Testing Transaction Class
.......................
----------------------------------------------------------------------
Ran 105 tests in 129.384s

OK
```
