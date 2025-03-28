# Table of Contents

* [crypto](#crypto)
  * [CustomFernet](#crypto.CustomFernet)
    * [\_\_init\_\_](#crypto.CustomFernet.__init__)
    * [generate\_key](#crypto.CustomFernet.generate_key)
    * [encrypt](#crypto.CustomFernet.encrypt)
    * [decrypt](#crypto.CustomFernet.decrypt)
* [database](#database)
  * [log\_method\_call](#database.log_method_call)
  * [\_process\_file\_chunk](#database._process_file_chunk)
  * [Database](#database.Database)
    * [\_\_init\_\_](#database.Database.__init__)
    * [create\_user\_manager](#database.Database.create_user_manager)
    * [create\_authorization](#database.Database.create_authorization)
    * [create\_session](#database.Database.create_session)
    * [delete\_session](#database.Database.delete_session)
    * [get\_username\_by\_session](#database.Database.get_username_by_session)
    * [register\_user](#database.Database.register_user)
    * [authenticate\_user](#database.Database.authenticate_user)
    * [get\_user](#database.Database.get_user)
    * [add\_role](#database.Database.add_role)
    * [check\_role](#database.Database.check_role)
    * [remove\_user](#database.Database.remove_user)
    * [\_is\_auth\_required](#database.Database._is_auth_required)
    * [\_check\_permission](#database.Database._check_permission)
    * [check\_permission](#database.Database.check_permission)
    * [create\_table](#database.Database.create_table)
    * [drop\_table](#database.Database.drop_table)
    * [update\_table](#database.Database.update_table)
    * [get\_table](#database.Database.get_table)
    * [\_create\_table\_from\_dict](#database.Database._create_table_from_dict)
    * [get\_table](#database.Database.get_table)
    * [create\_table\_from\_csv](#database.Database.create_table_from_csv)
    * [\_create\_table\_from\_csv\_mp](#database.Database._create_table_from_csv_mp)
    * [\_get\_file\_chunks](#database.Database._get_file_chunks)
    * [\_process\_file](#database.Database._process_file)
    * [add\_constraint](#database.Database.add_constraint)
    * [join\_tables](#database.Database.join_tables)
    * [aggregate\_table](#database.Database.aggregate_table)
    * [filter\_table](#database.Database.filter_table)
    * [add\_stored\_procedure](#database.Database.add_stored_procedure)
    * [execute\_stored\_procedure](#database.Database.execute_stored_procedure)
    * [delete\_stored\_procedure](#database.Database.delete_stored_procedure)
    * [get\_stored\_procedure](#database.Database.get_stored_procedure)
    * [\_stored\_procedure\_to\_string](#database.Database._stored_procedure_to_string)
    * [add\_trigger](#database.Database.add_trigger)
    * [execute\_triggers](#database.Database.execute_triggers)
    * [delete\_trigger](#database.Database.delete_trigger)
    * [create\_view](#database.Database.create_view)
    * [get\_view](#database.Database.get_view)
    * [delete\_view](#database.Database.delete_view)
    * [create\_materialized\_view](#database.Database.create_materialized_view)
    * [get\_materialized\_view](#database.Database.get_materialized_view)
    * [refresh\_materialized\_view](#database.Database.refresh_materialized_view)
    * [delete\_materialized\_view](#database.Database.delete_materialized_view)
    * [show\_db\_with\_curses](#database.Database.show_db_with_curses)
    * [print\_db](#database.Database.print_db)
    * [copy](#database.Database.copy)
    * [restore](#database.Database.restore)
    * [get\_db\_size](#database.Database.get_db_size)
    * [\_create\_orders\_table](#database.Database._create_orders_table)
    * [\_create\_users\_table](#database.Database._create_users_table)
    * [\_create\_products\_table](#database.Database._create_products_table)
    * [\_create\_reviews\_table](#database.Database._create_reviews_table)
    * [\_create\_categories\_table](#database.Database._create_categories_table)
    * [\_create\_suppliers\_table](#database.Database._create_suppliers_table)
    * [load\_sample\_database](#database.Database.load_sample_database)
* [databasePartial](#databasePartial)
  * [PartialDatabase](#databasePartial.PartialDatabase)
    * [\_\_init\_\_](#databasePartial.PartialDatabase.__init__)
    * [get\_table](#databasePartial.PartialDatabase.get_table)
    * [\_load\_table\_from\_storage](#databasePartial.PartialDatabase._load_table_from_storage)
    * [active\_tables](#databasePartial.PartialDatabase.active_tables)
    * [deactivate\_table](#databasePartial.PartialDatabase.deactivate_table)
    * [dormant\_tables](#databasePartial.PartialDatabase.dormant_tables)
    * [print\_db](#databasePartial.PartialDatabase.print_db)
* [db\_navigator](#db_navigator)
  * [KEY\_MAPPING](#db_navigator.KEY_MAPPING)
  * [safe\_execution](#db_navigator.safe_execution)
  * [is\_key](#db_navigator.is_key)
  * [safe\_addstr](#db_navigator.safe_addstr)
  * [remove\_leading\_spaces](#db_navigator.remove_leading_spaces)
  * [display\_popup](#db_navigator.display_popup)
  * [display\_help](#db_navigator.display_help)
  * [search\_prompt](#db_navigator.search_prompt)
  * [db\_navigator](#db_navigator.db_navigator)
  * [display\_info](#db_navigator.display_info)
  * [display\_main\_screen](#db_navigator.display_main_screen)
  * [display\_db\_info](#db_navigator.display_db_info)
  * [display\_tables](#db_navigator.display_tables)
  * [display\_table](#db_navigator.display_table)
  * [\_get\_record\_page](#db_navigator._get_record_page)
  * [display\_views](#db_navigator.display_views)
  * [display\_view](#db_navigator.display_view)
  * [display\_mv\_views](#db_navigator.display_mv_views)
  * [display\_mv\_view](#db_navigator.display_mv_view)
  * [display\_stored\_procedures](#db_navigator.display_stored_procedures)
  * [display\_procedure](#db_navigator.display_procedure)
  * [display\_trigger\_functions](#db_navigator.display_trigger_functions)
  * [display\_function](#db_navigator.display_function)
* [index](#index)
  * [Index](#index.Index)
    * [\_\_init\_\_](#index.Index.__init__)
    * [add](#index.Index.add)
    * [remove](#index.Index.remove)
    * [find](#index.Index.find)
    * [\_\_str\_\_](#index.Index.__str__)
    * [to\_dict](#index.Index.to_dict)
* [record](#record)
  * [Record](#record.Record)
    * [\_\_init\_\_](#record.Record.__init__)
    * [index](#record.Record.index)
    * [add\_to\_index](#record.Record.add_to_index)
    * [remove\_from\_index](#record.Record.remove_from_index)
    * [\_type](#record.Record._type)
  * [VectorRecord](#record.VectorRecord)
    * [\_\_init\_\_](#record.VectorRecord.__init__)
    * [vector](#record.VectorRecord.vector)
    * [magnitude](#record.VectorRecord.magnitude)
    * [normalize](#record.VectorRecord.normalize)
    * [dot\_product](#record.VectorRecord.dot_product)
    * [\_type](#record.VectorRecord._type)
  * [TimeSeriesRecord](#record.TimeSeriesRecord)
    * [\_\_init\_\_](#record.TimeSeriesRecord.__init__)
    * [time\_series](#record.TimeSeriesRecord.time_series)
    * [moving\_average](#record.TimeSeriesRecord.moving_average)
    * [\_type](#record.TimeSeriesRecord._type)
  * [ImageRecord](#record.ImageRecord)
    * [\_\_init\_\_](#record.ImageRecord.__init__)
    * [image\_data](#record.ImageRecord.image_data)
    * [image\_path](#record.ImageRecord.image_path)
    * [image\_size](#record.ImageRecord.image_size)
    * [get\_image](#record.ImageRecord.get_image)
    * [resize](#record.ImageRecord.resize)
    * [\_convert\_to\_base64](#record.ImageRecord._convert_to_base64)
    * [to\_dict](#record.ImageRecord.to_dict)
    * [\_type](#record.ImageRecord._type)
  * [TextRecord](#record.TextRecord)
    * [\_\_init\_\_](#record.TextRecord.__init__)
    * [text](#record.TextRecord.text)
    * [word\_count](#record.TextRecord.word_count)
    * [to\_uppercase](#record.TextRecord.to_uppercase)
    * [to\_lowercase](#record.TextRecord.to_lowercase)
    * [\_type](#record.TextRecord._type)
  * [EncryptedRecord](#record.EncryptedRecord)
    * [\_\_init\_\_](#record.EncryptedRecord.__init__)
    * [decrypt](#record.EncryptedRecord.decrypt)
    * [\_type](#record.EncryptedRecord._type)
* [storage](#storage)
  * [\_process\_chunk](#storage._process_chunk)
  * [Storage](#storage.Storage)
    * [generate\_key](#storage.Storage.generate_key)
    * [encrypt](#storage.Storage.encrypt)
    * [decrypt](#storage.Storage.decrypt)
    * [backup](#storage.Storage.backup)
    * [list\_backups](#storage.Storage.list_backups)
    * [restore](#storage.Storage.restore)
    * [save](#storage.Storage.save)
    * [load](#storage.Storage.load)
    * [delete](#storage.Storage.delete)
    * [db\_to\_dict](#storage.Storage.db_to_dict)
    * [save\_table](#storage.Storage.save_table)
    * [\_table\_to\_csv](#storage.Storage._table_to_csv)
    * [\_table\_to\_json](#storage.Storage._table_to_json)
    * [\_table\_to\_sqlite](#storage.Storage._table_to_sqlite)
    * [\_load\_table\_from\_db\_file](#storage.Storage._load_table_from_db_file)
    * [\_save\_table\_to\_db\_file](#storage.Storage._save_table_to_db_file)
    * [\_load\_viewsProcs\_from\_db\_file](#storage.Storage._load_viewsProcs_from_db_file)
    * [\_load\_mp](#storage.Storage._load_mp)
    * [\_get\_record\_chunks](#storage.Storage._get_record_chunks)
* [table](#table)
  * [log\_method\_call](#table.log_method_call)
  * [Table](#table.Table)
    * [\_\_init\_\_](#table.Table.__init__)
    * [add\_constraint](#table.Table.add_constraint)
    * [\_check\_constraints](#table.Table._check_constraints)
    * [insert](#table.Table.insert)
    * [\_insert](#table.Table._insert)
    * [try\_insert](#table.Table.try_insert)
    * [bulk\_insert](#table.Table.bulk_insert)
    * [\_bulk\_insert](#table.Table._bulk_insert)
    * [delete](#table.Table.delete)
    * [\_delete](#table.Table._delete)
    * [update](#table.Table.update)
    * [\_update](#table.Table._update)
    * [get\_id\_by\_column](#table.Table.get_id_by_column)
    * [select](#table.Table.select)
    * [join](#table.Table.join)
    * [aggregate](#table.Table.aggregate)
    * [filter](#table.Table.filter)
    * [sort](#table.Table.sort)
    * [print\_table](#table.Table.print_table)
    * [\_print\_table\_pretty](#table.Table._print_table_pretty)
* [transaction](#transaction)
  * [Transaction](#transaction.Transaction)
    * [\_\_init\_\_](#transaction.Transaction.__init__)
    * [begin](#transaction.Transaction.begin)
    * [commit](#transaction.Transaction.commit)
    * [rollback](#transaction.Transaction.rollback)
    * [add\_operation](#transaction.Transaction.add_operation)
    * [copy](#transaction.Transaction.copy)
    * [preview](#transaction.Transaction.preview)
* [users](#users)
  * [PRESET\_ROLES](#users.PRESET_ROLES)
  * [User](#users.User)
    * [\_\_init\_\_](#users.User.__init__)
    * [hash\_password](#users.User.hash_password)
    * [check\_password](#users.User.check_password)
  * [UserManager](#users.UserManager)
    * [\_\_init\_\_](#users.UserManager.__init__)
    * [register\_user](#users.UserManager.register_user)
    * [authenticate\_user](#users.UserManager.authenticate_user)
    * [get\_user\_permissions](#users.UserManager.get_user_permissions)
    * [login\_user](#users.UserManager.login_user)
    * [logout\_user](#users.UserManager.logout_user)
    * [remove\_user](#users.UserManager.remove_user)
  * [Authorization](#users.Authorization)
    * [\_\_init\_\_](#users.Authorization.__init__)
    * [add\_permission](#users.Authorization.add_permission)
    * [check\_permission](#users.Authorization.check_permission)
* [views](#views)
  * [View](#views.View)
    * [\_\_init\_\_](#views.View.__init__)
    * [get\_data](#views.View.get_data)
    * [\_query\_to\_string](#views.View._query_to_string)
  * [MaterializedView](#views.MaterializedView)
    * [\_\_init\_\_](#views.MaterializedView.__init__)
    * [refresh](#views.MaterializedView.refresh)
    * [get\_data](#views.MaterializedView.get_data)
    * [\_query\_to\_string](#views.MaterializedView._query_to_string)
* [\_\_init\_\_](#__init__)
  * [\_\_all\_\_](#__init__.__all__)

<a id="crypto"></a>

# crypto

<a id="crypto.CustomFernet"></a>

## CustomFernet Objects

```python
class CustomFernet()
```

A custom implementation of the Fernet symmetric encryption using AES in CBC mode.

<a id="crypto.CustomFernet.__init__"></a>

#### \_\_init\_\_

```python
def __init__(key)
```

Initializes the Crypto class with the given key.
Args:
    key (str): The base64 encoded key used for encryption and decryption.
Attributes:
    key (bytes): The decoded key used for encryption and decryption.
    backend (object): The backend used for cryptographic operations.
    block_size (int): The block size for the AES algorithm in bytes.

<a id="crypto.CustomFernet.generate_key"></a>

#### generate\_key

```python
@staticmethod
def generate_key()
```

Generates a secure random key.  
This function generates a 32-byte secure random key and encodes it using URL-safe Base64 encoding.
Returns:
    str: A URL-safe Base64 encoded string representing the secure random key.

<a id="crypto.CustomFernet.encrypt"></a>

#### encrypt

```python
def encrypt(data)
```

Encrypts the provided data using AES encryption with CBC mode and PKCS7 padding.
Args:
    data (str): The plaintext data to be encrypted.
Returns:
    str: The encrypted data encoded in URL-safe base64 format.

<a id="crypto.CustomFernet.decrypt"></a>

#### decrypt

```python
def decrypt(token)
```

Decrypts the given token using AES encryption in CBC mode.
Args:
    token (str): The base64 encoded string to be decrypted.
Returns:
    str: The decrypted data as a string.

<a id="database"></a>

# database

<a id="database.log_method_call"></a>

#### log\_method\_call

```python
def log_method_call(func)
```

Decorator to log method calls in the Database class.
Logs the method name, arguments, and return value.

Args:
    func (function): The function to decorate.
Returns:
    function: The decorated function.

<a id="database._process_file_chunk"></a>

#### \_process\_file\_chunk

```python
def _process_file_chunk(file_name,
                        chunk_start,
                        chunk_end,
                        delim=',',
                        column_names=None,
                        col_types=None,
                        progress=False,
                        headers=False)
```

Process each file chunk in a different process.  
IDs count up from chunk_start.  
Args:
    file_name (str): The name of the file to process.
    chunk_start (int): The start position of the chunk in the file.
    chunk_end (int): The end position of the chunk in the file.
    delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
    column_names (list, optional): List of column names to use if headers is False. Defaults to None.
    col_types (list, optional): List of types to cast the columns to. Defaults to None.
    progress (bool, optional): If True, displays a progress bar. Defaults to False.
    headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to False.
Returns:
    rows (list): A list of Record objects representing the rows in the chunk.

<a id="database.Database"></a>

## Database Objects

```python
class Database()
```

<a id="database.Database.__init__"></a>

#### \_\_init\_\_

```python
def __init__(name, db_logging=False, table_logging=False)
```

Initializes a new instance of the database with the given name.
Args:
    name (str): The name of the database.
    db_logging (bool, optional): If True, enables logging for the database. Defaults to False.
    table_logging (bool, optional): If True, enables logging for tables. Defaults to False.

<a id="database.Database.create_user_manager"></a>

#### create\_user\_manager

```python
def create_user_manager()
```

Create a new instance of the UserManager class.
Returns:
    UserManager: A new instance of the UserManager class.

<a id="database.Database.create_authorization"></a>

#### create\_authorization

```python
def create_authorization()
```

Create a new instance of the Authorization class.
Returns:
    Authorization: A new instance of the Authorization class.

<a id="database.Database.create_session"></a>

#### create\_session

```python
@log_method_call
def create_session(username)
```

Creates a new session for a user.
Args:
    username (str): The username of the user.
Returns:
    str: The session token.

<a id="database.Database.delete_session"></a>

#### delete\_session

```python
@log_method_call
def delete_session(session_token)
```

Deletes a session.
Args:
    session_token (str): The session token to delete.

<a id="database.Database.get_username_by_session"></a>

#### get\_username\_by\_session

```python
@log_method_call
def get_username_by_session(session_token)
```

Retrieves the username associated with a session token.
Args:
    session_token (str): The session token.
Returns:
    str: The username if the session exists, None otherwise.

<a id="database.Database.register_user"></a>

#### register\_user

```python
@log_method_call
def register_user(username, password, roles=None)
```

Registers a new user in the database.
Args:
    username (str): The username of the new user.
    password (str): The password of the new user.
    roles (list, optional): A list of roles assigned to the user. Defaults to an empty list.
Raises:
    ValueError: If the username already exists in the database.

<a id="database.Database.authenticate_user"></a>

#### authenticate\_user

```python
def authenticate_user(username, password)
```

Authenticates a user by their username and password.
Args:
    username (str): The username of the user.
    password (str): The password of the user.
Returns:
    dict or None: The user dictionary if authentication is successful, None otherwise.

<a id="database.Database.get_user"></a>

#### get\_user

```python
def get_user(username)
```

Retrieve user data from the Users table based on the provided username.
Args:
    username (str): The username of the user to retrieve.
Returns:
    dict: A dictionary containing the user's data if found, otherwise None.

<a id="database.Database.add_role"></a>

#### add\_role

```python
@log_method_call
def add_role(username, role)
```

Add a role to a user.
Args:
    username (str): The username of the user to whom the role will be added.
    role (str): The role to be added to the user.
Raises:
    ValueError: If the user is not found in the database.

<a id="database.Database.check_role"></a>

#### check\_role

```python
def check_role(username, role)
```

Check if a user has a specific role.
Args:
    username (str): The username of the user to check.
    role (str): The role to check for.
Returns:
    bool: True if the user has the specified role, False otherwise.

<a id="database.Database.remove_user"></a>

#### remove\_user

```python
@log_method_call
def remove_user(username)
```

Removes a user from the database.
Args:
    username (str): The username of the user to be removed.

<a id="database.Database._is_auth_required"></a>

#### \_is\_auth\_required

```python
def _is_auth_required()
```

Check if authorization is required based on the presence of users in the _users table.
Returns:
    bool: True if authorization is required, False otherwise.

<a id="database.Database._check_permission"></a>

#### \_check\_permission

```python
def _check_permission(session_token, permission)
```

Check if the user associated with the session token has the specified permission.
Args:
    session_token (str): The session token of the user.
    permission (str): The permission to check.
Raises:
    PermissionError: If the user does not have the required permission.

<a id="database.Database.check_permission"></a>

#### check\_permission

```python
def check_permission(username, permission)
```

Check if a user has a specific permission.
Args:
    username (str): The username of the user.
    permission (str): The permission to check for.
Returns:
    bool: True if the user has the permission, False otherwise.

<a id="database.Database.create_table"></a>

#### create\_table

```python
@log_method_call
def create_table(table_name,
                 columns,
                 session_token=None,
                 logging_override=False)
```

Creates a new table in the database.
Args:
    table_name (str): The name of the table to be created.
    columns (list): A list of column definitions for the table.
    session_token (str, optional): The session token of the user performing the action.
    logging_override (bool, optional): If True, enables logging for the table. Defaults to False.
        db_logging must be enabled for this to take effect.
Returns:
    None

<a id="database.Database.drop_table"></a>

#### drop\_table

```python
@log_method_call
def drop_table(table_name, session_token=None)
```

Drops a table from the database.
Args:
    table_name (str): The name of the table to be dropped.
    session_token (str, optional): The session token of the user performing the action.

<a id="database.Database.update_table"></a>

#### update\_table

```python
@log_method_call
def update_table(table_name, updates, session_token=None)
```

Updates a table in the database.
Args:
    table_name (str): The name of the table to be updated.
    updates (dict): A dictionary of updates to apply to the table.
    session_token (str, optional): The session token of the user performing the action.

<a id="database.Database.get_table"></a>

#### get\_table

```python
def get_table(table_name, session_token=None)
```

Retrieve a table from the database by its name.
Args:
    table_name (str): The name of the table to retrieve.
    session_token (str, optional): The session token of the user performing the action.
Returns:
    Table: The table object.

<a id="database.Database._create_table_from_dict"></a>

#### \_create\_table\_from\_dict

```python
def _create_table_from_dict(table_data)
```

Creates a new table in the database from a dictionary.  
Dictionary must contain the following keys: 'name', 'columns', 'records', and 'constraints'.

<a id="database.Database.get_table"></a>

#### get\_table

```python
def get_table(table_name)
```

Retrieve a table from the database by its name.
Args:
    table_name (str): The name of the table to retrieve.
Returns:
    Table: The table object.

<a id="database.Database.create_table_from_csv"></a>

#### create\_table\_from\_csv

```python
@log_method_call
def create_table_from_csv(dir,
                          table_name,
                          headers=True,
                          delim=',',
                          column_names=None,
                          col_types=None,
                          progress=False,
                          parrallel=False,
                          max_chunk_size=None)
```

Creates a table in the database from a CSV file.
Args:
    dir (str): The directory path to the CSV file.
    table_name (str): The name of the table to be created.
    headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
    delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
    column_names (list, optional): List of column names to use if headers is False. Defaults to None.
    col_types (list, optional): List of types to cast the columns to. Defaults to None.
    progress (bool, optional): If True, displays a progress bar. Defaults to False.
    parallel (bool, optional): If True, uses multiprocessing to process the file. Defaults to False.
Example:
    db.create_table_from_csv('/path/to/file.csv', 'my_table', headers=True, delim=';', column_names=['col1', 'col2'], col_types=[str, int], progress=True)

<a id="database.Database._create_table_from_csv_mp"></a>

#### \_create\_table\_from\_csv\_mp

```python
def _create_table_from_csv_mp(dir,
                              table_name,
                              headers=True,
                              delim=',',
                              column_names=None,
                              col_types=None,
                              progress=False,
                              max_chunk_size=None)
```

Creates a table in the database from a CSV file using multiprocessing.
Args:
    dir (str): The directory path to the CSV file.
    table_name (str): The name of the table to be created.
    headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
    delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
    column_names (list, optional): List of column names to use if headers is False. Defaults to None.
    col_types (list, optional): List of types to cast the columns to. Defaults to None.
    progress (bool, optional): If True, displays a progress bar. Defaults to False.

<a id="database.Database._get_file_chunks"></a>

#### \_get\_file\_chunks

```python
def _get_file_chunks(file_name, max_cpu, headers, max_chunk_size=10_000)
```

Split file into chunks for processing by multiple CPUs.
Args:
    file_name (str): The name of the file to process.
    max_cpu (int): The maximum number of CPU cores to use.
    headers (bool): Indicates whether the CSV file contains headers.
    max_chunk_size (int, optional): The maximum size of each chunk in bytes. Defaults to 10_000.
Returns:
    cpu_count (int): The number of CPU cores to use.
    start_end (list): A list of tuples containing the start and end positions of each file chunk.

<a id="database.Database._process_file"></a>

#### \_process\_file

```python
def _process_file(cpu_count, start_end, delim, column_names, col_types,
                  progress, headers)
```

Process the file in parallel using multiple CPUs.
Args:
    cpu_count (int): The number of CPU cores to use.
    start_end (list): A list of tuples containing the start and end positions of each file chunk.
    delim (str): The delimiter used in the CSV file.
    column_names (list): List of column names to use if headers is False.
    col_types (list): List of types to cast the columns to.
    progress (bool): If True, displays a progress bar.
    headers (bool): Indicates whether the CSV file contains headers.
Returns:
    records (list): A list of Record objects representing the rows in the file.

<a id="database.Database.add_constraint"></a>

#### add\_constraint

```python
@log_method_call
def add_constraint(table_name,
                   column,
                   constraint,
                   reference_table_name=None,
                   reference_column=None)
```

Adds a constraint to a specified column in a table.
Args:
    table_name (str): The name of the table to which the constraint will be added.
    column (str): The name of the column to which the constraint will be applied.
    constraint (str): The constraint to be added to the column.
    reference_table_name (str, optional): The name of the table that the foreign key references. Required for foreign key constraints.
    reference_column (str, optional): The column in the reference table that the foreign key references. Required for foreign key constraints.
Raises:
    ValueError: If the specified table does not exist.

<a id="database.Database.join_tables"></a>

#### join\_tables

```python
def join_tables(table_name1, table_name2, on_column, other_column)
```

Perform an inner join between two tables on specified columns.
Args:
    table_name1 (str): The name of the first table.
    table_name2 (str): The name of the second table.
    on_column (str): The column in the first table to join on.
    other_column (str): The column in the second table to join on.
Returns:
    Table: A new table containing the joined records.

<a id="database.Database.aggregate_table"></a>

#### aggregate\_table

```python
def aggregate_table(table_name, group_column, agg_column, agg_func)
```

Perform an aggregation on a specified column in a table using the provided aggregation function.
Args:
    table_name (str): The name of the table.
    group_column (str): The column to group by.
    agg_column (str): The column to aggregate.
    agg_func (str): The aggregation function to apply. Supported values are 'MIN', 'MAX', 'COUNT', 'SUM', 'AVG', 'COUNT_DISTINCT'.
Returns:
    Table: A new table containing the result of the aggregation.

<a id="database.Database.filter_table"></a>

#### filter\_table

```python
def filter_table(table_name, condition)
```

Filter records in a table based on a condition.
Args:
    table_name (str): The name of the table.
    condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
                          Example: lambda record: record.data["product"] == "Laptop"
Returns:
    Table: A new table containing the filtered records.

<a id="database.Database.add_stored_procedure"></a>

#### add\_stored\_procedure

```python
@log_method_call
def add_stored_procedure(name, procedure)
```

Add a new stored procedure to the database.
Args:
    name (str): The name of the stored procedure.
    procedure (function): The function representing the stored procedure.

<a id="database.Database.execute_stored_procedure"></a>

#### execute\_stored\_procedure

```python
@log_method_call
def execute_stored_procedure(name, *args, **kwargs)
```

Execute a stored procedure.
Args:
    name (str): The name of the stored procedure.
    *args: Positional arguments to pass to the stored procedure.
    **kwargs: Keyword arguments to pass to the stored procedure.
Returns:
    The result of the stored procedure execution.

<a id="database.Database.delete_stored_procedure"></a>

#### delete\_stored\_procedure

```python
@log_method_call
def delete_stored_procedure(name)
```

Delete a stored procedure from the database.
Args:
    name (str): The name of the stored procedure.

<a id="database.Database.get_stored_procedure"></a>

#### get\_stored\_procedure

```python
def get_stored_procedure(name)
```

Retrieve a stored procedure by name.
Args:
    name (str): The name of the stored procedure.
Returns:
    function: The stored procedure function.

<a id="database.Database._stored_procedure_to_string"></a>

#### \_stored\_procedure\_to\_string

```python
def _stored_procedure_to_string(procedure)
```

Return the source code of a stored procedure function as a string.
Args:
    procedure (function): The stored procedure function.
Returns:
    str: The source code of the stored procedure function.

<a id="database.Database.add_trigger"></a>

#### add\_trigger

```python
@log_method_call
def add_trigger(procedure_name, trigger_type, trigger_function)
```

Add a trigger for a stored procedure.
Args:
    procedure_name (str): The name of the stored procedure.
    trigger_type (str): The type of trigger ('before' or 'after').
    trigger_function (function): The function to be executed as the trigger.

<a id="database.Database.execute_triggers"></a>

#### execute\_triggers

```python
@log_method_call
def execute_triggers(procedure_name, trigger_type, *args, **kwargs)
```

Execute triggers for a stored procedure.
Args:
    procedure_name (str): The name of the stored procedure.
    trigger_type (str): The type of trigger ('before' or 'after').
    *args: Positional arguments to pass to the trigger function.
    **kwargs: Keyword arguments to pass to the trigger function.

<a id="database.Database.delete_trigger"></a>

#### delete\_trigger

```python
@log_method_call
def delete_trigger(procedure_name, trigger_type, trigger_function)
```

Delete a trigger for a stored procedure.
Args:
    procedure_name (str): The name of the stored procedure.
    trigger_type (str): The type of trigger ('before' or 'after').
    trigger_function (function): The function to be removed as the trigger.

<a id="database.Database.create_view"></a>

#### create\_view

```python
@log_method_call
def create_view(view_name, query)
```

Create a new view.
Args:
    view_name (str): The name of the view.
    query (function): A function that returns the data for the view.

<a id="database.Database.get_view"></a>

#### get\_view

```python
def get_view(view_name)
```

Retrieve a view by name.
Args:
    view_name (str): The name of the view.
Returns:
    View: The view object.

<a id="database.Database.delete_view"></a>

#### delete\_view

```python
@log_method_call
def delete_view(view_name)
```

Delete a view by name.
Args:
    view_name (str): The name of the view.

<a id="database.Database.create_materialized_view"></a>

#### create\_materialized\_view

```python
@log_method_call
def create_materialized_view(view_name, query)
```

Create a new materialized view.
Args:
    view_name (str): The name of the materialized view.
    query (function): A function that returns the data for the materialized view.

<a id="database.Database.get_materialized_view"></a>

#### get\_materialized\_view

```python
def get_materialized_view(view_name)
```

Retrieve a materialized view by name.
Args:
    view_name (str): The name of the materialized view.
Returns:
    MaterializedView: The materialized view object.

<a id="database.Database.refresh_materialized_view"></a>

#### refresh\_materialized\_view

```python
@log_method_call
def refresh_materialized_view(view_name)
```

Refresh a materialized view by name.
Args:
    view_name (str): The name of the materialized view.

<a id="database.Database.delete_materialized_view"></a>

#### delete\_materialized\_view

```python
@log_method_call
def delete_materialized_view(view_name)
```

Delete a materialized view by name.
Args:
    view_name (str): The name of the materialized view.

<a id="database.Database.show_db_with_curses"></a>

#### show\_db\_with\_curses

```python
def show_db_with_curses()
```

<a id="database.Database.print_db"></a>

#### print\_db

```python
def print_db(index=False,
             limit=None,
             tables=True,
             views=False,
             materialized_views=False,
             stored_procedures=False,
             triggers=False)
```

Print the database tables, including their names, columns, constraints, and records.
Args:
    index (bool, optional): Whether to print the index of each record. Defaults to False.
    limit (int, optional): The maximum number of records to print for each table. Defaults to None.
    tables (bool, optional): Whether to print the tables. Defaults to True.
    views (bool, optional): Whether to print the views. Defaults to False.
    materialized_views (bool, optional): Whether to print the materialized views. Defaults to False.

<a id="database.Database.copy"></a>

#### copy

```python
def copy()
```

Create a deep copy of the database state.  
This method uses the `copy` module to create a deep copy of the current
database instance, ensuring that all nested objects are also copied.
Returns:
    A new instance of the database with the same state as the original.

<a id="database.Database.restore"></a>

#### restore

```python
def restore(state)
```

Restore the database state from a shadow copy.
Args:
    state (object): An object containing the state to restore, including tables and name attributes.
Returns:
    self: The instance of the database with the restored state.

<a id="database.Database.get_db_size"></a>

#### get\_db\_size

```python
def get_db_size()
```

Calculate the size of the database in bytes.
Returns:
    int: The size of the database in bytes.

<a id="database.Database._create_orders_table"></a>

#### \_create\_orders\_table

```python
def _create_orders_table(num_records=100)
```

Create a sample orders table with random data.
Args:
    num_records (int): The number of records to generate.

<a id="database.Database._create_users_table"></a>

#### \_create\_users\_table

```python
def _create_users_table(num_records=10)
```

Create a sample users table with random data.
columns = ["user_id", "name", "email"]
    num_records (int): The number of records to generate.

<a id="database.Database._create_products_table"></a>

#### \_create\_products\_table

```python
def _create_products_table(num_records=50)
```

Create a sample products table with random data.
Args:
    num_records (int): The number of records to generate.

<a id="database.Database._create_reviews_table"></a>

#### \_create\_reviews\_table

```python
def _create_reviews_table(num_records=200)
```

Create a sample reviews table with random data.
Args:
    num_records (int): The number of records to generate.

<a id="database.Database._create_categories_table"></a>

#### \_create\_categories\_table

```python
def _create_categories_table(num_records=10)
```

Create a sample categories table with random data.
Args:
    num_records (int): The number of records to generate.

<a id="database.Database._create_suppliers_table"></a>

#### \_create\_suppliers\_table

```python
def _create_suppliers_table(num_records=20)
```

Create a sample suppliers table with random data.
Args:
    num_records (int): The number of records to generate.

<a id="database.Database.load_sample_database"></a>

#### load\_sample\_database

```python
@staticmethod
def load_sample_database(name="SampleDB",
                         n_users=10,
                         n_orders=100,
                         n_products=50,
                         n_reviews=200,
                         n_categories=10,
                         n_suppliers=20,
                         db_logging=False,
                         table_logging=False)
```

Create a sample database with predefined tables and data for testing and demonstration purposes.
Args:
    name (str): The name of the sample database.
Returns:
    Database: An instance of the Database class populated with sample data.

<a id="databasePartial"></a>

# databasePartial

<a id="databasePartial.PartialDatabase"></a>

## PartialDatabase Objects

```python
class PartialDatabase(Database)
```

<a id="databasePartial.PartialDatabase.__init__"></a>

#### \_\_init\_\_

```python
def __init__(name,
             file_path,
             views=True,
             materialized_views=True,
             stored_procedures=True,
             triggers=True)
```

Initialize a new partial database object.
Args:
    name (str): The name of the database.
    file_path (str): The path to the database file.
    views (bool, optional): Whether to load views. Defaults to None.
    materialized_views (bool, optional): Whether to load materialized views. Defaults to None.
    stored_procedures (bool, optional): Whether to load stored procedures. Defaults to None.
    triggers (bool, optional): Whether to load triggers. Defaults to None.

<a id="databasePartial.PartialDatabase.get_table"></a>

#### get\_table

```python
def get_table(table_name, session_token=None)
```

Retrieve a table from the database by its name. Load the table into memory if it is not already loaded.
Args:
    table_name (str): The name of the table to retrieve.
    session_token (str, optional): The session token of the user performing the action.
Returns:
    Table: The table object.

<a id="databasePartial.PartialDatabase._load_table_from_storage"></a>

#### \_load\_table\_from\_storage

```python
def _load_table_from_storage(table_name)
```

Load a table from storage into memory.
Args:
    table_name (str): The name of the table to load.
Returns:
    Table: The loaded table object.

<a id="databasePartial.PartialDatabase.active_tables"></a>

#### active\_tables

```python
def active_tables()
```

Get a list of active tables in the database.
Returns:
    list: A list of table names.

<a id="databasePartial.PartialDatabase.deactivate_table"></a>

#### deactivate\_table

```python
def deactivate_table(table_name)
```

Move a table from active memory to dormant storage.
Args:
    table_name (str): The name of the table to move.

<a id="databasePartial.PartialDatabase.dormant_tables"></a>

#### dormant\_tables

```python
def dormant_tables()
```

Get a list of tables that are not currently loaded into memory.
Returns:
    list: A list of table names.

<a id="databasePartial.PartialDatabase.print_db"></a>

#### print\_db

```python
def print_db(index=False,
             limit=None,
             tables=True,
             views=False,
             materialized_views=False,
             stored_procedures=False,
             triggers=False)
```

Print the database tables, including their names, columns, constraints, and records.
Args:
    index (bool, optional): Whether to print the index of each record. Defaults to False.
    limit (int, optional): The maximum number of records to print for each table. Defaults to None.
    tables (bool, optional): Whether to print the tables. Defaults to True.
    views (bool, optional): Whether to print the views. Defaults to False.
    materialized_views (bool, optional): Whether to print the materialized views. Defaults to False.

<a id="db_navigator"></a>

# db\_navigator

<a id="db_navigator.KEY_MAPPING"></a>

#### KEY\_MAPPING

<a id="db_navigator.safe_execution"></a>

#### safe\_execution

```python
def safe_execution(func)
```

Decorator to handle exceptions and log errors for functions.

<a id="db_navigator.is_key"></a>

#### is\_key

```python
def is_key(key, key_type)
```

Check if the input key matches any of the mapped keys for the given key type.

Args:
    key: The input key code
    key_type: The type of key to check ('UP', 'DOWN', 'LEFT', 'RIGHT', 'ENTER', 'QUIT')

    bool: True if the key matches any of the mapped keys, False otherwise
Returns:

<a id="db_navigator.safe_addstr"></a>

#### safe\_addstr

```python
def safe_addstr(stdscr, y: int, x: int, text: str, attr=None)
```

Safely add a string to the screen, handling boundary errors.

<a id="db_navigator.remove_leading_spaces"></a>

#### remove\_leading\_spaces

```python
def remove_leading_spaces(code: str) -> str
```

Remove leading spaces from each line of the given code.

<a id="db_navigator.display_popup"></a>

#### display\_popup

```python
@safe_execution
def display_popup(stdscr, message: str, timeout: int = 0)
```

Display a centered popup message.

<a id="db_navigator.display_help"></a>

#### display\_help

```python
def display_help(stdscr)
```

Display help information.

<a id="db_navigator.search_prompt"></a>

#### search\_prompt

```python
@safe_execution
def search_prompt(stdscr, items: List[str]) -> Optional[int]
```

Display search prompt and return index of matched item.

<a id="db_navigator.db_navigator"></a>

#### db\_navigator

```python
@safe_execution
def db_navigator(stdscr, db)
```

Navigates through the database options using a curses-based interface.
The function initializes the curses environment, sets up color pairs, and defines a menu with various database options.
It then enters a main loop where it displays the main screen layout, handles user input to navigate through the menu,
and calls the corresponding display functions based on the selected menu option.

Args:
    stdscr: The curses window object.
    db: The database connection object.

Menu Options:
    - "DB Info": Displays database information.
    - "View Tables": Displays the list of tables.
    - "View Views": Displays the list of views.
    - "View Materialized Views": Displays the list of materialized views.
    - "View Stored Procedures": Displays the list of stored procedures.
    - "View Trigger Functions": Displays the list of trigger functions.

User Input:
    - 'q': Quit the navigator.
    - KEY_UP: Move the selection up.
    - KEY_DOWN: Move the selection down.
    - KEY_ENTER, 10, 13, KEY_RIGHT: Select the current menu option and display the corresponding information.

<a id="db_navigator.display_info"></a>

#### display\_info

```python
@safe_execution
def display_info(stdscr, db)
```

Displays the database navigator information on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the name attribute to be displayed.

<a id="db_navigator.display_main_screen"></a>

#### display\_main\_screen

```python
@safe_execution
def display_main_screen(stdscr, menu_list, selected_row_idx, info_offset)
```

Displays the main screen layout with the menu options and highlights the selected row.

Args:
    stdscr: The curses window object where the main screen layout will be displayed.
    menu_list: A list of menu options to display.
    selected_row_idx: The index of the selected row in the menu list.
    info_offset: The vertical offset to display the menu options.

<a id="db_navigator.display_db_info"></a>

#### display\_db\_info

```python
@safe_execution
def display_db_info(stdscr, db, info_offset)
```

Displays the database information on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the information to be displayed.
    info_offset: The vertical offset to display the database information.

<a id="db_navigator.display_tables"></a>

#### display\_tables

```python
@safe_execution
def display_tables(stdscr, db, info_offset)
```

Displays the list of tables in the database on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the information to be displayed.
    info_offset: The vertical offset to display the database information.

<a id="db_navigator.display_table"></a>

#### display\_table

```python
@safe_execution
def display_table(stdscr, table, table_name, tables_offset)
```

Displays the table information and records on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    table: The table object containing the records to be displayed.
    table_name: The name of the table.
    tables_offset: The vertical offset to display the table information.

<a id="db_navigator._get_record_page"></a>

#### \_get\_record\_page

```python
@safe_execution
def _get_record_page(table, page_num, page_size)
```

Get a page of records based on the page number and page size.

Args:
    table: The table object containing the records.
    page_num: The page number to retrieve.
    page_size: The number of records per page.

Returns:
    A list of records for the specified page.

<a id="db_navigator.display_views"></a>

#### display\_views

```python
@safe_execution
def display_views(stdscr, db, info_offset)
```

Displays the list of views in the database on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the information to be displayed.
    info_offset: The vertical offset to display the database information.

<a id="db_navigator.display_view"></a>

#### display\_view

```python
@safe_execution
def display_view(stdscr, table, view_name, query, view_offset)
```

Displays the view information and records on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    table: The table object containing the records to be displayed.
    view_name: The name of the view.
    query: The query used to create the view.
    view_offset: The vertical offset to display the view information.

<a id="db_navigator.display_mv_views"></a>

#### display\_mv\_views

```python
@safe_execution
def display_mv_views(stdscr, db, info_offset)
```

Displays the list of materialized views in the database on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the information to be displayed.
    info_offset: The vertical offset to display the database information

<a id="db_navigator.display_mv_view"></a>

#### display\_mv\_view

```python
@safe_execution
def display_mv_view(stdscr, table, view_name, query, view_offset)
```

Displays the materialized view information and records on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    table: The table object containing the records to be displayed.
    view_name: The name of the materialized view.
    query: The query used to create the materialized view.
    view_offset: The vertical offset to display the materialized view information.

<a id="db_navigator.display_stored_procedures"></a>

#### display\_stored\_procedures

```python
@safe_execution
def display_stored_procedures(stdscr, db, info_offset)
```

Displays the list of stored procedures in the database on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the information to be displayed.
    info_offset: The vertical offset to display the database information.

<a id="db_navigator.display_procedure"></a>

#### display\_procedure

```python
@safe_execution
def display_procedure(stdscr, procedure, procedure_name, proc_offset)
```

Displays the stored procedure information on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    procedure: The stored procedure code to be displayed.
    procedure_name: The name of the stored procedure.
    proc_offset: The vertical offset to display the stored procedure information

<a id="db_navigator.display_trigger_functions"></a>

#### display\_trigger\_functions

```python
@safe_execution
def display_trigger_functions(stdscr, db, info_offset)
```

Displays the list of trigger functions in the database on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    db: The database object containing the information to be displayed.
    info_offset: The vertical offset to display the database information.

<a id="db_navigator.display_function"></a>

#### display\_function

```python
@safe_execution
def display_function(stdscr, function, function_name, func_offset)
```

Displays the trigger function information on the provided screen.

Args:
    stdscr: The curses window object where the information will be displayed.
    function: The trigger function code to be displayed.
    function_name: The name of the trigger function.
    func_offset: The vertical offset to display the trigger function information.

<a id="index"></a>

# index

<a id="index.Index"></a>

## Index Objects

```python
class Index()
```

<a id="index.Index.__init__"></a>

#### \_\_init\_\_

```python
def __init__()
```

Initializes the index dictionary for the class instance.

<a id="index.Index.add"></a>

#### add

```python
def add(key, record)
```

Adds a record to the index under the specified key.  
If the key does not exist in the index, it will be created.
Args:
    key (str): The key under which the record will be added.
    record (Any): The record to be added to the index.

<a id="index.Index.remove"></a>

#### remove

```python
def remove(key, record)
```

Remove a record from the index for a given key.
Args:
    key (str): The key from which the record should be removed.
    record (Any): The record to be removed from the index.

<a id="index.Index.find"></a>

#### find

```python
def find(key)
```

Find the value associated with the given key in the index.
Args:
    key: The key to search for in the index.
Returns:
    The value associated with the key if found, otherwise an empty list.

<a id="index.Index.__str__"></a>

#### \_\_str\_\_

```python
def __str__()
```

Returns a string representation of the index.
Returns:
    str: A string representation of the index.

<a id="index.Index.to_dict"></a>

#### to\_dict

```python
def to_dict()
```

Returns a dictionary representation of the index.
Returns:
    dict: A dictionary representation of the index.

<a id="record"></a>

# record

<a id="record.Record"></a>

## Record Objects

```python
class Record()
```

<a id="record.Record.__init__"></a>

#### \_\_init\_\_

```python
def __init__(record_id, data)
```

Initializes a new instance of the class.
Args:
    record_id (int): The unique identifier for the record.
    data (dict): The data associated with the record.

<a id="record.Record.index"></a>

#### index

```python
@property
def index()
```

Returns the index of the record. 
@property decorator is used to make the method behave like an attribute. (Read-only)

<a id="record.Record.add_to_index"></a>

#### add\_to\_index

```python
def add_to_index(key)
```

<a id="record.Record.remove_from_index"></a>

#### remove\_from\_index

```python
def remove_from_index(key)
```

<a id="record.Record._type"></a>

#### \_type

```python
def _type()
```

<a id="record.VectorRecord"></a>

## VectorRecord Objects

```python
class VectorRecord(Record)
```

<a id="record.VectorRecord.__init__"></a>

#### \_\_init\_\_

```python
def __init__(record_id, vector)
```

Initializes a new instance of the VectorRecord class.
Args:
    record_id (int): The unique identifier for the record.
    vector (list): The vector data associated with the record.

<a id="record.VectorRecord.vector"></a>

#### vector

```python
@property
def vector()
```

<a id="record.VectorRecord.magnitude"></a>

#### magnitude

```python
def magnitude()
```

Calculates the magnitude of the vector.
Returns:
    float: The magnitude of the vector.

<a id="record.VectorRecord.normalize"></a>

#### normalize

```python
def normalize()
```

Normalizes the vector.
Returns:
    list: The normalized vector.

<a id="record.VectorRecord.dot_product"></a>

#### dot\_product

```python
def dot_product(other_vector)
```

Calculates the dot product with another vector.
Args:
    other_vector (list): The other vector to calculate the dot product with.
Returns:
    float: The dot product of the two vectors.

<a id="record.VectorRecord._type"></a>

#### \_type

```python
def _type()
```

<a id="record.TimeSeriesRecord"></a>

## TimeSeriesRecord Objects

```python
class TimeSeriesRecord(Record)
```

<a id="record.TimeSeriesRecord.__init__"></a>

#### \_\_init\_\_

```python
def __init__(record_id, time_series)
```

<a id="record.TimeSeriesRecord.time_series"></a>

#### time\_series

```python
@property
def time_series()
```

<a id="record.TimeSeriesRecord.moving_average"></a>

#### moving\_average

```python
def moving_average(window_size)
```

Calculates the moving average of the time series.
Args:
    window_size (int): The window size for the moving average.
Returns:
    list: The moving average of the time series.

<a id="record.TimeSeriesRecord._type"></a>

#### \_type

```python
def _type()
```

<a id="record.ImageRecord"></a>

## ImageRecord Objects

```python
class ImageRecord(Record)
```

<a id="record.ImageRecord.__init__"></a>

#### \_\_init\_\_

```python
def __init__(record_id, image_path)
```

Initializes a new instance of the ImageRecord class.
Args:
    record_id (int): The unique identifier for the record.
    image_path (str): The file path to the image.

<a id="record.ImageRecord.image_data"></a>

#### image\_data

```python
@property
def image_data()
```

<a id="record.ImageRecord.image_path"></a>

#### image\_path

```python
@property
def image_path()
```

<a id="record.ImageRecord.image_size"></a>

#### image\_size

```python
@property
def image_size()
```

Returns the size of the image in bytes.

<a id="record.ImageRecord.get_image"></a>

#### get\_image

```python
def get_image()
```

Converts the image data to a PIL Image object.
Returns:
    Image: The PIL Image object.

<a id="record.ImageRecord.resize"></a>

#### resize

```python
def resize(percentage)
```

Resizes the image by a given percentage.
Args:
    percentage (float): The percentage to increase or decrease the size of the image.
Returns:
    Image: The resized PIL Image object.

<a id="record.ImageRecord._convert_to_base64"></a>

#### \_convert\_to\_base64

```python
def _convert_to_base64()
```

Converts the image data to a base64 encoded string.
Returns:
    str: The base64 encoded image data.

<a id="record.ImageRecord.to_dict"></a>

#### to\_dict

```python
def to_dict()
```

Converts the ImageRecord to a dictionary, encoding image data to base64.
Returns:
    dict: The dictionary representation of the ImageRecord.

<a id="record.ImageRecord._type"></a>

#### \_type

```python
def _type()
```

<a id="record.TextRecord"></a>

## TextRecord Objects

```python
class TextRecord(Record)
```

<a id="record.TextRecord.__init__"></a>

#### \_\_init\_\_

```python
def __init__(record_id, text)
```

Initializes a new instance of the TextRecord class.
Args:
    record_id (int): The unique identifier for the record.
    text (str): The text data associated with the record.

<a id="record.TextRecord.text"></a>

#### text

```python
@property
def text()
```

<a id="record.TextRecord.word_count"></a>

#### word\_count

```python
def word_count()
```

Counts the number of words in the text.
Returns:
    int: The number of words in the text.

<a id="record.TextRecord.to_uppercase"></a>

#### to\_uppercase

```python
def to_uppercase()
```

Converts the text to uppercase.
Returns:
    str: The text in uppercase.

<a id="record.TextRecord.to_lowercase"></a>

#### to\_lowercase

```python
def to_lowercase()
```

Converts the text to lowercase.
Returns:
    str: The text in lowercase.

<a id="record.TextRecord._type"></a>

#### \_type

```python
def _type()
```

<a id="record.EncryptedRecord"></a>

## EncryptedRecord Objects

```python
class EncryptedRecord(Record)
```

<a id="record.EncryptedRecord.__init__"></a>

#### \_\_init\_\_

```python
def __init__(record_id, data)
```

Initializes a new instance of the EncryptedRecord class.
Args:
    record_id (int): The unique identifier for the record.
    data (str): The data associated with the record. Must contain a 'data' and 'key' field.

<a id="record.EncryptedRecord.decrypt"></a>

#### decrypt

```python
def decrypt(key)
```

Decrypts the data using the encryption key.
Args:
    key (str): The key used for decryption
Returns:
    str: The decrypted data.

<a id="record.EncryptedRecord._type"></a>

#### \_type

```python
def _type()
```

<a id="storage"></a>

# storage

<a id="storage._process_chunk"></a>

#### \_process\_chunk

```python
def _process_chunk(records_chunk, table)
```

Process a chunk of the file. This function is used in the multiprocessing pool.
Args:
    records_chunk (list): A list of records to be processed.
    table (Table): The table object to which the records will be added.
Returns:
    list: A list of Record objects created from the records_chunk.

<a id="storage.Storage"></a>

## Storage Objects

```python
class Storage()
```

A utility class for saving, loading, and deleting database files.

<a id="storage.Storage.generate_key"></a>

#### generate\_key

```python
@staticmethod
def generate_key()
```

Generate a key for encryption.

<a id="storage.Storage.encrypt"></a>

#### encrypt

```python
@staticmethod
def encrypt(data, key)
```

Encrypt the data using the provided key.

<a id="storage.Storage.decrypt"></a>

#### decrypt

```python
@staticmethod
def decrypt(data, key)
```

Decrypt the data using the provided key.

<a id="storage.Storage.backup"></a>

#### backup

```python
@staticmethod
def backup(db, key=None, compress=False, dir=None, date=False)
```

Backup the database to a file. 
If none exists, a folder named 'backups_' + db.name will be created in the directory.
The database object is serialized into a JSON format and written to the specified file.
Each backup file is named 'db.name_backup_n.segadb', where n is the number of backups in the directory.
Args:
    db (Database): The database object to be saved.
    key (bytes, optional): The encryption key. If provided, the data will be encrypted before saving.
    compress (bool, optional): If True, the data will be compressed using zlib before saving.
    dir (str, optional): The directory where the backup will be saved. Default is None.       
    date (bool, optional): If True, the date will be appended to the filename. Default is False.
Returns:
    status (str): The status of the backup operation, as well as the path to the backup file.

<a id="storage.Storage.list_backups"></a>

#### list\_backups

```python
@staticmethod
def list_backups(dir=None, print_output=True)
```

List the backup files in the specified directory.
Args:
    dir (str, optional): The directory to search for backup files. Default is None.
Returns:
    list: A list of the backup files in the directory.

<a id="storage.Storage.restore"></a>

#### restore

```python
@staticmethod
def restore(db,
            key=None,
            compress=False,
            user=None,
            password=None,
            dir=None,
            backup_name=None)
```

Restore a database from a backup file.
Args:
    db (Database): The database object to be restored.
    key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
    compress (bool, optional): If True, the data will be decompressed using zlib after loading.
    user (str, optional): The username for authentication. Defaults to None.
    password (str, optional): The password for authentication. Defaults to None.
    dir (str, optional): The directory where the backup is saved. Default is None.
    backup_name (str, optional): The name of the backup file. Default is None. Will use the latest backup if None.
Returns:
    Database: An instance of the Database class populated with the data from the backup file.

<a id="storage.Storage.save"></a>

#### save

```python
@staticmethod
def save(db, filename, key=None, compress=False)
```

Save the database object to a JSON file.  
The database object is serialized into a JSON format and written to the specified file.  
The JSON structure includes the database name, tables, columns, records, next_id, and constraints.  
Args:
    db (Database): The database object to be saved.
    filename (str): The path to the file where the database will be saved.
    key (bytes, optional): The encryption key. If provided, the data will be encrypted before saving.
    compress (bool, optional): If True, the data will be compressed using zlib before saving.

<a id="storage.Storage.load"></a>

#### load

```python
@staticmethod
def load(filename,
         key=None,
         user=None,
         password=None,
         compression=False,
         parrallel=False)
```

Load a database from a JSON file.
Args:
    filename (str): The path to the JSON file containing the database data.
    key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
    compression (bool, optional): If True, the data will be decompressed using zlib after loading.
    parrallel (bool, optional): If True, the data will be loaded in parallel using multiprocessing.
Returns:
    Database: An instance of the Database class populated with the data from the JSON file.

<a id="storage.Storage.delete"></a>

#### delete

```python
@staticmethod
def delete(filename)
```

Delete the specified file from the filesystem.
Args:
    filename (str): The path to the file to be deleted.

<a id="storage.Storage.db_to_dict"></a>

#### db\_to\_dict

```python
@staticmethod
def db_to_dict(db)
```

Convert a database object to a dictionary.
Args:
    db (Database): The database object to be converted.
Returns:
    dict: A dictionary representation of the database object

<a id="storage.Storage.save_table"></a>

#### save\_table

```python
@staticmethod
def save_table(table, filename, format='csv')
```

Save the table to a file in the specified format.
Args:
    table (Table): The table object to be saved.
    filename (str): The path to the file where the table will be saved.
    format (str): The format in which the table will be saved. Default is 'csv'.

<a id="storage.Storage._table_to_csv"></a>

#### \_table\_to\_csv

```python
def _table_to_csv(table, filename)
```

Save the table to a CSV file.
Args:
    table (Table): The table object to be saved.
    filename (str): The path to the file where the table will be saved.

<a id="storage.Storage._table_to_json"></a>

#### \_table\_to\_json

```python
def _table_to_json(table, filename)
```

Save the table to a JSON file.
Args:
    table (Table): The table object to be saved.
    filename (str): The path to the file where the table will be saved.

<a id="storage.Storage._table_to_sqlite"></a>

#### \_table\_to\_sqlite

```python
def _table_to_sqlite(table, filename)
```

Save the table to a SQLite database file.  
If the database file already exists, it will be overwritten.
Args:
    table (Table): The table object to be saved.
    filename (str): The path to the file where the table will be saved.

<a id="storage.Storage._load_table_from_db_file"></a>

#### \_load\_table\_from\_db\_file

```python
@staticmethod
def _load_table_from_db_file(filename,
                             table_name,
                             db,
                             key=None,
                             user=None,
                             password=None,
                             compression=False)
```

Load a table from a segadb database file. Only the table data is loaded, not the entire database.
Args:
    filename (str): The path to the JSON file containing the database data.
    table_name (str): The name of the table to load.
    db (Database): The database object to which the table will be added.
    key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
    compression (bool, optional): If True, the data will be decompressed using zlib after loading.
Returns:
    Table: The loaded table object.

<a id="storage.Storage._save_table_to_db_file"></a>

#### \_save\_table\_to\_db\_file

```python
@staticmethod
def _save_table_to_db_file(filename, table, key=None, compression=False)
```

Save a table to a segadb database file. Only the table data is updated, not the entire database.
Args:
    filename (str): The path to the JSON file where the table will be saved.
    table (Table): The table object to be saved.
    key (bytes, optional): The encryption key. If provided, the data will be encrypted before saving.
    compression (bool, optional): If True, the data will be compressed using zlib before saving.

<a id="storage.Storage._load_viewsProcs_from_db_file"></a>

#### \_load\_viewsProcs\_from\_db\_file

```python
@staticmethod
def _load_viewsProcs_from_db_file(filename,
                                  db,
                                  key=None,
                                  user=None,
                                  password=None,
                                  compression=False,
                                  views=True,
                                  materialized_views=True,
                                  stored_procedures=True,
                                  triggers=True)
```

Load views from a segadb database file. Only the view data is loaded, not the entire database.
Args:
    filename (str): The path to the JSON file containing the database data.
    db (Database): The database object to which the views will be added.
    key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
    compression (bool, optional): If True, the data will be decompressed using zlib after loading.
    views (bool, optional): If True, load views. Default is True.
    materialized_views (bool, optional): If True, load materialized views. Default is True.
    stored_procedures (bool, optional): If True, load stored procedures. Default is True.
    triggers (bool, optional): If True, load triggers. Default is True.

<a id="storage.Storage._load_mp"></a>

#### \_load\_mp

```python
def _load_mp(filename, key=None, user=None, password=None, compression=False)
```

Load a database from a JSON file using multiprocessing.
Args:
    filename (str): The name of the file to load.
    key (str, optional): The encryption key. Defaults to None.
    user (str, optional): The username for authentication. Defaults to None.
    password (str, optional): The password for authentication. Defaults to None.
    compression (bool, optional): If True, decompress the file. Defaults to False.

<a id="storage.Storage._get_record_chunks"></a>

#### \_get\_record\_chunks

```python
def _get_record_chunks(records, num_chunks)
```

Split a list of records into chunks.

<a id="table"></a>

# table

<a id="table.log_method_call"></a>

#### log\_method\_call

```python
def log_method_call(func)
```

Decorator to log method calls in the Table class.
Logs the method name, arguments, and return value.

Args:
    func (function): The function to decorate.
Returns:
    function: The decorated function.

<a id="table.Table"></a>

## Table Objects

```python
class Table()
```

<a id="table.Table.__init__"></a>

#### \_\_init\_\_

```python
def __init__(name, columns, logger=None)
```

Initialize a new table with a name and columns.
Args:
    name (str): The name of the table.
    columns (list): A list of column names for the table.
    logger (Logger, optional): A logger object to use for logging. Defaults to None.
Attributes:
    name (str): The name of the table.
    columns (list): A list of column names for the table.
    records (list): A list to store the records of the table.
    next_id (int): The ID to be assigned to the next record.
    constraints (dict): A dictionary to store constraints for each column.

<a id="table.Table.add_constraint"></a>

#### add\_constraint

```python
@log_method_call
def add_constraint(column,
                   constraint,
                   reference_table=None,
                   reference_column=None)
```

Adds a constraint to a specified column in the table.
Args:
    column (str): The name of the column to which the constraint will be added.
    constraint (str): The constraint to be added to the column.
    reference_table (Table, optional): The table that the foreign key references. Required for foreign key constraints.
    reference_column (str, optional): The column in the reference table that the foreign key references. Required for foreign key constraints.
Raises:
    ValueError: If the specified column does not exist in the table.

<a id="table.Table._check_constraints"></a>

#### \_check\_constraints

```python
def _check_constraints(data)
```

Checks the constraints for each column in the provided data.
Args:
    data (dict): A dictionary where keys are column names and values are the data to be checked.
Raises:
    ValueError: If any constraint is violated for a column.

<a id="table.Table.insert"></a>

#### insert

```python
@log_method_call
def insert(data, record_type=Record, transaction=None, flex_ids=False)
```

Inserts a new record into the table.  
If a transaction is provided, the operation is added to the transaction.  
If the data contains an "id" field, it is used as the record ID; otherwise, the next available ID is used.  
Args:
    data (dict): The data to be inserted as a new record.
    transaction (Transaction, optional): An optional transaction object. If provided, the operation will be added to the transaction.
Raises:
    ConstraintError: If the data violates any table constraints.
    ValueError: If the ID is already in use.
Returns:
    None

<a id="table.Table._insert"></a>

#### \_insert

```python
def _insert(record)
```

<a id="table.Table.try_insert"></a>

#### try\_insert

```python
@log_method_call
def try_insert(data, record_type=Record, transaction=None)
```

Attempts to insert data into the table. If an error occurs during the insertion,it catches the ValueError and prints an error message.
Args:
    data (dict): The data to be inserted into the table.
    transaction (optional): The transaction context for the insertion. Default is None.
Raises:
    ValueError: If there is an issue with the data insertion.

<a id="table.Table.bulk_insert"></a>

#### bulk\_insert

```python
@log_method_call
def bulk_insert(record_list, transaction=None)
```

Inserts a list of records into the table.
Args:
    record_list (list): A list of dictionaries where each dictionary represents a record to be inserted.
    transaction (Transaction, optional): An optional transaction object. 
        If provided, the bulk insert operation will be added to the transaction. Defaults to None.

<a id="table.Table._bulk_insert"></a>

#### \_bulk\_insert

```python
def _bulk_insert(record_list)
```

<a id="table.Table.delete"></a>

#### delete

```python
@log_method_call
def delete(record_id, transaction=None)
```

Deletes a record from the database.
Args:
    record_id (int): The ID of the record to delete.
    transaction (Transaction, optional): An optional transaction object. If provided, the delete operation will be added to the transaction. Defaults to None.

<a id="table.Table._delete"></a>

#### \_delete

```python
def _delete(record_id)
```

Deletes a record from the records list based on the given record ID.
Args:
    record_id (int): The ID of the record to be deleted.

<a id="table.Table.update"></a>

#### update

```python
@log_method_call
def update(record_id, data, transaction=None)
```

Update a record in the database.
Args:
    record_id (int): The ID of the record to update.
    data (dict): A dictionary containing the updated data for the record.
    transaction (optional): A transaction object to add the update operation to. If not provided, the update is performed immediately.
Raises:
    ConstraintError: If the data violates any constraints.

<a id="table.Table._update"></a>

#### \_update

```python
def _update(record_id, data)
```

<a id="table.Table.get_id_by_column"></a>

#### get\_id\_by\_column

```python
@log_method_call
def get_id_by_column(column, value)
```

Get the ID of the record with the specified value in the specified column.
Args:
    column (str): The column to search for the value.
    value: The value to search for in the column.
Returns:
    int: The ID of the record with the specified value in the specified column.

<a id="table.Table.select"></a>

#### select

```python
@log_method_call
def select(condition)
```

Selects records from the table that satisfy the given condition.
Args:
    condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
Returns:
    list: A list of records that satisfy the condition.

<a id="table.Table.join"></a>

#### join

```python
def join(other_table, on_column, other_column)
```

Perform an inner join with another table on specified columns.
Args:
    other_table (Table): The table to join with.
    on_column (str): The column in the current table to join on.
    other_column (str): The column in the other table to join on.
Returns:
    Table: A new table containing the joined records.

<a id="table.Table.aggregate"></a>

#### aggregate

```python
def aggregate(group_column, agg_column, agg_func)
```

Perform an aggregation on a specified column using the provided aggregation function.
Args:
    group_column (str): The column to group by.
    agg_column (str): The column to aggregate.
    agg_func (str): The aggregation function to apply. Supported values are 'MIN', 'MAX', 'COUNT', 'SUM', 'AVG', 'COUNT_DISTINCT'.
Returns:
    Table: A new table containing the result of the aggregation.

<a id="table.Table.filter"></a>

#### filter

```python
def filter(condition)
```

Filter records based on a condition.
Args:
    condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
Returns:
    Table: A new table containing the filtered records.

<a id="table.Table.sort"></a>

#### sort

```python
def sort(column, ascending=True)
```

Sorts the records in the table based on the specified column.
Args:
    column (str): The column to sort by.
    ascending (bool, optional): If True, sorts in ascending order. Defaults to True.
Returns:
    Table: A new table containing the sorted records.

<a id="table.Table.print_table"></a>

#### print\_table

```python
def print_table(limit=None, pretty=False, index=False)
```

Prints the records in the table.
Args:
    limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
    pretty (bool, optional): If True, prints the table in a pretty format using the tabulate library. Defaults to False.

<a id="table.Table._print_table_pretty"></a>

#### \_print\_table\_pretty

```python
def _print_table_pretty(limit=None, index=False, max_data_length=25)
```

Prints the records in the table in a pretty format using the tabulate library.
Args:
    limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
    index (bool, optional): If True, includes the index in the printed table. Defaults to False.
    max_data_length (int, optional): The maximum length of the data to be printed. If None, the full data is printed. Defaults to None.

<a id="transaction"></a>

# transaction

<a id="transaction.Transaction"></a>

## Transaction Objects

```python
class Transaction()
```

<a id="transaction.Transaction.__init__"></a>

#### \_\_init\_\_

```python
def __init__(database)
```

Initializes a new transaction instance.
Args:
    database: The database object to associate with this transaction.

Attributes:
    database: The database object associated with this transaction.
    operations: A list to store the operations performed in this transaction.

<a id="transaction.Transaction.begin"></a>

#### begin

```python
def begin()
```

Begins a new transaction.  
This method initializes the operations list and creates a shadow copy
of the current state of the database. The shadow copy is stored in the
database to allow for rollback if needed.

<a id="transaction.Transaction.commit"></a>

#### commit

```python
def commit()
```

Commits the current transaction by executing all operations in the transaction.  
This method iterates over the list of operations and executes each one. After all operations
are executed, it clears the list of operations and discards the shadow copy of the database.

<a id="transaction.Transaction.rollback"></a>

#### rollback

```python
def rollback()
```

Reverts the database to its previous state using a shadow copy if available.  
This method restores the database to the state saved in the shadow copy,
clears the list of operations, and removes the shadow copy reference.

<a id="transaction.Transaction.add_operation"></a>

#### add\_operation

```python
def add_operation(operation)
```

Adds an operation to the list of operations.
Args:
    operation: The operation to be added to the list.

<a id="transaction.Transaction.copy"></a>

#### copy

```python
def copy()
```

Copies a transaction object.

<a id="transaction.Transaction.preview"></a>

#### preview

```python
def preview()
```

Previews the operations in the current transaction without committing or rolling back.

<a id="users"></a>

# users

<a id="users.PRESET_ROLES"></a>

#### PRESET\_ROLES

<a id="users.User"></a>

## User Objects

```python
class User()
```

<a id="users.User.__init__"></a>

#### \_\_init\_\_

```python
def __init__(username, password, roles=None)
```

Initialize a new User instance.
Args:
    username (str): The username of the user.
    password (str): The password of the user.
    roles (list, optional): A list of roles assigned to the user. Defaults to an empty list.
Attributes:
    username (str): The username of the user.
    password_hash (str): The hashed password of the user.
    roles (list): A list of roles assigned to the user.

<a id="users.User.hash_password"></a>

#### hash\_password

```python
@staticmethod
def hash_password(password)
```

Hashes a password using bcrypt.
Args:
    password (str): The password to be hashed.
Returns:
    bytes: The hashed password.

<a id="users.User.check_password"></a>

#### check\_password

```python
def check_password(password)
```

Check if the provided password matches the stored password hash.
Args:
    password (str): The plaintext password to check.
Returns:
    bool: True if the password matches the stored hash, False otherwise.

<a id="users.UserManager"></a>

## UserManager Objects

```python
class UserManager()
```

<a id="users.UserManager.__init__"></a>

#### \_\_init\_\_

```python
def __init__(db)
```

Initializes the Users class.
Args:
    db (Database): The database object to use for user management.

<a id="users.UserManager.register_user"></a>

#### register\_user

```python
def register_user(username, password, roles=None)
```

Registers a new user with the given username, password, and optional roles.
Args:
    username (str): The username for the new user.
    password (str): The password for the new user.
    roles (list, optional): A list of roles assigned to the new user. Defaults to None.
Raises:
    ValueError: If the username already exists in the users dictionary.

<a id="users.UserManager.authenticate_user"></a>

#### authenticate\_user

```python
def authenticate_user(username, password)
```

Authenticate a user by their username and password.
Args:
    username (str): The username of the user.
    password (str): The password of the user.
Returns:
    User: The authenticated user object if the username and password are correct.
    None: If the authentication fails.

<a id="users.UserManager.get_user_permissions"></a>

#### get\_user\_permissions

```python
def get_user_permissions(username)
```

Get the permissions associated with a user.
Args:
    username (str): The username of the user.
Returns:
    list: A list of permissions associated with the user.

<a id="users.UserManager.login_user"></a>

#### login\_user

```python
def login_user(username, password)
```

Logs in a user by their username and password.
Args:
    username (str): The username of the user.
    password (str): The password of the user.
Returns:
    str: A session token if login is successful.
    None: If the login fails.

<a id="users.UserManager.logout_user"></a>

#### logout\_user

```python
def logout_user(session_token)
```

Logs out a user by their session token.
Args:
    session_token (str): The session token of the user.

<a id="users.UserManager.remove_user"></a>

#### remove\_user

```python
def remove_user(username)
```

Removes a user from the database.
Args:
    username (str): The username of the user to be removed.

<a id="users.Authorization"></a>

## Authorization Objects

```python
class Authorization()
```

<a id="users.Authorization.__init__"></a>

#### \_\_init\_\_

```python
def __init__(db)
```

Initializes a new instance of the class.
Attributes:
    permissions (dict): A dictionary to store user permissions.

<a id="users.Authorization.add_permission"></a>

#### add\_permission

```python
def add_permission(username, role)
```

Adds a permission to a specified role.
Args:
    role (str): The role to which the permission will be added.
    permission (str): The permission to be added to the role.

<a id="users.Authorization.check_permission"></a>

#### check\_permission

```python
def check_permission(username, permission)
```

Check if a user has a specific permission.
Args:
    username (str): The username of the user.
    role (str): The role to check for.
Returns:
    bool: True if the user has the permission, False otherwise.

<a id="views"></a>

# views

<a id="views.View"></a>

## View Objects

```python
class View()
```

<a id="views.View.__init__"></a>

#### \_\_init\_\_

```python
def __init__(name, query)
```

Initialize a new view with a name and query.
Args:
    name (str): The name of the view.
    query (function): A function that returns the data for the view.

<a id="views.View.get_data"></a>

#### get\_data

```python
def get_data()
```

Execute the query and return the data for the view.
Returns:
    list: The data for the view.

<a id="views.View._query_to_string"></a>

#### \_query\_to\_string

```python
def _query_to_string()
```

Return the source code of the query function as a string.
Returns:
    str: The source code of the query function.

<a id="views.MaterializedView"></a>

## MaterializedView Objects

```python
class MaterializedView()
```

<a id="views.MaterializedView.__init__"></a>

#### \_\_init\_\_

```python
def __init__(name, query)
```

Initialize a new materialized view with a name and query.
Args:
    name (str): The name of the materialized view.
    query (function): A function that returns the data for the materialized view.

<a id="views.MaterializedView.refresh"></a>

#### refresh

```python
def refresh()
```

Refresh the data for the materialized view by re-executing the query.

<a id="views.MaterializedView.get_data"></a>

#### get\_data

```python
def get_data()
```

Return the data for the materialized view.
Returns:
    list: The data for the materialized view.

<a id="views.MaterializedView._query_to_string"></a>

#### \_query\_to\_string

```python
def _query_to_string()
```

Return the source code of the query function as a string.
Returns:
    str: The source code of the query function.

<a id="__init__"></a>

# \_\_init\_\_

<a id="__init__.__all__"></a>

#### \_\_all\_\_

