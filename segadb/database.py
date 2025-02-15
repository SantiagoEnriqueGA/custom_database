# Imports: Standard Library
import random
import os
import csv
import uuid
from math import inf
import multiprocessing as mp
import inspect
import logging
from pathlib import Path

# Imports: Third Party
import bcrypt
from tqdm import tqdm
from faker import Faker

# Imports: Local
from .users import User, UserManager, Authorization, PRESET_ROLES
from .table import Table
from .record import Record
from .views import View, MaterializedView
from .db_navigator import db_navigator

def log_method_call(func):
    """
    Decorator to log method calls in the Database class.
    Logs the method name, arguments, and return value.
    
    Args:
        func (function): The function to decorate.
    Returns:
        function: The decorated function.
    """
    def wrapper(self, *args, **kwargs):
        if not self.log:
            return func(self, *args, **kwargs)
            
        # Get method name and calling arguments
        method_name = func.__name__
        arg_names = inspect.getfullargspec(func).args[1:]  # Skip 'self'
        
        # Format positional arguments
        args_dict = dict(zip(arg_names, args))
        
        # Combine with keyword arguments
        all_args = {**args_dict, **kwargs}
        
        # Filter out sensitive information (like passwords)
        filtered_args = {
            k: (v if k not in ['password', 'password_hash'] else '[REDACTED]') 
            for k, v in all_args.items()
        }
        
        try:
            # Log the method call
            self.logger.info(
                f"Method Call: {method_name} | " 
                f"Args: {filtered_args}"
            )
            
            # Execute the method
            result = func(self, *args, **kwargs)
            
            # Log successful completion
            self.logger.info(
                f"Method Complete: {method_name} | "
                f"Status: Success"
            )
            
            return result
            
        except Exception as e:
            # Log any exceptions
            self.logger.error(
                f"Method Error: {method_name} | "
                f"Args: {filtered_args} | "
                f"Error: {str(e)}"
            )
            raise

    return wrapper

# Helper function for processing file chunks in parallel (cannot be defined within the Database class)
def _process_file_chunk(file_name, chunk_start, chunk_end, delim=',', column_names=None, col_types=None, progress=False, headers=False):
    """
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
    """
    rows = []
    with open(file_name, 'r', encoding='utf-8') as file:
        # Find the start of the first line in the chunk
        file.seek(chunk_start)
        
        # Skip the header row if it exists
        if headers and file.tell() == 0:
            file.readline()
            
        # Progress bar for processing the chunk
        if progress:
            total_lines = chunk_end - chunk_start
            pbar = tqdm(total=total_lines, desc="Processing chunks", unit="line")
        
        # Iterate over the lines in the chunk
        while file.tell() < chunk_end:
            # Store the current position in the file
            line = file.readline().strip()
            if line:
                # If the line is not empty, process the line
                row_data = line.split(delim)
                if col_types:
                    row_data = [col_type(value) for col_type, value in zip(col_types, row_data)]
                record = Record(file.tell(), dict(zip(column_names, row_data)))
                rows.append(record)
            # Update progress bar with the length of the line plus newline character
            if progress:
                pbar.update(len(line) + 1)
        if progress:
            pbar.close()
    return rows

class Database:  
    # Initialization and Configuration
    # ---------------------------------------------------------------------------------------------
    def __init__(self, name, db_logging=False, table_logging=False):
        """
        Initializes a new instance of the database with the given name.
        Args:
            name (str): The name of the database.
            db_logging (bool, optional): If True, enables logging for the database. Defaults to False.
            table_logging (bool, optional): If True, enables logging for tables. Defaults to False.
        """
        self.name = name
        self.tables = {}
        self.views = {}
        self.materialized_views = {}
        self.sessions = {}
        self.active_session = None
        self.stored_procedures = {}
        self.triggers = {"before": {}, "after": {}}

        # Logging setup
        self.log = db_logging
        self.table_log = table_logging
        if self.log:
            # Create logs directory if it doesn't exist
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            
            # Set up log file path
            log_file = log_dir / f'{self.name}_transaction_log.log'
            
            # Configure logger
            self.logger = logging.getLogger(f'database_{self.name}')
            self.logger.setLevel(logging.INFO)
            
            # Prevent duplicate handlers
            if not self.logger.handlers:
                # Create file handler
                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(logging.INFO)
                
                # Create formatter
                formatter = logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(message)s'
                )
                file_handler.setFormatter(formatter)
                
                # Add handler to logger
                self.logger.addHandler(file_handler)
            
            self.logger.info(f"Database '{self.name}' initialized with logging enabled")

        # Create the _users table for user management
        self.create_table("_users" , ["username", "password_hash", "roles"])        
        
    def create_user_manager(self):
        """
        Create a new instance of the UserManager class.
        Returns:
            UserManager: A new instance of the UserManager class.
        """
        self.user_manager = UserManager(self)
        return self.user_manager

    def create_authorization(self):
        """
        Create a new instance of the Authorization class.
        Returns:
            Authorization: A new instance of the Authorization class.
        """
        self.authorization = Authorization(self)
        return self.authorization        

    # Connection and Session Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def create_session(self, username):
        """
        Creates a new session for a user.
        Args:
            username (str): The username of the user.
        Returns:
            str: The session token.
        """
        session_token = str(uuid.uuid4())
        self.sessions[session_token] = username
        return session_token
    
    @log_method_call
    def delete_session(self, session_token):
        """
        Deletes a session.
        Args:
            session_token (str): The session token to delete.
        """
        if session_token in self.sessions:
            del self.sessions[session_token]

    @log_method_call
    def get_username_by_session(self, session_token):
        """
        Retrieves the username associated with a session token.
        Args:
            session_token (str): The session token.
        Returns:
            str: The username if the session exists, None otherwise.
        """
        return self.sessions.get(session_token)
    
    # User and Role Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def register_user(self, username, password, roles=None):
        """
        Registers a new user in the database.
        Args:
            username (str): The username of the new user.
            password (str): The password of the new user.
            roles (list, optional): A list of roles assigned to the user. Defaults to an empty list.
        Raises:
            ValueError: If the username already exists in the database.
        """
        if self.get_user(username):
            raise ValueError("Username already exists")
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        roles = roles if roles else []
        self.tables["_users"].insert({"username": username, "password_hash": password_hash, "roles": roles})

    def authenticate_user(self, username, password):
        """
        Authenticates a user by their username and password.
        Args:
            username (str): The username of the user.
            password (str): The password of the user.
        Returns:
            dict or None: The user dictionary if authentication is successful, None otherwise.
        """
        user = self.get_user(username)
        if user and bcrypt.checkpw(password.encode('utf-8'), user["password_hash"]):
            return user
        return None

    def get_user(self, username):
        """
        Retrieve user data from the Users table based on the provided username.
        Args:
            username (str): The username of the user to retrieve.
        Returns:
            dict: A dictionary containing the user's data if found, otherwise None.
        """
        users_table = self.get_table("_users")
        for record in users_table.records:
            if record.data["username"] == username:
                return record.data
        return None

    @log_method_call
    def add_role(self, username, role):
        """
        Add a role to a user.
        Args:
            username (str): The username of the user to whom the role will be added.
            role (str): The role to be added to the user.
        Raises:
            ValueError: If the user is not found in the database.
        """
        user = self.get_user(username)
        if user:
            user["roles"].append(role)
            self.tables["_users"].update(user["username"], user)
        else:
            raise ValueError("User not found")

    def check_role(self, username, role):
        """
        Check if a user has a specific role.
        Args:
            username (str): The username of the user to check.
            role (str): The role to check for.
        Returns:
            bool: True if the user has the specified role, False otherwise.
        """
        user = self.get_user(username)
        if user:
            return role in user["roles"]
        return False
    
    @log_method_call
    def remove_user(self, username):
        """
        Removes a user from the database.
        Args:
            username (str): The username of the user to be removed.
        """
        id = self.tables["_users"].get_id_by_column("username", username)
        
        self.tables["_users"].delete(id)
    
    # Authorization and Permissions
    # ---------------------------------------------------------------------------------------------
    def _is_auth_required(self):
        """
        Check if authorization is required based on the presence of users in the _users table.
        Returns:
            bool: True if authorization is required, False otherwise.
        """
        # If there is not _users table, authorization is not required
        if "_users" not in self.tables:
            return False      
        
        return len(self.tables["_users"].records) > 0

    def _check_permission(self, session_token, permission):
        """
        Check if the user associated with the session token has the specified permission.
        Args:
            session_token (str): The session token of the user.
            permission (str): The permission to check.
        Raises:
            PermissionError: If the user does not have the required permission.
        """
        # If no session token is provided, check if active_session is set
        if not session_token:
            session_token = self.active_session
        
        if self._is_auth_required():
            username = self.get_username_by_session(session_token)
            if not username or not self.check_permission(username, permission):
                raise PermissionError(f"User does not have permission: {permission}")
            
    def check_permission(self, username, permission):
        """
        Check if a user has a specific permission.
        Args:
            username (str): The username of the user.
            permission (str): The permission to check for.
        Returns:
            bool: True if the user has the permission, False otherwise.
        """
        user = self.get_user(username)
        if user:
            for role in user["roles"]:
                if permission in PRESET_ROLES.get(role, []):
                    return True
        return False

    # Table Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def create_table(self, table_name, columns, session_token=None, logging_override=False):
        """
        Creates a new table in the database.
        Args:
            table_name (str): The name of the table to be created.
            columns (list): A list of column definitions for the table.
            session_token (str, optional): The session token of the user performing the action.
            logging_override (bool, optional): If True, enables logging for the table. Defaults to False.
                db_logging must be enabled for this to take effect.
        Returns:
            None
        """
        self._check_permission(session_token, "create_table")
        if self.table_log or (logging_override and self.log):
            self.tables[table_name] = Table(table_name, columns, logger=self.logger)
        else:
            self.tables[table_name] = Table(table_name, columns)

    @log_method_call
    def drop_table(self, table_name, session_token=None):
        """
        Drops a table from the database.
        Args:
            table_name (str): The name of the table to be dropped.
            session_token (str, optional): The session token of the user performing the action.
        """
        self._check_permission(session_token, "delete_table")
        del self.tables[table_name]

    @log_method_call
    def update_table(self, table_name, updates, session_token=None):
        """
        Updates a table in the database.
        Args:
            table_name (str): The name of the table to be updated.
            updates (dict): A dictionary of updates to apply to the table.
            session_token (str, optional): The session token of the user performing the action.
        """
        self._check_permission(session_token, "update_table")
        table = self.get_table(table_name)
        if table:
            table.update(updates)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def get_table(self, table_name, session_token=None):
        """
        Retrieve a table from the database by its name.
        Args:
            table_name (str): The name of the table to retrieve.
            session_token (str, optional): The session token of the user performing the action.
        Returns:
            Table: The table object.
        """
        self._check_permission(session_token, "read_table")
        return self.tables.get(table_name)

    def _create_table_from_dict(self, table_data):
        """
        Creates a new table in the database from a dictionary.  
        Dictionary must contain the following keys: 'name', 'columns', 'records', and 'constraints'.
        """
        table = Table(table_data['name'], table_data['columns'])
        table.records = [Record(record['id'], record['data']) for record in table_data['records']]
        table.constraints = table_data['constraints']
        self.tables[table_data['name']] = table

    def get_table(self, table_name):
        """
        Retrieve a table from the database by its name.
        Args:
            table_name (str): The name of the table to retrieve.
        Returns:
            Table: The table object.
        """
        return self.tables.get(table_name)
    
    @log_method_call
    def create_table_from_csv(self, dir, table_name, headers=True, delim=',', column_names=None, col_types=None, progress=False, parrallel=False, max_chunk_size=None):
        """
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
        """
        if parrallel:
            self._create_table_from_csv_mp(dir, table_name, headers, delim, column_names, col_types, progress, max_chunk_size)
            return
            
        with open(dir, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=delim)
            if headers:
                headers = next(reader)
            else:
                headers = column_names if column_names else [f"column{i}" for i in range(len(next(reader)))]
    
            self.create_table(table_name, headers)
    
            if progress:
                reader = tqdm(reader, desc="Processing rows")
    
            for row in reader:
                if col_types:
                    row = [col_type(value) for col_type, value in zip(col_types, row)]
                self.tables[table_name].insert(dict(zip(headers, row)))

    def _create_table_from_csv_mp(self, dir, table_name, headers=True, delim=',', column_names=None, col_types=None, progress=False, max_chunk_size=None):
        """
        Creates a table in the database from a CSV file using multiprocessing.
        Args:
            dir (str): The directory path to the CSV file.
            table_name (str): The name of the table to be created.
            headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
            delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
            column_names (list, optional): List of column names to use if headers is False. Defaults to None.
            col_types (list, optional): List of types to cast the columns to. Defaults to None.
            progress (bool, optional): If True, displays a progress bar. Defaults to False.
        """
        # Warn the user that column_names will be ignored if headers is True
        if headers and column_names:
            print("--Warning: column_names will be ignored if headers is True.--")
        
        # If headers is False and column_names is not provided, generate column names
        with open(dir, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=delim)
            if headers:
                column_names = next(reader)                
            else:
                column_names = column_names if column_names else [f"column{i}" for i in range(len(next(reader)))]
                

        # Get the number of CPU cores and split the file into chunks for each core
        cpu_count, file_chunks = self._get_file_chunks(file_name=dir, max_cpu=mp.cpu_count(), headers=headers, max_chunk_size=max_chunk_size)
        
        # Process the file in parallel using multiple CPUs
        records = self._process_file(cpu_count, file_chunks, delim, column_names, col_types, progress, headers)
        
        # Create the table and add the records
        self.create_table(table_name, column_names)
        self.tables[table_name].records = records
                    
    def _get_file_chunks(self, file_name, max_cpu, headers, max_chunk_size=10_000):
        """
        Split file into chunks for processing by multiple CPUs.
        Args:
            file_name (str): The name of the file to process.
            max_cpu (int): The maximum number of CPU cores to use.
            headers (bool): Indicates whether the CSV file contains headers.
            max_chunk_size (int, optional): The maximum size of each chunk in bytes. Defaults to 10_000.
        Returns:
            cpu_count (int): The number of CPU cores to use.
            start_end (list): A list of tuples containing the start and end positions of each file chunk.
        """
        if max_chunk_size is None:
            max_chunk_size = inf
        
        cpu_count = min(max_cpu, mp.cpu_count())    # Determine the number of CPU cores to use
        file_size = os.path.getsize(file_name)      # Get the total size of the file
        chunk_size = min(file_size // cpu_count, max_chunk_size)  # Calculate the size of each chunk based on the number of CPU cores and max_chunk_size

        start_end = list()                          # List to store the start and end positions of each chunk
        
        with open(file_name, mode="r+b") as f:
            # Function to check if the position is at the start of a new line   
            def is_new_line(position):
                if position == 0:
                    return True
                else:
                    f.seek(position - 1)        # Move the file pointer to one byte before the current position
                    return f.read(1) == b"\n"   # Read the byte to check if it is a newline character

            # Function to find the start of the next line from the given position
            def next_line(position):
                f.seek(position)    # Move the file pointer to the given position
                f.readline()        # Read the line to move to the end of it
                return f.tell()     # Return the current position, which is now the start of the next line

            if headers:
                f.readline()  # Skip the header row

            chunk_start = 0
            while chunk_start < file_size:
                chunk_end = min(file_size, chunk_start + chunk_size)    # End of the current chunk

                while not is_new_line(chunk_end):                       # Adjust chunk_end to the start of the next line if necessary
                    chunk_end -= 1

                if chunk_start == chunk_end:                            # If the chunk size is very small, ensure it is moved to the start of the next line
                    chunk_end = next_line(chunk_end)
               
                if chunk_end - chunk_start < chunk_size:                # If the chunk size is less than the target size, adjust the end position
                    chunk_end = next_line(chunk_end)
                
                start_end.append((file_name, chunk_start, chunk_end))   # Append the current chunk information (file name, start, end) to the list
                chunk_start = chunk_end                                 # Move to the next chunk

        return (cpu_count, start_end)
    
    def _process_file(self, cpu_count, start_end, delim, column_names, col_types, progress, headers):
        """
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
        """       
        # Tasks to be processed by each CPU core
        tasks = [(file_name, chunk_start, chunk_end, delim, column_names, col_types, progress, headers) for file_name, chunk_start, chunk_end in start_end]
    
        with mp.Pool(cpu_count) as pool:
            chunk_rows = pool.starmap(_process_file_chunk, tasks)
        
        if progress: print("Processing complete, combining chunks...")
        
        # Combine the records from each chunk
        return [record for chunk in chunk_rows for record in chunk]       
        
    # Table Operations
    # ---------------------------------------------------------------------------------------------    
    @log_method_call
    def add_constraint(self, table_name, column, constraint, reference_table_name=None, reference_column=None):
        """
        Adds a constraint to a specified column in a table.
        Args:
            table_name (str): The name of the table to which the constraint will be added.
            column (str): The name of the column to which the constraint will be applied.
            constraint (str): The constraint to be added to the column.
            reference_table_name (str, optional): The name of the table that the foreign key references. Required for foreign key constraints.
            reference_column (str, optional): The column in the reference table that the foreign key references. Required for foreign key constraints.
        Raises:
            ValueError: If the specified table does not exist.
        """
        table = self.get_table(table_name)
        if table:
            reference_table = self.get_table(reference_table_name) if reference_table_name else None
            table.add_constraint(column, constraint, reference_table, reference_column)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def join_tables(self, table_name1, table_name2, on_column, other_column):
        """
        Perform an inner join between two tables on specified columns.
        Args:
            table_name1 (str): The name of the first table.
            table_name2 (str): The name of the second table.
            on_column (str): The column in the first table to join on.
            other_column (str): The column in the second table to join on.
        Returns:
            Table: A new table containing the joined records.
        """
        table1 = self.get_table(table_name1)
        table2 = self.get_table(table_name2)
        if table1 and table2:
            return table1.join(table2, on_column, other_column)
        else:
            raise ValueError("One or both tables do not exist.")

    def aggregate_table(self, table_name, group_column,  agg_column, agg_func):
        """
        Perform an aggregation on a specified column in a table using the provided aggregation function.
        Args:
            table_name (str): The name of the table.
            group_column (str): The column to group by.
            agg_column (str): The column to aggregate.
            agg_func (str): The aggregation function to apply. Supported values are 'MIN', 'MAX', 'COUNT', 'SUM', 'AVG', 'COUNT_DISTINCT'.
        Returns:
            Table: A new table containing the result of the aggregation.
        """
        table = self.get_table(table_name)
        if table:
            return table.aggregate(group_column,  agg_column, agg_func)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def filter_table(self, table_name, condition):
        """
        Filter records in a table based on a condition.
        Args:
            table_name (str): The name of the table.
            condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
                                  Example: lambda record: record.data["product"] == "Laptop"
        Returns:
            Table: A new table containing the filtered records.
        """
        table = self.get_table(table_name)
        if table:
            return table.filter(condition)
        else:
            raise ValueError(f"Table {table_name} does not exist.")
     
    # Stored Procedures Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def add_stored_procedure(self, name, procedure):
        """
        Add a new stored procedure to the database.
        Args:
            name (str): The name of the stored procedure.
            procedure (function): The function representing the stored procedure.
        """
        if name in self.stored_procedures:
            raise ValueError(f"Stored procedure {name} already exists.")
        self.stored_procedures[name] = procedure
    
    @log_method_call
    def execute_stored_procedure(self, name, *args, **kwargs):
        """
        Execute a stored procedure.
        Args:
            name (str): The name of the stored procedure.
            *args: Positional arguments to pass to the stored procedure.
            **kwargs: Keyword arguments to pass to the stored procedure.
        Returns:
            The result of the stored procedure execution.
        """
        if name not in self.stored_procedures:
            raise ValueError(f"Stored procedure {name} does not exist.")
        
        # Execute 'before' triggers
        self.execute_triggers(name, "before", *args, **kwargs)
        
        # Execute the stored procedure
        result = self.stored_procedures[name](self, *args, **kwargs)
        
        # Execute 'after' triggers
        self.execute_triggers(name, "after", *args, **kwargs)
        
        return result

    @log_method_call
    def delete_stored_procedure(self, name):
        """
        Delete a stored procedure from the database.
        Args:
            name (str): The name of the stored procedure.
        """
        if name not in self.stored_procedures:
            raise ValueError(f"Stored procedure {name} does not exist.")
        del self.stored_procedures[name]
        
    def get_stored_procedure(self, name):
        """
        Retrieve a stored procedure by name.
        Args:
            name (str): The name of the stored procedure.
        Returns:
            function: The stored procedure function.
        """
        if name not in self.stored_procedures:
            raise ValueError(f"Stored procedure {name} does not exist.")
        return self.stored_procedures[name]
    
    def _stored_procedure_to_string(self, procedure):
        """
        Return the source code of a stored procedure function as a string.
        Args:
            procedure (function): The stored procedure function.
        Returns:
            str: The source code of the stored procedure function.
        """
        return inspect.getsource(procedure)
    
    # Trigger Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def add_trigger(self, procedure_name, trigger_type, trigger_function):
        """
        Add a trigger for a stored procedure.
        Args:
            procedure_name (str): The name of the stored procedure.
            trigger_type (str): The type of trigger ('before' or 'after').
            trigger_function (function): The function to be executed as the trigger.
        """
        if trigger_type not in ["before", "after"]:
            raise ValueError("Trigger type must be 'before' or 'after'.")
        if procedure_name not in self.stored_procedures:
            raise ValueError(f"Stored procedure {procedure_name} does not exist.")
        if procedure_name not in self.triggers[trigger_type]:
            self.triggers[trigger_type][procedure_name] = []
        self.triggers[trigger_type][procedure_name].append(trigger_function)

    @log_method_call
    def execute_triggers(self, procedure_name, trigger_type, *args, **kwargs):
        """
        Execute triggers for a stored procedure.
        Args:
            procedure_name (str): The name of the stored procedure.
            trigger_type (str): The type of trigger ('before' or 'after').
            *args: Positional arguments to pass to the trigger function.
            **kwargs: Keyword arguments to pass to the trigger function.
        """
        if procedure_name in self.triggers[trigger_type]:
            for trigger in self.triggers[trigger_type][procedure_name]:
                trigger(self, procedure_name, *args, **kwargs)

    
    @log_method_call
    def delete_trigger(self, procedure_name, trigger_type, trigger_function):
        """
        Delete a trigger for a stored procedure.
        Args:
            procedure_name (str): The name of the stored procedure.
            trigger_type (str): The type of trigger ('before' or 'after').
            trigger_function (function): The function to be removed as the trigger.
        """
        if procedure_name in self.triggers[trigger_type]:
            self.triggers[trigger_type][procedure_name].remove(trigger_function)
            if not self.triggers[trigger_type][procedure_name]:
                del self.triggers[trigger_type][procedure_name]
            
    # View Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def create_view(self, view_name, query):
        """
        Create a new view.
        Args:
            view_name (str): The name of the view.
            query (function): A function that returns the data for the view.
        """
        # If the view already exists, raise an error
        if view_name in self.views:
            raise ValueError(f"View {view_name} already exists.")
        
        # If the query function does not have the same name as the view, raise an error
        if query.__name__ != view_name:
            raise ValueError(f"Query function name must match the view name: {view_name}")
        
        self.views[view_name] = View(view_name, query)

    def get_view(self, view_name):
        """
        Retrieve a view by name.
        Args:
            view_name (str): The name of the view.
        Returns:
            View: The view object.
        """
        if view_name not in self.views:
            raise ValueError(f"View {view_name} does not exist.")
        return self.views[view_name]

    @log_method_call
    def delete_view(self, view_name):
        """
        Delete a view by name.
        Args:
            view_name (str): The name of the view.
        """
        if view_name not in self.views:
            raise ValueError(f"View {view_name} does not exist.")
        del self.views[view_name]    
    
    # Materialized View Management
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def create_materialized_view(self, view_name, query):
        """
        Create a new materialized view.
        Args:
            view_name (str): The name of the materialized view.
            query (function): A function that returns the data for the materialized view.
        """
        # If the materialized view already exists, raise an error
        if view_name in self.materialized_views:
            raise ValueError(f"Materialized view {view_name} already exists.")
        
        # If the query function does not have the same name as the view, raise an error
        if query.__name__ != view_name:
            raise ValueError(f"Query function name must match the view name: {view_name}")
        
        self.materialized_views[view_name] = MaterializedView(view_name, query)

    def get_materialized_view(self, view_name):
        """
        Retrieve a materialized view by name.
        Args:
            view_name (str): The name of the materialized view.
        Returns:
            MaterializedView: The materialized view object.
        """
        if view_name not in self.materialized_views:
            raise ValueError(f"Materialized view {view_name} does not exist.")
        return self.materialized_views[view_name]

    @log_method_call
    def refresh_materialized_view(self, view_name):
        """
        Refresh a materialized view by name.
        Args:
            view_name (str): The name of the materialized view.
        """
        if view_name not in self.materialized_views:
            raise ValueError(f"Materialized view {view_name} does not exist.")
        self.materialized_views[view_name].refresh()

    @log_method_call
    def delete_materialized_view(self, view_name):
        """
        Delete a materialized view by name.
        Args:
            view_name (str): The name of the materialized view.
        """
        if view_name not in self.materialized_views:
            raise ValueError(f"Materialized view {view_name} does not exist.")
        del self.materialized_views[view_name]    
        
    # Utility Methods
    # ---------------------------------------------------------------------------------------------
    def show_db_with_curses(self):
        import curses
        curses.wrapper(db_navigator, self)
    
    def print_db(self, index=False, 
                 limit=None, tables=True, 
                 views=False, materialized_views=False,
                 stored_procedures=False, triggers=False
                 ):
        """
        Print the database tables, including their names, columns, constraints, and records.
        Args:
            index (bool, optional): Whether to print the index of each record. Defaults to False.
            limit (int, optional): The maximum number of records to print for each table. Defaults to None.
            tables (bool, optional): Whether to print the tables. Defaults to True.
            views (bool, optional): Whether to print the views. Defaults to False.
            materialized_views (bool, optional): Whether to print the materialized views. Defaults to False.
        """
        # Display database details
        print("DATABASE DETAILS")
        print("-" * 100)
        print(f"Database Name: {self.name}")
        print(f"Database Size (MB): {self.get_db_size() / (1024 * 1024):.4}")
        print(f"Tables: {len(self.tables)}")
        print(f"Authorization Required: {self._is_auth_required()}")
        print(f"Active Session: \"{self.get_username_by_session(self.active_session)}:{self.active_session}\"")
        
        print(f"Database Objects:")
        
        print(f"  --Users: {len(self.tables.get('_users').records)}")
        for record in self.tables.get("_users").records:
            print(f"\t{record.data['username']} | Roles: {record.data['roles']}")
        
        print(f"  --Tables: {len(self.tables)}")
        for table_name in self.tables:
            if table_name != "_users":
                print(f"\t{table_name} | Length: {len(self.tables.get(table_name).records)}")
        
        print(f"  --Materialized Views: {len(self.materialized_views)}")
        for view_name in self.materialized_views:
            print(f"\t{view_name}")
        
        print(f"  --Views: {len(self.views)}")
        for view_name in self.views:
            print(f"\t{view_name}")
        
        print(f"  --Stored Procedures: {len(self.stored_procedures)}")
        for procedure_name in self.stored_procedures:
            if self.stored_procedures[procedure_name].__doc__:
                doc = self.stored_procedures[procedure_name].__doc__.split('\n')[1].strip()
                print(f"\t{procedure_name} | {doc}")
            else:
                print(f"\t{procedure_name}")
        
        print(f"  --Triggers: {len(self.triggers['before']) + len(self.triggers['after'])}")
        for trigger_type in self.triggers:
            for trigger_name in self.triggers[trigger_type]:
                print(f"\t{trigger_name} | {trigger_type.capitalize()} Triggers: {len(self.triggers[trigger_type][trigger_name])}")
        
        print("-" * 100)
        
        
        # Display table details
        if tables:
            print("\n\nTABLE DETAILS")
            print("-" * 100)
            for table_name, table in self.tables.items():
                if table_name == "_users":
                    print(f"Table: {table_name} (User management table)")                
                    print(f"Registered Users: {len(table.records)}")
                
                else:
                    print(f"\nTable: {table_name}")
                    print(f"Records: {len(table.records)}")
                    print(f"Record Types: {table.records[0]._type()}")
                    print(f"Columns: {table.columns}")
                
                consts = []
                for constraint in table.constraints:
                    if len(table.constraints[constraint]) == 1:
                        consts.append(f"{constraint}: {table.constraints[constraint][0].__name__}")
                    
                print(f"Constraints: {consts if consts else 'None'}")
                    
                table.print_table(pretty=True, index=index, limit=limit)
            
        if materialized_views and self.materialized_views:
            # Display materialized view details
            print("\n\nMATERIALIZED VIEW DETAILS")
            print("-" * 100)
            first_view = True
            for view_name, view in self.materialized_views.items():
                if not first_view: print("")
                
                print(f"View: {view_name}")
                print(f"Records: {len(view.data.records)}")
                print(f"Columns: {view.data.columns}")
                view.data.print_table(pretty=True, index=index, limit=limit)
                
                first_view = False
        
        if views and self.views:
            # Display view details
            print("\n\nVIEW DETAILS")
            print("-" * 100)
            first_view = True
            for view_name, view in self.views.items():
                if not first_view: print("")
                
                view = view.get_data()
                
                print(f"View: {view_name}")
                print(f"Records: {len(view.records)}")
                print(f"Columns: {view.columns}")
                view.print_table(pretty=True, index=index, limit=limit)
                
                first_view = False
                
        if stored_procedures and self.stored_procedures:
            # Display stored procedure details
            print("\n\nSTORED PROCEDURE DETAILS")
            print("-" * 100)
            for procedure_name, procedure in self.stored_procedures.items():
                print(f"Procedure: {procedure_name}")
                print(f"Source Code:\n{self._stored_procedure_to_string(procedure)}")
        
        if triggers and (self.triggers['before'] or self.triggers['after']):
            # Display trigger details
            print("\n\nTRIGGER DETAILS")
            print("-" * 100)
            for trigger_type in self.triggers:
                for trigger_name in self.triggers[trigger_type]:
                    print(f"{trigger_type.capitalize()} Trigger: {trigger_name}")
                    for trigger in self.triggers[trigger_type][trigger_name]:
                        print(f"Source Code:\n{self._stored_procedure_to_string(trigger)}")
            
    def copy(self):
        """
        Create a deep copy of the database state.  
        This method uses the `copy` module to create a deep copy of the current
        database instance, ensuring that all nested objects are also copied.
        Returns:
            A new instance of the database with the same state as the original.
        """
        import copy
        return copy.deepcopy(self)

    def restore(self, state):
        """
        Restore the database state from a shadow copy.
        Args:
            state (object): An object containing the state to restore, including tables and name attributes.
        Returns:
            self: The instance of the database with the restored state.
        """
        # Restore the database state from shadow copy
        self.tables = state.tables
        self.name = state.name
        return self

    def get_db_size(self):
        """
        Calculate the size of the database in bytes.
        Returns:
            int: The size of the database in bytes.
        """
        total_size = 0
        for table in self.tables.values():
            for record in table.records:
                total_size += sum(len(str(value)) for value in record.data.values())
        return total_size

    # Example Databases
    # ---------------------------------------------------------------------------------------------
    def _create_orders_table(self, num_records=100):
        """
        Create a sample orders table with random data.
        Args:
            num_records (int): The number of records to generate.
        """
        columns = ["order_id", "user_id", "product", "price", "order_date"]
        self.create_table("orders", columns)
        for i in range(num_records):
            self.tables["orders"].insert({
                "order_id": i + 1,
                "user_id": random.randint(1, num_records),
                "product": random.choice(["Laptop", "Phone", "Tablet", "Smartwatch"]),
                "price": random.randint(500, 2000),
                "order_date": f"{random.randint(2010, 2021)}-{random.randint(1, 12)}-{random.randint(1, 28)}"
            })
    
    def _create_users_table(self, num_records=10):
        """
        Create a sample users table with random data.
        columns = ["user_id", "name", "email"]
            num_records (int): The number of records to generate.
        """
        columns = ["user_id", "name", "email"]
        self.create_table("users", columns)
        fake = Faker()  # Create an instance of Faker
    
        for i in range(num_records):
            self.tables["users"].insert({
                "user_id": i + 1,
                "name": fake.name(),  # Use the instance to generate data
                "email": fake.email()  # Use the instance to generate data
            })
    
    def _create_products_table(self, num_records=50):
        """
        Create a sample products table with random data.
        Args:
            num_records (int): The number of records to generate.
        """
        columns = ["product_id", "name", "category", "price"]
        self.create_table("products", columns)
        fake = Faker()
        for i in range(num_records):
            self.tables["products"].insert({
                "product_id": i + 1,
                "name": fake.word(),
                "category": random.choice(["Electronics", "Clothing", "Home", "Sports"]),
                "price": random.uniform(10.0, 500.0)
            })

    def _create_reviews_table(self, num_records=200):
        """
        Create a sample reviews table with random data.
        Args:
            num_records (int): The number of records to generate.
        """
        columns = ["review_id", "product_id", "user_id", "rating", "comment"]
        self.create_table("reviews", columns)
        fake = Faker()
        for i in range(num_records):
            self.tables["reviews"].insert({
                "review_id": i + 1,
                "product_id": random.randint(1, 50),
                "user_id": random.randint(1, 10),
                "rating": random.randint(1, 5),
                "comment": fake.sentence()
            })

    def _create_categories_table(self, num_records=10):
        """
        Create a sample categories table with random data.
        Args:
            num_records (int): The number of records to generate.
        """
        columns = ["category_id", "name", "description"]
        self.create_table("categories", columns)
        fake = Faker()
        for i in range(num_records):
            self.tables["categories"].insert({
                "category_id": i + 1,
                "name": fake.word(),
                "description": fake.sentence()
            })

    def _create_suppliers_table(self, num_records=20):
        """
        Create a sample suppliers table with random data.
        Args:
            num_records (int): The number of records to generate.
        """
        columns = ["supplier_id", "name", "contact_name", "contact_email"]
        self.create_table("suppliers", columns)
        fake = Faker()
        for i in range(num_records):
            self.tables["suppliers"].insert({
                "supplier_id": i + 1,
                "name": fake.company(),
                "contact_name": fake.name(),
                "contact_email": fake.email()
            })

    @staticmethod
    def load_sample_database(name="SampleDB", n_users=10, n_orders=100, n_products=50, n_reviews=200, n_categories=10, n_suppliers=20, db_logging=False, table_logging=False):
        """
        Create a sample database with predefined tables and data for testing and demonstration purposes.
        Args:
            name (str): The name of the sample database.
        Returns:
            Database: An instance of the Database class populated with sample data.
        """
        # Create a new database
        db = Database(name, db_logging=db_logging, table_logging=table_logging)
        user_manager = db.create_user_manager()
        auth = db.create_authorization()

        # Register users with different roles
        user_manager.register_user("admin", "password123", roles=["admin"])
        user_manager.register_user("user1", "password123", roles=["read_only"])

        # Log in as admin
        admin_session = user_manager.login_user("admin", "password123")

        # Create two tables: users and orders
        db._create_orders_table(num_records=n_orders)
        db._create_users_table(num_records=n_users)

        # Add a unique constraint to the users t;able on the id column
        db.add_constraint("users", "user_id", "UNIQUE")

        # Add a foreign key constraint to the orders table on the user_id column (user_id in orders must exist in users)
        db.add_constraint("orders", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")
        
        # Create tables: products and reviews
        db._create_products_table(num_records=n_products)
        db._create_reviews_table(num_records=n_reviews)

        # Add a foreign key constraint to the reviews table on the product_id column (product_id in reviews must exist in products)
        db.add_constraint("reviews", "product_id", "FOREIGN_KEY", reference_table_name="products", reference_column="product_id")

        # Add a foreign key constraint to the reviews table on the user_id column (user_id in reviews must exist in users)
        db.add_constraint("reviews", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")

        # Create additional tables: categories and suppliers
        db._create_categories_table(num_records=n_categories)
        db._create_suppliers_table(num_records=n_suppliers)

        # Add a foreign key constraint to the products table on the category column (category in products must exist in categories)
        db.add_constraint("products", "category", "FOREIGN_KEY", reference_table_name="categories", reference_column="name")

        # Example usage, create a view of laptops
        # ----------------------------------------------------------------------------------
        def laptop_view():
            return db.filter_table("orders", lambda record: record.data["product"] == "Laptop")

        db.create_view("laptop_view", laptop_view)

        # Example usage, create a view of users with orders
        # ----------------------------------------------------------------------------------
        def users_with_orders_view():
            return db.join_tables("users", "orders", "user_id", "user_id")

        db.create_view("users_with_orders_view", users_with_orders_view)

        # Example usage, create a materialized view of Orders by User 2
        # ----------------------------------------------------------------------------------
        def mv_ordersUser2():
            return db.filter_table("orders", lambda record: record.data["user_id"] == 2)

        db.create_materialized_view("mv_ordersUser2", mv_ordersUser2)

        # Example usage, create a materialized view of Orders by even User IDs
        # ----------------------------------------------------------------------------------
        def mv_ordersUserEven():
            return db.filter_table("orders", lambda record: record.data["user_id"] % 2 == 0)

        db.create_materialized_view("mv_ordersUserEven", mv_ordersUserEven)

        # Example usage, create a materialized view of Orders by odd User IDs
        # ----------------------------------------------------------------------------------
        def mv_ordersUserOdd():
            return db.filter_table("orders", lambda record: record.data["user_id"] % 2 == 1)

        db.create_materialized_view("mv_ordersUserOdd", mv_ordersUserOdd)
        
        # Define a stored procedure to get orders by user
        # ----------------------------------------------------------------------------------
        def get_orders_by_user(db, user_id):
            """
            Retrieve orders for a specific user.
            Args:
                db (Database): The database instance.
                user_id (int): The ID of the user.
            Returns:
                list: A list of orders for the specified user.
            """
            return db.filter_table("orders", lambda record: record.data["user_id"] == user_id)

        # Add the stored procedure to the database
        db.add_stored_procedure("get_orders_by_user", get_orders_by_user)

        # Define a stored procedure to drop users with no orders
        # ----------------------------------------------------------------------------------
        def drop_users_with_no_orders(db):
            """
            Drop users who have no orders.
            Args:
                db (Database): The database instance.
            """
            orders = db.get_table("orders")
            users = db.get_table("users")
            users_to_drop = set()
            for user in users.records:
                user_orders = orders.filter(lambda record: record.data["user_id"] == user.data["user_id"])
                if len(user_orders.records) == 0:
                    users_to_drop.add(user.data["user_id"])
            
            print(f"\nDropping users with no orders:")
            for user_id in users_to_drop:
                users.delete(user_id)
            
            print(f"{len(users_to_drop)} users dropped. IDs: {users_to_drop}")
                
        # Add the stored procedure to the database
        db.add_stored_procedure("drop_users_with_no_orders", drop_users_with_no_orders)

        # Trigger Functions
        # ----------------------------------------------------------------------------------
        # Define a trigger function to log before executing a stored procedure
        def log_before_procedure(db, procedure_name, *args, **kwargs):
            print(f"Log before executing stored procedure: {procedure_name} with args: {args} and kwargs: {kwargs}")

        # Define a trigger function to log after executing a stored procedure
        def log_after_procedure(db, procedure_name, *args, **kwargs):
            print(f"Log after executing stored procedure: {procedure_name} with args: {args} and kwargs: {kwargs}")

        # Add triggers for the stored procedure
        db.add_trigger("get_orders_by_user", "before", log_before_procedure)
        db.add_trigger("get_orders_by_user", "after", log_after_procedure)

        # Add triggers for the stored procedure
        db.add_trigger("drop_users_with_no_orders", "before", log_before_procedure)
        db.add_trigger("drop_users_with_no_orders", "after", log_after_procedure)

        return db
