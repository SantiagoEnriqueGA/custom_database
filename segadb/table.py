# Imports: Standard Library
import logging
import inspect
from typing import Callable, Any
from multiprocessing import Pool, cpu_count
from functools import partial
import math

# Imports: Local
from .record import Record

def log_method_call(func):
    """
    Decorator to log method calls in the Table class.
    Logs the method name, arguments, and return value.
    
    Args:
        func (function): The function to decorate.
    Returns:
        function: The decorated function.
    """
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, 'logger') or not self.logger:
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
                f"Table Log: {self.name} | "
                f"Method Call: {method_name} | " 
                f"Args: {filtered_args}"
            )
            
            # Execute the method
            result = func(self, *args, **kwargs)
            
            # Log successful completion
            self.logger.info(
                f"Table Log: {self.name} | "
                f"Method Complete: {method_name} | "
                f"Status: Success"
            )
            
            return result
            
        except Exception as e:
            # Log any exceptions
            self.logger.error(
                f"Table Log: {self.name} | "
                f"Method Error: {method_name} | "
                f"Args: {filtered_args} | "
                f"Error: {str(e)}"
            )
            raise

    return wrapper

class _ChunkProcessor:
    """
    Helper class to process chunks of records in parallel while maintaining original order.
    """
    @staticmethod
    def process_chunk(args):
        """
        Worker function to process a chunk of records.
        Args:
            args (tuple): Contains (chunk, start_id, start_index, record_type, chunk_number)
        Returns:
            tuple: Contains (chunk_number, list of processed records)
        """
        chunk, start_id, start_index, record_type, chunk_number = args
        new_records = []
        current_id = start_id
        current_index = start_index
        
        for record in chunk:
            # Remove ID if present
            record_data = record.copy()
            if "id" in record_data:
                del record_data["id"]
            
            # Create new record
            new_record = record_type(current_id, record_data)
            new_record.add_to_index(current_index)
            new_records.append(new_record)
            
            current_id += 1
            current_index += 1
            
        return (chunk_number, new_records)

class Table:
    # Initialization and Configuration
    # ---------------------------------------------------------------------------------------------
    def __init__(self, name, columns, logger=None):
        """
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
        """
        self.name = name
        self.columns = columns
        self.records = []
        self.next_id = 1
        self.constraints = {column: [] for column in columns}
        self.index_cnt = 0
        
        # Logging
        if logger:
            self.logger = logger
    
    def _is_valid_constraint_function(self, constraint: Any) -> bool:
        """
        Validates if a constraint is a proper callable function with the correct signature.
        
        Args:
            constraint: The constraint to validate
            
        Returns:
            bool: True if the constraint is valid, False otherwise
        """
        # Check if the constraint is callable
        if not callable(constraint):
            return False
            
        # Get the function signature
        try:
            sig = inspect.signature(constraint)
        except ValueError:
            # Built-in functions might raise ValueError
            return False
            
        # Check if the function takes exactly one parameter
        if len(sig.parameters) != 1:
            return False
            
        # Check if the parameter can accept any value (no strict type annotations)
        param = list(sig.parameters.values())[0]
        if param.annotation != inspect.Parameter.empty:
            if param.annotation not in (Any, type(None)):
                return False
                
        return True

    @log_method_call
    def add_constraint(self, column, constraint, reference_table=None, reference_column=None):
        """
        Adds a constraint to a specified column in the table.
        Args:
            column (str): The name of the column to which the constraint will be added.
            constraint (str): The constraint to be added to the column.
            reference_table (Table, optional): The table that the foreign key references. Required for foreign key constraints.
            reference_column (str, optional): The column in the reference table that the foreign key references. Required for foreign key constraints.
        Raises:
            ValueError: If the specified column does not exist in the table.
        """
        # If the column exists in the table, add the constraint to the column's list of constraints
        if column in self.constraints:
            # For UNIQUE constraints, add a lambda function to check if the value is unique
            if constraint == 'UNIQUE':
                def unique_constraint(value):
                    return all(record.data.get(column) != value for record in self.records)
                unique_constraint.__name__ = "unique_constraint"
                self.constraints[column].append(unique_constraint)
            
            # For FOREIGN_KEY constraints, add a lambda function to check if the value exists in the reference table
            elif constraint == 'FOREIGN_KEY':
                if not reference_table or not reference_column:
                    raise ValueError("Foreign key constraints require a reference table and column.")
                
                def foreign_key_constraint(value):
                    return any(record.data.get(reference_column) == value for record in reference_table.records)
                foreign_key_constraint.__name__ = "foreign_key_constraint"
                foreign_key_constraint.reference_table = reference_table.name
                foreign_key_constraint.reference_column = reference_column
                self.constraints[column].append(foreign_key_constraint)
            
            # For OTHER constraints, add the provided constraint function
            elif not self._is_valid_constraint_function(constraint):
                raise ValueError(
                    "Invalid constraint function. Constraint must be a callable that:"
                    "\n- Takes exactly one parameter"
                    "\n- Has compatible type annotations (if any)"
                )
            else:
                self.constraints[column].append(constraint)

        else:
            raise ValueError(f"Column {column} does not exist in the table.")

    def _check_constraints(self, data):
        """
        Checks the constraints for each column in the provided data.
        Args:
            data (dict): A dictionary where keys are column names and values are the data to be checked.
        Raises:
            ValueError: If any constraint is violated for a column.
        """
        for column, constraints in self.constraints.items():
            for constraint in constraints:
                if not constraint(data.get(column)):
                    print(f"Constraint violation on column {column} for value {data.get(column)}")
                    raise ValueError(f"Constraint violation on column {column} for value {data.get(column)}")

    # CRUD Operations
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def insert(self, data, record_type=Record, transaction=None, flex_ids = False):
        """
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
        """
        self._check_constraints(data)
        record_id = int(data.get("id", self.next_id))
        if any(record.id == record_id for record in self.records):
            if flex_ids:
                record_id = max([record.id for record in self.records]) + 1
            else:
                raise ValueError(f"ID {record_id} is already in use.")
        if "id" in data:
            del data["id"]

        # Check if the data column names match the table column names (minus the id column)
        if record_type == Record:   # Only check for standard records (custom records can have unique column behavior)
            if set(data.keys()) != set(self.columns) - {"id"}:
                raise ValueError(f"Data column names do not match table column names."
                                f"Expected: {self.columns}"
                                f"Received: {data.keys()}")

        record = record_type(record_id, data)
        record.add_to_index(self.index_cnt)
        self.index_cnt += 1
        if transaction:
            transaction.add_operation(lambda: self._insert(record))
        else:
            self._insert(record)
        self.next_id = max(self.next_id, record_id + 1)

    def _insert(self, record):
        self.records.append(record)

    @log_method_call
    def try_insert(self, data, record_type=Record, transaction=None):
        """
        Attempts to insert data into the table. If an error occurs during the insertion,it catches the ValueError and prints an error message.
        Args:
            data (dict): The data to be inserted into the table.
            transaction (optional): The transaction context for the insertion. Default is None.
        Raises:
            ValueError: If there is an issue with the data insertion.
        """
        try:
            self.insert(data, record_type, transaction)
        except ValueError as e:
            print(f"Error on insert: {e}")
            
    @log_method_call
    def bulk_insert(self, record_list, transaction=None):
        """
        Inserts a list of records into the table.
        Args:
            record_list (list): A list of dictionaries where each dictionary represents a record to be inserted.
            transaction (Transaction, optional): An optional transaction object. 
                If provided, the bulk insert operation will be added to the transaction. Defaults to None.
        """
        # Check constraints for all records to ensure consistency
        for record in record_list:
            self._check_constraints(record)
        if transaction:
            transaction.add_operation(lambda: self._bulk_insert(record_list))
        else:
            self._bulk_insert(record_list)
        
    def _bulk_insert(self, record_list):
        new_records = []
        for record in record_list:
            if "id" in record:
                del record["id"]
            record_id = self.next_id
            new_record = Record(record_id, record)
            new_record.add_to_index(self.index_cnt)
            self.index_cnt += 1
            new_records.append(new_record)
            self.next_id += 1
        self.records.extend(new_records)

    @log_method_call
    def parallel_insert(self, record_list, max_workers=None, chunk_size=None, record_type=Record):
        """
        Inserts records in parallel using multiprocessing while maintaining original order.
        
        Args:
            record_list (list): List of dictionaries where each dictionary represents a record to be inserted.
            max_workers (int, optional): Maximum number of worker processes. Defaults to CPU count - 1.
            chunk_size (int, optional): Size of chunks to process in parallel. Defaults to len(record_list)/workers.
            record_type (type, optional): Type of record to create. Defaults to Record.
        
        Returns:
            int: Number of records successfully inserted.
            
        Raises:
            ValueError: If record_list is empty or if there are constraint violations.
        """
        if not record_list:
            raise ValueError("Record list cannot be empty")
        
        # Validate all records to ensure data consistency
        if record_list:
            for record in record_list:
                self._check_constraints(record)
        
        # If less than 5001 records, use the regular insert method
        if len(record_list) <= 5000:
            for record in record_list:
                self.insert(record)
            return len(record_list)
        
        # Configure workers and chunk size
        max_workers = max_workers or max(1, cpu_count() - 1)
        if chunk_size is None:
            chunk_size = math.ceil(len(record_list) / max_workers)
        
        # Split records into chunks
        chunks = [record_list[i:i + chunk_size] 
                 for i in range(0, len(record_list), chunk_size)]
        
        # Calculate starting IDs and indices for each chunk
        start_ids = [(self.next_id + i * chunk_size) - 1 for i in range(len(chunks))]
        start_indices = [self.index_cnt + i * chunk_size for i in range(len(chunks))]
        
        # Prepare arguments for each chunk, including chunk number
        chunk_args = [(chunk, start_id, start_index, record_type, i) 
                     for i, (chunk, start_id, start_index) in enumerate(zip(chunks, start_ids, start_indices))]
        
        # Process chunks in parallel
        with Pool(max_workers) as pool:
            results = pool.map(_ChunkProcessor.process_chunk, chunk_args)
        
        # Sort results by chunk number and flatten
        sorted_results = sorted(results, key=lambda x: x[0])
        all_new_records = []
        for _, chunk_records in sorted_results:
            all_new_records.extend(chunk_records)
        
        # Update table state
        self.records.extend(all_new_records)
        self.next_id = max(r.id for r in all_new_records) + 1
        self.index_cnt += len(all_new_records)
        
        return len(all_new_records)

    def parallel_try_insert(self, record_list, max_workers=None, chunk_size=None, record_type=Record):
        """
        Attempts to insert records in parallel, catching and logging any errors.
        
        Args:
            record_list (list): List of dictionaries where each dictionary represents a record to be inserted.
            max_workers (int, optional): Maximum number of worker processes.
            chunk_size (int, optional): Size of chunks to process in parallel.
            record_type (type, optional): Type of record to create.
        
        Returns:
            int: Number of records successfully inserted.
        """
        try:
            return self.parallel_insert(record_list, max_workers, chunk_size, record_type)
        except ValueError as e:
            print(f"Error on parallel insert: {e}")
            return 0
        
    @log_method_call
    def delete(self, record_id, transaction=None):
        """
        Deletes a record from the database.
        Args:
            record_id (int): The ID of the record to delete.
            transaction (Transaction, optional): An optional transaction object. If provided, the delete operation will be added to the transaction. Defaults to None.
        """
        if transaction:
            transaction.add_operation(lambda: self._delete(record_id))
        else:
            self._delete(record_id)
            
    def _delete(self, record_id):
        """
        Deletes a record from the records list based on the given record ID.
        Args:
            record_id (int): The ID of the record to be deleted.
        """
        record = next((r for r in self.records if r.id == record_id), None)
        if record:
            self.records.remove(record)
            record.remove_from_index(record_id)

    @log_method_call
    def update(self, record_id, data, transaction=None):
        """
        Update a record in the database.
        Args:
            record_id (int): The ID of the record to update.
            data (dict): A dictionary containing the updated data for the record.
            transaction (optional): A transaction object to add the update operation to. If not provided, the update is performed immediately.
        Raises:
            ConstraintError: If the data violates any constraints.
        """
        self._check_constraints(data)
        if transaction:
            transaction.add_operation(lambda: self._update(record_id, data))
        else:
            self._update(record_id, data)

    def _update(self, record_id, data):
        # Record is None if record_id is not found in the table
        record = next((r for r in self.records if r.id == record_id), None)
        if record:
            record.remove_from_index(record.id)
            record.data = data
            record.add_to_index(self.index_cnt)
            self.index_cnt += 1
            
    @log_method_call
    def get_id_by_column(self, column, value):
        """
        Get the ID of the record with the specified value in the specified column.
        Args:
            column (str): The column to search for the value.
            value: The value to search for in the column.
        Returns:
            int: The ID of the record with the specified value in the specified column.
        """
        record = next((r for r in self.records if r.data.get(column) == value), None)
        return record.id if record else None

    @log_method_call
    def select(self, condition):
        """
        Selects records from the table that satisfy the given condition.
        Args:
            condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
        Returns:
            list: A list of records that satisfy the condition.
        """
        return [record for record in self.records if condition(record)]
    
    @log_method_call
    def truncate(self):
        """
        Truncates the table by deleting all records.
        """
        self.records = []
        self.next_id = 1
        self.index_cnt = 0

    # Table Operations
    # ---------------------------------------------------------------------------------------------
    def join(self, other_table, on_column, other_column):
        """
        Perform an inner join with another table on specified columns.
        Args:
            other_table (Table): The table to join with.
            on_column (str): The column in the current table to join on.
            other_column (str): The column in the other table to join on.
        Returns:
            Table: A new table containing the joined records.
        """
        # Create a lookup dictionary for the other table
        other_table_lookup = {}
        for other_record in other_table.records:
            key = other_record.data.get(other_column)
            if key not in other_table_lookup:
                other_table_lookup[key] = []
            other_table_lookup[key].append(other_record.data)
    
        # Perform the join using the lookup dictionary
        joined_records = []
        for record in self.records:
            key = record.data.get(on_column)
            if key in other_table_lookup:
                for other_data in other_table_lookup[key]:
                    joined_record = {**record.data, **other_data}
                    joined_records.append(joined_record)
    
        # Create the resulting table
        joined_columns = list(set(self.columns + other_table.columns))
        joined_table = Table(f"{self.name}_join_{other_table.name}", joined_columns)
    
        joined_table.bulk_insert(joined_records)
    
        return joined_table

    def aggregate(self, group_column, agg_column, agg_func):
        """
        Perform an aggregation on a specified column using the provided aggregation function.
        Args:
            group_column (str): The column to group by.
            agg_column (str): The column to aggregate.
            agg_func (str): The aggregation function to apply. Supported values are 'MIN', 'MAX', 'COUNT', 'SUM', 'AVG', 'COUNT_DISTINCT'.
        Returns:
            Table: A new table containing the result of the aggregation.
        """
        grouped_data = {}
        for record in self.records:
            group_value = record.data.get(group_column)
            agg_value = record.data.get(agg_column)
            if group_value not in grouped_data:
                grouped_data[group_value] = []
            if agg_value is not None:
                grouped_data[group_value].append(agg_value)

        result_data = []
        for group_value, values in grouped_data.items():
            if agg_func == 'MIN':
                result = min(values)
            elif agg_func == 'MAX':
                result = max(values)
            elif agg_func == 'COUNT':
                result = len(values)
            elif agg_func == 'SUM':
                result = sum(values)
            elif agg_func == 'AVG':
                result = sum(values) / len(values) if values else 0
            elif agg_func == 'COUNT_DISTINCT':
                result = len(set(values))
            else:
                raise ValueError(f"Unsupported aggregation function: {agg_func}")
            result_data.append({group_column: group_value, agg_column: result})

        agg_table = Table(f"{self.name}_agg_{group_column}_{agg_column}_{agg_func}", [group_column, agg_column])
        agg_table.bulk_insert(result_data)

        return agg_table

    def filter(self, condition):
        """
        Filter records based on a condition.
        Args:
            condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
        Returns:
            Table: A new table containing the filtered records.
        """
        filtered_records = [record for record in self.records if condition(record)]
        filtered_table = Table(f"{self.name}_filtered", self.columns)
        
        if len(filtered_records) > 0:
            filtered_table.bulk_insert([record.data for record in filtered_records])
            
        else:
            for record in filtered_records:
                filtered_table.insert(record.data)
        return filtered_table
    
    def sort(self, column, ascending=True):
        """
        Sorts the records in the table based on the specified column.
        Args:
            column (str): The column to sort by.
            ascending (bool, optional): If True, sorts in ascending order. Defaults to True.
        Returns:
            Table: A new table containing the sorted records.
        """
        sorted_records = sorted(self.records, key=lambda record: record.data.get(column), reverse=not ascending)
        new_table = Table(f"{self.name}_sorted", self.columns)
        
        if len(sorted_records) > 0:
            new_table.bulk_insert([record.data for record in sorted_records])
        else:
            for record in sorted_records:
                new_table.insert(record.data)
        return new_table
    
    # Utility Methods
    # ---------------------------------------------------------------------------------------------
    def print_table(self, limit=None, pretty=False, index=False):
        """
        Prints the records in the table.
        Args:
            limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
            pretty (bool, optional): If True, prints the table in a pretty format using the tabulate library. Defaults to False.
        """
        if pretty: 
            self._print_table_pretty(limit, index)
            return
        
        count = 0
        for record in self.records:
            if limit is not None and count >= limit:
                break
            print(f"Record ID: {record.id}, Data: {record.data}")
            count += 1
            
    def _print_table_pretty(self, limit=None, index=False, max_data_length=25):
        """
        Prints the records in the table in a pretty format using the tabulate library.
        Args:
            limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
            index (bool, optional): If True, includes the index in the printed table. Defaults to False.
            max_data_length (int, optional): The maximum length of the data to be printed. If None, the full data is printed. Defaults to None.
        """
        from tabulate import tabulate
        table = []
        count = 0
        for record in self.records:
            if limit is not None and count >= limit:
                break
            
            data = record.data    
            if max_data_length is not None:
                data = {k: (str(v)[:max_data_length] + '...' if len(str(v)) > max_data_length else v) for k, v in data.items()}
            
            # Format numerical values to 2 decimal places
            data = {k: (f"{v:.2f}" if isinstance(v, (int, float)) else v) for k, v in data.items()}
            
            if index:
                table.append([record.index.__str__(), record.id, data])
            else:
                table.append([record.id, data])
            count += 1
        
        if index:
            print(tabulate(table, headers=["Index", "ID", "Data"]))
        else:
            print(tabulate(table, headers=["ID", "Data"]))

        if limit is not None and len(self.records) > limit:
            print(f"--Showing first {limit} of {len(self.records)} records.--")
