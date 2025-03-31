# Imports: Standard Library
import logging
import inspect
from typing import Callable, Any, List, Dict, Optional, Set
from multiprocessing import Pool, cpu_count
from functools import partial
import math

# Imports: Local
from .record import Record
from .index import Index

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
                           Note: start_index is now unused here.
        Returns:
            tuple: Contains (chunk_number, list of processed records)
        """
        # Unpack args, note start_index is ignored with _
        chunk, start_id, _, record_type, chunk_number = args
        new_records = []
        current_id = start_id
        # current_index is no longer needed or used here

        # Iterate through the data dictionaries in the chunk
        for record_data in chunk:
            # Remove 'id' if present in the input data dictionary
            if "id" in record_data:
                del record_data["id"]

            # Create new record object using the determined ID and data
            new_record = record_type(current_id, record_data)
            new_records.append(new_record)

            # Increment the ID for the next record in this chunk
            current_id += 1
            # current_index += 1 # This line was removed

        return (chunk_number, new_records)

class Table:
    # Initialization and Configuration
    # ---------------------------------------------------------------------------------------------
    def __init__(self, name: str, columns: List[str], logger: Optional[logging.Logger] = None):
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
            record_map (dict): A map from record ID to Record object for O(1) lookup.
            next_id (int): The ID to be assigned to the next record.
            constraints (dict): Stores validation constraints (like FOREIGN KEY).
            indexes (dict): Stores Index objects for the table, keyed by index name.
            logger (Logger, optional): Logger instance.
        """
        self.name = name
        if not columns:
            raise ValueError("Table must have at least one column.")
        self.columns = columns
        self.records: List[Record] = []
        self.record_map: Dict[int, Record] = {} # Map for faster ID lookups
        self.next_id = 1
        # Column validation constraints (lambdas, FK checks)
        self.constraints: Dict[str, List[Callable]] = {column: [] for column in columns}
        # Indexes for faster lookups (maps column value to record IDs)
        self.indexes: Dict[str, Index] = {} # Key: index_name, Value: Index object

        # Logging
        self.logger = logger
    
    
    # Constraint Management
    # ---------------------------------------------------------------------------------------------
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
        if column not in self.constraints:
             raise ValueError(f"Column {column} does not exist in the table.")
        if constraint == 'UNIQUE':
             # Create a unique index instead of a simple lambda constraint for efficiency
             index_name = f"idx_{self.name}_{column}_unique"
             try:
                 self.create_index(index_name, column, unique=True)
                 if self.logger:
                      self.logger.info(f"Table Log: {self.name} | Unique constraint added via index '{index_name}' on column '{column}'.")
             except ValueError as e:
                  # If index creation fails (e.g., duplicates exist), report it
                  raise ValueError(f"Cannot add UNIQUE constraint on column '{column}': {e}")

        elif constraint == 'FOREIGN_KEY':
            if not reference_table or not reference_column:
                raise ValueError("Foreign key constraints require a reference table and column.")
            # The FK check remains a lambda constraint as it involves another table
            def foreign_key_constraint(value):
                 # TODO: Consider optimizing this check if the reference table has an index on reference_column
                 return any(record.data.get(reference_column) == value for record in reference_table.records)
            foreign_key_constraint.__name__ = "foreign_key_constraint"
            foreign_key_constraint.reference_table_name = reference_table.name # Store names for serialization
            foreign_key_constraint.reference_column = reference_column
            # Add attribute for serialization purposes
            foreign_key_constraint._constraint_type = "FOREIGN_KEY"
            self.constraints[column].append(foreign_key_constraint)
        elif self._is_valid_constraint_function(constraint):
             constraint._constraint_type = "CUSTOM" # Mark for serialization if needed
             self.constraints[column].append(constraint)
        else:
            raise ValueError(
                "Invalid constraint. Use 'UNIQUE', 'FOREIGN_KEY', or a valid callable."
            )

    def _check_constraints(self, data):
        for column, constraints in self.constraints.items():
            # Skip UNIQUE constraints here, they are handled by indexes
            value = data.get(column)
            for constraint in constraints:
                if getattr(constraint, '_constraint_type', 'CUSTOM') != "UNIQUE": # Skip index-based unique check
                     if not constraint(value):
                          c_name = getattr(constraint, "__name__", "custom")
                          raise ValueError(f"Constraint '{c_name}' violation on column '{column}' for value: {value}")
                
    # Index Operations
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def create_index(self, index_name: str, column: str, unique: bool = False):
        """
        Creates a new index on the specified column.

        Args:
            index_name (str): A unique name for the index within the table.
            column (str): The name of the column to index.
            unique (bool): If True, the index will enforce unique values in the column.

        Raises:
            ValueError: If the index name already exists, the column doesn't exist,
                        or if creating a unique index fails due to existing duplicates.
        """
        if index_name in self.indexes:
            raise ValueError(f"Index '{index_name}' already exists on table '{self.name}'.")
        if column not in self.columns:
            raise ValueError(f"Column '{column}' does not exist in table '{self.name}'.")

        # Create the index object
        new_index = Index(name=index_name, column=column, unique=unique)

        # Build the index from existing data
        try:
            for record in self.records:
                key = record.data.get(column)
                new_index.add(key, record.id)
        except ValueError as e:
            # Cleanup partially built index if unique constraint failed during build
            raise ValueError(f"Cannot create unique index '{index_name}': {e}")

        # Store the successfully built index
        self.indexes[index_name] = new_index
        if self.logger:
            self.logger.info(f"Table Log: {self.name} | Index '{index_name}' created on column '{column}' (unique={unique}).")

    @log_method_call
    def drop_index(self, index_name: str):
        """
        Removes an index from the table.

        Args:
            index_name (str): The name of the index to remove.

        Raises:
            ValueError: If the index name does not exist.
        """
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist on table '{self.name}'.")

        del self.indexes[index_name]
        if self.logger:
            self.logger.info(f"Table Log: {self.name} | Index '{index_name}' dropped.")

    def get_index(self, index_name: str) -> Optional[Index]:
        """Retrieve an index object by its name."""
        return self.indexes.get(index_name)

    def _update_indexes_add(self, record: Record):
        """Helper to add a new record to all relevant indexes."""
        for index in self.indexes.values():
            if index.column in record.data:
                key = record.data.get(index.column)
                # Add to index (this will raise ValueError on unique violation)
                index.add(key, record.id)

    def _update_indexes_remove(self, record: Record):
        """Helper to remove an existing record from all relevant indexes."""
        for index in self.indexes.values():
            if index.column in record.data:
                key = record.data.get(index.column)
                index.remove(key, record.id)

    def _update_indexes_update(self, record: Record, old_data: Dict[str, Any], new_data: Dict[str, Any]):
        """
        Helper to update a record in all relevant indexes.
        Checks for unique constraints *before* applying changes.
        """
        record_id = record.id
        potential_new_keys: Dict[str, Any] = {} # Store potential new index keys

        # 1. Check for potential unique violations with new data *before* changing anything
        for index in self.indexes.values():
            if index.column in new_data and index.column in old_data:
                old_key = old_data.get(index.column)
                new_key = new_data.get(index.column)
                potential_new_keys[index.name] = new_key # Store for later update

                if old_key != new_key and index.unique:
                     # Check if the new key would cause a violation
                     existing_ids = index.find(new_key)
                     if existing_ids and (len(existing_ids) > 1 or existing_ids[0] != record_id):
                          raise ValueError(f"Unique constraint violation in index '{index.name}' on column '{index.column}' for value: {new_key}")

        # 2. If all checks pass, update the indexes
        for index in self.indexes.values():
            if index.column in new_data and index.column in old_data:
                 old_key = old_data.get(index.column)
                 # Use the potential new key we stored earlier
                 new_key = potential_new_keys.get(index.name)
                 if old_key != new_key:
                      index.remove(old_key, record_id)
                      index.add(new_key, record_id) # Already checked uniqueness

    # CRUD Operations
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def insert(self, data: Dict[str, Any], record_type: type = Record, transaction: Optional[Any] = None, flex_ids: bool = False):
        """
        Inserts a new record into the table, updating indexes.

        Args:
            data (dict): The data for the new record. Can include 'id'.
            record_type (type): The class of the record (e.g., Record, ImageRecord).
            transaction: Optional transaction object.
            flex_ids (bool): If True and provided ID conflicts, assign next available ID.

        Raises:
            ValueError: If constraints or unique index constraints are violated,
                        if ID conflicts and flex_ids is False, or if data keys mismatch columns.
        """
        # 1. Determine Record ID
        provided_id = data.get("id")
        record_id = int(provided_id) if provided_id is not None else self.next_id

        if record_id in self.record_map:
            if flex_ids:
                record_id = self.next_id # Use the next available ID
            else:
                raise ValueError(f"ID {record_id} is already in use in table '{self.name}'.")

        # Prepare data for constraint check and schema check (remove id if present)
        check_data = data.copy()
        if "id" in check_data: del check_data["id"]

        # --- Start: Reintroduced Schema Check ---
        if record_type == Record: # Only check schema for standard Record types
            table_columns_set = set(self.columns)
            if 'id' in table_columns_set: table_columns_set.remove('id') # Don't require 'id' in input data
            data_columns_set = set(check_data.keys())

            if data_columns_set != table_columns_set:
                missing = table_columns_set - data_columns_set
                extra = data_columns_set - table_columns_set
                error_msg = f"Data columns mismatch table schema for table '{self.name}'."
                if missing: error_msg += f" Missing: {missing}."
                if extra: error_msg += f" Extra: {extra}."
                raise ValueError(error_msg)
        # --- End: Reintroduced Schema Check ---

        # 2. Check Column Constraints (FK, custom lambdas)
        self._check_constraints(check_data) # Use check_data (which has id removed)

        # 3. Create Record Object
        record = record_type(record_id, check_data) # Use check_data

        # 4. Check and Update Indexes (Handles Unique Constraint via Index.add)
        try:
            self._update_indexes_add(record)
        except ValueError as e:
            # If index update fails (unique constraint), re-raise
            raise ValueError(f"Index constraint violation during insert: {e}")

        # 5. Add to Table (if all checks passed)
        if transaction:
             # ... (transaction logic remains complex, assumes lambda handles record+index) ...
             original_id = record.id # Capture id in case it changes with flex_ids
             # Define the operation to be added
             def insert_op():
                  # Re-determine ID and create record inside the lambda if needed,
                  # or assume the pre-created record is sufficient.
                  # This current implementation uses the pre-created record, which might
                  # have issues if next_id changed between adding op and commit.
                  # A safer way involves passing data and type to the lambda.
                  self._perform_insert(record) # Use the record created outside
                  # Attempt index update again inside transaction execution? Risky.
                  # Or rely on the pre-check being sufficient.

             transaction.add_operation(insert_op)

             # Alternative safer lambda:
             # transaction.add_operation(lambda data_copy=data.copy(), rt=record_type, f_ids=flex_ids: self.insert(data_copy, rt, None, f_ids))

        else:
             self._perform_insert(record) # Directly perform the insert and update maps/counters


    def _perform_insert(self, record: Record):
         """Internal method to actually add the record and update state."""
         self.records.append(record)
         self.record_map[record.id] = record
         self.next_id = max(self.next_id, record.id + 1)


    # _insert is now _perform_insert, keeping _insert as the public transactional entry if needed
    def _insert(self, record): # Might be called by old transaction code? Keep for compatibility or refactor transactions.
        self._perform_insert(record)
        # Manually update indexes if called directly (e.g., by old transaction code)
        # This is risky as uniqueness checks might not have happened.
        try:
             self._update_indexes_add(record)
        except ValueError as e:
             # If index update fails here, the record is already in self.records - needs rollback logic!
             self.records.remove(record)
             del self.record_map[record.id]
             # Don't decrement next_id, potential gaps are okay.
             print(f"CRITICAL: Index error after direct _insert for ID {record.id}. Attempted rollback. Error: {e}")
             raise e # Re-raise the critical error

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
    def delete(self, record_id, transaction=None):
        """
        Deletes a record from the database.
        Args:
            record_id (int): The ID of the record to delete.
            transaction (Transaction, optional): An optional transaction object. If provided, the delete operation will be added to the transaction. Defaults to None.
        """
        if transaction:
            # Similar transaction complexity as insert regarding indexes.
            # Lambda needs to capture state or perform both record and index removal.
            transaction.add_operation(lambda: self._perform_delete(record_id))
        else:
            self._perform_delete(record_id)
            
    def _perform_delete(self, record_id: int):
        """Internal method to perform deletion and update state."""
        record = self.record_map.get(record_id)
        if record:
            # 1. Remove from indexes *first*
            self._update_indexes_remove(record)

            # 2. Remove from record list and map
            # Using pop from map and remove from list
            try:
                 self.records.remove(record) # O(n) - could be slow for large tables
                 del self.record_map[record_id] # O(1)
            except ValueError:
                 # Should not happen if record_map is consistent with records
                 if self.logger:
                      self.logger.warning(f"Table Log: {self.name} | Record ID {record_id} found in map but not in list during delete.")
                 # Ensure it's removed from map anyway
                 if record_id in self.record_map: del self.record_map[record_id]

        else:
            # Record ID not found
             if self.logger:
                  self.logger.warning(f"Table Log: {self.name} | Attempted to delete non-existent record ID: {record_id}")
             # Optionally raise ValueError("Record ID not found")


    # Keep _delete for potential transaction compatibility or refactor transactions
    def _delete(self, record_id):
         self._perform_delete(record_id)

    @log_method_call
    def update(self, record_id: int, data: Dict[str, Any], transaction: Optional[Any] = None):
        """
        Updates a record by ID, checking constraints and updating indexes.

        Args:
            record_id (int): The ID of the record to update.
            data (dict): Dictionary of columns and new values to update.
            transaction: Optional transaction object.

        Raises:
            ValueError: If record ID not found, or constraints/unique indexes are violated.
        """
        record = self.record_map.get(record_id)
        if not record:
            raise ValueError(f"Record with ID {record_id} not found in table '{self.name}'.")

        # Create combined data for full constraint check
        old_data = record.data.copy()
        updated_data = old_data.copy()
        updated_data.update(data) # Apply changes from input 'data'

        # 1. Check Column Constraints (FK, custom) on the potentially *fully updated* data
        self._check_constraints(updated_data)

        # 2. Check and Update Indexes (handles unique constraints)
        try:
             # Pass the original record, its old data, and the proposed *changes* (data dict)
             # The helper function needs the full old data to find the old keys
             self._update_indexes_update(record, old_data, data)
        except ValueError as e:
            # Index constraint violation (e.g., unique)
             raise ValueError(f"Index constraint violation during update: {e}")

        # 3. Apply Update to Record Object (if all checks passed)
        if transaction:
             # Transaction complexity applies here too.
             transaction.add_operation(lambda: self._perform_update(record_id, data))
        else:
             self._perform_update(record_id, data) # Directly update

    def _perform_update(self, record_id: int, data: Dict[str, Any]):
         """Internal method to apply updates to the record object."""
         record = self.record_map.get(record_id)
         if record: # Should exist after initial check in update()
             record.data.update(data) # Update the record's data dictionary
         else:
             # This indicates a logic error if reached
              if self.logger:
                   self.logger.error(f"Table Log: {self.name} | CRITICAL: Record ID {record_id} disappeared during update.")


    # Keep _update for potential transaction compatibility or refactor transactions
    def _update(self, record_id, data):
         # This direct call is risky without prior index checks/updates handled in the main `update` method.
         record = self.record_map.get(record_id)
         if record:
             old_data = record.data.copy()
             # Manually handle index update if called directly
             try:
                  self._update_indexes_update(record, old_data, data)
                  # Apply update *after* index success
                  record.data.update(data)
             except ValueError as e:
                   print(f"CRITICAL: Index error during direct _update for ID {record_id}. Update aborted. Error: {e}")
                   # Do NOT update record.data if index update failed
                   raise e # Re-raise critical error
             
    # Bulk/Parallel CRUD Operations
    # --------------------------------------------------------------------------------------------
    @log_method_call
    def parallel_insert(self, record_list: List[Dict[str, Any]], max_workers: Optional[int] = None, chunk_size: Optional[int] = None, record_type: type = Record):
        """
        Inserts records in parallel using multiprocessing. Builds indexes afterwards.

        Args:
            record_list: List of record data dictionaries.
            max_workers: Max worker processes. Defaults to CPU count - 1.
            chunk_size: Size of chunks. Defaults based on workers/list size.
            record_type: Type of record object to create.

        Returns:
            Number of records inserted.

        Raises:
            ValueError: If record_list is empty or constraints are violated during pre-check.
        """
        if not record_list:
             raise ValueError("Record list cannot be empty")

        # 1. Pre-check constraints (excluding unique index checks for now)
        #    This is a basic check; full unique checks happen during index build.
        if record_list:
             for record_data in record_list:
                  try:
                       self._check_constraints(record_data)
                  except ValueError as e:
                       raise ValueError(f"Constraint violation in input data (record {record_data}): {e}")

        # If small list, use sequential insert (handles indexes correctly)
        if len(record_list) <= 5000: # Keep threshold or adjust
            count = 0
            for record_data in record_list:
                try:
                    self.insert(record_data, record_type=record_type, flex_ids=True) # Use flex_ids for simplicity in bulk
                    count += 1
                except ValueError as e:
                    if self.logger:
                        self.logger.warning(f"Table Log: {self.name} | Skipping record during small bulk insert due to error: {e}. Record: {record_data}")
            return count

        # Configure workers and chunk size
        max_workers = max_workers or max(1, cpu_count() - 1)
        chunk_size = chunk_size or math.ceil(len(record_list) / max_workers)

        # Split records into chunks
        chunks = [record_list[i:i + chunk_size] for i in range(0, len(record_list), chunk_size)]

        # Calculate starting IDs
        start_ids = [self.next_id + i * chunk_size for i in range(len(chunks))]
        # Parallel processing doesn't need start_indices anymore

        # Prepare arguments
        chunk_args = [(chunk, start_id, 0, record_type, i) # index is dummy here
                     for i, (chunk, start_id) in enumerate(zip(chunks, start_ids))]

        # Process chunks in parallel
        with Pool(max_workers) as pool:
            results = pool.map(_ChunkProcessor.process_chunk, chunk_args)

        # Sort results by chunk number and flatten
        sorted_results = sorted(results, key=lambda x: x[0])
        all_new_records: List[Record] = []
        for _, chunk_records in sorted_results:
            all_new_records.extend(chunk_records)

        # Update table state (records and next_id)
        self.records.extend(all_new_records)
        for r in all_new_records: self.record_map[r.id] = r # Update map
        self.next_id = max(r.id for r in all_new_records) + 1 if all_new_records else self.next_id
        # self.index_cnt is deprecated with record_map

        # 2. Rebuild/Update Indexes *after* parallel insertion
        if self.indexes:
             if self.logger: self.logger.info(f"Table Log: {self.name} | Rebuilding indexes after parallel insert...")
             for index in self.indexes.values():
                  index.clear() # Clear existing index data
                  try:
                       for record in self.records: # Iterate through *all* records now
                            key = record.data.get(index.column)
                            index.add(key, record.id)
                  except ValueError as e:
                       # If unique constraint fails during rebuild, the insert was partially inconsistent.
                       # This is a limitation of this parallel approach. Log error.
                       if self.logger: self.logger.error(f"Table Log: {self.name} | CRITICAL: Unique constraint violation while rebuilding index '{index.name}' after parallel insert. Index may be incomplete. Error: {e}")
                       # Optionally raise error, or just warn and continue with potentially broken index.
                       # raise ValueError(f"Unique constraint failed during index rebuild: {e}")
             if self.logger: self.logger.info(f"Table Log: {self.name} | Index rebuild complete.")


        return len(all_new_records)

    @log_method_call
    def try_insert(self, data, record_type=Record, transaction=None):
        try:
            self.insert(data, record_type, transaction)
        except ValueError as e:
            print(f"Error on insert: {e}")

    @log_method_call
    def bulk_insert(self, record_list, transaction=None):
        # Simple sequential bulk insert calling the updated `insert`
        count = 0
        operation_list = []

        for record_data in record_list:
            # Pre-check constraints (basic)
            try:
                self._check_constraints(record_data)
                # If using transaction, add insert op to list
                if transaction:
                     # Create a closure to capture the current record_data
                     def create_insert_op(data_to_insert):
                          return lambda: self.insert(data_to_insert, flex_ids=True)
                     operation_list.append(create_insert_op(record_data.copy()))
                else:
                     # Perform insert directly
                     self.insert(record_data, flex_ids=True)
                     count += 1
            except ValueError as e:
                if self.logger:
                    self.logger.warning(f"Table Log: {self.name} | Skipping record during bulk insert due to error: {e}. Record: {record_data}")

        if transaction:
             # Add all collected operations to the transaction
             def bulk_op_executor(ops):
                  for op in ops: op()
             transaction.add_operation(partial(bulk_op_executor, operation_list))
             return len(operation_list) # Return number of potential inserts
        else:
            return count # Return number of actual inserts


    def parallel_try_insert(self, record_list, max_workers=None, chunk_size=None, record_type=Record):
        try:
            return self.parallel_insert(record_list, max_workers, chunk_size, record_type)
        except ValueError as e:
            print(f"Error on parallel insert: {e}")
            return 0
        

    # Read/Query Operations
    # ---------------------------------------------------------------------------------------------
    @log_method_call
    def get_record_by_id(self, record_id: int) -> Optional[Record]:
        """Efficiently retrieves a record by its ID using the record map."""
        return self.record_map.get(record_id)
    
    @log_method_call
    def get_id_by_column(self, column: str, value: Any) -> Optional[int]:
        """
        Get the ID of the first record matching the value in the specified column.
        Uses index if available for the column.
        """
        # Check if an index exists for this column
        index = next((idx for idx in self.indexes.values() if idx.column == column), None)

        if index:
             # Use the index to find potential IDs
             record_ids = index.find(value)
             return record_ids[0] if record_ids else None
        else:
             # Fallback to linear scan if no index
             if self.logger: self.logger.debug(f"Table Log: {self.name} | No index found for column '{column}', performing linear scan for get_id_by_column.")
             for record in self.records:
                  if record.data.get(column) == value:
                       return record.id
             return None

    @log_method_call
    def select(self, condition: Callable[[Record], bool]) -> List[Record]:
        """
        Selects records from the table that satisfy the given condition.
        (Currently performs a full table scan).

        Args:
            condition (function): A function taking a Record object, returning True/False.

        Returns:
            List[Record]: A list of records satisfying the condition.
        """
        # Basic implementation - full scan
        # TODO: Query Planning - Add logic to detect if condition uses an index.
        # Example: If condition is `lambda r: r.data['email'] == 'test@example.com'`
        # and an index exists on 'email', use the index first.
        return [record for record in self.records if condition(record)]
    
    @log_method_call
    def filter(self, condition: Callable[[Record], bool]) -> 'Table':
        """
        Filter records based on a condition. Returns a *new* Table.
        (Currently performs a full table scan).

        Args:
            condition (function): A function taking a Record object, returning True/False.

        Returns:
            Table: A new table containing the filtered records.
        """
        # TODO: Query Planning - Use index if condition allows.
        filtered_records_data = [record.data for record in self.records if condition(record)]

        # Create a new table (without constraints or indexes from the original)
        filtered_table = Table(f"{self.name}_filtered", self.columns)
        if filtered_records_data:
            # Use bulk insert for efficiency, assuming no transactions needed here
            filtered_table.bulk_insert(filtered_records_data)

        return filtered_table

    @log_method_call
    def truncate(self):
        """Truncates the table, clearing records, map, and indexes."""
        self.records = []
        self.record_map = {}
        self.next_id = 1
        for index in self.indexes.values():
            index.clear()
        if self.logger: self.logger.info(f"Table Log: {self.name} | Table truncated.")

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
    
    def sort(self, column, ascending=True):
        """
        Sorts the records in the table based on the specified column.
        Args:
            column (str): The column to sort by.
            ascending (bool, optional): If True, sorts in ascending order. Defaults to True.
        Returns:
            Table: A new table containing the sorted records.
        """
        if column not in self.columns: raise ValueError(f"Column '{column}' not found.")
        # Handle potential None values during sort
        sorted_records = sorted(
            self.records,
            key=lambda record: record.data.get(column) if record.data.get(column) is not None else (float('-inf') if ascending else float('inf')),
            reverse=not ascending
        )
        new_table = Table(f"{self.name}_sorted", self.columns)
        if sorted_records: new_table.bulk_insert([record.data for record in sorted_records])
        return new_table
    
    # Utility Methods
    # ---------------------------------------------------------------------------------------------
    def print_table(self, limit=None, pretty=False):
        """
        Prints the records in the table.
        Args:
            limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
            pretty (bool, optional): If True, prints the table in a pretty format using the tabulate library. Defaults to False.
        """
        if pretty: 
            self._print_table_pretty(limit)
            return
        
        count = 0
        for record in self.records:
            if limit is not None and count >= limit:
                break
            print(f"Record ID: {record.id}, Data: {record.data}")
            count += 1
            
    def _print_table_pretty(self, limit=None, max_data_length=25):
        """
        Prints the records in the table in a pretty format using the tabulate library.
        Args:
            limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
            index (bool, optional): If True, includes the index in the printed table. Defaults to False.
            max_data_length (int, optional): The maximum length of the data to be printed. If None, the full data is printed. Defaults to None.
        """
        from tabulate import tabulate
        # Headers now only include ID and the actual columns
        headers = ["ID"] + self.columns
        table_data = []
        count = 0

        for record in self.records:
            if limit is not None and count >= limit:
                break

            row_data = [record.id] # Start row with just the ID
            for col in self.columns:
                 value = record.data.get(col)
                 # Truncate long strings
                 str_value = str(value)
                 if max_data_length is not None and len(str_value) > max_data_length:
                      display_value = str_value[:max_data_length] + '...'
                 else:
                      display_value = str_value
                 row_data.append(display_value)

            table_data.append(row_data)
            count += 1

        # Print using the updated headers
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        if limit is not None and len(self.records) > limit:
            print(f"--Showing first {limit} of {len(self.records)} records.--")
