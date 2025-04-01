# Imports: Standard Library
import textwrap
import re
import os
import types
from typing import List, Dict, Any, Union, Tuple, Optional
from math import inf
import json
import zlib
import multiprocessing as mp
from datetime import datetime
import logging

# Imports: Local
from .crypto import CustomFernet
from .database import Database
from .record import Record, VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord, EncryptedRecord
from .index import Index
from .table import Table

def _process_chunk(records_chunk, table):
    """
    Process a chunk of the file. This function is used in the multiprocessing pool.
    Args:
        records_chunk (list): A list of records to be processed.
        table (Table): The table object to which the records will be added.
    Returns:
        list: A list of Record objects created from the records_chunk.
    """
    record_objects = []
    for record in records_chunk:
        record_data = {k: (v.encode() if isinstance(v, str) and k == "password_hash" else v) for k, v in record["data"].items()}
        
        r = Record(record["id"], record_data)
        idx = Index()
        idx.index = record["index"]
        r._index = idx
        record_objects.append(r)
    return record_objects

class Storage:
    """A utility class for saving, loading, and deleting database files."""
    
    # Encryption and Decryption
    # ---------------------------------------------------------------------------------------------
    @staticmethod
    def generate_key():
        """Generate a key for encryption."""
        return CustomFernet.generate_key()

    @staticmethod
    def encrypt(data, key):
        """Encrypt the data using the provided key."""
        fernet = CustomFernet(key)
        return fernet.encrypt(data)

    @staticmethod
    def decrypt(data, key):
        """Decrypt the data using the provided key."""
        fernet = CustomFernet(key)
        return fernet.decrypt(data)
    
    # Database Operations
    # ---------------------------------------------------------------------------------------------
    @staticmethod
    def backup(db, key=None, compress=False, dir=None, date=False):
        """
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
        """
        # Create the directory if it does not exist
        if dir is None: dir = os.path.join(os.getcwd(), "backups_" + db.name)
        if not os.path.exists(dir): os.makedirs(dir)
        
        # Generate the filename
        files = os.listdir(dir)
        n = len([f for f in files if db.name in f])
        filename = f"{db.name}_backup_{n}.segadb"
        
        if date:
            date_str = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{db.name}_backup_{n}_{date_str}.segadb"
        
        filepath = os.path.join(dir, filename)
        
        # Save the database
        Storage.save(db, filepath, key=key, compress=compress)
        
        return f"Backup saved to: {filepath}"
    
    @staticmethod
    def list_backups(dir=None, print_output=True):
        """
        List the backup files in the specified directory.
        Args:
            dir (str, optional): The directory to search for backup files. Default is None.
        Returns:
            list: A list of the backup files in the directory.
        """
        if dir is None: dir = os.path.join(os.getcwd(), "backups")
        files = os.listdir(dir)
        backups = [f for f in files if f.endswith(".segadb")]
        
        # Sort backups by the number in descending order
        # backups.sort(key=lambda x: int(x.split('_')[2]), reverse=True)
        backups.sort(key=lambda x: int(x.split('_')[2].split('.')[0]), reverse=True)
        
        if print_output:
            print("\n".join(backups))
        
        return backups
            
    @staticmethod
    def restore(db, key=None, compress=False, user=None, password=None, dir=None, backup_name=None):
        """
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
        """
        if dir is None: dir = os.path.join(os.getcwd(), "backups_" + db.name)
        
        if backup_name is None:
            backup_name = Storage.list_backups(dir, print_output=False)[0]
        else:
            backups = Storage.list_backups(dir, print_output=False)
            matching_backups = [f for f in backups if backup_name in f]
            if not matching_backups:
                raise FileNotFoundError(f"No backup file found containing '{backup_name}'")
            
            if len(matching_backups) > 1:
                print(f"--Multiple backup files found containing '{backup_name}'. Using the first match: {matching_backups[0]}")
            
            backup_name = matching_backups[0]
        
        filepath = os.path.join(dir, backup_name)
        
        return Storage.load(filepath, key=key, user=user, password=password, compression=compress)
    
    @staticmethod
    def save(db: Database, filename: str, key: Optional[str] = None, compress: bool = False):
        """
        Saves the database object to a file, including index definitions.
        """
        data = {
            "name": db.name,
            "tables": {},
            "views": {},
            "materialized_views": {},
            "stored_procedures": {},
            "triggers": {"before": {}, "after": {}}
        }

        def serialize_table(table: Table) -> Dict[str, Any]:
            # Serialize constraints (FK needs special handling for table name)
            serializable_constraints = {}
            for column, constraints_list in table.constraints.items():
                 serializable_constraints[column] = []
                 for constraint_func in constraints_list:
                      constraint_info = {"name": constraint_func.__name__}
                      # Add details needed for reloading specific constraints
                      if getattr(constraint_func, "_constraint_type", "CUSTOM") == "FOREIGN_KEY":
                           constraint_info["reference_table"] = getattr(constraint_func, "reference_table_name", None)
                           constraint_info["reference_column"] = getattr(constraint_func, "reference_column", None)
                           constraint_info["type"] = "FOREIGN_KEY" # Explicitly add type
                      elif getattr(constraint_func, "_constraint_type", "CUSTOM") == "UNIQUE":
                            # UNIQUE constraints are handled by indexes now, don't save them here.
                           continue # Skip saving UNIQUE lambda constraint
                      else: # Custom constraints are harder to serialize/deserialize safely
                            # Omit custom lambdas for now, or add unsafe serialization if needed
                            # print(f"Warning: Skipping serialization of custom constraint '{constraint_info['name']}' on {table.name}.{column}")
                            continue # Skip custom constraints
                      serializable_constraints[column].append(constraint_info)

            return {
                "name": table.name,
                "columns": table.columns,
                "records": [{
                    "id": record.id,
                    "type": record._type(),
                    # Handle specific record types like ImageRecord
                    "data": record.to_dict() if hasattr(record, 'to_dict') else {
                        k: (v.decode() if isinstance(v, bytes) else v)
                        for k, v in record.data.items()
                    },
                    # No longer saving record._index
                } for record in table.records],
                "next_id": table.next_id,
                "constraints": serializable_constraints, # Save processed constraints
                 # Save index definitions
                "indexes": [index.to_dict_definition() for index in table.indexes.values()],
            }

        # Serialize tables
        for table_name, table in db.tables.items():
            data["tables"][table_name] = serialize_table(table)

        # Serialize views, MVs, SPs, Triggers (unchanged from previous version)
        for view_name, view in db.views.items():
             data["views"][view_name] = {"name": view.name, "query": view._query_to_string()}
        for mv_name, mv in db.materialized_views.items():
             data["materialized_views"][mv_name] = {"name": mv.name, "query": mv._query_to_string()}
        for sp_name, sp in db.stored_procedures.items():
             query = db._stored_procedure_to_string(sp)
             data["stored_procedures"][sp_name] = {"name": sp_name, "query": query}
        for trigger_type in db.triggers:
            for proc_name, triggers_source in db.triggers_source[trigger_type].items():
                data["triggers"][trigger_type][proc_name] = [triggers_source]

        # Convert to JSON, compress/encrypt, and write
        try:
            json_data_str = json.dumps(data, indent=4)
            payload = json_data_str.encode('utf-8') # Start with bytes

            if compress:
                payload = zlib.compress(payload)

            if key:
                payload = Storage.encrypt(payload.decode('utf-8'), key).encode('utf-8') # Encrypt expects string

            mode = 'wb' # Always write in binary mode now
            with open(filename, mode) as f:
                f.write(payload)

        except TypeError as e:
             print(f"Error serializing database '{db.name}' to JSON: {e}")
             print("Check for non-serializable data types in records or constraints.")
             raise
        except Exception as e:
             print(f"Error saving database to {filename}: {e}")
             raise

    @staticmethod
    def load(filename: str, key: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, compression: bool = False, parrallel: bool = False) -> Database:
        """
        Loads a database from a file, including rebuilding indexes.
        """
        # Parallel loading might need adjustments to handle index rebuilding correctly.
        # Sticking to sequential loading for index simplicity first.
        # if parrallel:
        #     return Storage._load_mp(filename, key, user, password, compression)

        try:
            mode = 'rb' # Always read in binary mode
            with open(filename, mode) as f:
                payload = f.read()

                if key:
                    # Decrypt needs string, assumes payload is utf-8 encoded encrypted string
                    payload = Storage.decrypt(payload.decode('utf-8'), key).encode('utf-8')

                if compression:
                    payload = zlib.decompress(payload)

                # Decode final payload to string for json.loads
                json_data_str = payload.decode('utf-8')
                data = json.loads(json_data_str)

        except FileNotFoundError:
            print(f"Error: Database file not found: {filename}")
            raise
        except (json.JSONDecodeError, zlib.error, TypeError, ValueError) as e:
             # Catch broad range of errors during load/decode/decrypt/decompress
             print(f"Error loading/decoding database file {filename}: {e}")
             print("Check file integrity, compression settings, and encryption key.")
             raise
        except Exception as e:
            print(f"Unexpected error loading database file {filename}: {e}")
            raise


        db = Database(data["name"])

        # Pre-create all tables to resolve FK dependencies during constraint loading
        for table_name, table_data in data["tables"].items():
             if not db.get_table(table_name):
                  db.create_table(table_name, table_data["columns"])

        # Load records into tables
        for table_name, table_data in data["tables"].items():
            table = db.get_table(table_name)
            if not table: continue # Should exist from pre-creation pass

            table.next_id = table_data.get("next_id", 1) # Use get with default

            record_objects_to_insert = []
            record_type_map = { # Map string names to classes
                 "Record": Record,
                 "VectorRecord": VectorRecord,
                 "TimeSeriesRecord": TimeSeriesRecord,
                 "ImageRecord": ImageRecord,
                 "TextRecord": TextRecord,
                 "EncryptedRecord": EncryptedRecord,
            }

            for record_info in table_data.get("records", []):
                record_id = record_info["id"]
                record_type_name = record_info.get("type", "Record")
                record_class = record_type_map.get(record_type_name, Record)
                record_data = record_info["data"]

                # Handle potential byte encoding for specific fields like password_hash
                processed_data = {
                    k: (v.encode('utf-8') if isinstance(v, str) and k == "password_hash" else v)
                    for k, v in record_data.items()
                }

                 # Special handling for EncryptedRecord (key not saved)
                if record_class == EncryptedRecord:
                     processed_data["key"] = None # Indicate data is already encrypted

                try:
                    # Create record instance, but don't add to table yet
                    record_obj = record_class(record_id, processed_data)
                    record_objects_to_insert.append(record_obj)
                except Exception as e:
                    print(f"Warning: Error creating record object ID {record_id} for table '{table_name}'. Skipping. Error: {e}")

            # Add records to the table structure (map and list) *without* running full insert logic yet
            for record_obj in record_objects_to_insert:
                 if record_obj.id not in table.record_map: # Avoid duplicates if load is re-run
                      table.records.append(record_obj)
                      table.record_map[record_obj.id] = record_obj
                 else:
                      print(f"Warning: Duplicate record ID {record_obj.id} encountered during load for table '{table_name}'. Skipping duplicate.")


        # Load constraints AFTER records are loaded but BEFORE indexes are built
        for table_name, table_data in data["tables"].items():
            table = db.get_table(table_name)
            if not table: continue

            loaded_constraints = table_data.get("constraints", {})
            for column, constraints_list in loaded_constraints.items():
                 if column not in table.columns:
                      print(f"Warning: Constraint column '{column}' not found in table '{table_name}' during load. Skipping constraints for this column.")
                      continue

                 for constraint_info in constraints_list:
                      constraint_type = constraint_info.get("type")
                      if constraint_type == "FOREIGN_KEY":
                           ref_table_name = constraint_info.get("reference_table")
                           ref_column = constraint_info.get("reference_column")
                           ref_table = db.get_table(ref_table_name)
                           if ref_table and ref_column:
                                try:
                                     # Add constraint using the loaded definition
                                     table.add_constraint(column, "FOREIGN_KEY", ref_table, ref_column)
                                except ValueError as e:
                                     print(f"Warning: Error adding FOREIGN_KEY constraint on {table_name}.{column} during load: {e}")
                           else:
                                print(f"Warning: Could not find reference table '{ref_table_name}' or column '{ref_column}' for FOREIGN_KEY constraint on {table_name}.{column} during load.")
                      # Add elif for other reloadable constraint types if needed
                      else:
                           # Skip non-reloadable types (like UNIQUE handled by index, or custom lambdas)
                           pass


        # Rebuild Indexes AFTER records and constraints (like FK) are loaded
        for table_name, table_data in data["tables"].items():
            table = db.get_table(table_name)
            if not table: continue

            index_definitions = table_data.get("indexes", [])
            for index_def in index_definitions:
                try:
                    table.create_index(
                        index_name=index_def["name"],
                        column=index_def["column"],
                        unique=index_def["unique"]
                    )
                except ValueError as e:
                    print(f"Warning: Failed to rebuild index '{index_def['name']}' on table '{table_name}' during load: {e}")
                except Exception as e:
                     print(f"Unexpected error rebuilding index '{index_def['name']}' on table '{table_name}': {e}")


        # Load Views, MVs, SPs, Triggers (AFTER tables/records/constraints/indexes are ready)
        try:
            # Pass db instance into the execution context for queries/procedures
            exec_context = {"db": db}
            # Helper to safely execute code strings
            def safe_exec(code, name, item_type):
                try:
                    dedented_code = textwrap.dedent(code)
                    # Merge globals and exec_context into one context dictionary
                    combined_context = dict(globals())
                    combined_context.update(exec_context)
                    exec(dedented_code, combined_context)
                    # Update exec_context with any new globals from combined_context
                    exec_context.update(combined_context)
                    return combined_context.get(name)
                except Exception as e:
                    print(f"Warning: Error executing {item_type} '{name}' code during load: {e}")
                    print(f"Code: {code}") # Show the code for debugging purposes
                    return None

            # Load Views
            for view_name, view_data in data.get("views", {}).items():
                query_func = safe_exec(view_data["query"], view_name, "view")
                if query_func and callable(query_func):
                     try: 
                         db.create_view(view_name, query_func)
                         view = db.get_view(view_name) 
                         view.query_string = view_data["query"] 
                         
                     except ValueError as e: print(f"Warning: Error creating view '{view_name}': {e}")
                else: print(f"Warning: Failed to load or invalid query function for view '{view_name}'.")

            # Load Materialized Views
            for mv_name, mv_data in data.get("materialized_views", {}).items():
                 query_func = safe_exec(mv_data["query"], mv_name, "materialized view")
                 if query_func and callable(query_func):
                      try: 
                          db.create_materialized_view(mv_name, query_func)
                          mv = db.get_materialized_view(mv_name)
                          mv.query_string = mv_data["query"] # Store the original query string for refresh purposes
                      except ValueError as e: print(f"Warning: Error creating materialized view '{mv_name}': {e}")
                 else: print(f"Warning: Failed to load or invalid query function for materialized view '{mv_name}'.")


            # Load Stored Procedures
            for sp_name, sp_data in data.get("stored_procedures", {}).items():
                proc_func = safe_exec(sp_data["query"], sp_name, "stored procedure")
                if proc_func and callable(proc_func):
                     try: 
                         db.add_stored_procedure(sp_name, proc_func)
                         sp = db.get_stored_procedure(sp_name)
                         db.stored_procedure_source[sp_name] = sp_data["query"]
                     except ValueError as e: print(f"Warning: Error adding stored procedure '{sp_name}': {e}")
                else: print(f"Warning: Failed to load or invalid function for stored procedure '{sp_name}'.")

            # Load Triggers
            for trigger_type, procs in data.get("triggers", {}).items():
                if trigger_type not in ("before", "after"): continue
                for proc_name, triggers_list in procs.items():
                    for trigger_code in triggers_list:
                         # Extract function name from code (simple regex)
                         match = re.search(r"def\s+(\w+)\s*\(", trigger_code)
                         if match:
                              trigger_func_name = match.group(1)
                              trigger_func = safe_exec(trigger_code, trigger_func_name, f"trigger function {trigger_func_name}")
                              if trigger_func and callable(trigger_func):
                                   try: db.add_trigger(proc_name, trigger_type, trigger_func)
                                   except ValueError as e: print(f"Warning: Error adding trigger to '{proc_name}': {e}")
                              else: print(f"Warning: Failed to load or invalid function for trigger '{trigger_func_name}'.")
                         else: print(f"Warning: Could not determine function name from trigger code for procedure '{proc_name}'.")

        except Exception as e:
            print(f"Error loading views/procedures/triggers: {e}")

        # Authenticate user if credentials provided AFTER everything is loaded
        if user and password and db._is_auth_required():
            user_manager = db.create_user_manager() # Ensure manager exists
            auth_success = user_manager.login_user(user, password)
            if not auth_success:
                 # Raise PermissionError if auth fails after successful load
                 raise PermissionError(f"Authentication failed for user '{user}' after loading database.")

        return db
        
    @staticmethod
    def delete(filename):
        """
        Delete the specified file from the filesystem.
        Args:
            filename (str): The path to the file to be deleted.
        """
        import os
        os.remove(filename)
    
    @staticmethod
    def db_to_dict(db):
        """
        Convert a database object to a dictionary.
        Args:
            db (Database): The database object to be converted.
        Returns:
            dict: A dictionary representation of the database object
        """
        data = {
            "name": db.name,
            "tables": {}
        }
        for table_name, table in db.tables.items():
            data["tables"][table_name] = {
                "name": table.name,
                "columns": table.columns,
                "records": [{
                    "id": record.id, 
                    "data": record.data,
                    "index": record.index.to_dict(),
                } for record in table.records],
                "next_id": table.next_id,
                "constraints": {
                    column: [
                        {
                            "name": constraint.__name__,
                            "reference_table": getattr(constraint, "reference_table", None),
                            "reference_column": getattr(constraint, "reference_column", None)
                        } for constraint in constraints
                    ] for column, constraints in table.constraints.items()
                },
                
            }
        return data
    
    # Table Operations
    # ---------------------------------------------------------------------------------------------
    @staticmethod
    def save_table(table, filename, format='csv'):
        """
        Save the table to a file in the specified format.
        Args:
            table (Table): The table object to be saved.
            filename (str): The path to the file where the table will be saved.
            format (str): The format in which the table will be saved. Default is 'csv'.
        """
        if format == 'csv':
            Storage._table_to_csv(table, filename)
        elif format == 'json':
            Storage._table_to_json(table, filename)
        elif format == 'sqlite':
            Storage._table_to_sqlite(table, filename)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def _table_to_csv(table, filename):
        """
        Save the table to a CSV file.
        Args:
            table (Table): The table object to be saved.
            filename (str): The path to the file where the table will be saved.
        """
        import csv
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(table.columns)
            for record in table.records:
                writer.writerow([record.data[column] for column in table.columns])
                
    def _table_to_json(table, filename):
        """
        Save the table to a JSON file.
        Args:
            table (Table): The table object to be saved.
            filename (str): The path to the file where the table will be saved.
        """
        data = {
            "columns": table.columns,
            "records": [record.data for record in table.records]
        }
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
            
    def _table_to_sqlite(table, filename):
        """
        Save the table to a SQLite database file.  
        If the database file already exists, it will be overwritten.
        Args:
            table (Table): The table object to be saved.
            filename (str): The path to the file where the table will be saved.
        """
        import sqlite3
        import os
    
        # Delete the file if it already exists
        if os.path.exists(filename):
            os.remove(filename)
    
        conn = sqlite3.connect(filename)
        c = conn.cursor()
        c.execute(f"CREATE TABLE {table.name} ({', '.join([f'{column} TEXT' for column in table.columns])})")
        for record in table.records:
            values = ', '.join([f'"{record.data[column]}"' for column in table.columns])
            c.execute(f"INSERT INTO {table.name} VALUES ({values})")
        conn.commit()
        conn.close()
        
    @staticmethod
    def _load_table_from_db_file(filename: str, table_name: str, db: 'Database', key: Optional[str]=None, compression: bool=False) -> 'Table':
        """
        Loads only a specific table's data and definition from a db file.
        Intended for use with PartialDatabase. Rebuilds indexes for the loaded table.

        Args:
            filename (str): Path to the .segadb file.
            table_name (str): Name of the table to load.
            db (Database): The main Database context (needed for FK lookups).
            key (str, optional): Encryption key.
            compression (bool, optional): Whether the file is compressed.

        Returns:
            Table: A fully loaded Table object (not yet added to db.tables).

        Raises:
            ValueError: If the table is not found in the file.
            FileNotFoundError: If the file cannot be found.
            Exception: For various file reading, decryption, or parsing errors.
        """
        try:
            mode = 'rb' # Always read binary
            with open(filename, mode) as f:
                payload = f.read()
                if key:
                    # Decrypt needs string, assumes payload is utf-8 encoded encrypted string
                    payload = Storage.decrypt(payload.decode('utf-8'), key).encode('utf-8')
                if compression:
                    payload = zlib.decompress(payload)
                # Decode final payload to string for json.loads
                json_data_str = payload.decode('utf-8')
                data = json.loads(json_data_str)
        except FileNotFoundError:
            print(f"Error: Database file not found: {filename}")
            raise
        except (json.JSONDecodeError, zlib.error, TypeError, ValueError) as e:
             print(f"Error loading/decoding database file {filename} to load table '{table_name}': {e}")
             raise
        except Exception as e:
            print(f"Unexpected error reading database file {filename}: {e}")
            raise

        # Find the specific table data within the loaded file data
        table_data = data.get("tables", {}).get(table_name)
        if table_data is None:
            raise ValueError(f"Table '{table_name}' not found in database file '{filename}'")

        # --- Create and Populate the Standalone Table Object ---

        # 1. Initialize the Table instance
        try:
             table = Table(table_name, table_data["columns"])
             table.next_id = table_data.get("next_id", 1)
        except KeyError as e:
             raise ValueError(f"Missing essential table data ('columns') for table '{table_name}' in file '{filename}'. Error: {e}")
        except Exception as e:
             raise ValueError(f"Error initializing table '{table_name}' from file data: {e}")


        # 2. Load Records into the table instance's structures
        record_type_map = { # Map type names to actual classes
             "Record": Record,
             "VectorRecord": VectorRecord,
             "TimeSeriesRecord": TimeSeriesRecord,
             "ImageRecord": ImageRecord,
             "TextRecord": TextRecord,
             "EncryptedRecord": EncryptedRecord,
        }
        for record_info in table_data.get("records", []):
            try:
                record_id = record_info["id"]
                record_type_name = record_info.get("type", "Record")
                record_class = record_type_map.get(record_type_name, Record)
                record_data = record_info["data"]

                # Handle potential byte encoding (e.g., password_hash)
                processed_data = {
                    k: (v.encode('utf-8') if isinstance(v, str) and k == "password_hash" else v)
                    for k, v in record_data.items()
                }
                # Special handling for loading EncryptedRecord (key not saved)
                if record_class == EncryptedRecord:
                     processed_data["key"] = None # Indicate data is already encrypted

                # Create the record object
                record_obj = record_class(record_id, processed_data)

                # Add directly to the standalone table's structures
                if record_obj.id not in table.record_map:
                     table.records.append(record_obj)
                     table.record_map[record_obj.id] = record_obj
                # else: # Optional warning about duplicate IDs in the file data itself
                #      print(f"Warning: Duplicate record ID {record_obj.id} in source data for table '{table_name}'.")

            except Exception as e:
                # Log/print warning and skip the problematic record
                record_id_str = record_info.get('id', 'UNKNOWN')
                print(f"Warning: Error processing record ID {record_id_str} while loading table '{table_name}'. Skipping. Error: {e}")
                continue # Skip to the next record

        # 3. Load Reloadable Constraints (like Foreign Keys)
        loaded_constraints = table_data.get("constraints", {})
        for column, constraints_list in loaded_constraints.items():
            if column not in table.columns:
                # print(f"Warning: Constraint column '{column}' not found in table '{table_name}' schema during load. Skipping.")
                continue
            for constraint_info in constraints_list:
                 # Only load FK constraints here. Unique handled by index, custom not reloadable.
                if constraint_info.get("type") == "FOREIGN_KEY":
                    ref_table_name = constraint_info.get("reference_table")
                    ref_column = constraint_info.get("reference_column")
                    # Use the provided 'db' object (main DB context) to find the reference table
                    # This is essential for PartialDatabase where FK targets might not be loaded yet *in the partial DB*,
                    # but they *must* exist in the main DB structure represented by 'db'.
                    ref_table = db.get_table(ref_table_name) # Use the main DB instance
                    if ref_table and ref_column:
                        try:
                             # Add the constraint to the *standalone* table object being built
                             table.add_constraint(column, "FOREIGN_KEY", ref_table, ref_column)
                        except ValueError as e:
                             print(f"Warning: Error adding FK constraint on {table_name}.{column} during partial load: {e}")
                    # else: # Optional warning if reference table/column is missing *in the main DB context*
                    #      print(f"Warning: Reference table '{ref_table_name}' or column '{ref_column}' not found in main DB context for FK constraint on {table_name}.{column}.")


        # 4. Rebuild Indexes on the standalone table using loaded records
        index_definitions = table_data.get("indexes", [])
        for index_def in index_definitions:
            try:
                # Call create_index on the *standalone* table object we are building
                table.create_index(
                    index_name=index_def["name"],
                    column=index_def["column"],
                    unique=index_def["unique"]
                )
                # print(f"Successfully rebuilt index '{index_def['name']}' for table '{table_name}'.") # Optional success log
            except ValueError as e:
                # This usually means a unique constraint was violated during rebuild
                print(f"Warning: Failed to rebuild index '{index_def['name']}' on table '{table_name}' during partial load: {e}. Data might violate constraints.")
            except Exception as e:
                 print(f"Unexpected error rebuilding index '{index_def['name']}' on table '{table_name}': {e}")


        return table # Return the fully populated, standalone Table object

    @staticmethod
    def _save_table_to_db_file(filename: str, table: 'Table', key: Optional[str]=None, compression: bool=False):
        """
        Saves a given table's current data and definition back into a db file,
        overwriting the data for that specific table within the file structure.

        Args:
            filename (str): Path to the .segadb file to update.
            table (Table): The Table object (presumably modified) to save back.
            key (str, optional): Encryption key.
            compression (bool, optional): Whether the file is compressed.

        Raises:
            FileNotFoundError: If the target db file doesn't exist.
            ValueError: If the table name doesn't exist in the loaded file data.
            Exception: For various file reading/writing, encryption, or parsing errors.
        """
        # 1. Read the *entire* existing database file
        try:
            mode = 'rb' # Always read binary
            with open(filename, mode) as f:
                payload = f.read()
                if key:
                    payload = Storage.decrypt(payload.decode('utf-8'), key).encode('utf-8')
                if compression:
                    payload = zlib.decompress(payload)
                json_data_str = payload.decode('utf-8')
                data = json.loads(json_data_str)
        except FileNotFoundError:
             print(f"Error: Database file not found: {filename}")
             raise
        except (json.JSONDecodeError, zlib.error, TypeError, ValueError) as e:
             print(f"Error reading/decoding existing db file {filename} to save table '{table.name}': {e}")
             raise
        except Exception as e:
            print(f"Unexpected error reading existing database file {filename}: {e}")
            raise

        # 2. Define the serialization logic for a single table (consistent with Storage.save)
        def serialize_table_for_update(target_table: 'Table') -> Dict[str, Any]:
            # Serialize constraints (FK needs special handling for table name)
            serializable_constraints = {}
            for column, constraints_list in target_table.constraints.items():
                 serializable_constraints[column] = []
                 for constraint_func in constraints_list:
                      constraint_info = {"name": constraint_func.__name__}
                      constraint_type = getattr(constraint_func, "_constraint_type", "CUSTOM")
                      # Only save reloadable constraint definitions
                      if constraint_type == "FOREIGN_KEY":
                           constraint_info["reference_table"] = getattr(constraint_func, "reference_table_name", None)
                           constraint_info["reference_column"] = getattr(constraint_func, "reference_column", None)
                           constraint_info["type"] = "FOREIGN_KEY" # Explicitly add type
                           serializable_constraints[column].append(constraint_info)
                      # Skip UNIQUE (handled by index) and CUSTOM (not reloadable)
                      else:
                           continue

            # Serialize records
            serialized_records = []
            for record in target_table.records:
                 record_data_dict = {}
                 # Use to_dict() if available (e.g., ImageRecord), else process normally
                 if hasattr(record, 'to_dict') and callable(record.to_dict):
                      # Assume to_dict handles necessary encoding (like base64 for images)
                      record_data_dict = record.to_dict()
                 else:
                      # Default processing: decode bytes (like password hash) for JSON
                      record_data_dict = {
                           k: (v.decode('utf-8') if isinstance(v, bytes) else v)
                           for k, v in record.data.items()
                      }

                 serialized_records.append({
                     "id": record.id,
                     "type": record._type(),
                     "data": record_data_dict,
                     # No longer saving record._index
                 })

            return {
                "name": target_table.name,
                "columns": target_table.columns,
                "records": serialized_records,
                "next_id": target_table.next_id,
                "constraints": serializable_constraints, # Save processed constraints
                "indexes": [index.to_dict_definition() for index in target_table.indexes.values()], # Save index definitions
            }

        # 3. Serialize the input 'table' object
        try:
             serialized_table_data = serialize_table_for_update(table)
        except Exception as e:
             print(f"Error serializing table '{table.name}' for saving: {e}")
             raise

        # 4. Update the table data within the loaded dictionary structure
        if "tables" not in data:
            data["tables"] = {} # Ensure 'tables' key exists

        # Check if the table actually exists in the loaded structure before replacing
        if table.name not in data["tables"]:
             # This could happen if the file was modified externally or table name changed
             print(f"Warning: Table '{table.name}' was not found in the loaded file structure of '{filename}'. Adding it.")
             # If adding, ensure other parts of 'data' (views, etc.) remain consistent.

        data["tables"][table.name] = serialized_table_data

        # 5. Convert the *entire modified* data structure back to JSON
        try:
            json_data_str_updated = json.dumps(data, indent=4)
            payload_updated = json_data_str_updated.encode('utf-8') # Start with bytes

            # 6. Compress/Encrypt if necessary
            if compression:
                payload_updated = zlib.compress(payload_updated)
            if key:
                # Encrypt expects string, payload should be bytes if compressed, decode first
                payload_updated = Storage.encrypt(payload_updated.decode('utf-8'), key).encode('utf-8')

            # 7. Write the entire structure back to the file, overwriting it
            mode_write = 'wb' # Always write binary
            with open(filename, mode_write) as f:
                f.write(payload_updated)

        except TypeError as e:
             print(f"Error serializing updated database data for file '{filename}': {e}")
             raise
        except Exception as e:
             print(f"Error writing updated database to {filename}: {e}")
             raise
    
    @staticmethod
    def _load_viewsProcs_from_db_file(filename, db, key=None, user=None, password=None, compression=False, views=True, materialized_views=True, stored_procedures=True, triggers=True):
        """
        Load views/procedures/triggers from a segadb database file.
        
        Args:
            filename (str): The path to the JSON file containing the database data.
            db (Database): The database object to which the views will be added.
            key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
            compression (bool, optional): If True, the data will be decompressed using zlib after loading.
            views (bool, optional): If True, load views. Default is True.
            materialized_views (bool, optional): If True, load materialized views. Default is True.
            stored_procedures (bool, optional): If True, load stored procedures. Default is True.
            triggers (bool, optional): If True, load triggers. Default is True.
        """
        import textwrap
        with open(filename, 'rb' if compression or key else 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            if compression:
                json_data = zlib.decompress(json_data).decode()
    
            data = json.loads(json_data)
            
        def safe_exec(code, name, item_type):
            try:
                dedented_code = textwrap.dedent(code)
                combined_context = dict(globals())
                combined_context.update({"db": db})
                exec(dedented_code, combined_context)
                return combined_context.get(name)
            except Exception as e:
                print(f"Warning: Error executing {item_type} '{name}' code during load: {e}")
                print(f"Code: {code}")
                return None
        
        # Load views
        if views:
            for view_name, view_data in data.get("views", {}).items():
                query_func = safe_exec(view_data["query"], view_name, "view")
                if query_func and callable(query_func):
                    try:
                        db.create_view(view_name, query_func)
                        view = db.get_view(view_name)
                        view.query_string = view_data["query"]
                    except ValueError as e:
                        print(f"Warning: Error creating view '{view_name}': {e}")
                else:
                    print(f"Warning: Failed to load or invalid query function for view '{view_name}'.")
        
        # Load materialized views
        if materialized_views:
            for mv_name, mv_data in data.get("materialized_views", {}).items():
                query_func = safe_exec(mv_data["query"], mv_name, "materialized view")
                if query_func and callable(query_func):
                    try:
                        db.create_materialized_view(mv_name, query_func)
                        mv = db.get_materialized_view(mv_name)
                        mv.query_string = mv_data["query"]
                    except ValueError as e:
                        print(f"Warning: Error creating materialized view '{mv_name}': {e}")
                else:
                    print(f"Warning: Failed to load or invalid query function for materialized view '{mv_name}'.")
        
        # Load stored procedures
        if stored_procedures and data.get("stored_procedures"):
            for sp_name, sp_data in data.get("stored_procedures", {}).items():
                proc_func = safe_exec(sp_data["query"], sp_name, "stored procedure")
                if proc_func and callable(proc_func):
                    try:
                        db.add_stored_procedure(sp_name, proc_func)
                        sp = db.get_stored_procedure(sp_name)
                        db.stored_procedure_source[sp_name] = sp_data["query"]
                    except ValueError as e:
                        print(f"Warning: Error adding stored procedure '{sp_name}': {e}")
                else:
                    print(f"Warning: Failed to load or invalid function for stored procedure '{sp_name}'.")
        
        # Load triggers
        if triggers and data.get("triggers"):
            for trigger_type, triggers_dict in data.get("triggers", {}).items():
                for proc_name, triggers_list in triggers_dict.items():
                    for trigger_code in triggers_list:
                        match = re.search(r"def\s+(\w+)\s*\(", trigger_code)
                        if match:
                            trigger_func_name = match.group(1)
                            trigger_func = safe_exec(trigger_code, trigger_func_name, f"trigger function {trigger_func_name}")
                            if trigger_func and callable(trigger_func):
                                try:
                                    db.add_trigger(proc_name, trigger_type, trigger_func)
                                except ValueError as e:
                                    print(f"Warning: Error adding trigger to '{proc_name}': {e}")
                            else:
                                print(f"Warning: Failed to load or invalid function for trigger '{trigger_func_name}'.")
                        else:
                            print(f"Warning: Could not determine function name from trigger code for procedure '{proc_name}'.")

    # Utility Functions
    # ---------------------------------------------------------------------------------------------
    def _load_mp(filename, key=None, user=None, password=None, compression=False):
        """
        Load a database from a JSON file using multiprocessing.
        Args:
            filename (str): The name of the file to load.
            key (str, optional): The encryption key. Defaults to None.
            user (str, optional): The username for authentication. Defaults to None.
            password (str, optional): The password for authentication. Defaults to None.
            compression (bool, optional): If True, decompress the file. Defaults to False.
        """
        with open(filename, 'rb' if compression or key else 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            if compression:
                json_data = zlib.decompress(json_data).decode()

            data = json.loads(json_data)

        db = Database(data["name"])
        cpu_count = mp.cpu_count()
        
        for table_name, table_data in data["tables"].items():
            # If table does not exist, or if it is _users table and there are no users, create it
            if not db.get_table(table_name) or (table_name == "_users" and db.get_table(table_name).records == []):
                db.create_table(table_name, table_data["columns"])     
                table = db.get_table(table_name)
                table.next_id = table_data["next_id"]
                
                # Split the records into chunks
                record_chunks = Storage._get_record_chunks(table_data["records"], cpu_count)
                
                # Process the chunks in parallel
                with mp.Pool(cpu_count) as pool:
                    results = [pool.apply_async(_process_chunk, args=(chunk, table)) for chunk in record_chunks]
                    record_objects = [result.get() for result in results]
                for record_chunk in record_objects:
                    for record in record_chunk:
                        table.records.append(record)
                    
                for column, constraints in table_data["constraints"].items():
                    for constraint in constraints:
                        if constraint["name"] == "unique_constraint":
                            table.add_constraint(column, "UNIQUE")
                        elif constraint["name"] == "foreign_key_constraint":
                            reference_table = db.get_table(constraint["reference_table"])
                            reference_column = constraint["reference_column"]
                            table.add_constraint(column, "FOREIGN_KEY", reference_table, reference_column)
                        else:
                            # Handle other constraints if necessary
                            pass
            
            if user and password and len(db.get_table("_users").records) > 0 and not db.active_session:
                user_manager = db.create_user_manager()
                auth = db.create_authorization()
                user_manager.login_user(user, password)     
        return db
    
    def _get_record_chunks(records, num_chunks):
        """
        Split a list of records into chunks.
        """
        chunk_size = len(records) // num_chunks
        if chunk_size == 0:
            chunk_size = 1
        chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
        return chunks

