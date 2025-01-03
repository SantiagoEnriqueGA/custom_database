# Imports: Standard Library
import re
import os
import types
from math import inf
import json
import zlib
import multiprocessing as mp
from datetime import datetime

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
    def save(db, filename, key=None, compress=False):
        """
        Save the database object to a JSON file.  
        The database object is serialized into a JSON format and written to the specified file.  
        The JSON structure includes the database name, tables, columns, records, next_id, and constraints.  
        Args:
            db (Database): The database object to be saved.
            filename (str): The path to the file where the database will be saved.
            key (bytes, optional): The encryption key. If provided, the data will be encrypted before saving.
            compress (bool, optional): If True, the data will be compressed using zlib before saving.
        """
        data = {
            "name": db.name,
            "tables": {},
            "views": {},
            "materialized_views": {},
            "stored_procedures": {},
            "triggers": {"before": {}, "after": {}}
        }
        
        def serialize_table(table):
            return {
                "name": table.name,
                "columns": table.columns,
                "records": [{
                    "id": record.id,
                    "type": record._type(),
                    "data": record.to_dict() if isinstance(record, ImageRecord) else {k: (v.decode() if isinstance(v, bytes) else v) for k, v in record.data.items()},
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
        
        # Serialize tables
        for table_name, table in db.tables.items():
            data["tables"][table_name] = serialize_table(table)
        
        # Serialize views
        for view_name, view in db.views.items():
            data["views"][view_name] = {
                "name": view.name,
                "query": view._query_to_string(),
                
                # Not needed, data is retrieved when the view is called
                # "data": serialize_table(view.get_data()),
            }
        
        # Serialize materialized views
        for mv_name, mv in db.materialized_views.items():
            data["materialized_views"][mv_name] = {
                "name": mv.name,
                "query": mv._query_to_string(),
                
                # Not needed, data is recalculated when the MV is created 
                # "data": serialize_table(mv.get_data()),
            }
            
        # Serialize stored procedures
        for sp_name, sp in db.stored_procedures.items():
            query = db._stored_procedure_to_string(sp)
            
            data["stored_procedures"][sp_name] = {
                "name": sp_name,
                "query": query,
            }
        
        # Serialize triggers
        for trigger_type in db.triggers:
            for proc_name, triggers in db.triggers[trigger_type].items():
                data["triggers"][trigger_type][proc_name] = [db._stored_procedure_to_string(trigger) for trigger in triggers]
        
        json_data = json.dumps(data, indent=4)
        if compress:
            json_data = zlib.compress(json_data.encode())
        
        with open(filename, 'wb' if compress or key else 'w') as f:
            if key:
                json_data = Storage.encrypt(json_data, key)
                f.write(json_data.encode())  # Ensure data is written as bytes
            else:
                f.write(json_data)

    @staticmethod
    def load(filename, key=None, user=None, password=None, compression=False, parrallel=False):
        """
        Load a database from a JSON file.
        Args:
            filename (str): The path to the JSON file containing the database data.
            key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
            compression (bool, optional): If True, the data will be decompressed using zlib after loading.
            parrallel (bool, optional): If True, the data will be loaded in parallel using multiprocessing.
        Returns:
            Database: An instance of the Database class populated with the data from the JSON file.
        """
        if parrallel:
            return Storage._load_mp(filename, key, user, password, compression)
        
        with open(filename, 'rb' if compression or key else 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            if compression:
                json_data = zlib.decompress(json_data).decode()

            data = json.loads(json_data)
        
        db = Database(data["name"])
        
        # Load tables
        for table_name, table_data in data["tables"].items():
            # If table does not exist, or if it is _users table and there are no users, create it
            if not db.get_table(table_name) or (table_name == "_users" and db.get_table(table_name).records == []):
                db.create_table(table_name, table_data["columns"])     
                table = db.get_table(table_name)
                table.next_id = table_data["next_id"]
                for record in table_data["records"]:
                    record_data = {k: (v.encode() if isinstance(v, str) and k == "password_hash" else v) for k, v in record["data"].items()}
                    
                    # For each record type, create the record object and insert it into the table
                    if record["type"] == "Record":
                        table.insert(record_data, record_type=Record, flex_ids = True)
                    elif record["type"] == "VectorRecord":
                        table.insert(record_data, record_type=VectorRecord, flex_ids = True)
                    elif record["type"] == "TimeSeriesRecord":
                        table.insert(record_data, record_type=TimeSeriesRecord, flex_ids = True)
                    elif record["type"] == "ImageRecord":
                        table.insert(record_data, record_type=ImageRecord, flex_ids = True)
                    elif record["type"] == "TextRecord":
                        table.insert(record_data, record_type=TextRecord, flex_ids = True)
                    elif record["type"] == "EncryptedRecord":                        
                        try:
                            record_data["key"] = None
                            table.insert(record_data, record_type=EncryptedRecord, flex_ids = True)
                        except Exception as e:
                            print(f"Error loading EncryptedRecord: {e}")
                            table.insert(record_data, record_type=Record, flex_ids = True)
                    
                    table.records[-1].id = record["id"]
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
            
            # If user and password are provided, and there are users in the _users table, login the user
            if user and password and len(db.get_table("_users").records) > 0 and not db.active_session:
                user_manager = db.create_user_manager()
                auth = db.create_authorization()
                user_manager.login_user(user, password)
                
        # Load views
        # Executes the query function in the global namespace, typecasts the result to a function, and creates a view
        for view_name, view_data in data["views"].items():
            exec(view_data["query"], globals())
            globals()[view_name] = eval(view_name)
            db.create_view(view_name, types.FunctionType(globals()[view_name].__code__, {"db": db}))
            
        # Load materialized views
        # Executes the query function in the global namespace, typecasts the result to a function, and creates a materialized view
        for mv_name, mv_data in data["materialized_views"].items():
            exec(mv_data["query"], globals())
            globals()[mv_name] = eval(mv_name)
            db.create_materialized_view(mv_name, types.FunctionType(globals()[mv_name].__code__, {"db": db}))
            
        # Load stored procedures
        # Executes the query function in the global namespace, typecasts the result to a function, and creates a stored procedure
        if data.get("stored_procedures"):    
            for sp_name, sp_data in data["stored_procedures"].items():
                exec(sp_data["query"], globals())
                globals()[sp_name] = eval(sp_name)
                db.add_stored_procedure(sp_name, types.FunctionType(globals()[sp_name].__code__, {"db": db}))
        
        # Load triggers
        if data.get("triggers"):
            for trigger_type in data["triggers"]:
                for proc_name, triggers in data["triggers"][trigger_type].items():
                    for trigger_data in triggers:
                        exec(trigger_data, globals())
                        trigger_name_match = re.search(r'def (\w+)', trigger_data)
                        if trigger_name_match:
                            trigger_name = trigger_name_match.group(1)
                            trigger_function = eval(trigger_name)
                            db.add_trigger(proc_name, trigger_type, types.FunctionType(trigger_function.__code__, {"db": db}))
        
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
    def _load_table_from_db_file(filename, table_name, db, key=None, user=None, password=None, compression=False):
        """
        Load a table from a segadb database file. Only the table data is loaded, not the entire database.
        Args:
            filename (str): The path to the JSON file containing the database data.
            table_name (str): The name of the table to load.
            db (Database): The database object to which the table will be added.
            key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
            compression (bool, optional): If True, the data will be decompressed using zlib after loading.
        Returns:
            Table: The loaded table object.
        """
        with open(filename, 'rb' if compression or key else 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            if compression:
                json_data = zlib.decompress(json_data).decode()

            data = json.loads(json_data)
        
        # Find the table data in the database file
        table_data = None
        for table in data["tables"]:
            if table == table_name:
                table_data = data["tables"][table]
                break
        if table_data is None:
            raise ValueError(f"Table '{table_name}' not found in database file")
        
        table = Table(table_name, table_data["columns"])
        table.next_id = table_data["next_id"]
        for record in table_data["records"]:
            record_data = {k: (v.encode() if isinstance(v, str) and k == "password_hash" else v) for k, v in record["data"].items()}
            table.insert(record_data, record_type=Record, flex_ids = True)
            table.records[-1].id = record["id"]
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
        
        return table
    
    @staticmethod
    def _save_table_to_db_file(filename, table, key=None, compression=False):
        """
        Save a table to a segadb database file. Only the table data is updated, not the entire database.
        Args:
            filename (str): The path to the JSON file where the table will be saved.
            table (Table): The table object to be saved.
            key (bytes, optional): The encryption key. If provided, the data will be encrypted before saving.
            compression (bool, optional): If True, the data will be compressed using zlib before saving.
        """
        # Read the existing JSON file
        with open(filename, 'rb' if compression or key else 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            if compression:
                json_data = zlib.decompress(json_data).decode()
            data = json.loads(json_data)
        
        def serialize_table(table):
            return {
                "name": table.name,
                "columns": table.columns,
                "records": [{
                    "id": record.id,
                    "data": record.to_dict() if isinstance(record, ImageRecord) else {k: (v.decode() if isinstance(v, bytes) else v) for k, v in record.data.items()},
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
            
        # Update the table data
        data["tables"][table.name] = serialize_table(table)
        
        # Convert the data back to JSON
        json_data = json.dumps(data)
        if compression:
            json_data = zlib.compress(json_data.encode())
        if key:
            json_data = Storage.encrypt(json_data, key)

        # Write the updated JSON back to the file
        with open(filename, 'wb' if compression or key else 'w') as f:
            f.write(json_data)
    
    @staticmethod
    def _load_viewsProcs_from_db_file(filename, db, key=None, user=None, password=None, compression=False, views=True, materialized_views=True, stored_procedures=True, triggers=True):
        """
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
        """
        with open(filename, 'rb' if compression or key else 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            if compression:
                json_data = zlib.decompress(json_data).decode()

            data = json.loads(json_data)
        
        # Load views
        # Executes the query function in the global namespace, typecasts the result to a function, and creates a view
        if views:
            for view_name, view_data in data["views"].items():
                exec(view_data["query"].strip(), globals())
                globals()[view_name] = eval(view_name)
                db.create_view(view_name, types.FunctionType(globals()[view_name].__code__, {"db": db}))
            
        # Load materialized views
        # Executes the query function in the global namespace, typecasts the result to a function, and creates a materialized view
        if materialized_views:
            for mv_name, mv_data in data["materialized_views"].items():
                exec(mv_data["query"].strip(), globals())
                globals()[mv_name] = eval(mv_name)
                db.create_materialized_view(mv_name, types.FunctionType(globals()[mv_name].__code__, {"db": db}))
            
        # Load stored procedures
        # Executes the query function in the global namespace, typecasts the result to a function, and creates a stored procedure
        if stored_procedures:
            if data.get("stored_procedures"):    
                for sp_name, sp_data in data["stored_procedures"].items():
                    exec(sp_data["query"].strip(), globals())
                    globals()[sp_name] = eval(sp_name)
                    db.add_stored_procedure(sp_name, types.FunctionType(globals()[sp_name].__code__, {"db": db}))
        
        # Load triggers
        if triggers:
            if data.get("triggers"):
                for trigger_type in data["triggers"]:
                    for proc_name, triggers in data["triggers"][trigger_type].items():
                        for trigger_data in triggers:
                            exec(trigger_data.strip(), globals())
                            trigger_name_match = re.search(r'def (\w+)', trigger_data)
                            if trigger_name_match:
                                trigger_name = trigger_name_match.group(1)
                                trigger_function = eval(trigger_name)
                                db.add_trigger(proc_name, trigger_type, types.FunctionType(trigger_function.__code__, {"db": db}))

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

