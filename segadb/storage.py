# Imports: Standard Library
import os
from math import inf
import json
import zlib
import multiprocessing as mp

# Imports: Third Party
from cryptography.fernet import Fernet

# Imports: Local
from .database import Database
from .record import Record
from .index import Index
from .record import ImageRecord

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

# Note: these are static methods, so they don't need to be instantiated
class Storage:
    """A utility class for saving, loading, and deleting database files."""
    
    # Encryption and Decryption
    # ---------------------------------------------------------------------------------------------
    @staticmethod
    def generate_key():
        """Generate a key for encryption."""
        return Fernet.generate_key()

    @staticmethod
    def encrypt(data, key):
        """Encrypt the data using the provided key."""
        fernet = Fernet(key)
        return fernet.encrypt(data.encode())

    @staticmethod
    def decrypt(data, key):
        """Decrypt the data using the provided key."""
        fernet = Fernet(key)
        return fernet.decrypt(data).decode()
    
    # Database Operations
    # ---------------------------------------------------------------------------------------------
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
            "tables": {}
        }
        for table_name, table in db.tables.items():
            data["tables"][table_name] = {
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
        
        json_data = json.dumps(data, indent=4)
        if compress:
            json_data = zlib.compress(json_data.encode())
        
        with open(filename, 'wb' if compress or key else 'w') as f:
            if key:
                json_data = Storage.encrypt(json_data, key)
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
        
        for table_name, table_data in data["tables"].items():
            # If table does not exist, or if it is _users table and there are no users, create it
            if not db.get_table(table_name) or (table_name == "_users" and db.get_table(table_name).records == []):
                db.create_table(table_name, table_data["columns"])     
                table = db.get_table(table_name)
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
            
            if user and password and len(db.get_table("_users").records) > 0 and not db.active_session:
                user_manager = db.create_user_manager()
                auth = db.create_authorization()
                user_manager.login_user(user, password)     
        return db
    
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
            c.execute(f"INSERT INTO {table.name} VALUES ({', '.join([f'\"{record.data[column]}\"' for column in table.columns])})")
        conn.commit()
        conn.close()

