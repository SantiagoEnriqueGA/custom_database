import json
from cryptography.fernet import Fernet

from .database import Database
from .record import Record
from .index import Index

# Note: these are static methods, so they don't need to be instantiated
class Storage:
    """A utility class for saving, loading, and deleting database files."""
    
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
    
    @staticmethod
    def save(db, filename, key=None):
        """
        Save the database object to a JSON file.  
        The database object is serialized into a JSON format and written to the specified file.  
        The JSON structure includes the database name, tables, columns, records, next_id, and constraints.  
        Args:
            db (Database): The database object to be saved.
            filename (str): The path to the file where the database will be saved.
            key (bytes, optional): The encryption key. If provided, the data will be encrypted before saving.
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
        with open(filename, 'wb' if key else 'w') as f:
            json_data = json.dumps(data, indent=4)
            if key:
                json_data = Storage.encrypt(json_data, key)
                f.write(json_data)
            else:
                f.write(json_data)

    @staticmethod
    def load(filename, key=None):
        """
        Load a database from a JSON file.
        Args:
            filename (str): The path to the JSON file containing the database data.
            key (bytes, optional): The encryption key. If provided, the data will be decrypted after loading.
        Returns:
            Database: An instance of the Database class populated with the data from the JSON file.
        """
        with open(filename, 'r') as f:
            json_data = f.read()
            if key:
                json_data = Storage.decrypt(json_data, key)
            data = json.loads(json_data)
        
        db = Database(data["name"])
        for table_name, table_data in data["tables"].items():
            db.create_table(table_name, table_data["columns"])
            table = db.get_table(table_name)
            table.next_id = table_data["next_id"]
            for record in table_data["records"]:
                table.insert(record["data"], record_type=Record)
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
        Args:
            table (Table): The table object to be saved.
            filename (str): The path to the file where the table will be saved.
        """
        import sqlite3
        conn = sqlite3.connect(filename)
        c = conn.cursor()
        c.execute(f"CREATE TABLE {table.name} ({', '.join([f'{column} TEXT' for column in table.columns])})")
        for record in table.records:
            c.execute(f"INSERT INTO {table.name} VALUES ({', '.join([f'"{record.data[column]}"' for column in table.columns])})")
        conn.commit()
        conn.close()
        
        