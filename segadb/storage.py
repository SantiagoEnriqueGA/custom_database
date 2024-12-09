import json
from .database import Database
from .record import Record
from .index import Index

# Note: these are static methods, so they don't need to be instantiated
class Storage:
    """A utility class for saving, loading, and deleting database files."""
    @staticmethod
    def save(db, filename):
        """
        Save the database object to a JSON file.  
        The database object is serialized into a JSON format and written to the specified file.  
        The JSON structure includes the database name, tables, columns, records, next_id, and constraints.  
        Args:
            db (Database): The database object to be saved.
            filename (str): The path to the file where the database will be saved.
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
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def load(filename):
        """
        Load a database from a JSON file.
        Args:
            filename (str): The path to the JSON file containing the database data.
        Returns:
            Database: An instance of the Database class populated with the data from the JSON file.
        """
        with open(filename, 'r') as f:
            data = json.load(f)
        
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