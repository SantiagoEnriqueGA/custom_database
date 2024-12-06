import json
from .database import Database
from .record import Record

# Note: these are static methods, so they don't need to be instantiated
class Storage:
    @staticmethod
    def save(db, filename):
        data = {
            "name": db.name,
            "tables": {}
        }
        for table_name, table in db.tables.items():
            data["tables"][table_name] = {
                "name": table.name,
                "columns": table.columns,
                "records": [{"id": record.id, "data": record.data} for record in table.records],
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
        with open(filename, 'r') as f:
            data = json.load(f)
        
        db = Database(data["name"])
        for table_name, table_data in data["tables"].items():
            db.create_table(table_name, table_data["columns"])
            table = db.get_table(table_name)
            table.next_id = table_data["next_id"]
            for record in table_data["records"]:
                table.insert(record["data"], record_type=Record)
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
        