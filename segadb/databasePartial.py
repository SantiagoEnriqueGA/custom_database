# Imports: Local
from .database import Database
from .table import Table
from .record import Record
from .storage import Storage

class PartialDatabase(Database):
    def __init__(self, name, file_path):
        super().__init__(name)
        self.file_path = file_path
        self.loaded_tables = {}
        
        # Load _users table
        self.loaded_tables["_users"] = self._load_table_from_storage("_users")

    def get_table(self, table_name, session_token=None):
        """
        Retrieve a table from the database by its name. Load the table into memory if it is not already loaded.
        Args:
            table_name (str): The name of the table to retrieve.
            session_token (str, optional): The session token of the user performing the action.
        Returns:
            Table: The table object.
        """
        self._check_permission(session_token, "read_table")
        if table_name not in self.loaded_tables:
            self.loaded_tables[table_name] = self._load_table_from_storage(table_name)
        return self.loaded_tables[table_name]

    def _load_table_from_storage(self, table_name):
        """
        Load a table from storage into memory.
        Args:
            table_name (str): The name of the table to load.
        Returns:
            Table: The loaded table object.
        """
        table = Storage._load_table_from_db_file(self.file_path, table_name, self)
        self.loaded_tables[table_name] = table
        return table
        
    def active_tables(self):
        """
        Get a list of active tables in the database.
        Returns:
            list: A list of table names.
        """
        return list(self.loaded_tables.keys())
    
    def dormant_tables(self):
        """
        Get a list of tables that are not currently loaded into memory.
        Returns:
            list: A list of table names.
        """
        import json
        with open(self.file_path, 'rb') as f:
            json_data = f.read()
            data = json.loads(json_data)
            
        dormant_tables = []
        for table in data["tables"]:
            if table not in self.loaded_tables:
                dormant_tables.append(table)
        return dormant_tables
            
    
    # Utility Methods
    # ---------------------------------------------------------------------------------------------
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
        print(f"Loaded Tables: {len(self.loaded_tables)}")
        print(f"Authorization Required: {self._is_auth_required()}")
        print(f"Active Session: \"{self.get_username_by_session(self.active_session)}:{self.active_session}\"")
        
        print(f"Database Objects:")
        
        print(f"  --Users: {len(self.loaded_tables.get('_users').records)}")
        for record in self.loaded_tables.get("_users").records:
            print(f"\t{record.data['username']} | Roles: {record.data['roles']}")
        
        print(f"  --Active Tables: {len(self.loaded_tables)}")
        for table_name in self.loaded_tables:
            print(f"\t{table_name} | Length: {len(self.loaded_tables.get(table_name).records)}")
        
        print(f"  --Dormant Tables: {len(self.dormant_tables())}")
        for table_name in self.dormant_tables():
            print(f"\t{table_name}")    
        
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
            for table_name, table in self.loaded_tables.items():
                if table_name == "_users":
                    print(f"Table: {table_name} (User management table)")                
                    print(f"Registered Users: {len(table.records)}")
                
                else:
                    print(f"\nTable: {table_name}")
                    print(f"Records: {len(table.records)}")
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