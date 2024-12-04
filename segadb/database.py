from .table import Table

class Database:
    def __init__(self, name):
        """
        Initializes a new instance of the database with the given name.
        Args:
            name (str): The name of the database.
        """
        self.name = name
        self.tables = {}

    def create_table(self, table_name, columns):
        """
        Creates a new table in the database.
        Args:
            table_name (str): The name of the table to be created.
            columns (list): A list of column definitions for the table.
        Returns:
            None
        """
        self.tables[table_name] = Table(table_name, columns)

    def drop_table(self, table_name):
        """
        Drops a table from the database.
        Args:
            table_name (str): The name of the table to be dropped.
        """
        del self.tables[table_name]

    def get_table(self, table_name):
        """
        Retrieve a table from the database by its name.
        Args:
            table_name (str): The name of the table to retrieve.
        Returns:
            Table: The table object.
        """
        return self.tables.get(table_name)

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
    
    def create_table_from_csv(self, dir, table_name, headers=True):
        """
        Creates a table in the database from a CSV file.
        Args:
            dir (str): The directory path to the CSV file.
            table_name (str): The name of the table to be created.
            headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
        Example:
            db.create_table_from_csv('/path/to/file.csv', 'my_table', headers=True)
        """
        import csv
        with open(dir, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader) if headers else [f"column{i}" for i in range(len(next(reader)))]
            self.create_table(table_name, headers)
            for row in reader:
                self.tables[table_name].insert(dict(zip(headers, row)))

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
