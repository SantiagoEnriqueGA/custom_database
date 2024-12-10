from .table import Table
from .record import Record

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
    
    def _create_table_from_dict(self, table_data):
        """
        Creates a new table in the database from a dictionary.  
        Dictionary must contain the following keys: 'name', 'columns', 'records', and 'constraints'.
        """
        table = Table(table_data['name'], table_data['columns'])
        table.records = [Record(record['id'], record['data']) for record in table_data['records']]
        table.constraints = table_data['constraints']
        self.tables[table_data['name']] = table
        
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
    
    def create_table_from_csv(self, dir, table_name, headers=True, delim=',', column_names=None):
        """
        Creates a table in the database from a CSV file.
        Args:
            dir (str): The directory path to the CSV file.
            table_name (str): The name of the table to be created.
            headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
            delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
            column_names (list, optional): List of column names to use if headers is False. Defaults to None.
        Example:
            db.create_table_from_csv('/path/to/file.csv', 'my_table', headers=True, delim=';', column_names=['col1', 'col2'])
        """
        import csv
        with open(dir, 'r') as file:
            reader = csv.reader(file, delimiter=delim)
            if headers:
                headers = next(reader)
            else:
                headers = column_names if column_names else [f"column{i}" for i in range(len(next(reader)))]
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

    def join_tables(self, table_name1, table_name2, on_column, other_column):
        """
        Perform an inner join between two tables on specified columns.
        Args:
            table_name1 (str): The name of the first table.
            table_name2 (str): The name of the second table.
            on_column (str): The column in the first table to join on.
            other_column (str): The column in the second table to join on.
        Returns:
            Table: A new table containing the joined records.
        """
        table1 = self.get_table(table_name1)
        table2 = self.get_table(table_name2)
        if table1 and table2:
            return table1.join(table2, on_column, other_column)
        else:
            raise ValueError("One or both tables do not exist.")

    def aggregate_table(self, table_name, group_column,  agg_column, agg_func):
        """
        Perform an aggregation on a specified column in a table using the provided aggregation function.
        Args:
            table_name (str): The name of the table.
            group_column (str): The column to group by.
            agg_column (str): The column to aggregate.
            agg_func (str): The aggregation function to apply. Supported values are 'MIN', 'MAX', 'COUNT', 'SUM', 'AVG', 'COUNT_DISTINCT'.
        Returns:
            Table: A new table containing the result of the aggregation.
        """
        table = self.get_table(table_name)
        if table:
            return table.aggregate(group_column,  agg_column, agg_func)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def filter_table(self, table_name, condition):
        """
        Filter records in a table based on a condition.
        Args:
            table_name (str): The name of the table.
            condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
                                  Example: lambda record: record.data["product"] == "Laptop"
        Returns:
            Table: A new table containing the filtered records.
        """
        table = self.get_table(table_name)
        if table:
            return table.filter(condition)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def print_db(self, index=False):
        """
        Print the database tables, including their names, columns, constraints, and records.
        Args:
            index (bool, optional): Whether to print the index of each record. Defaults to False.
        """
        print(f"Database: {self.name}")
        for table_name, table in self.tables.items():
            print(f"\nTable: {table_name}")
            print(f"Columns: {table.columns}")
            
            consts = []
            for constraint in table.constraints:
                if len(table.constraints[constraint]) == 1:
                    consts.append(f"{constraint}: {table.constraints[constraint][0].__name__}")
                
            print(f"Constraints: {consts}")
                
            table.print_table(pretty=True, index=index)