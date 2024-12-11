from math import inf
from .table import Table
from .record import Record
import multiprocessing as mp
import os
import csv
from tqdm import tqdm

def _process_file_chunk(file_name, chunk_start, chunk_end, delim=',', column_names=None, col_types=None, progress=False, headers=False):
    """
    Process each file chunk in a different process.  
    IDs count up from chunk_start.  
    Args:
        file_name (str): The name of the file to process.
        chunk_start (int): The start position of the chunk in the file.
        chunk_end (int): The end position of the chunk in the file.
        delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
        column_names (list, optional): List of column names to use if headers is False. Defaults to None.
        col_types (list, optional): List of types to cast the columns to. Defaults to None.
        progress (bool, optional): If True, displays a progress bar. Defaults to False.
        headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to False.
    Returns:
        rows (list): A list of Record objects representing the rows in the chunk.
    """
    rows = []
    with open(file_name, 'r') as file:
        file.seek(chunk_start)
        if headers and file.tell() == 0:
            file.readline()  # Skip the header row
        if progress:
            total_lines = chunk_end - chunk_start
            pbar = tqdm(total=total_lines, desc="Processing chunk", unit="line")
        
        while file.tell() < chunk_end:
            line = file.readline().strip()
            if line:
                row_data = line.split(delim)
                if col_types:
                    row_data = [col_type(value) for col_type, value in zip(col_types, row_data)]
                record = Record(file.tell(), dict(zip(column_names, row_data)))
                rows.append(record)
            if progress:
                pbar.update(len(line) + 1)  # Update progress bar with the length of the line plus newline character
        
        if progress:
            pbar.close()
    return rows

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
    
    def create_table_from_csv(self, dir, table_name, headers=True, delim=',', column_names=None, col_types=None, progress=False, parrallel=False, max_chunk_size=None):
        """
        Creates a table in the database from a CSV file.
        Args:
            dir (str): The directory path to the CSV file.
            table_name (str): The name of the table to be created.
            headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
            delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
            column_names (list, optional): List of column names to use if headers is False. Defaults to None.
            col_types (list, optional): List of types to cast the columns to. Defaults to None.
            progress (bool, optional): If True, displays a progress bar. Defaults to False.
            parallel (bool, optional): If True, uses multiprocessing to process the file. Defaults to False.
        Example:
            db.create_table_from_csv('/path/to/file.csv', 'my_table', headers=True, delim=';', column_names=['col1', 'col2'], col_types=[str, int], progress=True)
        """
        if parrallel:
            self._create_table_from_csv_mp(dir, table_name, headers, delim, column_names, col_types, progress, max_chunk_size)
            return
            
        with open(dir, 'r') as file:
            reader = csv.reader(file, delimiter=delim)
            if headers:
                headers = next(reader)
            else:
                headers = column_names if column_names else [f"column{i}" for i in range(len(next(reader)))]
    
            self.create_table(table_name, headers)
    
            if progress:
                reader = tqdm(reader, desc="Processing rows")
    
            for row in reader:
                if col_types:
                    row = [col_type(value) for col_type, value in zip(col_types, row)]
                self.tables[table_name].insert(dict(zip(headers, row)))

    def _create_table_from_csv_mp(self, dir, table_name, headers=True, delim=',', column_names=None, col_types=None, progress=False, max_chunk_size=None):
        """
        Creates a table in the database from a CSV file using multiprocessing.
        Args:
            dir (str): The directory path to the CSV file.
            table_name (str): The name of the table to be created.
            headers (bool, optional): Indicates whether the CSV file contains headers. Defaults to True.
            delim (str, optional): The delimiter used in the CSV file. Defaults to ','.
            column_names (list, optional): List of column names to use if headers is False. Defaults to None.
            col_types (list, optional): List of types to cast the columns to. Defaults to None.
            progress (bool, optional): If True, displays a progress bar. Defaults to False.
        """
        # Warn the user that column_names will be ignored if headers is True
        if headers and column_names:
            print("--Warning: column_names will be ignored if headers is True.--")
        
        # If headers is False and column_names is not provided, generate column names
        with open(dir, 'r') as file:
            reader = csv.reader(file, delimiter=delim)
            if headers:
                column_names = next(reader)                
            else:
                column_names = column_names if column_names else [f"column{i}" for i in range(len(next(reader)))]
                

        # Get the number of CPU cores and split the file into chunks for each core
        cpu_count, file_chunks = self._get_file_chunks(file_name=dir, max_cpu=mp.cpu_count(), headers=headers, max_chunk_size=max_chunk_size)
        
        # Process the file in parallel using multiple CPUs
        records = self._process_file(cpu_count, file_chunks, delim, column_names, col_types, progress, headers)
        
        # Create the table and add the records
        self.create_table(table_name, column_names)
        self.tables[table_name].records = records
                    
    def _get_file_chunks(self, file_name, max_cpu, headers, max_chunk_size=10_000):
        """
        Split file into chunks for processing by multiple CPUs.
        Args:
            file_name (str): The name of the file to process.
            max_cpu (int): The maximum number of CPU cores to use.
            headers (bool): Indicates whether the CSV file contains headers.
            max_chunk_size (int, optional): The maximum size of each chunk in bytes. Defaults to 10_000.
        Returns:
            cpu_count (int): The number of CPU cores to use.
            start_end (list): A list of tuples containing the start and end positions of each file chunk.
        """
        if max_chunk_size is None:
            max_chunk_size = inf
        
        cpu_count = min(max_cpu, mp.cpu_count())    # Determine the number of CPU cores to use
        file_size = os.path.getsize(file_name)      # Get the total size of the file
        chunk_size = min(file_size // cpu_count, max_chunk_size)  # Calculate the size of each chunk based on the number of CPU cores and max_chunk_size

        start_end = list()                          # List to store the start and end positions of each chunk
        
        with open(file_name, mode="r+b") as f:
            # Function to check if the position is at the start of a new line   
            def is_new_line(position):
                if position == 0:
                    return True
                else:
                    f.seek(position - 1)        # Move the file pointer to one byte before the current position
                    return f.read(1) == b"\n"   # Read the byte to check if it is a newline character

            # Function to find the start of the next line from the given position
            def next_line(position):
                f.seek(position)    # Move the file pointer to the given position
                f.readline()        # Read the line to move to the end of it
                return f.tell()     # Return the current position, which is now the start of the next line

            if headers:
                f.readline()  # Skip the header row

            chunk_start = 0
            while chunk_start < file_size:
                chunk_end = min(file_size, chunk_start + chunk_size)    # End of the current chunk

                while not is_new_line(chunk_end):                       # Adjust chunk_end to the start of the next line if necessary
                    chunk_end -= 1

                if chunk_start == chunk_end:                            # If the chunk size is very small, ensure it is moved to the start of the next line
                    chunk_end = next_line(chunk_end)
               
                if chunk_end - chunk_start < chunk_size:                # If the chunk size is less than the target size, adjust the end position
                    chunk_end = next_line(chunk_end)
                
                start_end.append((file_name, chunk_start, chunk_end))   # Append the current chunk information (file name, start, end) to the list
                chunk_start = chunk_end                                 # Move to the next chunk

        return (cpu_count, start_end)
    
    def _process_file(self, cpu_count, start_end, delim, column_names, col_types, progress, headers):
        """
        Process the file in parallel using multiple CPUs.
        Args:
            cpu_count (int): The number of CPU cores to use.
            start_end (list): A list of tuples containing the start and end positions of each file chunk.
            delim (str): The delimiter used in the CSV file.
            column_names (list): List of column names to use if headers is False.
            col_types (list): List of types to cast the columns to.
            progress (bool): If True, displays a progress bar.
            headers (bool): Indicates whether the CSV file contains headers.
        Returns:
            records (list): A list of Record objects representing the rows in the file.
        """       
        # Tasks to be processed by each CPU core
        tasks = [(file_name, chunk_start, chunk_end, delim, column_names, col_types, progress, headers) for file_name, chunk_start, chunk_end in start_end]
    
        with mp.Pool(cpu_count) as pool:
            chunk_rows = pool.starmap(_process_file_chunk, tasks)
            
        # Combine the records from each chunk
        return [record for chunk in chunk_rows for record in chunk]       
    
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