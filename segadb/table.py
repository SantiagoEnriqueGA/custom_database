from .record import Record
from .index import Index

class Table:
    def __init__(self, name, columns):
        """
        Initialize a new table with a name and columns.
        Args:
            name (str): The name of the table.
            columns (list): A list of column names for the table.
        Attributes:
            name (str): The name of the table.
            columns (list): A list of column names for the table.
            records (list): A list to store the records of the table.
            next_id (int): The ID to be assigned to the next record.
            constraints (dict): A dictionary to store constraints for each column.
        """
        self.name = name
        self.columns = columns
        self.records = []
        self.next_id = 1
        self.constraints = {column: [] for column in columns}

    def add_constraint(self, column, constraint, reference_table=None, reference_column=None):
        """
        Adds a constraint to a specified column in the table.
        Args:
            column (str): The name of the column to which the constraint will be added.
            constraint (str): The constraint to be added to the column.
            reference_table (Table, optional): The table that the foreign key references. Required for foreign key constraints.
            reference_column (str, optional): The column in the reference table that the foreign key references. Required for foreign key constraints.
        Raises:
            ValueError: If the specified column does not exist in the table.
        """
        if column in self.constraints:
            if constraint == 'UNIQUE':
                def unique_constraint(value):
                    return all(record.data.get(column) != value for record in self.records)
                unique_constraint.__name__ = "unique_constraint"
                self.constraints[column].append(unique_constraint)
            
            elif constraint == 'FOREIGN_KEY':
                if not reference_table or not reference_column:
                    raise ValueError("Foreign key constraints require a reference table and column.")
                
                def foreign_key_constraint(value):
                    return any(record.data.get(reference_column) == value for record in reference_table.records)
                foreign_key_constraint.__name__ = "foreign_key_constraint"
                foreign_key_constraint.reference_table = reference_table.name
                foreign_key_constraint.reference_column = reference_column
                self.constraints[column].append(foreign_key_constraint)
            
            else:
                self.constraints[column].append(constraint)
        else:
            raise ValueError(f"Column {column} does not exist in the table.")

    def _check_constraints(self, data):
        """
        Checks the constraints for each column in the provided data.
        Args:
            data (dict): A dictionary where keys are column names and values are the data to be checked.
        Raises:
            ValueError: If any constraint is violated for a column.
        """
        for column, constraints in self.constraints.items():
            for constraint in constraints:
                if not constraint(data.get(column)):
                    print(f"Constraint violation on column {column} for value {data.get(column)}")
                    raise ValueError(f"Constraint violation on column {column} for value {data.get(column)}")

    def insert(self, data, record_type=Record, transaction=None):
        """
        Inserts a new record into the table.  
        If a transaction is provided, the operation is added to the transaction.  
        If the data contains an "id" field, it is used as the record ID; otherwise, the next available ID is used.  
        Args:
            data (dict): The data to be inserted as a new record.
            transaction (Transaction, optional): An optional transaction object. If provided, the operation will be added to the transaction.
        Raises:
            ConstraintError: If the data violates any table constraints.
            ValueError: If the ID is already in use.
        Returns:
            None
        """
        self._check_constraints(data)
        record_id = int(data.get("id", self.next_id))
        if any(record.id == record_id for record in self.records):
            raise ValueError(f"ID {record_id} is already in use.")
        if "id" in data:
            del data["id"]
        record = record_type(record_id, data)
        if transaction:
            transaction.add_operation(lambda: self.records.append(record))
        else:
            self.records.append(record)
        self.next_id = max(self.next_id, record_id + 1)
        
    def try_insert(self, data, record_type=Record, transaction=None):
        """
        Attempts to insert data into the table. If an error occurs during the insertion,it catches the ValueError and prints an error message.
        Args:
            data (dict): The data to be inserted into the table.
            transaction (optional): The transaction context for the insertion. Default is None.
        Raises:
            ValueError: If there is an issue with the data insertion.
        """
        try:
            self.insert(data, record_type, transaction)
        except ValueError as e:
            print(f"Error on insert: {e}")

    def delete(self, record_id, transaction=None):
        """
        Deletes a record from the database.
        Args:
            record_id (int): The ID of the record to delete.
            transaction (Transaction, optional): An optional transaction object. If provided, the delete operation will be added to the transaction. Defaults to None.
        """
        if transaction:
            transaction.add_operation(lambda: self._delete(record_id))
        else:
            self._delete(record_id)

    def _delete(self, record_id):
        """
        Deletes a record from the records list based on the given record ID.
        Args:
            record_id (int): The ID of the record to be deleted.
        """
        self.records = [record for record in self.records if record.id != record_id]

    def update(self, record_id, data, transaction=None):
        """
        Update a record in the database.
        Args:
            record_id (int): The ID of the record to update.
            data (dict): A dictionary containing the updated data for the record.
            transaction (optional): A transaction object to add the update operation to. If not provided, the update is performed immediately.
        Raises:
            ConstraintError: If the data violates any constraints.
        """
        self._check_constraints(data)
        if transaction:
            transaction.add_operation(lambda: self._update(record_id, data))
        else:
            self._update(record_id, data)

    def _update(self, record_id, data):
        for record in self.records:
            if record.id == record_id:
                record.data = data

    def select(self, condition):
        """
        Selects records from the table that satisfy the given condition.
        Args:
            condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
        Returns:
            list: A list of records that satisfy the condition.
        """
        return [record for record in self.records if condition(record)]
    
    def print_table(self, limit=None, pretty=False):
        """
        Prints the records in the table.
        Args:
            limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
            pretty (bool, optional): If True, prints the table in a pretty format using the tabulate library. Defaults to False.
        """
        if pretty: 
            self._print_table_pretty(limit)
            return
        
        count = 0
        for record in self.records:
            if limit is not None and count >= limit:
                break
            print(f"Record ID: {record.id}, Data: {record.data}")
            count += 1
            
    def _print_table_pretty(self, limit=None):
        """
        Prints the records in the table in a pretty format using the tabulate library.
        Args:
            limit (int, optional): The maximum number of records to print. If None, all records are printed. Defaults to None.
        """
        from tabulate import tabulate
        table = []
        count = 0
        for record in self.records:
            if limit is not None and count >= limit:
                break
            table.append([record.id, record.data])
            count += 1
        print(tabulate(table, headers=["ID", "Data"]))

    def join(self, other_table, on_column, other_column):
        """
        Perform an inner join with another table on specified columns.
        Args:
            other_table (Table): The table to join with.
            on_column (str): The column in the current table to join on.
            other_column (str): The column in the other table to join on.
        Returns:
            Table: A new table containing the joined records.
        """
        joined_records = []
        for record in self.records:
            for other_record in other_table.records:
                if record.data.get(on_column) == other_record.data.get(other_column):
                    joined_record = {**record.data, **other_record.data}
                    joined_records.append(joined_record)
        
        joined_columns = list(set(self.columns + other_table.columns))
        joined_table = Table(f"{self.name}_join_{other_table.name}", joined_columns)
        for record in joined_records:
            joined_table.insert(record)
        return joined_table

    def aggregate(self, column, agg_func):
        """
        Perform an aggregation on a specified column using the provided aggregation function.
        Args:
            column (str): The column to aggregate.
            agg_func (str): The aggregation function to apply. Supported values are 'MIN', 'MAX', 'COUNT', 'SUM', 'AVG', 'COUNT_DISTINCT'.
        Returns:
            Table: A new table containing the result of the aggregation.
        """
        values = [record.data.get(column) for record in self.records if record.data.get(column) is not None]
        
        if agg_func == 'MIN':
            result = min(values)
        elif agg_func == 'MAX':
            result = max(values)
        elif agg_func == 'COUNT':
            result = len(values)
        elif agg_func == 'SUM':
            result = sum(values)
        elif agg_func == 'AVG':
            result = sum(values) / len(values) if values else 0
        elif agg_func == 'COUNT_DISTINCT':
            result = len(set(values))
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_func}")

        agg_table = Table(f"{self.name}_agg_{column}_{agg_func}", [column])
        agg_table.insert({column: result})
        return agg_table

    def filter(self, condition):
        """
        Filter records based on a condition.
        Args:
            condition (function): A function that takes a record as input and returns True if the record satisfies the condition, False otherwise.
        Returns:
            Table: A new table containing the filtered records.
        """
        filtered_records = [record for record in self.records if condition(record)]
        filtered_table = Table(f"{self.name}_filtered", self.columns)
        for record in filtered_records:
            filtered_table.insert(record.data)
        return filtered_table