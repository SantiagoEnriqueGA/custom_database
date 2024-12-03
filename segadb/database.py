from .table import Table

class Database:
    def __init__(self, name):
        self.name = name
        self.tables = {}

    def create_table(self, table_name, columns):
        self.tables[table_name] = Table(table_name, columns)

    def drop_table(self, table_name):
        del self.tables[table_name]

    def get_table(self, table_name):
        return self.tables.get(table_name)

    def copy(self):
        # Create a deep copy of the database state
        import copy
        return copy.deepcopy(self)

    def restore(self, state):
        # Restore the database state from shadow copy
        self.tables = state.tables
        self.name = state.name
        return self