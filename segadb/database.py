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