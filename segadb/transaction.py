class Transaction:
    def __init__(self, database):
        self.database = database
        self.operations = []
        self.database.shadow_copy = None

    def begin(self):
        self.operations = []
        self.database.shadow_copy = self.database.copy()  # Store the shadow copy in the database

    def commit(self):
        for operation in self.operations:
            operation()
        self.operations = []
        self.database.shadow_copy = None  # Discard the shadow copy

    def rollback(self):
        if self.database.shadow_copy:
            self.database.restore(self.database.shadow_copy)  # Restore the database from the shadow copy
        self.operations = []
        self.database.shadow_copy = None

    def add_operation(self, operation):
        self.operations.append(operation)
