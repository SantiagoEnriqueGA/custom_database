class Transaction:
    def __init__(self):
        self.operations = []

    def add_operation(self, operation):
        self.operations.append(operation)

    def commit(self):
        for operation in self.operations:
            operation.execute()

    def rollback(self):
        for operation in reversed(self.operations):
            operation.undo()