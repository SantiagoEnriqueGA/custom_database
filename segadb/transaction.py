class Transaction:
    def __init__(self, database):
        """
        Initializes a new transaction instance.
        Args:
            database: The database object to associate with this transaction.

        Attributes:
            database: The database object associated with this transaction.
            operations: A list to store the operations performed in this transaction.
        """
        self.database = database
        self.operations = []
        self.database.shadow_copy = None

    def begin(self):
        """
        Begins a new transaction.  
        This method initializes the operations list and creates a shadow copy
        of the current state of the database. The shadow copy is stored in the
        database to allow for rollback if needed.
        """
        self.operations = []
        self.database.shadow_copy = self.database.copy()  # Store the shadow copy in the database

    def commit(self):
        """
        Commits the current transaction by executing all operations in the transaction.  
        This method iterates over the list of operations and executes each one. After all operations
        are executed, it clears the list of operations and discards the shadow copy of the database.
        """
        for operation in self.operations:
            operation()
        self.operations = []
        self.database.shadow_copy = None  # Discard the shadow copy

    def rollback(self):
        """
        Reverts the database to its previous state using a shadow copy if available.  
        This method restores the database to the state saved in the shadow copy,
        clears the list of operations, and removes the shadow copy reference.
        """
        if self.database.shadow_copy:
            self.database.restore(self.database.shadow_copy)  # Restore the database from the shadow copy
        self.operations = []
        self.database.shadow_copy = None

    def add_operation(self, operation):
        """
        Adds an operation to the list of operations.
        Args:
            operation: The operation to be added to the list.
        """
        self.operations.append(operation)
        
    def copy(self):
        """
        Copies a transaction object.
        """
        import copy
        return copy.deepcopy(self)

    def preview(self):
        """
        Previews the operations in the current transaction without committing or rolling back.
        """
        # TODO: Compare with original DB to show differences
        for operation in self.operations:
            operation()
            
        # Add preview to DB name
        self.database.name = self.database.name + "_preview"
            
        self.database.print_db()
        
        # Reset DB name
        self.database.name = self.database.name[:-8]
        
