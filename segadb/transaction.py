import tempfile
import json
from .storage import Storage
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
        original = self.database.copy()
        for operation in self.operations:
            operation()
        new = self.database
        
        # Save original DB to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".segadb") as temp_orig:
            Storage.save(original, temp_orig.name)
            orig_path = temp_orig.name

        # Save modified DB to another temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".segadb") as temp_mod:
            Storage.save(new, temp_mod.name)
            mod_path = temp_mod.name
        
        print(f"Original DB saved to: {orig_path}")
        print(f"Modified DB saved to: {mod_path}")

        # Load the JSON content from the files for comparison
        with open(orig_path, 'rb') as f:
            orig_data = json.loads(f.read().decode('utf-8'))

        with open(mod_path, 'rb') as f:
            mod_data = json.loads(f.read().decode('utf-8'))
        
        # Compare the two JSONs
        try:
            from deepdiff import DeepDiff
            diff = DeepDiff(orig_data, mod_data, ignore_order=True)
            diff_formated = json.dumps(diff, indent=4)
            print("Differences:", diff_formated)
        except:
            print("DeepDiff not available. Performing manual diff:")
            differences = {}
            # Compare keys present in original data
            for key in orig_data:
                if key not in mod_data:
                    differences[key] = {"only_in_original": orig_data[key]}
                elif orig_data[key] != mod_data[key]:
                    differences[key] = {
                        "original": orig_data[key],
                        "modified": mod_data[key]
                    }
            # Check for keys only present in modified data
            for key in mod_data:
                if key not in orig_data:
                    differences[key] = {"only_in_modified": mod_data[key]}
            if differences:
                print("Manual Differences:", json.dumps(differences, indent=4))
            else:
                print("No differences found (manual comparison).")
