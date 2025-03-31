from typing import Any, List, Dict
class Index:
    """
    Represents an index on a specific column within a table.

    Provides efficient lookups based on column values. Supports unique constraints.
    """
    def __init__(self, name: str, column: str, unique: bool = False):
        """
        Initializes the index.

        Args:
            name (str): The name of the index (e.g., 'idx_email').
            column (str): The name of the column being indexed.
            unique (bool): If True, enforces uniqueness on the indexed column values.
        """
        if not name:
            raise ValueError("Index name cannot be empty.")
        if not column:
            raise ValueError("Index column cannot be empty.")

        self.name = name
        self.column = column
        self.unique = unique
        # The core index structure: maps indexed value to a list of record IDs
        self.index_data: Dict[Any, List[int]] = {}

    def add(self, key: Any, record_id: int):
        """
        Adds a record ID to the index under the specified key (column value).

        Enforces uniqueness if the index is marked as unique.

        Args:
            key: The value from the indexed column.
            record_id: The ID of the record to add.

        Raises:
            ValueError: If adding the key violates the unique constraint.
        """
        if self.unique and key in self.index_data:
            # If key exists and the index should be unique, raise error
            raise ValueError(f"Unique constraint violation in index '{self.name}' on column '{self.column}' for value: {key}")

        if key not in self.index_data:
            self.index_data[key] = []
        # Add record_id only if it's not already present for this key (handles edge cases)
        if record_id not in self.index_data[key]:
             self.index_data[key].append(record_id)

    def remove(self, key: Any, record_id: int):
        """
        Removes a record ID from the index for a given key.

        Args:
            key: The value from the indexed column.
            record_id: The ID of the record to remove.
        """
        if key in self.index_data:
            try:
                self.index_data[key].remove(record_id)
                # If the list for this key becomes empty, remove the key itself
                if not self.index_data[key]:
                    del self.index_data[key]
            except ValueError:
                # Record ID wasn't found for this key, which might happen in complex scenarios
                # but generally indicates a potential inconsistency. Log or handle if necessary.
                # print(f"Warning: Record ID {record_id} not found under key {key} in index {self.name}")
                pass # Silently ignore if id not found for this key

    def find(self, key: Any) -> List[int]:
        """
        Finds the list of record IDs associated with the given key.

        Args:
            key: The value to search for in the index.

        Returns:
            List[int]: A list of record IDs matching the key, or an empty list if the key is not found.
                       Returns a *copy* to prevent external modification of the index.
        """
        return self.index_data.get(key, []).copy()

    def update(self, old_key: Any, new_key: Any, record_id: int):
        """
        Updates a record's position in the index when its indexed value changes.
        Checks for unique violations before modifying the index.

        Args:
            old_key: The previous value in the indexed column.
            new_key: The new value in the indexed column.
            record_id: The ID of the record being updated.

        Raises:
            ValueError: If adding the new_key violates a unique constraint.
        """
        if old_key == new_key:
            return # No change needed

        # --- Check for potential unique violation BEFORE modifying ---
        if self.unique and new_key in self.index_data:
            # Check if the new key belongs to a *different* record ID
            existing_ids = self.index_data[new_key]
            # Important: Ensure existing_ids is not empty before accessing [0]
            if existing_ids and existing_ids[0] != record_id:
                 # Violation: new_key exists and belongs to another record
                 raise ValueError(f"Unique constraint violation in index '{self.name}' on column '{self.column}' for value: {new_key}")
            # If new_key exists but only contains the *current* record_id (e.g., inconsistent state),
            # allow the update to proceed, effectively consolidating.

        # --- If check passes OR it's not a unique index, perform remove and add ---
        self.remove(old_key, record_id)
        try:
            # Add the new key. The self.add method itself might contain redundant
            # unique check depending on its implementation, but it's safer here.
            self.add(new_key, record_id)
        except ValueError as e_add:
            # If add fails unexpectedly *after* the pre-check (e.g., race condition in theory,
            # or flaw in pre-check logic), we MUST try to revert the remove.
            print(f"CRITICAL WARNING: Index.update consistency issue? Attempting to revert remove for {old_key}/{record_id}. Error on add({new_key}): {e_add}")
            # Re-add the old key/id pair. This assumes add won't fail for the old key.
            # Use internal dict directly to bypass potential unique checks in add for the revert.
            if old_key not in self.index_data:
                 self.index_data[old_key] = []
            if record_id not in self.index_data[old_key]:
                 self.index_data[old_key].append(record_id)
            # Re-raise the exception that caused the failure during add
            raise e_add

    def get_all_keys(self) -> List[Any]:
        """Returns a list of all keys currently in the index."""
        return list(self.index_data.keys())

    def clear(self):
        """Removes all entries from the index."""
        self.index_data = {}

    def to_dict_definition(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the index *definition* (for saving).
        Does NOT include the actual index data map.
        """
        return {
            "name": self.name,
            "column": self.column,
            "unique": self.unique
        }

    def __len__(self) -> int:
        """Returns the number of unique keys in the index."""
        return len(self.index_data)

    def __str__(self) -> str:
        """Returns a string representation of the index definition."""
        return f"Index(name='{self.name}', column='{self.column}', unique={self.unique}, keys={len(self)})"

    def __repr__(self) -> str:
        return self.__str__()