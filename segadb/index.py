class Index:
    def __init__(self):
        """
        Initializes the index dictionary for the class instance.
        """
        self.index = {}

    def add(self, key, record_id):
        """
        Adds a record ID to the index under the specified key.  
        If the key does not exist in the index, it will be created.
        Args:
            key (str): The key under which the record ID will be added.
            record_id (int): The ID of the record to be added to the index.
        """
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(record_id)

    def remove(self, key, record_id):
        """
        Remove a record ID from the index for a given key.
        Args:
            key (str): The key from which the record ID should be removed.
            record_id (Any): The record ID to be removed from the index.
        """
        if key in self.index:
            self.index[key].remove(record_id)
            if not self.index[key]:
                del self.index[key]

    def find(self, key):
        """
        Find the value associated with the given key in the index.
        Args:
            key: The key to search for in the index.
        Returns:
            The value associated with the key if found, otherwise an empty list.
        """
        return self.index.get(key, [])