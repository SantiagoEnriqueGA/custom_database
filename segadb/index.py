class Index:
    def __init__(self):
        """
        Initializes the index dictionary for the class instance.
        """
        self.index = {}

    def add(self, key, record):
        """
        Adds a record to the index under the specified key.  
        If the key does not exist in the index, it will be created.
        Args:
            key (str): The key under which the record will be added.
            record (Any): The record to be added to the index.
        """
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(record)

    def remove(self, key, record):
        """
        Remove a record from the index for a given key.
        Args:
            key (str): The key from which the record should be removed.
            record (Any): The record to be removed from the index.
        """
        if key in self.index:
            self.index[key].remove(record)
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
    
    def __str__(self):
        """
        Returns a string representation of the index.
        Returns:
            str: A string representation of the index.
        """
        return str(f"Index Object: {self.index}")