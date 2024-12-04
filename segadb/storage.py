import json

# Note: these are static methods, so they don't need to be instantiated
class Storage:
    @staticmethod
    def save(database, filename):
        """
        Save the database to a file in JSON format.
        Args:
            database (dict): The database to be saved.
            filename (str): The name of the file where the database will be saved.
        """
        with open(filename, 'w') as file:
            json.dump(database, file, default=lambda o: o.__dict__, indent=4)

    @staticmethod
    def load(filename):
        """
        Load data from a JSON file and reconstruct the database object.
        Args:
            filename (str): The path to the JSON file to be loaded.
        Returns:
            dict: The data loaded from the JSON file.
        """
        with open(filename, 'r') as file:
            data = json.load(file)
            return data
        
    @staticmethod
    def delete(filename):
        """
        Delete the specified file from the filesystem.
        Args:
            filename (str): The path to the file to be deleted.
        """
        import os
        os.remove(filename)
        