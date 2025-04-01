import inspect

class View:
    def __init__(self, name, query):
        """
        Initialize a new view with a name and query.
        Args:
            name (str): The name of the view.
            query (function): A function that returns the data for the view.
        """
        self.name = name
        self.query = query
        self.query_string = None

    def get_data(self):
        """
        Execute the query and return the data for the view.
        Returns:
            list: The data for the view.
        """
        return self.query()
    
    def _query_to_string(self):
        """
        Return the source code of the query function as a string.
        Returns:
            str: The source code of the query function.
        """
        try:
            return inspect.getsource(self.query)
        except OSError:
            if self.query_string:
                return self.query_string  # Fallback to the previously stored string if source retrieval fails (e.g., in some environments)
            else:
                return "<unable to retrieve source code>"
    
class MaterializedView:
    def __init__(self, name, query):
        """
        Initialize a new materialized view with a name and query.
        Args:
            name (str): The name of the materialized view.
            query (function): A function that returns the data for the materialized view.
        """
        self.name = name
        self.query = query
        self.data = self.query()
        self.query_string = None

    def refresh(self):
        """
        Refresh the data for the materialized view by re-executing the query.
        """
        self.data = self.query()

    def get_data(self):
        """
        Return the data for the materialized view.
        Returns:
            list: The data for the materialized view.
        """
        return self.data
    
    def _query_to_string(self):
        """
        Return the source code of the query function as a string.
        Returns:
            str: The source code of the query function.
        """
        try:
            return inspect.getsource(self.query)
        except OSError:
            if self.query_string:
                return self.query_string  # Fallback to the previously stored string if source retrieval fails (e.g., in some environments)
            else:
                return "<unable to retrieve source code>"