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

    def get_data(self):
        """
        Execute the query and return the data for the view.
        Returns:
            list: The data for the view.
        """
        return self.query()
    
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