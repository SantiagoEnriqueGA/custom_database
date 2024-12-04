class Record:
    def __init__(self, record_id, data):
        """
        Initializes a new instance of the class.
        Args:
            record_id (int): The unique identifier for the record.
            data (dict): The data associated with the record.
        """
        self.id = record_id
        self.data = data