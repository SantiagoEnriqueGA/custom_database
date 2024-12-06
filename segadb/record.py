from .index import Index

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
        self._index = Index()
        
    @property
    def index(self):
        """
        Returns the index of the record. 
        @property decorator is used to make the method behave like an attribute. (Read-only)
        """
        return self._index

    def add_to_index(self, key):
        self._index.add(key, self)

    def remove_from_index(self, key):
        self._index.remove(key, self)
        
class VectorRecord(Record):
    def __init__(self, record_id, vector):
        """
        Initializes a new instance of the VectorRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            vector (list): The vector data associated with the record.
        """
        super().__init__(record_id, {"vector": vector})
        
class TimeSeriesRecord(Record):
    def __init__(self, record_id, time_series):
        """
        Initializes a new instance of the TimeSeriesRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            time_series (list): The time-series data associated with the record.
        """
        super().__init__(record_id, {"time_series": time_series})

class ImageRecord(Record):
    def __init__(self, record_id, image_data):
        """
        Initializes a new instance of the ImageRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            image_data (bytes): The image data associated with the record.
        """
        super().__init__(record_id, {"image_data": image_data})

class TextRecord(Record):
    def __init__(self, record_id, text):
        """
        Initializes a new instance of the TextRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            text (str): The text data associated with the record.
        """
        super().__init__(record_id, {"text": text})