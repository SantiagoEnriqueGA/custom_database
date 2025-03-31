# Imports: Standard Library
import math
import io
import base64
import os

# Imports: Third Party
from PIL import Image

# Imports: Local
# No longer need to import Index here directly for Record itself
# from .index import Index
from .crypto import CustomFernet

class Record:
    def __init__(self, record_id, data):
        """
        Initializes a new instance of the Record class.
        Args:
            record_id (int): The unique identifier for the record.
            data (dict): The data associated with the record.
        """
        self.id = record_id
        self.data = data
        # REMOVED: self._index = Index() - Records don't own indexes

    # REMOVED: index property getter
    # @property
    # def index(self):
    #     """
    #     Returns the index of the record.
    #     @property decorator is used to make the method behave like an attribute. (Read-only)
    #     """
    #     return self._index

    # REMOVED: add_to_index method
    # def add_to_index(self, key):
    #     self._index.add(key, self)

    # REMOVED: remove_from_index method
    # def remove_from_index(self, key):
    #     self._index.remove(key, self)

    def _type(self):
        return "Record"

# --- Subclasses (VectorRecord, TimeSeriesRecord, etc.) ---
# No changes are needed in the subclasses as they inherit from the fixed Record class.

class VectorRecord(Record):
    def __init__(self, record_id, vector):
        """
        Initializes a new instance of the VectorRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            vector (list or dict): The vector data associated with the record.
                                  If dict, expects a 'vector' key.
        """
        # Handle if input is just the list or a dict containing it
        if isinstance(vector, dict) and 'vector' in vector:
             vector_data = vector
        elif isinstance(vector, list):
             vector_data = {"vector": vector}
        else:
            raise ValueError("VectorRecord requires a list or a dict with a 'vector' key.")
        super().__init__(record_id, vector_data)


    @property
    def vector(self):
        # Ensure the 'vector' key exists
        return self.data.get("vector", []) # Return empty list if key missing

    def magnitude(self):
        """
        Calculates the magnitude of the vector.
        Returns:
            float: The magnitude of the vector.
        """
        vec = self.vector
        if not vec: return 0.0
        try:
             # Ensure elements are numeric
             return math.sqrt(sum(float(x)**2 for x in vec))
        except (ValueError, TypeError):
             # Handle non-numeric data gracefully
             print(f"Warning: Non-numeric data found in vector for record {self.id}. Cannot calculate magnitude.")
             return 0.0

    def normalize(self):
        """
        Normalizes the vector.
        Returns:
            list: The normalized vector. Returns original if magnitude is 0 or vector is invalid.
        """
        mag = self.magnitude()
        vec = self.vector
        if mag == 0 or not vec:
            return vec # Return original vector (or empty list)
        try:
             return [float(x) / mag for x in vec]
        except (ValueError, TypeError):
            print(f"Warning: Non-numeric data found in vector for record {self.id}. Cannot normalize.")
            return vec # Return original vector

    def dot_product(self, other_vector):
        """
        Calculates the dot product with another vector.
        Args:
            other_vector (list): The other vector to calculate the dot product with.
        Returns:
            float: The dot product of the two vectors. Returns 0.0 if vectors are invalid or mismatched length.
        """
        vec = self.vector
        if not vec or len(vec) != len(other_vector):
            print(f"Warning: Vector length mismatch or empty vector for record {self.id}. Cannot calculate dot product.")
            return 0.0
        try:
            return sum(float(x) * float(y) for x, y in zip(vec, other_vector))
        except (ValueError, TypeError):
             print(f"Warning: Non-numeric data found in vectors for record {self.id}. Cannot calculate dot product.")
             return 0.0

    def _type(self):
        return "VectorRecord"


class TimeSeriesRecord(Record):
    def __init__(self, record_id, time_series):
        """
        Initializes a new instance of the TimeSeriesRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            time_series (list or dict): The time series data. If dict, expects 'time_series' key.
        """
        if isinstance(time_series, dict) and 'time_series' in time_series:
            ts_data = time_series
        elif isinstance(time_series, list):
            ts_data = {"time_series": time_series}
        else:
            raise ValueError("TimeSeriesRecord requires a list or a dict with a 'time_series' key.")
        super().__init__(record_id, ts_data)

    @property
    def time_series(self):
        return self.data.get("time_series", [])

    def moving_average(self, window_size):
        """
        Calculates the moving average of the time series.
        Args:
            window_size (int): The window size for the moving average.
        Returns:
            list: The moving average. Returns empty list if data is invalid or window is too large.
        """
        ts = self.time_series
        if not ts or window_size <= 0 or window_size > len(ts):
            return []
        try:
             # Ensure data is numeric
             numeric_ts = [float(x) for x in ts]
             return [sum(numeric_ts[i:i+window_size]) / window_size for i in range(len(numeric_ts) - window_size + 1)]
        except (ValueError, TypeError):
             print(f"Warning: Non-numeric data found in time series for record {self.id}. Cannot calculate moving average.")
             return []

    def _type(self):
        return "TimeSeriesRecord"

class ImageRecord(Record):
    def __init__(self, record_id, image_data_input):
        """
        Initializes a new instance of the ImageRecord class.

        Args:
            record_id (int): The unique identifier for the record.
            image_data_input (dict or str): Either a dictionary containing the key 'image_data'
                                           with the path or base64 string, OR just the path/base64 string directly.
        """
        image_path_or_b64 = None
        if isinstance(image_data_input, dict):
            image_path_or_b64 = image_data_input.get('image_data')
        elif isinstance(image_data_input, str):
            image_path_or_b64 = image_data_input

        if not image_path_or_b64:
            raise ValueError("ImageRecord requires 'image_data' key in dict or a non-empty string.")

        image_data_bytes = None
        resolved_path = "N/A"

        # Try interpreting as a file path first
        try:
            # Check if it *looks* like a path before trying to open
            # This is imperfect but avoids trying to open base64 strings as files
            if isinstance(image_path_or_b64, str) and ('/' in image_path_or_b64 or '\\' in image_path_or_b64 or os.path.exists(image_path_or_b64)):
                with open(image_path_or_b64, "rb") as image_file:
                    image_data_bytes = image_file.read()
                resolved_path = image_path_or_b64
        except (FileNotFoundError, OSError, TypeError):
             # If it's not a valid path or opening fails, try base64 decoding
             try:
                  # Attempt decoding only if it wasn't successfully read as a file
                  if image_data_bytes is None and isinstance(image_path_or_b64, str):
                       # Basic check if it might be base64 (length, padding)
                       if len(image_path_or_b64) % 4 == 0 and len(image_path_or_b64) > 10:
                            image_data_bytes = base64.b64decode(image_path_or_b64, validate=True)
                       else:
                            raise ValueError("Input string doesn't look like a file path or valid Base64.")
                  elif image_data_bytes is None: # If input wasn't a string path or b64
                     raise ValueError("Invalid input for ImageRecord.")

             except (base64.binascii.Error, ValueError, Exception) as e:
                  raise ValueError(f"Invalid image input for record {record_id}. Not a valid file path or Base64 string. Error: {e}")


        # Ensure we have bytes by the end
        if image_data_bytes is None:
             raise ValueError(f"Could not load image data for record {record_id} from input: {image_path_or_b64}")


        # Initialize the parent Record class
        super().__init__(record_id, {"image_data": image_data_bytes, "image_path": resolved_path})

    @property
    def image_data(self):
        """Raw image data as bytes."""
        return self.data.get("image_data")

    @property
    def image_path(self):
        """Original path if loaded from file, otherwise 'N/A'."""
        return self.data.get("image_path", "N/A")

    @property
    def image_size(self):
        """Returns the size of the image data in bytes."""
        img_data = self.image_data
        return len(img_data) if img_data else 0

    def get_image(self):
        """
        Converts the image data to a PIL Image object.
        Returns:
            Image: The PIL Image object, or None if data is invalid.
        """
        img_data = self.image_data
        if not img_data:
            return None
        try:
            return Image.open(io.BytesIO(img_data))
        except Exception as e:
            print(f"Error opening image data for record {self.id}: {e}")
            return None

    def resize(self, percentage):
        """
        Resizes the image by a given percentage.
        Args:
            percentage (float): The percentage factor (e.g., 0.5 for 50%).
        Returns:
            Image: The resized PIL Image object, or None if resizing fails.
        """
        image = self.get_image()
        if not image or percentage <= 0:
            return None
        try:
            width, height = image.size
            new_width = int(width * percentage)
            new_height = int(height * percentage)
            # Ensure minimum size of 1x1
            new_width = max(1, new_width)
            new_height = max(1, new_height)
            resized_image = image.resize((new_width, new_height))
            return resized_image
        except Exception as e:
            print(f"Error resizing image for record {self.id}: {e}")
            return None

    def _convert_to_base64(self):
        """
        Converts the image data to a base64 encoded string.
        Returns:
            str: The base64 encoded image data, or None if no data.
        """
        img_data = self.image_data
        return base64.b64encode(img_data).decode() if img_data else None

    def to_dict(self):
        """
        Converts the ImageRecord to a dictionary suitable for saving (encodes image data).
        Returns:
            dict: The dictionary representation of the ImageRecord.
        """
        # Start with a copy of the data, but we'll overwrite image_data
        save_data = {
             "image_path": self.image_path, # Keep original path info
             "image_data": self._convert_to_base64() # Store encoded data
        }
        return save_data

    def _type(self):
        return "ImageRecord"


class TextRecord(Record):
    def __init__(self, record_id, text):
        """
        Initializes a new instance of the TextRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            text (str or dict): The text data. If dict, expects a 'text' key.
        """
        if isinstance(text, dict) and 'text' in text:
            text_data = text
        elif isinstance(text, str):
            text_data = {"text": text}
        else:
             raise ValueError("TextRecord requires a string or a dict with a 'text' key.")
        super().__init__(record_id, text_data)


    @property
    def text(self):
        return self.data.get("text", "")

    def word_count(self):
        """
        Counts the number of words in the text.
        Returns:
            int: The number of words in the text.
        """
        return len(self.text.split())

    def to_uppercase(self):
        """
        Converts the text to uppercase.
        Returns:
            str: The text in uppercase.
        """
        return self.text.upper()

    def to_lowercase(self):
        """
        Converts the text to lowercase.
        Returns:
            str: The text in lowercase.
        """
        return self.text.lower()

    def _type(self):
        return "TextRecord"


class EncryptedRecord(Record):
    # TODO: add max try count and timeout options for decryption attempts
    def __init__(self, record_id, data):
        """
        Initializes a new instance of the EncryptedRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            data (dict): Data containing 'data' (plaintext or ciphertext) and optionally 'key'.
                         If 'key' is None or missing, assumes 'data' is already encrypted ciphertext.
                         If 'key' is present, 'data' is treated as plaintext to be encrypted.
        """
        key = data.get("key")
        data_value = data.get("data")

        if data_value is None:
             raise ValueError("EncryptedRecord requires 'data' field in input dictionary.")

        if key is None:
            # Assume data_value is already encrypted ciphertext (likely during load)
            encrypted_data_str = data_value
            # Ensure it's a string, might be bytes during load? Handle defensively.
            if isinstance(encrypted_data_str, bytes):
                try:
                    encrypted_data_str = encrypted_data_str.decode('utf-8')
                except UnicodeDecodeError:
                    # If it's not valid UTF-8, it's likely raw encrypted bytes, not b64 string yet.
                    # This scenario shouldn't happen if save/load uses b64 properly.
                    # If it does, we might need to handle raw bytes differently or fix save/load.
                    # For now, assume it should be a base64 string representation.
                    raise ValueError("Encrypted data loaded in unexpected format (non-UTF8 bytes).")

        else:
            # Key is provided, so data_value is plaintext to be encrypted
            if not isinstance(data_value, str):
                 # Ensure plaintext is a string before encryption
                 data_value = str(data_value)
            try:
                fernet = CustomFernet(key)
                encrypted_data_str = fernet.encrypt(data_value) # encrypt returns base64 string
            except Exception as e:
                 raise ValueError(f"Failed to encrypt data for record {record_id}: {e}")

        # Store only the encrypted data string in the record's data dict
        super().__init__(record_id, {"data": encrypted_data_str})
        # Store the encrypted data separately for direct access if needed (optional)
        # self._encrypted_data = encrypted_data_str


    @property
    def encrypted_data(self):
        """Returns the encrypted data string stored in the record."""
        return self.data.get("data")

    def decrypt(self, key):
        """
        Decrypts the data using the provided decryption key.
        Args:
            key (str): The base64 encoded key used for decryption.
        Returns:
            str: The decrypted data.
        Raises:
            ValueError: If decryption fails (e.g., wrong key, corrupted data).
        """
        encrypted_data_str = self.encrypted_data
        if not encrypted_data_str:
            return "" # Or None, or raise error?

        try:
            fernet = CustomFernet(key)
            return fernet.decrypt(encrypted_data_str) # decrypt takes base64 string
        except Exception as e:
            # Don't return error messages directly, raise an exception
            # This allows calling code to handle the failure appropriately.
            raise ValueError(f"Decryption failed for record {self.id}: {e}")

    def _type(self):
        return "EncryptedRecord"