import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import os

class CustomFernet:
    """
    A custom implementation of the Fernet symmetric encryption using AES in CBC mode.
    """
    def __init__(self, key):
        """
        Initializes the Crypto class with the given key.
        Args:
            key (str): The base64 encoded key used for encryption and decryption.
        Attributes:
            key (bytes): The decoded key used for encryption and decryption.
            backend (object): The backend used for cryptographic operations.
            block_size (int): The block size for the AES algorithm in bytes.
        """
        self.key = base64.urlsafe_b64decode(key)            # Decode the key
        self.backend = default_backend()                    # Get the default backend
        self.block_size = algorithms.AES.block_size // 8    # Get the block size in bytes

    @staticmethod
    def generate_key():
        """
        Generates a secure random key.  
        This function generates a 32-byte secure random key and encodes it using URL-safe Base64 encoding.
        Returns:
            str: A URL-safe Base64 encoded string representing the secure random key.
        """
        return base64.urlsafe_b64encode(os.urandom(32)).decode()

    def encrypt(self, data):
        """
        Encrypts the provided data using AES encryption with CBC mode and PKCS7 padding.
        Args:
            data (str): The plaintext data to be encrypted.
        Returns:
            str: The encrypted data encoded in URL-safe base64 format.
        """
        iv = os.urandom(self.block_size)                                                # Generate a random IV (Initialization Vector)
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=self.backend)  # Create a new AES cipher in CBC mode (cipher block chaining)
        encryptor = cipher.encryptor()                                                  # Create an encryptor object
        padder = padding.PKCS7(algorithms.AES.block_size).padder()                      # Create a padder object (PKCS7 padding)
        padded_data = padder.update(data.encode()) + padder.finalize()                  # Pad the data and encode it
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()           # Encrypt the padded data
        return base64.urlsafe_b64encode(iv + encrypted_data).decode()                   # Return the base64 encoded IV + encrypted data

    def decrypt(self, token):
        """
        Decrypts the given token using AES encryption in CBC mode.
        Args:
            token (str): The base64 encoded string to be decrypted.
        Returns:
            str: The decrypted data as a string.
        """
        token = base64.urlsafe_b64decode(token)                                         # Decode the token from URL-safe base64 format
        iv = token[:self.block_size]                                                    # Get the IV from the token
        encrypted_data = token[self.block_size:]                                        # Get the encrypted data from the token
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=self.backend)  # Create a new AES cipher in CBC mode
        decryptor = cipher.decryptor()                                                  # Create a decryptor object
        padded_data = decryptor.update(encrypted_data) + decryptor.finalize()           # Decrypt the data
        unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()                  # Create an unpadder object
        data = unpadder.update(padded_data) + unpadder.finalize()                       # Unpad the data
        return data.decode()                                                            # Decode the data and return it
