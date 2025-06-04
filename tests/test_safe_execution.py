
import unittest
import logging
import os
import tempfile

from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.db_navigator import *

# TODO: Stop error logging in tests

def reset_logging():
    """Resets logging handlers to release the log file."""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        handler.close()  # Ensure file handlers are closed

class TestSafeExecution(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("\nTesting safe_execution decorator", end="", flush=True)

    def test_successful_function(self):
        @safe_execution
        def add(a, b):
            return a + b
        self.assertEqual(add(2, 3), 5)

    def test_function_with_exception(self):
        @safe_execution
        def divide(a, b):
            return a / b  # This will raise a ZeroDivisionError if b = 0
        self.assertIsNone(divide(4, 0))  # Expect None since division by zero should be caught

    def test_function_with_side_effect(self):
        @safe_execution
        def risky_function():
            raise ValueError("This is a test error")
        self.assertIsNone(risky_function())  # The function should return None on exception

    def test_function_with_arguments(self):
        @safe_execution
        def greet(name):
            return f"Hello, {name}!"
        self.assertEqual(greet("Alice"), "Hello, Alice!")


    def test_logging_on_exception(self):
        with tempfile.NamedTemporaryFile(delete=False) as temp_log:
            log_file = temp_log.name  # Get temporary file name

        reset_logging()
        
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        @safe_execution
        def error_function():
            raise RuntimeError("Intentional error")

        error_function()

        reset_logging()

        with open(log_file, "r") as file:
            logs = file.read()

        self.assertIn("Error in error_function", logs)

        os.remove(log_file)  # Cleanup


if __name__ == '__main__':
    unittest.main()
