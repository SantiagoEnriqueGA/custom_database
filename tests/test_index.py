
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.index import Index

class TestIndex(unittest.TestCase):
    """
    Unit tests for the Index class.
    Methods:
    - setUp: Initializes a new instance of the Index class before each test method is run.
    - test_add_and_find: Tests that an item can be added to the index and subsequently found.
    - test_remove: Tests that an item can be removed from the index and is no longer found.
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Index Class")

    def setUp(self):
        self.index = Index()

    def test_add_and_find(self):
        self.index.add("name", 1)
        self.assertIn(1, self.index.find("name"))

    def test_remove(self):
        self.index.add("name", 1)
        self.index.remove("name", 1)
        self.assertNotIn(1, self.index.find("name"))

if __name__ == '__main__':
    unittest.main()