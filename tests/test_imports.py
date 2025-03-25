import unittest
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb.database import Database as db
from segadb.index import Index as i
from segadb.record import Record as r
from segadb.record import VectorRecord as vr
from segadb.record import TimeSeriesRecord as tsr
from segadb.record import ImageRecord as ir
from segadb.record import TextRecord as tr
from segadb.record import EncryptedRecord as er
from segadb.storage import Storage as s
from segadb.table import Table as tbl
from segadb.transaction import Transaction as trans 
from segadb.users import User as u
from segadb.users import UserManager as um
from segadb.users import Authorization as auth
from segadb.views import View as v
from segadb.views import MaterializedView as mv
from segadb.crypto import CustomFernet as cf

from segadb import *

class TestImports(unittest.TestCase):
    """
    Tests that the segadb package can be imported.
    Methods:
    - setUpClass: Initializes a new instance of the Index class before each test method is run.
    - test_individual_imports: Tests that each module in the segadb package can be imported individually.
    - test_wildcard_import: Tests that the segadb package can be imported using a wildcard import.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting Imports", end="", flush=True)
    
    def test_individual_imports(self):
        assert db is not None
        assert i is not None
        assert r is not None
        assert vr is not None
        assert tsr is not None
        assert ir is not None
        assert tr is not None
        assert er is not None
        assert s is not None
        assert tbl is not None
        assert trans is not None
        assert u is not None
        assert um is not None
        assert auth is not None
        assert v is not None
        assert mv is not None
        assert cf is not None

    def test_wildcard_import(self):
        assert Database is not None
        assert Index is not None
        assert Record is not None
        assert VectorRecord is not None
        assert TimeSeriesRecord is not None
        assert ImageRecord is not None
        assert TextRecord is not None
        assert EncryptedRecord is not None
        assert Storage is not None
        assert Table is not None
        assert Transaction is not None
        assert User is not None
        assert UserManager is not None
        assert Authorization is not None
        assert View is not None
        assert MaterializedView is not None
        assert CustomFernet is not None

if __name__ == '__main__':
    unittest.main()