import unittest
import os
import sys

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def load_tests(loader, standard_tests, pattern):
    suite = unittest.TestSuite()
    for all_test_suite in unittest.defaultTestLoader.discover('tests', pattern='test*.py'):
        for test_suite in all_test_suite:
            if isinstance(test_suite, unittest.TestSuite):
                for test_case in test_suite:
                    if test_case.__class__.__name__ not in ['TestDatabasePerformance', 'TestExamples', 'TestExampleExceptions']:
                        suite.addTest(test_case)
    return suite

if __name__ == '__main__':
    # Discover and run selected tests in the 'tests' directory
    loader = unittest.TestLoader()
    tests = load_tests(loader, None, 'test*.py')
    testRunner = unittest.TextTestRunner()
    testRunner.run(tests)
