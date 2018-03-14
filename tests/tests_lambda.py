import logging
import os
import unittest

from logging.config import fileConfig
from src.lambda_function import validate_configurations as validate

# create logger assuming running from ./run script
fileConfig('tests/logging_config.ini')
logger = logging.getLogger(__name__)


class TestLambdaFunction(unittest.TestCase):
    """ Unit testing logzio lambda function """

    def setUp(self):
        # Set os.environ for tests
        os.environ['FILEPATH'] = "tests/configurations/valid_configure.json"
        os.environ['URL'] = "url"
        os.environ['TOKEN'] = "1234567890"

    def test_good_config_file(self):
        logger.info("TEST: test_good_config_file")
        try:
            validate()
        except (ValueError, KeyError, RuntimeError, EnvironmentError):
            assert True, "Failed to validate a good configuration file"

    def test_wrong_variable(self):
        logger.info("TEST: test_wrong_variable")

        os.environ['FILEPATH'] = "wrong"
        with self.assertRaises(EnvironmentError):
            validate()
        logger.info("Catched the correct exception, wrong no such file at FILEPATH")

        os.environ['FILEPATH'] = "tests/configurations/missing_variable.json"
        with self.assertRaises(KeyError):
            validate()
        logger.info("Catched the correct exception, missing TimeInterval")

        del os.environ['FILEPATH']
        with self.assertRaises(RuntimeError):
            validate()
        logger.info("Catched the correct exception, no 'FILEPATH'")

    def test_wrong_variable_format(self):
        logger.info("TEST: test_wrong_variable_format")

        os.environ['FILEPATH'] = "tests/configurations/wrong_variable_format.json"
        with self.assertRaises(RuntimeError):
            validate()
        logger.info("Catched the correct exception, wrong format for period")

    def test_wrong_time_ranges(self):
        logger.info("TEST: test_wrong_time_ranges")

        os.environ['FILEPATH'] = "tests/configurations/wrong_time_ranges.json"
        with self.assertRaises(RuntimeError):
            validate()
        logger.info("Catched the correct exception, period can't be bigger than timeInterval")

    def test_duplicate_statistics(self):
        logger.info("TEST: test_duplicate_statistics")

        os.environ['FILEPATH'] = "tests/configurations/duplicate_statistics.json"
        with self.assertRaises(RuntimeError):
            validate()
        logger.info("Catched the correct exception, can't have both Statistics and ExtendedStatistics")


if __name__ == '__main__':
    unittest.main()