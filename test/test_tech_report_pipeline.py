import unittest
import hashlib
from unittest.mock import patch
from modules.tech_report_pipeline import technology_hash_id
from modules import constants

class TestTechnologyHashId(unittest.TestCase):
    @patch('modules.tech_report_pipeline.constants')
    def test_technology_hash_id_valid(self, patched_constants):
        # Test with valid input
        element = {'key1': 'value1', 'key2': 'value2'}
        query_type = 'valid_query_type'
        patched_constants.TECHNOLOGY_QUERY_ID_KEYS = {query_type: ['key1', 'key2']}
        expected_hash = hashlib.sha256('value1-value2'.encode()).hexdigest()
        self.assertEqual(technology_hash_id(element, query_type), expected_hash)

    def test_technology_hash_id_invalid_query_type(self):
        # Test with invalid query type
        element = {'key1': 'value1', 'key2': 'value2'}
        query_type = 'invalid_query_type'
        with self.assertRaises(ValueError):
            technology_hash_id(element, query_type)

    def test_technology_hash_id_missing_key(self):
        # Test with missing key in element
        element = {'key1': 'value1'}
        query_type = 'valid_query_type'
        constants.TECHNOLOGY_QUERY_ID_KEYS[query_type] = ['key1', 'key2']
        with self.assertRaises(ValueError):
            technology_hash_id(element, query_type)

if __name__ == '__main__':
    unittest.main()