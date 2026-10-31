import unittest
from unittest.mock import Mock

from ingestion.src.api_client import DummyJsonClient


class DummyJsonClientTests(unittest.TestCase):
    def setUp(self):
        self.client = DummyJsonClient()

    def tearDown(self):
        self.client.close()

    def test_fetches_and_validates_paginated_records(self):
        pages = [
            {"products": [{"id": 1}], "total": 2, "skip": 0},
            {"products": [{"id": 2}], "total": 2, "skip": 1},
        ]
        responses = []
        for page in pages:
            response = Mock()
            response.json.return_value = page
            response.raise_for_status.return_value = None
            responses.append(response)
        self.client.session.get = Mock(side_effect=responses)
        self.assertEqual([{"id": 1}, {"id": 2}], self.client.fetch_all("products", page_size=1))
        self.assertEqual(2, self.client.session.get.call_count)

    def test_rejects_invalid_record_id(self):
        response = Mock()
        response.json.return_value = {"users": [{"id": "bad"}], "total": 1, "skip": 0}
        response.raise_for_status.return_value = None
        self.client.session.get = Mock(return_value=response)
        with self.assertRaisesRegex(ValueError, "integer id"):
            self.client.fetch_all("users")


if __name__ == "__main__":
    unittest.main()
