import unittest
from unittest import mock
from doorda_sdk.host.client import connect
from requests.auth import HTTPBasicAuth
from doorda_sdk.util.exc import DatabaseError, ProgrammingError
import os
import json

"""
This is the response to a HTTP request (a GET) from an actual
Presto session. It is deliberately not truncated to document such response
and allow to use it for other tests.
::
    >>> cur.fetchall()
"""
RESP_DATA_GET_0 = json.load(
    open(
        os.path.dirname(os.path.realpath(__file__))
        + "/data/test_success.json",
        "r",
    )
)

RESP_ERROR_GET_0 = json.load(
    open(
        os.path.dirname(os.path.realpath(__file__)) + "/data/test_error.json",
        "r",
    )
)


def get_json_get_0():
    return RESP_DATA_GET_0


def get_json_get_error_0():
    return RESP_ERROR_GET_0


class TestCursor(unittest.TestCase):
    def setUp(self) -> None:
        super().__init__()
        conn = connect(
            username="test",
            password="test",
            catalog="test_catalog",
            schema="test_schema",
        )
        self.cursor = conn.cursor()

    def test_variables(self):
        self.assertEqual(self.cursor.username, "test")
        self.assertEqual(self.cursor.password, "test")
        self.assertEqual(self.cursor.catalog, "test_catalog")
        self.assertEqual(self.cursor.schema, "test_schema")
        self.assertEqual(
            self.cursor._requests_kwargs,
            {"auth": HTTPBasicAuth("test", "test")},
        )

    @mock.patch("requests.post")
    def test_presto_fetchall(self, mock_requests):
        mock_requests.return_value.json = get_json_get_0
        mock_requests.return_value.status_code = 200
        self.cursor.execute("SELECT 1")
        results = self.cursor.fetchone()
        self.assertEqual(list(results), RESP_DATA_GET_0["data"][0])

    @mock.patch("requests.post")
    def test_presto_error(self, mock_requests):
        mock_requests.return_value.json = get_json_get_error_0
        mock_requests.return_value.status_code = 200
        with self.assertRaises(DatabaseError) as error:
            self.cursor.execute("SELECT 1")
        self.assertEqual(error.exception.args[0], RESP_ERROR_GET_0["error"])

    def test_invalid_show_schema(self):
        self.cursor.catalog = None
        with self.assertRaises(ProgrammingError) as error:
            self.cursor.show_schema()
        self.assertEqual(error.exception.args[0], "Catalog not valid")

    def test_invalid_show_table(self):
        self.cursor.catalog = None
        self.cursor.schema = None
        with self.assertRaises(ProgrammingError) as error:
            self.cursor.show_tables()
        self.assertEqual(error.exception.args[0], "Catalog/Schema not valid")
