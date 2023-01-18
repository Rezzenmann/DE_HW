"""
Tests sales_api.py module.
# TODO: write tests
"""
from unittest import TestCase, mock
# NB: avoid relative imports when you will write your code:
from lesson_02.ht_template.job1.dal.sales_api import get_sales


class GetSalesTestCase(TestCase):
    """
    Test sales_api.get_sales function.
    """
    @mock.patch("lesson_02.ht_template.job1.dal.sales_api.requests")
    def test_get_sales_success(self, requests_mock):
        requests_response_mock = mock.MagicMock()
        requests_response_mock.status_code = 200
        requests_mock.get.return_value = requests_response_mock

        self.assertGreater(len(get_sales('2022-08-09')), 0)

    @mock.patch("lesson_02.ht_template.job1.dal.sales_api.requests")
    def test_get_sales_failure(self, requests_mock):
        requests_response_mock = mock.MagicMock()
        requests_response_mock.status_code = 400
        requests_response_mock.json.return_value = []
        requests_mock.get.return_value = requests_response_mock

        self.assertEqual(get_sales('2022-08-01')[1], 400)
