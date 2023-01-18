from unittest import TestCase, mock
from lesson_02.ht_template.job2.dal.read_sales import get_sales_from_file
import json


SAMPLE_DATA = [{"client": "Michael Wilkerson", "purchase_date": "2022-08-10",
                "product": "Vacuum cleaner", "price": 346}]


class TestReadSales(TestCase):
    """
    tests read_sales function
    """
    def test_read_sales_success(self):
        mock_open = mock.mock_open(read_data=json.dumps(SAMPLE_DATA))
        with mock.patch('builtins.open', mock_open):
            result = get_sales_from_file('lesson_02/ht_template/raw/sales/2022-08-10/2022-08-10.json', '2022-08-10')
        self.assertEqual(SAMPLE_DATA, result)

    def test_read_sales_wrong_(self):
        with self.assertRaises(FileNotFoundError) as e:
            get_sales_from_file("haha", "lol")
