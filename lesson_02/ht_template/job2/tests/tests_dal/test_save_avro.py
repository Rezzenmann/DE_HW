from unittest import TestCase
import fastavro.validation
from lesson_02.ht_template.job2.dal.save_avro import save_data_to_avro

SAMPLE_DATA_WRONG = [{"client": "Michael Wilkerson", "purchase_date": "2022-08-10",
                      'quantity': 42.0}]


class TestSaveAvro(TestCase):
    """
    testing dal.save_avro.save_data_to_avro function
    """
    # test if input data file does not pass schema
    def test_save_data_to_avro_wrong_file_schema(self):
        with self.assertRaises(fastavro.validation.ValidationError) as e:
            save_data_to_avro(SAMPLE_DATA_WRONG,
                              'lesson_02/ht_template/stg/sales/2022-08-10/',
                              '2022-08-10')
