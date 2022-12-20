"""
Tests dal.local_disk.py module
# TODO: write tests
"""
from unittest import TestCase, main
import os
from lesson_02.ht_template.job1.dal.local_disk import save_to_disk


BASE_DIR = "D:\DE_HW\lesson_02\ht_template"
DATE = "2022-08-10"
RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", DATE)
SAMPLE_DATA = [{"client": "Michael Wilkerson", "purchase_date": "2022-08-10", "product": "Vacuum cleaner", "price": 346}]


class SaveToDiskTestCase(TestCase):
    """
    Test dal.local_disk.save_to_disk function.
    """
    def test_save_to_disk_success(self):
        save_to_disk(SAMPLE_DATA, RAW_DIR, DATE)
        is_saved = os.path.exists(RAW_DIR)
        self.assertTrue(is_saved)

    def test_save_to_disk_wrong_DIR(self):
        with self.assertRaises(ValueError) as e:
            save_to_disk(SAMPLE_DATA, os.path.join(BASE_DIR, "haha", "lol"), DATE)

