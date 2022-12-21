import os
from unittest import TestCase, mock
from lesson_02.ht_template.job2 import main


class MainFunctionTestCases(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    def test_return_400_raw_dir_missed(self):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'stg_dir': '/foo/bar/',
                # no 'raw_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_stg_dir_missed(self):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'stg_dir' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_no_data_for_raw_dir(self):
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                'stg_dir': '/foo/bar/',
            },
        )
        self.assertEqual(400, resp.status_code)

    def test_return_400_different_dirs(self):
        raw_dir = 'lesson_02/ht_template/raw/sales/2022-08-10/2022-08-10.json'
        stg_dir = '/foo/bar/2021-08-10'

        resp = self.client.post(
            '/',
            json={
                'raw_dir': raw_dir,
                'stg_dir': stg_dir,
            },
        )

        self.assertEqual(400, resp.status_code)