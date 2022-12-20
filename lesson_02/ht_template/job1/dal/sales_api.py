from typing import List, Dict, Any
import requests
import os
from dotenv import load_dotenv


load_dotenv('D:\DE_HW\lesson_02\.env')
API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/'
AUTH_TOKEN = os.environ['AUTH_TOKEN']


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.
    :param date: data retrieve the data from
    :return: list of records
    """
    page = 1
    data = []

    while True:
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )
        if int(response.status_code) != 200 or len(response.json()) < 1:
            if page == 1:
                return [{
                           "message": "date parameter is wrong",
                       }, 400]
            break
        data = data + response.json()
        page = page + 1

    return data

