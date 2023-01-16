from typing import List, Dict, Any
import json
import os


def get_sales_from_file(raw_dir: str, date: str) -> List[Dict[str, Any]]:
    """
    :param raw_dir: directory where sales json file located
    :param date: date
    :return: List of Dicts with sales data
    """
    file_path = os.path.join(raw_dir, f'{date}.json')

    with open(file_path, 'r') as f_o:
        data = json.load(f_o)

    return data
