from typing import List, Dict, Any
from fastavro import writer, parse_schema
import shutil
import os


def save_data_to_avro(data: List[Dict[str, Any]], stg_dir: str, date: str) -> None:
    """
    :param data: sales data list
    :param stg_dir: directory to save AVRO file
    :param date: date
    :return: None
    """
    schema = {
        'doc': 'Sales data',
        'name': 'Sales',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'client', 'type': 'string'},
            {'name': 'purchase_date', 'type': 'string'},
            {'name': 'product', 'type': 'string'},
            {'name': 'price', 'type': 'int'},
        ],
    }
    parsed_schema = parse_schema(schema)

    if os.path.exists(stg_dir):
        shutil.rmtree(os.path.abspath(os.path.join(stg_dir, os.pardir)))

    os.makedirs(stg_dir)

    with open(f'{stg_dir}/{date}.avro', 'wb') as f_o:
        writer(f_o, parsed_schema, data)
