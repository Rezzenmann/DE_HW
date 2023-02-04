from typing import List, Dict, Any
import json
import os
import shutil


BASE_DIR = os.environ.get('BASE_DIR')


# if sales directory exists, it will be deleted
# then creates new directory sales/date and writes file there
def save_to_disk(json_content: List[Dict[str, Any]], path: str, date: str) -> None:
    full_path = os.path.join(BASE_DIR, path)
    if os.path.join('raw', 'sales') not in full_path:
        raise ValueError

    sales_path = os.path.abspath(os.path.join(full_path, os.pardir))

    if os.path.exists(sales_path) and "sales" in str(sales_path):
        shutil.rmtree(sales_path)

    os.makedirs(full_path)
    file_path = os.path.join(full_path, date)

    with open(f'{file_path}.json', 'w') as f_o:
        f_o.write(json.dumps(json_content))