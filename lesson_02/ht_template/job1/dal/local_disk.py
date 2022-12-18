from typing import List, Dict, Any
import json
import os
import shutil


# if sales directory exists, it will be deleted
# then creates new directory sales/date and writes file there
def save_to_disk(json_content: List[Dict[str, Any]], path: str, date: str) -> None:
    sales_path = os.path.abspath(os.path.join(path, os.pardir))
    print(sales_path)
    if os.path.exists(sales_path) and "sales" in str(sales_path):
        shutil.rmtree(sales_path)

    os.makedirs(path)
    file_path = os.path.join(path, date)

    with open(f'{file_path}.json', 'w') as f_o:
        f_o.write(json.dumps(json_content))