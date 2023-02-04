"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing
from dotenv import load_dotenv

from lesson_02.ht_template.job2.bll.sales_avro import save_sales_to_local_disk_in_avro

load_dotenv('../../.env')
BASE_DIR = os.environ.get("BASE_DIR")

app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer
    Proposed POST body in JSON:
    {
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
      "stg_dir": "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    if not raw_dir:
        return {
                   "message": "raw_dir parameter missed",
               }, 400

    if not stg_dir:
        return {
                   "message": "stg_dir parameter missed",
               }, 400

    if not os.path.exists(os.path.join(BASE_DIR, raw_dir)):
        return {
            'message': 'no data for that raw_dir'
        }, 400

    if raw_dir[-10:] != stg_dir[-10:]:
        return {
                   'message': 'different dates were sent'
               }, 400

    save_sales_to_local_disk_in_avro(os.path.join(BASE_DIR, raw_dir),
                                     os.path.join(BASE_DIR, stg_dir),
                                     date=raw_dir[-10:])

    return {
               "message": "Data formatted to Avro successfully",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8082)