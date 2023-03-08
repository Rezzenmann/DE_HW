customers_csv = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "Id",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "FirstName",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "LastName",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "Email",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "RegistrationDate",
                "mode": "NULLABLE",
                "type": "STRING"
            },
            {
                "name": "State",
                "mode": "NULLABLE",
                "type": "STRING"
            }
        ]
    },
    "csvOptions": {
        "allowJaggedRows": False,
        "allowQuotedNewlines": False,
        "maxBadRecords": 0,
        "encoding": "UTF-8",
        "quote": "\"",
        "fieldDelimiter": ",",
        "skipLeadingRows": 1
    },
    "sourceFormat": "CSV",
    "sourceUris": [
        (
            "gs://{{ params.data_lake_raw_bucket }}"
            "/raw/customers"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d') }}"
            "/*.csv"
        )
    ]
}
