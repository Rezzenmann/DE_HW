from lesson_02.ht_template.job1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    # 1. get data from the API
    sales_data = sales_api.get_sales(date)
    # 2. save data to disk
    local_disk.save_to_disk(sales_data, raw_dir, date)
