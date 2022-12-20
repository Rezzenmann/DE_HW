from lesson_02.ht_template.job2.dal import read_sales, save_avro


def save_sales_to_local_disk_in_avro(raw_dir: str, stg_dir: str, date: str) -> None:
    """
    :param raw_dir: directory where sales json file located
    :param stg_dir: directory to save AVRO file
    :param date: sales date
    :return: None
    """
    # 1. get data from the file
    sales = read_sales.get_sales_from_file(raw_dir, date)
    # 2. save data to avro
    save_avro.save_data_to_avro(sales, stg_dir, date)

