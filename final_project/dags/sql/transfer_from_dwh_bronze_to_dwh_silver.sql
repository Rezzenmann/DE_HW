DELETE FROM `de2022-stanislav-rezen.silver.sales`
WHERE DATE(purchase_date) = "{{ ds }}"
;

INSERT `de2022-stanislav-rezen.silver.sales` (
    client,
    purchase_date,
    product,
    price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    client,
    CAST(purchase_date AS DATE) AS purchase_date,
    product,
    safe_cast(replace(replace(price, '$', ''), 'USD','') as INTEGER) as price,
    _id,
    _logical_dt,
    _job_start_dt
FROM `de2022-stanislav-rezen.bronze.sales`
WHERE DATE(_logical_dt) = "{{ ds }}"
;