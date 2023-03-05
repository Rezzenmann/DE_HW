DELETE FROM `gold.user_profiles_enriched` WHERE TRUE;

insert into `gold.user_profiles_enriched`
select
client_id,
split(full_name, ' ')[ORDINAL(2)] as first_name,
split(full_name, ' ')[ORDINAL(1)] as last_name,
email,
registration_date,
sup.state,
birth_date,
phone_number,
GENERATE_UUID() AS _id,
CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS _logical_dt,
CAST('{{ dag_run.start_date }}'AS TIMESTAMP) AS _job_start_dt
from `silver.customers` as sc
full join `silver.user_profiles` as sup
using(email);

