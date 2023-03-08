MERGE `de2022-stanislav-rezen.silver.user_profiles` main
    USING user_profiles_jsonl as stage
    ON main.email = stage.email
    WHEN MATCHED THEN
    UPDATE SET
    full_name = stage.full_name,
    state = stage.state,
    birth_date = CAST(stage.birth_date as DATE),
    phone_number = stage.phone_number
    WHEN NOT MATCHED THEN
INSERT (email, full_name, state, birth_date, phone_number, _id, _logical_dt, _job_start_dt)
VALUES(
    stage.email,
    stage.full_name,
    stage.state,
    cast(stage.birth_date as DATE),
    stage.phone_number,
    GENERATE_UUID(),
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP),
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP)
    );
