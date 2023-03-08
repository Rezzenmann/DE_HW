MERGE `de2022-stanislav-rezen.silver.customers` main
    USING `de2022-stanislav-rezen.bronze.customers` stage
    ON main.client_id = stage.Id
    WHEN MATCHED THEN
    UPDATE SET
    first_name = stage.FirstName,
    last_name = stage.LastName,
    email = stage.Email,
    registration_date =  cast(stage.RegistrationDate as DATE),
    state = stage.State
    WHEN NOT MATCHED THEN
INSERT (client_id, first_name, last_name, email, registration_date, state, _id, _logical_dt, _job_start_dt)
VALUES(
    stage.Id,
    stage.FirstName,
    stage.LastName,
    stage.Email,
    cast(stage.RegistrationDate as DATE),
    stage.State,
    _id,
    _logical_dt,
    _job_start_dt
    );
