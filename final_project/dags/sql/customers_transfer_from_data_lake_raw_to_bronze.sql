MERGE `de2022-stanislav-rezen.bronze.customers` main
    USING (
        select distinct * from customers_csv) stage
    ON main.Id = stage.Id
    WHEN MATCHED THEN
    UPDATE SET
    FirstName = stage.FirstName,
    LastName = stage.LastName,
    Email = stage.Email,
    RegistrationDate = stage.RegistrationDate,
    State = stage.State
WHEN NOT MATCHED THEN
INSERT (Id, FirstName, LastName, Email, RegistrationDate, State, _id, _logical_dt, _job_start_dt)
VALUES(
    stage.Id,
    stage.FirstName,
    stage.LastName,
    stage.Email,
    stage.RegistrationDate,
    stage.State,
    GENERATE_UUID(),
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP),
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP)
    );
