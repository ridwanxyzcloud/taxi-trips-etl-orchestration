CREATE OR REPLACE PROCEDURE "STG".agg_trip_data()
RETURNS STRING
LANGUAGE SQL
AS $$
BEGIN
    -- Begin transaction
    BEGIN TRANSACTION;

    -- Insert aggregated data into daily_agg_trip_data
    INSERT INTO EDW.daily_agg_trip_data
    SELECT CAST(pickup_date AS DATE),
        SUM(passenger_count) AS tot_passenger_count,
        ROUND(AVG(passenger_count)::numeric, 0) AS avg_passenger_count,
        ROUND(SUM(trip_distance)::numeric, 2) AS tot_distance,
        ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
        ROUND(SUM(fare_amount)::numeric, 2) AS tot_fare,
        ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
        ROUND(SUM(tip_amount)::numeric, 2) AS total_tip,
        ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip
    FROM "STG".src_tripdata
    GROUP BY CAST(pickup_date AS DATE);


    -- Insert aggregated data into channel_daily_agg_trip_data
    INSERT INTO EDW.channel_daily_agg_trip_data
    SELECT CAST(pickup_date AS DATE), payment_type,
        SUM(passenger_count) AS tot_passenger_count,
        ROUND(AVG(passenger_count)::numeric, 0) AS avg_passenger_count,
        ROUND(SUM(trip_distance)::numeric, 2) AS tot_distance,
        ROUND(AVG(trip_distance)::numeric, 2) AS avg_distance,
        ROUND(SUM(fare_amount)::numeric, 2) AS tot_fare,
        ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare,
        ROUND(SUM(tip_amount)::numeric, 2) AS total_tip,
        ROUND(AVG(tip_amount)::numeric, 2) AS avg_tip
    FROM "STG".src_tripdata
    GROUP BY CAST(pickup_date AS DATE), payment_type;

    -- Log the successful run
    INSERT INTO STG.procedure_log (run_time, status, error_message)
    VALUES (CURRENT_TIMESTAMP, 'SUCCESS', NULL);

    -- Commit transaction
    COMMIT;

EXCEPTION
    WHEN OTHER THEN
        -- Rollback transaction in case of an error
        ROLLBACK;
        -- Log the error
        INSERT INTO STG.procedure_log (run_time, status, error_message)
        VALUES (CURRENT_TIMESTAMP, 'FAILURE', 'Error during procedure execution');
        -- Rethrow the exception
        RAISE;
END;
$$;