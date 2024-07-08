CREATE TABLE IF NOT EXISTS EDW.daily_agg_trip_data (
    pickup_date DATE PRIMARY KEY,
    tot_passenger_count NUMBER,
    avg_passenger_count NUMBER,
    tot_distance NUMBER,
    avg_distance NUMBER,
    tot_fare NUMBER,
    avg_fare NUMBER,
    total_tip NUMBER,
    avg_tip NUMBER
);

CREATE TABLE IF NOT EXISTS EDW.channel_daily_agg_trip_data (
    pickup_date DATE,
    payment_type NUMBER,
    tot_passenger_count NUMBER,
    avg_passenger_count NUMBER,
    tot_distance NUMBER,
    avg_distance NUMBER,
    tot_fare NUMBER,
    avg_fare NUMBER,
    total_tip NUMBER,
    avg_tip NUMBER,
    PRIMARY KEY (pickup_date, payment_type)
);

CREATE TABLE IF NOT EXISTS STG.procedure_logs (
    run_time TIMESTAMP,
    status VARCHAR(10),
    error_message VARCHAR(100)
);
