CREATE OR REPLACE PROCEDURE "stg".agg_tripdata()
LANGUAGE plpgsql
AS $$
DECLARE
	--- declare variables for procedure_logs
	v_runtime TIMESTAMP;
	v_status TEXT;
	v_error_msg TEXT;
BEGIN
	--- set deafault value for declared variables
	v_runtime := NOW();
	v_status := 'SUCESS';
	v_error_msg := NULL;
	--- procedure for daily_agg_tripsdata
	INSERT INTO "EDW".daily_agg_tripsdata
	SELECT 
		cast(pickup_date as date) as pickup_date,
		sum(passenger_count) as total_passenger_count,
		avg(passenger_count) as avg_passenger_count,
		sum(trip_distance) as total_distance,
		avg(trip_distance) as avg_distance,
		sum(fare_amount) as total_fare,
		avg(fare_amount) as avg_fare,
		sum(tip_amount) as total_tip,
		avg(tip_amount) as avg_tip
	FROM stg.tripsdata
	GROUP BY cast(pickup_date as date);
	
	--- Procedure for daily_agg_tripdata_channel 
	INSERT INTO "EDW".daily_agg_tripsdata_channel
	SELECT 
		cast(pickup_date as date) as pickup_date,
		payment_type,
		sum(passenger_count) as total_passenger_count,
		avg(passenger_count) as avg_passenger_count,
		sum(trip_distance) as total_distance,
		avg(trip_distance) as avg_distance,
		sum(fare_amount) as total_fare,
		avg(fare_amount) as avg_fare,
		sum(tip_amount) as total_tip,
		avg(tip_amount) as avg_tip
	FROM stg.tripsdata
	GROUP BY cast(pickup_date as date), payment_type;
	
	-- log the outcome of the procedure 
	INSERT INTO "stg".procedure_logs(runtime, status, error_msg)
	VALUES (v_runtime, v_status, v_error_msg);
-- the exeception captures general errorS 
EXCEPTION
	WHEN OTHERS THEN
		v_status := 'FAILED';
		v_error_msg := SQLERRM;
		
		INSERT INTO "stg".procedure_logs(runtime, status, error_msg)
		VALUES (v_runtime, v_status, v_error_msg);

END;
$$