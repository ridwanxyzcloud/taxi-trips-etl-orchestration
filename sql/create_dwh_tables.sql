
--- daily aggregate tripdata table
CREATE TABLE IF NOT EXISTS "EDW".daily_agg_tripdata(
	pickup_date date primary key,
	total_passenger_count int,
	avg_passenger_count float,
	total_distance float,
	avg_distance float,
	total_tfare float,
	avg_tfare float,
	total_tip float,
	avg_tip float
);

--- daily aggregate tripdata payment channel table  
CREATE TABLE IF NOT EXISTS "EDW".daily_avg_tripdata_channel(
	pickup_date date NOT NULL,
	payment_type text,
	toatl_passenger_count int,
	avg_passenger_count float,
	total_distance float,
	avg_distance float,
	total_tfare float,
	avg_tfare float,
	total_tip float,
	avg_tip float,
	PRIMARY KEY (pickup_date, payment_type)
);

