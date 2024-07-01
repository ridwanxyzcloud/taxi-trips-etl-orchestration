--- The procedure_logs table helps keep track of activities of the procedure for proper error handling and documentation since it is done automatically
-- Procedure logs table
CREATE TABLE "stg".procedure_logs(
	runtime TIMESTAMP NOT NULL,
	status text,
	error_msg text
);
