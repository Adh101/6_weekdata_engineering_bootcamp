-- Creating a Slowly Changing Dimenision Table (SCD Type 2) for actors
-- Step 1: Creating SCD Table

CREATE TABLE actors_history_scd (
	actor TEXT,
	actorid TEXT,
	is_active BOOLEAN,
	quality_class quality_class,
	start_date INTEGER,
	end_date INTEGER,
	current_flag BOOLEAN
);