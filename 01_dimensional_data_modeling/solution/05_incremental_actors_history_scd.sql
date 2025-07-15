--Incremental Query for actors_history_scd
-- Step 1: Create a new SCD Table
CREATE TABLE actors_history_scd_incremental (
	actor TEXT,
	actorid TEXT,
	is_active BOOLEAN,
	quality_class quality_class,
	start_date INTEGER,
	end_date INTEGER,
	current_flag BOOLEAN
);
-- Step 2: Inserting the first year values(i.e. 1970) in new SCD table
INSERT INTO actors_history_scd_incremental (
    actor,
    actorid,
    is_active,
    quality_class,
    start_date,
    end_date,
    current_flag
)
SELECT
    actor,
    actorid,
    is_active,
    quality_class,
    current_year AS start_date,
    NULL AS end_date,
    TRUE AS current_flag
FROM actors
WHERE current_year = 1970;

SELECT * FROM actors_history_scd_incremental;

-- Step 3: Incremental Query for Next year i.e. 1971
-- Step 3.1: Expire previous records if a change is detected
WITH current_data AS (
    SELECT
        actor,
        actorid,
        quality_class,
        is_active,
        current_year AS year
    FROM actors
    WHERE current_year = 1971
),
latest_scd AS (
    SELECT DISTINCT ON (actorid)
        actorid,
        quality_class,
        is_active,
        start_date
    FROM actors_history_scd_incremental
    WHERE current_flag = TRUE
    ORDER BY actorid, start_date DESC
),
changes AS (
    SELECT
        c.actorid,
        c.year AS new_start_date
    FROM current_data c
    LEFT JOIN latest_scd s
        ON c.actorid = s.actorid
    WHERE
        s.actorid IS NOT NULL AND (
            c.quality_class IS DISTINCT FROM s.quality_class
            OR c.is_active IS DISTINCT FROM s.is_active
        )
)
UPDATE actors_history_scd_incremental s
SET end_date = c.new_start_date - 1,
    current_flag = FALSE
FROM changes c
WHERE s.actorid = c.actorid AND s.current_flag = TRUE;

-- Step 3.2: Insert new records for actors who changed
INSERT INTO actors_history_scd_incremental (
    actor,
    actorid,
    is_active,
    quality_class,
    start_date,
    end_date,
    current_flag
)
WITH current_data AS (
    SELECT
        actor,
        actorid,
        quality_class,
        is_active,
        current_year AS year
    FROM actors
    WHERE current_year = 1971
),
latest_scd AS (
    SELECT DISTINCT ON (actorid)
        actorid,
        quality_class,
        is_active,
        start_date
    FROM actors_history_scd_incremental
    WHERE current_flag = TRUE
    ORDER BY actorid, start_date DESC
),
changes AS (
    SELECT
        c.actor,
        c.actorid,
        c.quality_class,
        c.is_active,
        c.year AS start_date
    FROM current_data c
    LEFT JOIN latest_scd s
        ON c.actorid = s.actorid
    WHERE
        s.actorid IS NULL
        OR c.quality_class IS DISTINCT FROM s.quality_class
        OR c.is_active IS DISTINCT FROM s.is_active
)
SELECT
    actor,
    actorid,
    is_active,
    quality_class,
    start_date,
    NULL AS end_date,
    TRUE AS current_flag
FROM changes;