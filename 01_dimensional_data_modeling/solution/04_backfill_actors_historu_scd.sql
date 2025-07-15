-- Backfilling Query for actors_history_scd
INSERT INTO actors_history_scd (
	actor,
	actorid,
	is_active,
	quality_class,
	start_date,
	end_date,
	current_flag
)
WITH base AS (
	SELECT
		actor,
		actorid,
		current_year,
		is_active,
		quality_class,
		LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_active,
		LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS prev_quality
	FROM actors
),
changed AS (
	SELECT *,
		SUM(
			CASE 
				WHEN prev_active IS DISTINCT FROM is_active OR prev_quality IS DISTINCT FROM quality_class THEN 1
				ELSE 0
			END
		) OVER (PARTITION BY actorid ORDER BY current_year) AS change_group
	FROM base
),
scd_ranges AS (
	SELECT
		actor,
		actorid,
		is_active,
		quality_class,
		MIN(current_year) AS start_date,
		MAX(current_year) AS end_year
	FROM changed
	GROUP BY actor, actorid, is_active, quality_class, change_group
),
final AS (
	SELECT *,
		LEAD(start_date) OVER (PARTITION BY actorid ORDER BY start_date) - 1 AS computed_end_date
	FROM scd_ranges
)
SELECT
	actor,
	actorid,
	is_active,
	quality_class,
	start_date,
	COALESCE(computed_end_date, NULL) AS end_date,
	computed_end_date IS NULL AS current_flag
FROM final;

SELECT * FROM actors_history_scd;