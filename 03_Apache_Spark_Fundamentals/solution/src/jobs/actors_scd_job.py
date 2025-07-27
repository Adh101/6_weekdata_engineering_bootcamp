#Import the spark session
from pyspark.sql import SparkSession

# Define the SQL query for Slowly Changing Dimension (SCD) transformation
query = """
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
            CASE WHEN prev_active IS DISTINCT FROM is_active OR prev_quality IS DISTINCT FROM quality_class THEN 1 ELSE 0 END AS change_flag
        FROM base
    ),
    grouped AS (
        SELECT *,
            SUM(change_flag) OVER (PARTITION BY actorid ORDER BY current_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS change_group
        FROM changed
    ),
    scd_ranges AS (
        SELECT
            actor,
            actorid,
            is_active,
            quality_class,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date
        FROM grouped
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
    FROM final
    """

# Define the function to perform the SCD transformation
def do_actors_scd_transformation(spark,dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)

# Define the main function to execute the transformation and write the results
def main():
    spark = SparkSession.builder \
         .master("local") \
         .appName("actors_scd") \
         .getOrCreate()
    output_df = do_actors_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")