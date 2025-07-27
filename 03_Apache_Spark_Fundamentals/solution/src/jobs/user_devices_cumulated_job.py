#Import spark session
from pyspark.sql import SparkSession

# Define the SQL query for user devices cumulated transformation
query = """ 
    WITH yesterday AS (
    SELECT 
        user_id,
        browser_type,
        device_activity_datelist,
        date
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT 
        CAST(e.user_id AS string) AS user_id,
        CAST(d.browser_type AS string) AS browser_type,
        DATE(CAST(e.event_time AS TIMESTAMP)) AS date_active	
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND e.user_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP))
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN array(t.date_active)
        WHEN t.date_active IS NULL THEN y.device_activity_datelist
        ELSE array_distinct(array_union(y.device_activity_datelist, array(t.date_active)))
    END AS device_activity_datelist,
    COALESCE(t.date_active, DATE_ADD(y.date, 1)) AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id AND t.browser_type = y.browser_type
"""

# Define the function to perform the user devices cumulated transformation
def do_user_devices_cumulated_transformation(spark):
    #dataframe.createOrReplaceTempView("user_devices") #No need to create temp view as we creating mock data for yesterday, events and devices
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("user_devices_cumulated") \
        .getOrCreate()
    output_df = do_user_devices_cumulated_transformation(spark, spark.table("user_devices"))
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")
