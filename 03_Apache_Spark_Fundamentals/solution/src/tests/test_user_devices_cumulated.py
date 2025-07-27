from chispa.dataframe_comparer import *
from ..jobs.user_devices_cumulated_job import do_user_devices_cumulated_transformation
from collections import namedtuple

from pyspark.sql import Row
from datetime import date

# Define namedtuples
Yesterday = namedtuple("Yesterday", "user_id browser_type device_activity_datelist date")
Event = namedtuple("Event", "user_id device_id event_time")
Device = namedtuple("Device", "device_id browser_type")
Expected = namedtuple("Expected", "user_id browser_type device_activity_datelist date")

def test_user_devices_cumulated_transformation(spark):
    # Mock yesterday's cumulated data
    yesterday_input = [
        Yesterday("user1", "Chrome", [date(2023, 1, 30)], date(2023, 1, 30)),
        Yesterday("user2", "Firefox", [date(2023, 1, 30)], date(2023, 1, 30))
    ]
    spark.createDataFrame(yesterday_input).createOrReplaceTempView("user_devices_cumulated")

    # Mock events and devices
    events_input = [
        Event("user1", "d1", "2023-01-31 10:00:00"),
        Event("user3", "d3", "2023-01-31 12:00:00")
    ]
    spark.createDataFrame(events_input).createOrReplaceTempView("events")

    devices_input = [
        Device("d1", "Chrome"),
        Device("d3", "Safari")
    ]
    spark.createDataFrame(devices_input).createOrReplaceTempView("devices")

    # Run transformation
    output_df = do_user_devices_cumulated_transformation(spark)

    # Expected output
    expected_output = [
        Expected("user1", "Chrome", [date(2023, 1, 30), date(2023, 1, 31)], date(2023, 1, 31)),
        Expected("user2", "Firefox", [date(2023, 1, 30)], date(2023, 1, 31)),
        Expected("user3", "Safari", [date(2023, 1, 31)], date(2023, 1, 31)),
    ]
    expected_df = spark.createDataFrame(expected_output)
    
    # Compare result
    assert_df_equality(output_df, expected_df, ignore_nullable=True)