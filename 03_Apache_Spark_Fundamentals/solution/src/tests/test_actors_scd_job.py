# Import necessary libraries and packages
from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actors_scd_transformation
from collections import namedtuple

# Define namedtuples for actor snapshots and SCD results
ActorSnapshot = namedtuple("ActorSnapshot", "actor actorid current_year is_active quality_class")
ActorSCD = namedtuple("ActorSCD", "actor actorid is_active quality_class start_date end_date current_flag")

# Define the test function for actor SCD logic
def test_actor_scd_logic(spark):
    source_data = [
        ActorSnapshot("Leonardo DiCaprio", "a1", 2000, True, "good"),
        ActorSnapshot("Leonardo DiCaprio", "a1", 2001, True, "good"),
        ActorSnapshot("Leonardo DiCaprio", "a1", 2002, True, "star"),
        ActorSnapshot("Leonardo DiCaprio", "a1", 2003, False, "star"),
        ActorSnapshot("Leonardo DiCaprio", "a1", 2004, False, "bad"),
        ActorSnapshot("Meryl Streep", "a2", 2001, True, "good"),
        ActorSnapshot("Meryl Streep", "a2", 2002, True, "good"),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_scd_transformation(spark, source_df)
    expected_data = [
        ActorSCD("Leonardo DiCaprio", "a1", True, "good", 2000, 2001, False),
        ActorSCD("Leonardo DiCaprio", "a1", True, "star", 2002, 2002, False),
        ActorSCD("Leonardo DiCaprio", "a1", False, "star", 2003, 2003, False),
        ActorSCD("Leonardo DiCaprio", "a1", False, "bad", 2004, None, True),
        ActorSCD("Meryl Streep", "a2", True, "good", 2001, None, True),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

