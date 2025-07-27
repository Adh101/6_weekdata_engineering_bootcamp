#Initializing the Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast

spark = SparkSession.builder.appName("Jupyter").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").config("spark.sql.shuffle.partitions", "64").getOrCreate()
spark

#Disabling auto broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

#Loading CSV Files into DataFrames
match_details = spark.read.option("header","true").option("inferSchema",True).csv("/home/iceberg/data/match_details.csv")
matches = spark.read.option("header","true").option("inferSchema",True).csv("/home/iceberg/data/matches.csv")
medals_matches_players = spark.read.option("header","true").option("inferSchema",True).csv("/home/iceberg/data/medals_matches_players.csv")

medals = spark.read.option("header","true").option("inferSchema",True).csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header","true").option("inferSchema",True).csv("/home/iceberg/data/maps.csv")


#Broadcast join the medals with medals_matches_players
medals_broadcasted = medals_matches_players.join(broadcast(medals), on ="medal_id", how= "left")

#Broadcast join the maps with matches
maps_broadcasted = matches.join(broadcast(maps), on ="mapid", how="left")

#Show the broadcasted data
medals_broadcasted.show()
maps_broadcasted.show()

#Create bucketed table for match_details in Iceberg
spark.sql("DROP TABLE IF EXISTS bootcamp.match_details_bucketed_v2")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed_v2 (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank INT,
    spartan_rank INT,
    previous_total_xp INT,
    total_xp INT,
    previous_csr_tier STRING,
    previous_csr_designation STRING,
    previous_csr INT,
    previous_csr_percent_to_next_tier DOUBLE,
    previous_csr_rank INT,
    current_csr_tier STRING,
    current_csr_designation STRING,
    current_csr INT,
    current_csr_percent_to_next_tier DOUBLE,
    current_csr_rank INT,
    player_rank_on_team INT,
    player_finished BOOLEAN,
    player_average_life DOUBLE,
    player_total_kills INT,
    player_total_headshots INT,
    player_total_weapon_damage DOUBLE,
    player_total_shots_landed INT,
    player_total_melee_kills INT,
    player_total_melee_damage DOUBLE,
    player_total_assassinations INT,
    player_total_ground_pound_kills INT,
    player_total_shoulder_bash_kills INT,
    player_total_grenade_damage DOUBLE,
    player_total_power_weapon_damage DOUBLE,
    player_total_power_weapon_grabs INT,
    player_total_deaths INT,
    player_total_assists INT,
    player_total_grenade_kills INT,
    did_win BOOLEAN,
    team_id STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")
#Write to the bucketed table using writeTo() method
match_details.select(
    col("match_id").cast("string"),
    col("player_gamertag").cast("string"),
    col("previous_spartan_rank").cast("int"),
    col("spartan_rank").cast("int"),
    col("previous_total_xp").cast("int"),
    col("total_xp").cast("int"),
    col("previous_csr_tier").cast("string"),
    col("previous_csr_designation").cast("string"),
    col("previous_csr").cast("int"),
    col("previous_csr_percent_to_next_tier").cast("double"),
    col("previous_csr_rank").cast("int"),
    col("current_csr_tier").cast("string"),
    col("current_csr_designation").cast("string"),
    col("current_csr").cast("int"),
    col("current_csr_percent_to_next_tier").cast("double"),
    col("current_csr_rank").cast("int"),
    col("player_rank_on_team").cast("int"),
    col("player_finished").cast("boolean"),
    col("player_average_life").cast("double"),
    col("player_total_kills").cast("int"),
    col("player_total_headshots").cast("int"),
    col("player_total_weapon_damage").cast("double"),
    col("player_total_shots_landed").cast("int"),
    col("player_total_melee_kills").cast("int"),
    col("player_total_melee_damage").cast("double"),
    col("player_total_assassinations").cast("int"),
    col("player_total_ground_pound_kills").cast("int"),
    col("player_total_shoulder_bash_kills").cast("int"),
    col("player_total_grenade_damage").cast("double"),
    col("player_total_power_weapon_damage").cast("double"),
    col("player_total_power_weapon_grabs").cast("int"),
    col("player_total_deaths").cast("int"),
    col("player_total_assists").cast("int"),
    col("player_total_grenade_kills").cast("int"),
    col("did_win").cast("boolean"),
    col("team_id").cast("string")
).writeTo("bootcamp.match_details_bucketed_v2").append()

#Create bucketed table for matches
spark.sql("DROP TABLE IF EXISTS bootcamp.matches_bucketed_v2")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed_v2 (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
""")
#Write to the bucketed matches table
matches.select(
    col("match_id").cast("string"),
    col("mapid").cast("string"),
    col("is_team_game").cast("boolean"),
    col("playlist_id").cast("string"),
    col("game_variant_id").cast("string"),
    col("is_match_over").cast("boolean"),
    col("completion_date").cast("timestamp"),
    col("match_duration").cast("string"),
    col("game_mode").cast("string"),
    col("map_variant_id").cast("string")  
    ).writeTo("bootcamp.matches_bucketed_v2").append()

#Create bucketed table for medals_matches_players in Iceberg
spark.sql("DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed_v2")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed_v2 (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    count INT
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
""")
#Write to bucketed medals_matches_players table
medals_matches_players.select(
    col("match_id").cast("string"),
    col("player_gamertag").cast("string"),
    col("medal_id").cast("string"),
    col("count").cast("int")
     ).writeTo("bootcamp.medals_matches_players_bucketed_v2").append()

#Load the bucketed table
match_details_v2 = spark.table("bootcamp.match_details_bucketed_v2")
matches_v2 = spark.table("bootcamp.matches_bucketed_v2")
medals_matches_players_v2 = spark.table("bootcamp.medals_matches_players_bucketed_v2")

#Fixing the column names in broadcasted tables
maps_broadcasted_clean = maps_broadcasted.select(
    col("match_id"),
    col("mapid"),
    col("is_team_game"),
    col("playlist_id"),
    col("game_variant_id"),
    col("is_match_over"),
    col("completion_date"),
    col("match_duration"),
    col("game_mode"),
    col("map_variant_id"),
    col("name").alias("map_name"), #Changing the ambiguous column name
    col("description").alias("map_description") #Changing the ambiguous column name
)

medals_broadcasted_clean = medals_broadcasted.select(
    col("match_id"),
    col("player_gamertag"),
    col("medal_id"),
    col("count"),
    col("sprite_uri"),
    col("sprite_left"),
    col("sprite_top"),
    col("sprite_sheet_width"),
    col("sprite_sheet_height"),
    col("sprite_width"),
    col("sprite_height"),
    col("classification"),
    col("description").alias("medal_description"), #Changing the ambiguous column name
    col("name").alias("medal_name"), #Changing the ambiguous column name
    col("difficulty")
)

#Aggregating the joined table
aggregated_dataFrame = match_details.join(maps_broadcasted_clean, on="match_id", how="left") \
    .join(medals_broadcasted_clean, on=["match_id", "player_gamertag"], how="left")

#1. Which player averages the most kills per game?
from pyspark.sql.functions import avg, desc
avg_kills_df = aggregated_dataFrame.groupBy("player_gamertag") \
    .agg(avg("player_total_kills").alias("avg_kills")) \
    .orderBy(desc("avg_kills"))

avg_kills_df.show()

#2. Which playlist gets played the most?
most_played_playlist =aggregated_dataFrame.groupBy("playlist_id") \
    .count() \
    .orderBy(desc("count"))

most_played_playlist.show()

#3. Which map gets played the most?
most_played_map = aggregated_dataFrame.groupBy("mapid","map_name") \
    .count() \
    .orderBy(desc("count"))

most_played_map.show()

#4. Which map do players get the most Killing Spree medals on?
from pyspark.sql.functions import sum as _sum

killing_spree_map = aggregated_dataFrame.filter(col("medal_name") == "Killing Spree") \
    .groupBy("map_name") \
    .agg(_sum("count").alias("total_killing_sprees")) \
    .orderBy(desc("total_killing_sprees"))

killing_spree_map.show()

#Sorting the aggregated data using playlist_id and map_id
sorted_by_playlist = aggregated_dataFrame.sortWithinPartitions("playlist_id")
sorted_by_map = aggregated_dataFrame.sortWithinPartitions("mapid")
sorted_by_map_name = aggregated_dataFrame.sortWithinPartitions("map_name")

#Calculating the size of the sorted DataFrames
playlist_size = sorted_by_playlist.rdd.map(lambda row: len(str(row))).sum()
map_size = sorted_by_map.rdd.map(lambda row: len(str(row))).sum()
map_name_size = sorted_by_map_name.rdd.map(lambda row: len(str(row))).sum()

print(f"Total data size (sorted by playlist_id): {playlist_size} bytes")
print(f"Total data size (sorted by map_id): {map_size} bytes")
print(f"Total data size (sorted by map_name): {map_name_size} bytes")