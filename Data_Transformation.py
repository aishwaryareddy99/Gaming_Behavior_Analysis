spark.conf.set("fs.azure.account.key.adlsgamingbehavior.dfs.core.windows.net", "SAS")

# Import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# URL to the data directory
data_directory_url = "abfss://refined-data@adlsgamingbehavior.dfs.core.windows.net/cleaned_data"

# Load the data from the directory
df = spark.read.format("csv").option("header", "true").load(data_directory_url)

# Show the data to verify
df.show()

# Calculate average playtime hours by GameGenre
df_agg = df.groupBy("GameGenre").agg(
    avg("PlayTimeHours").alias("AveragePlayTimeHours"),
    count("PlayerID").alias("PlayerCount"))
df_agg.show()

# Average playtime hours and sessions per week by gender
df_gender_agg = df.groupBy("Gender").agg(
    avg("PlayTimeHours").alias("AveragePlayTimeHours"),
    avg("SessionsPerWeek").alias("AverageSessionsPerWeek"),
    count("PlayerID").alias("PlayerCount")
)
df_gender_agg.show()

# Count of players by game difficulty
df_difficulty_count = df.groupBy("GameDifficulty").agg(
    count("PlayerID").alias("PlayerCount")
)
df_difficulty_count.show()

# Maximum and minimum playtime hours by game genre
df_genre_playtime_extremes = df.groupBy("GameGenre").agg(
    max("PlayTimeHours").alias("MaxPlayTimeHours"),
    min("PlayTimeHours").alias("MinPlayTimeHours")
)
df_genre_playtime_extremes.show()

# Average session duration by gender
df_gender_session_duration = df.groupBy("Gender").agg(
    avg("AvgSessionDurationMinutes").alias("AverageSessionDuration")
)
df_gender_session_duration.show()

# Find top 10 players with the highest playtime hours
df_top_players = df.orderBy(col("PlayTimeHours").desc()).limit(10)
df_top_players.show()

# Count of players by location
df_location_player_count = df.groupBy("Location").agg(
    count("PlayerID").alias("PlayerCount")
)
df_location_player_count.show()

# Most common game difficulty
df_difficulty_most_common = df.groupBy("GameDifficulty").agg(
    count("PlayerID").alias("PlayerCount")
).orderBy(col("PlayerCount").desc()).limit(1)
df_difficulty_most_common.show()

# Save DataFrames to ADLS
df_agg.write.mode("overwrite").option('header', True).csv("abfss://transformed-data@adlsgamingbehavior.dfs.core.windows.net/processed_data/df_agg")
df_gender_agg.write.mode("overwrite").option('header', True).csv("abfss://transformed-data@adlsgamingbehavior.dfs.core.windows.net/processed_data/df_gender_agg")
df_top_players.write.mode("overwrite").option('header', True).csv("abfss://transformed-data@adlsgamingbehavior.dfs.core.windows.net/processed_data/df_top_players")