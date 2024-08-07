spark.conf.set("fs.azure.account.auth.type.adlsgamingbehavior.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsgamingbehavior.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider") 

# Import modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("DatacleaningAndJoining").getOrCreate()

# File Paths
player_info_path = 'abfss://temp-storage@adlsgamingbehavior.dfs.core.windows.net/PlayerInfo.csv'
gaming_behavior_path = 'abfss://temp-storage@adlsgamingbehavior.dfs.core.windows.net/GamingBehavior.csv'

# Read data into DataFrames
player_info_df = spark.read.format("csv").option("header", "true").load(player_info_path)
gaming_behavior_df = spark.read.format("csv").option("header", "true").load(gaming_behavior_path)

# Display the data to verify
player_info_df.show()
gaming_behavior_df.show()

# Data Cleaning by Removing any Duplicate datasets
player_info_df = player_info_df.dropDuplicates()
gaming_behavior_df = gaming_behavior_df.dropDuplicates()

# Data Cleaning by doing the necessary Data Type conversion 
player_info_df_transformed = player_info_df \
    .withColumn("PlayerID", col("PlayerID").cast("int")) \
    .withColumn("Age", col("Age").cast("int"))

gaming_behavior_df_transformed = gaming_behavior_df \
    .withColumn("PlayerID", col("PlayerID").cast("int")) \
    .withColumn("PlayTimeHours", col("PlayTimeHours").cast("int")) \
    .withColumn("SessionsPerWeek", col("SessionsPerWeek").cast("int")) \
    .withColumn("AvgSessionDurationMinutes", col("AvgSessionDurationMinutes").cast("int")) \
    .withColumn("PlayerLevel", col("PlayerLevel").cast("int")) \
    .withColumn("AchievementsUnlocked", col("AchievementsUnlocked").cast("int"))

# Printing the Schemas to verify the data types 
player_info_df_transformed.printSchema()
gaming_behavior_df_transformed.printSchema()

# Joining the DataFrames on PlayerID
joined_df = player_info_df_transformed.join(gaming_behavior_df_transformed, "PlayerID", "inner")

# Handle missing values
# Example: Fill remaining null values with a default value or strategy (mean, median, etc.)
joined_df = joined_df.na.fill({
    'Age': joined_df.agg({'Age': 'mean'}).collect()[0][0],
    'PlayTimeHours': joined_df.agg({'PlayTimeHours': 'mean'}).collect()[0][0],
    'SessionsPerWeek': joined_df.agg({'SessionsPerWeek': 'mean'}).collect()[0][0],
    'AvgSessionDurationMinutes': joined_df.agg({'AvgSessionDurationMinutes': 'mean'}).collect()[0][0],
    'PlayerLevel': joined_df.agg({'PlayerLevel': 'mean'}).collect()[0][0],
    'AchievementsUnlocked': joined_df.agg({'AchievementsUnlocked': 'mean'}).collect()[0][0]
})

# Remove duplicates
joined_df = joined_df.dropDuplicates()

# Display the cleaned and transformed DataFrame to verify
joined_df.show()

# Save the cleaned and joined DataFrame back to Azure Storage
joined_df.write.csv("abfss://refined-data@adlsgamingbehavior.dfs.core.windows.net/cleaned_data", header='True')
