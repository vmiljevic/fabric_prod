# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1bc1173b-7d22-48c2-afa1-74485be57354",
# META       "default_lakehouse_name": "lakehouse_finance_silver",
# META       "default_lakehouse_workspace_id": "bd777a6f-f8b4-4d15-bcad-2edf46657ec3",
# META       "known_lakehouses": [
# META         {
# META           "id": "1bc1173b-7d22-48c2-afa1-74485be57354"
# META         },
# META         {
# META           "id": "1bc1173b-7d22-48c2-afa1-74485be57354"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# delete data from the time card daily table and keeping the schema
df = spark.read.table("fact_ppm_time_cards_daily")
empty_df = df.limit(0)
empty_df.write.mode("overwrite").saveAsTable("fact_ppm_time_cards_daily")
abfss://bd777a6f-f8b4-4d15-bcad-2edf46657ec3@onelake.dfs.fabric.microsoft.com/1bc1173b-7d22-48c2-afa1-74485be57354/Tables/dim_materials_archive

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("1bc1173b-7d22-48c2-afa1-74485be57354.fact_ppm_time_cards_daily")
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.
df = spark.sql("SELECT * FROM lakehouse_finance_silver.fact_ppm_time_cards_daily LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read from the existing lakehouse table
df = spark.read.table("PBI_IT_Finance.dbo.fact_ppm_time_cards_daily")

# Create empty dataframe with same schema
empty_df = df.limit(0)

# Write to your current/empty lakehouse
empty_df.write.mode("overwrite").saveAsTable("fact_ppm_time_cards_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# delete data from the time card interface daily table and keeping the schema
df = spark.read.table("fact_ppm_time_card_interface_daily")
empty_df = df.limit(0)
empty_df.write.mode("overwrite").saveAsTable("fact_ppm_time_card_interface_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT current_catalog(), current_database()").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# reading all files in the lakehouse
mssparkutils.fs.ls("Tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# Read the table
df = spark.read.format("delta").load("abfss://bd777a6f-f8b4-4d15-bcad-2edf46657ec3@onelake.dfs.fabric.microsoft.com/1bc1173b-7d22-48c2-afa1-74485be57354/Tables/fact_ppm_time_card_interface_daily")

# Create empty table with same schema
empty_df = df.limit(0)
empty_df.write.mode("overwrite").saveAsTable("fact_ppm_time_card_interface_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the table
df = spark.read.format("delta").load("abfss://bd777a6f-f8b4-4d15-bcad-2edf46657ec3@onelake.dfs.fabric.microsoft.com/1bc1173b-7d22-48c2-afa1-74485be57354/Tables/fact_ppm_time_cards_daily")

# Display first few rows
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
