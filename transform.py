# %%
from pyspark.sql import SparkSession
import json
import os
from dotenv import load_dotenv
import pyspark.sql.functions as f
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')

velib_status_path = "s3a://velib-forecast-data-elies/raw/status/year=2026/month=08/day=06/hour=21/snapshot_20260806T212007Z.json"
weather_path = "s3a://velib-forecast-data-elies/raw/weather/year=2026/month=08/day=06/hour=21/snapshot_20260806T212008Z.json"
velib_info_path = "s3a://velib-forecast-data-elies/raw/info/year=2026/month=08/day=06/hour=21/snapshot_20260806T212008Z.json"
spark = (SparkSession.builder
         .appName('Velib')
         .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.5.0"
         )
         .getOrCreate())
         
spark.sparkContext.setLogLevel("ERROR")

# %%

velib_status_df = spark.read.json(velib_status_path)

# %%

velib_status_exploded = velib_status_df.withColumn("stations", f.explode(f.col("data.data.stations")))
# %%

velib_status_exploded.show(5)

# %%

velib_status_exploded.select("stations").printSchema()
# %%

velib_status_final = velib_status_exploded.select(
   f.col("stations.station_id").alias("station_id"),
   f.col("stations.numBikesAvailable").alias("num_bikes_available"),
   f.col("stations.numDocksAvailable").alias("num_docks_available"),
   f.col("stations.is_installed").alias("is_installed"),
   f.col("stations.is_renting").alias("is_renting"),
   f.col("stations.is_returning").alias("is_returning"),
   f.col("metadata.collected_at").alias("collected_at")
)
# %%
velib_status_final.show(5)
# %%
velib_status_exploded.select("metadata").printSchema()
# %%
velib_status_final.show(5)
# %%
# INFORMATION VELIB

velib_info_df = spark.read.json(velib_info_path)

velib_info_df.select('data').printSchema()
# %%
velib_info_df.show(5)
# %%
velib_info_exploded = velib_info_df.withColumn('stations', f.explode(f.col('data.data.stations')))
# %%
velib_info_exploded.show(5)
# %%
velib_info_final = velib_info_exploded.select(
   f.col('stations.station_id').alias('station_id'),
   f.col('stations.capacity').alias('capacity'),
   f.col('stations.lat').alias('lat'),
   f.col('stations.lon').alias('lon'),
   f.col('stations.name').alias('name')
)

velib_info_final.show(5)
# %%

velib_joined = velib_status_final.join(
   velib_info_final,
   on="station_id",
   how="left"
)
# %%
velib_joined.count()# %%

# %%
# %%
velib_joined.filter(
   f.col("lat").isNull()
).count()
# %%
