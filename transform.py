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

weather_df = spark.read.json(weather_path)
# %%
weather_df.show(5)
# %%
weather_exploded = weather_df.withColumn(
   "temperatures",
   f.arrays_zip(
      f.col("hourly.temperature_2m"),
      f.col("hourly.time"),
      f.col("hourly.weather_code")
   )
)
# %%
weather_exploded.select('temperatures').printSchema()
# %%
weather_exploded.show()
# %%
weather_exploded = weather_exploded.withColumn('exploded_temperatures', f.explode(f.col('temperatures')))

# %%
weather_exploded.show()
# %%

weather_final = weather_exploded.select(
   f.col('exploded_temperatures.temperature_2m').alias('temperature'),
   f.col('exploded_temperatures.time').alias('hour'),
   f.col('exploded_temperatures.weather_code').alias('weather_code')
)

# %%
weather_final.show(5)
# %%
weather_mapping_data = [
    (0, "Ciel dégagé"),
    (1, "Principalement dégagé"),
    (2, "Partiellement nuageux"),
    (3, "Couvert"),
    (45, "Brouillard"),
    (48, "Brouillard"),
    (51, "Bruine faible"),
    (53, "Bruine modérée"),
    (55, "Bruine forte"),
    (56, "Bruine verglaçante"),
    (57, "Bruine verglaçante"),
    (61, "Pluie faible"),
    (63, "Pluie modérée"),
    (65, "Pluie forte"),
    (66, "Pluie verglaçante"),
    (67, "Pluie verglaçante"),
    (71, "Neige faible"),
    (73, "Neige modérée"),
    (75, "Neige forte"),
    (77, "Grains de neige"),
    (80, "Averses faibles"),
    (81, "Averses modérées"),
    (82, "Averses fortes"),
    (85, "Averses de neige"),
    (86, "Averses de neige"),
    (95, "Orage"),
    (96, "Orage avec grêle"),
    (99, "Orage avec grêle"),
]

# %%
mapping_df = spark.createDataFrame(weather_mapping_data, ["weather_code", "weather_description"])

# %%
weather_final = weather_final.join(
    f.broadcast(mapping_df),
    on="weather_code",
    how="left"
)

# %%
weather_final.show(10)

# %%
velib_prepared = velib_joined.withColumn(
    "hour_match",
    f.date_trunc("hour", f.to_timestamp("collected_at"))
)

# %%
weather_prepared = weather_final.withColumn(
    "hour_match",
    f.date_trunc("hour", f.to_timestamp("hour"))
)

# %%
velib_with_weather = velib_prepared.join(
    f.broadcast(weather_prepared),
    on="hour_match",
    how="left"
)

# %%
velib_with_weather.show(10)



# %%

velib_with_weather.filter(f.col("temperature").isNull()).count()
# %%
