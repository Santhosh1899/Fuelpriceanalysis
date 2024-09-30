# Databricks notebook source

from pyspark.sql import SparkSession
import requests
import os
from pyspark import SparkFiles
from pyspark.sql.types import StructType, StructField, DateType, StringType, FloatType, ArrayType, DoubleType
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import current_date
from pyspark.sql.functions import max
from datetime import date

url='https://fuel.motorfuelgroup.com/fuel_prices_data.json'

schema = StructType([
    StructField("last_updated", StringType(), True),
    StructField("stations", ArrayType(
        StructType([
            StructField("site_id", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("address", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("location", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True),
            StructField("prices", StructType([
                StructField("B7", DoubleType(), True),
                StructField("E10", DoubleType(), True),
                StructField("E5", DoubleType(), True),
                StructField("SDV", DoubleType(), True)
            ]), True)
        ])
    ), True)
])


response = requests.get(url)

if response.status_code == 200:
    json_data = response.json()
    df = spark.createDataFrame([json_data],schema=schema)


# COMMAND ----------

df_tesco = df

df_tesco = df_tesco.select(explode("stations").alias("stations")
).select("stations.*")

df_tesco = df_tesco.select(
    col("site_id"),
    col("brand"),
    col("address"),
    col("postcode"),
    col("location.latitude").alias("latitude"),
    col("location.longitude").alias("longitude"),
    col("prices.B7").alias("B7"),
    col("prices.E5").alias("E5"),
    col("prices.SDV").alias("SDV"),
    col("prices.E10").alias("E10")
)

df_tesco=df_tesco.withColumn('Dateofoccurance',current_date())
df_tesco.dropDuplicates(subset=['address','postcode'])
df_tesco=df_tesco.filter(df_tesco.brand.isin('Shell','Murco','Texaco'))
max_date_query=spark.sql("SELECT MAX(Dateofoccurance) AS max_date FROM Fuelprice.Tesco").collect()[0]
max_date = max_date_query["max_date"]

if max_date!=date.today():
    df_tesco.write.mode("append").saveAsTable("Fuelprice.Tesco")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Fuelprice.Tesco (
# MAGIC site_id STRING,
# MAGIC brand STRING, 
# MAGIC address STRING, 
# MAGIC postcode STRING,
# MAGIC latitude Double,
# MAGIC longitude Double,
# MAGIC B7 Double,
# MAGIC E5 Double,
# MAGIC SDV Double,
# MAGIC E10 Double,
# MAGIC Dateofoccurance Date
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fuelprice.tesco
