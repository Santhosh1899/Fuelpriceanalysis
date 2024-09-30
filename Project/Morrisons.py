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

url='https://www.morrisons.com/fuel-prices/fuel.json'

schema = StructType([
    StructField("last_updated", StringType(), True),
    StructField("stations", ArrayType(
        StructType([
            StructField("site_id", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("address", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("location", StructType([
                StructField("latitude", StringType(), True),
                StructField("longitude", StringType(), True)
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



df_tesco = df




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
df_tesco=df_tesco.withColumn("latitude",col('latitude').cast(DoubleType()))
df_tesco=df_tesco.withColumn("longitude",col('longitude').cast(DoubleType()))

df_tesco.dropDuplicates(subset=['address','postcode'])
max_date_query=spark.sql("SELECT MAX(Dateofoccurance) AS max_date FROM Fuelprice.Morrisons").collect()[0]
max_date = max_date_query["max_date"]

if max_date!=date.today():
    df_tesco.write.mode("append").saveAsTable("Fuelprice.Morrisons")
