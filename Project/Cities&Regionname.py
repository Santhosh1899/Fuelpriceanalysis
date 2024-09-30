# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import current_date
from datetime import date
import requests
spark = SparkSession.builder \
    .appName("Postcodes API Integration") \
    .getOrCreate()

# COMMAND ----------

postcode_df = spark.sql("SELECT Distinct Postcode FROM Fuelprice.priceofallbrand")

postcode_df.count()

# COMMAND ----------


def city_name(postcode):
    api = f"https://api.postcodes.io/postcodes/{postcode}"
    response = requests.get(api)
    if response.status_code == 200:
        data = response.json()
        return data["result"]["admin_district"]
def region_name(postcode):
    api = f"https://api.postcodes.io/postcodes/{postcode}"
    response = requests.get(api)
    if response.status_code == 200:
        data = response.json()
        return data["result"]["region"]
def country_name(postcode):
    api = f"https://api.postcodes.io/postcodes/{postcode}"
    response = requests.get(api)
    if response.status_code == 200:
        data = response.json()
        return data["result"]["country"]

# COMMAND ----------

city_udf = F.udf(city_name)
postcode_df=postcode_df.withColumn('City',city_udf(postcode_df['Postcode']))
country_udf = F.udf(country_name)
postcode_df=postcode_df.withColumn('country',country_udf(postcode_df['Postcode']))
region_udf = F.udf(region_name)
postcode_df=postcode_df.withColumn('region',region_udf(postcode_df['Postcode']))
postcode_without_null = postcode_df.filter(postcode_df['city'].isNotNull())
postcode_with_null = postcode_df.filter(postcode_df['city'].isNull())


# COMMAND ----------

postcode_without_null.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Fuelprice.Postcodedetails (
# MAGIC Postcode STRING,
# MAGIC City STRING, 
# MAGIC country STRING, 
# MAGIC region STRING
# MAGIC
# MAGIC );
# MAGIC CREATE TABLE IF NOT EXISTS Fuelprice.Nullvalues (
# MAGIC Postcode STRING,
# MAGIC City STRING, 
# MAGIC country STRING, 
# MAGIC region STRING
# MAGIC
# MAGIC )

# COMMAND ----------

postcode_without_null.write.mode("overwrite").saveAsTable("Fuelprice.Postcodedetails")
postcode_with_null.write.mode("overwrite").saveAsTable("Fuelprice.Nullvalues")


# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from Fuelprice.priceofallbrand as t1
# MAGIC where t1.postcode in(Select postcode from fuelprice.Nullvalues)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE fuelprice.postcodedetails
# MAGIC Add COLUMN dateofevent DATE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE fuelprice.nullvalues
# MAGIC Add COLUMN dateofevent DATE;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE fuelprice.nullvalues
# MAGIC SET dateofevent = CAST(GETDATE() AS DATE);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fuelprice.postcodedetails
