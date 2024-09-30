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

postcode_df = spark.sql("SELECT Distinct t1.Postcode FROM Fuelprice.priceofallbrand as t1 where t1.dateofoccurance=CAST(GETDATE() AS DATE) and t1.postcode not in(select DISTINCT postcode from fuelprice.postcodedetails) ")

# COMMAND ----------

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
postcode_df=postcode_df.withColumn('dateofevent',current_date())
postcode_without_null = postcode_df.filter(postcode_df['city'].isNotNull())
postcode_with_null = postcode_df.filter(postcode_df['city'].isNull())


# COMMAND ----------


max_date_query=spark.sql("SELECT MAX(dateofevent) AS max_date FROM Fuelprice.postcodedetails").collect()[0]
max_date = max_date_query["max_date"]

if max_date!=date.today():
    postcode_without_null.write.mode("append").saveAsTable("Fuelprice.Postcodedetails")
    postcode_with_null.write.mode("append").saveAsTable("Fuelprice.Nullvalues")

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from Fuelprice.priceofallbrand as t1
# MAGIC where t1.postcode in(Select postcode from fuelprice.Nullvalues)
