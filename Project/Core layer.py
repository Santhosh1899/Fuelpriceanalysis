# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT OVERWRITE Fuelprice.Priceofallbrand 
# MAGIC select * from fuelprice.applegreen
# MAGIC UNION
# MAGIC SELECT * FROM fuelprice.ASDA
# MAGIC union 
# MAGIC Select * FROM fuelprice.bp
# MAGIC UNION
# MAGIC select * FROM fuelprice.esso
# MAGIC UNION
# MAGIC select * FROM fuelprice.jet
# MAGIC UNION
# MAGIC select * FROM fuelprice.sainsbury
# MAGIC UNION
# MAGIC SELECT * FROM fuelprice.sgn
# MAGIC UNION
# MAGIC select * FROM fuelprice.tesco
# MAGIC UNION
# MAGIC select * FROM fuelprice.morrisons
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Fuelprice.Priceofallbrand (
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fuelprice.priceofallbrand
