# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.window import *

# COMMAND ----------

# Mention your Hive path where the raw file is ingested in the variable
uploaded_parh=""

# COMMAND ----------

dbutils.fs.cp(uploaded_path,"/FileStore/tables/Annual_finance_raw",True)

# COMMAND ----------

# %fs ls /FileStore/tables/Annual_finance_raw

# COMMAND ----------

# MAGIC %md ######1. Rename the "industry_code_ANZSIC" and "industry_name_ANZSIC" with "industry_code" and "industry_name” respectively

# COMMAND ----------

df=spark.read.format("csv").option("header",True).option("delimiter",",").option("Inferscheam",True).load("/FileStore/tables/Annual_finance_raw")
df=df.withColumnRenamed("industry_code_ANZSIC","industry_code").withColumnRenamed("industry_name_ANZSIC","industry_name")
df.show(5,truncate=False)

# COMMAND ----------

# MAGIC %md #####
# MAGIC 2. Create a new columns called "Amount_in_Dollers" using "unit" column where if the unit value is equal to "DOLLARS(millions)" 
# MAGIC then multiply the "value" column with 10000000 else assign it as 0
# MAGIC 3. Create a new columns called "Amount_in_Rupees" using "unit" column where if the unit value is equal to "COUNT" 
# MAGIC then multiply the "value" column with 10000000 else assign it as 0

# COMMAND ----------

df_final=df.withColumn("Amount_in_Dollers",when(df.unit=="DOLLARS(millions)",df.value*10000000).when(df.unit!="DOLLARS(millions)",0)).withColumn("Amount_in_Rupees",when(df.unit=="COUNT",df.value*10000000).when(df.unit!="COUNT",0))
df_final.show(5,truncate=False)

# COMMAND ----------

# MAGIC %md ####
# MAGIC 4. Remove the columns "value","unit","rme_size_grp" from the df and save the new dataframe to a new path

# COMMAND ----------

df_final=df_final.drop("value","unit","rme_size_grp")

# COMMAND ----------

df_final.write.format("csv").mode("overwrite").option("delimiter","|").option("Header",True).save("/FileStore/tables/Annual_finance")

# COMMAND ----------

df_a=spark.read.format("csv").option("delimiter","|").option("Header",True).option("Inferschema",True).load("/FileStore/tables/Annual_finance")
df_a.show(5,truncate=False)

# COMMAND ----------

# MAGIC %md ####
# MAGIC 5. Use the new path data and print Industry wise max amount in dollers and rupees 

# COMMAND ----------

df_max=df_a.groupBy("industry_name","industry_code").agg(max(df_a.Amount_in_Dollers).alias("maxInDollers"),max(df_a.Amount_in_Rupees).alias("maxInRupees"))
df_max.show(truncate=False)

# COMMAND ----------

# MAGIC %md ####
# MAGIC 6. Find the rolling sum of Dollers and Rupees based on years

# COMMAND ----------

windowSpec  = Window.partitionBy("year").orderBy("year")
df_roll=df_a.withColumn("rollingSumOfDollersByYear",sum(df_a.Amount_in_Dollers).over(windowSpec))\
    .withColumn("rollingSumOfRupeesByYear",sum(df_a.Amount_in_Rupees).over(windowSpec))
df_roll=df_roll.select("year","rollingSumOfDollersByYear","rollingSumOfRupeesByYear").distinct()
df_roll.show()

# COMMAND ----------

# MAGIC %md ####
# MAGIC 7. Identifing the Even rows and finding Rolling Sum based on distinct Indust

# COMMAND ----------

df_even=df_a.withColumn("id",row_number().over(Window.orderBy("year","industry_code","industry_name","variable","Amount_in_Dollers","Amount_in_Rupees")))
df_even=df_even.filter((df_even.id)%2==0)
df_even=df_even.withColumn("rollingSumRupeesByIndustry",sum(df_a.Amount_in_Rupees).over(Window.partitionBy("industry_code","industry_Name").orderBy("industry_code","industry_Name"))).withColumn("rollingSumDollersByIndustry",sum(df_a.Amount_in_Dollers).over(Window.partitionBy("industry_code","industry_Name").orderBy("industry_code","industry_Name")))
df_even=df_even.select("industry_name","rollingSumRupeesByIndustry","rollingSumDollersByIndustry").distinct()
df_even.show(truncate=False)

# COMMAND ----------

# MAGIC %md ####
# MAGIC 8. identifing the max of value of Amount_in_Rupees and display the year and industry name 

# COMMAND ----------

max_in_rupees=df_a.agg(max("Amount_in_Rupees")).collect()[0][0]
df_industry=df_a.select("industry_name","year").filter(df_a.Amount_in_Rupees==max_in_rupees)
print(max_in_rupees)
df_industry.show(truncate=False)
