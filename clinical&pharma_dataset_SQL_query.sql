-- Databricks notebook source
-- MAGIC %python
-- MAGIC fileroot = "clinicaltrial_2021"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Clinicaltrial_file = ("/FileStore/tables/"+fileroot+".csv")
-- MAGIC Pharma = ('dbfs:/FileStore/tables/pharma.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/"+fileroot+".csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #reading the clinicaltrial datset into Spark DataFrame 
-- MAGIC
-- MAGIC clinicaltrial_df = spark.read.csv("/FileStore/tables/"+fileroot+".csv",sep="|",header=True,inferSchema=True)
-- MAGIC
-- MAGIC clinicaltrial_df.show(2, truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #creating a temporary table (view) from the clinicaltrial DataFrame 
-- MAGIC
-- MAGIC clinicaltrial_df.createOrReplaceTempView("clinicaltrial_sql")

-- COMMAND ----------

SELECT *
FROM clinicaltrial_sql
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Q1
--The number of studies in the dataset.

SELECT DISTINCT COUNT('Id') 
FROM clinicaltrial_sql;

-- COMMAND ----------

-- DBTITLE 1,Q2
--list all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type.

SELECT Type, COUNT(Type) AS frequency FROM clinicaltrial_sql
GROUP BY Type
ORDER BY frequency DESC;

-- COMMAND ----------

-- DBTITLE 1,Q3
--top 5 conditions (from Conditions) with their frequencies.

--splitting Conditions by delimiter
WITH top_conditions AS (
SELECT SPLIT(Conditions, ',')AS conditions
FROM clinicaltrial_sql
),

--mapping to key and values
explode_conditions AS (
SELECT EXPLODE(Conditions) AS conditions
FROM top_conditions
)

--counting, grouping, and ordering
SELECT Conditions, COUNT(*) AS frequency
FROM explode_conditions
GROUP BY Conditions
ORDER BY frequency DESC
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,Q4
--Find the 10 most common sponsors that are not pharmaceutical companies, alongwith the number of clinical trials they have sponsored. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #reading the parma datset into Spark DataFrame 
-- MAGIC
-- MAGIC pharma_df = spark.read.csv(Pharma,header=True,inferSchema=True,sep=",")
-- MAGIC
-- MAGIC pharma_df.show(2, truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #creating a temporary table (view) from the pharma DataFrame 
-- MAGIC
-- MAGIC pharma_df.createOrReplaceTempView("pharma_sql")

-- COMMAND ----------

SELECT *
FROM pharma_sql
LIMIT 2;

-- COMMAND ----------

SELECT Sponsor, COUNT('Sponsor') AS freq FROM clinicaltrial_sql
LEFT JOIN pharma_sql ON clinicaltrial_sql.Sponsor=pharma_sql.Parent_Company
WHERE pharma_sql.Parent_Company IS NULL
GROUP BY Sponsor
ORDER BY freq DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Q5
--Plot number of completed studies each month in a given year – for the submission dataset, the year is 2021. You need to include your visualization as well as a table of all the values you have plotted for each month

SELECT Completion AS month, COUNT(*) AS no_of_completed_studies
FROM clinicaltrial_sql
WHERE Completion LIKE '%2021' AND Status='Completed'
GROUP BY month
ORDER BY month(CAST(TO_DATE(Completion,'MMM yyyy')AS date));
