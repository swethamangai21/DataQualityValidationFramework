# Databricks notebook source
dbutils.widgets.text("Job_User", "", "Loaded_By")
dbutils.widgets.text("Database_Name", "All", "Database_Name")

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.types import StringType, LongType, IntegerType, DecimalType

# COMMAND ----------

# #@@@@@@@@@@@@ rep
# %scala
# val Loaded_By = dbutils.widgets.get("Job_User").replace("@cummins.com","").toLowerCase()
# val database_name = dbutils.widgets.get("Database_Name").toLowerCase()

# COMMAND ----------

Loaded_By = (
    dbutils.widgets.get("Job_User")
    .replace("@cummins.com", "")
    .lower()
    .strip()
)

database_name = (
    dbutils.widgets.get("Database_Name")
    .lower()
    .strip()
)

# COMMAND ----------

# MAGIC
# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

# MAGIC %run ../Reference/03_FN_Function

# COMMAND ----------

Rules_df = read_from_MDS_DQCOE_server('[mdm].[vw_DQ_Rules]')\
.select(
    F.col("Source Name_Code").alias("Source_Name"),
    "Database_Name",
    F.col("Table Name").alias("Table_Name"),
    F.col("Column Name").alias("Column_Name_Source"),
    F.col("Dimension_Code").alias("Dimension"),
    F.col("Technical_Rule").alias("Rule_Description"),
    "X",
    "Y",
    F.col("Data Quality Rule").alias("Business_Rule_Description"),
    "Supporting_CDE_Values",
    F.col("HealthScoreFlag_Code").alias("Health_Score_Flag"),
    F.col("Development_Status_Code").alias("Active"),
    F.col("RequestType").alias("RequestType"),
    F.col("Dashboard_Name_Code").alias("Dashboard_Name_Code")
)

# Rules_df.display()

# COMMAND ----------

# #Actual project
# Rules_df = Rules_df.where(
#     'Source_Name in ("Bolt","1OM","CIL","BZL","hhp","C360") '
#     'and Database_Name in ("CORE_PRD_RL_BOLT_DB","scc1_raw","CORE_PRD_RL_CIL_DB",'
#     '"CORE_PRD_RL_CBZ1_DB","CORE_PRD_RL_HHP1_DB","mdm_c360_raw")'
# )

# COMMAND ----------

Rules_df = Rules_df.where(
    'Source_Name in ("Bolt","1OM","CIL","BZL","HHP","C360") '
    'and upper(Database_Name) = "FINANCE_DQ_SOURCE_DEV"'
)

# COMMAND ----------

Rules_df = Rules_df.filter(
    (Rules_df.Active == "Active") |
    (Rules_df.Active == "Testing")
)

Rules_df.display()

# COMMAND ----------

# cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")  #rep
cluster_name = "serverless"  #serverless

script = "FN_DQ_Rules"
source = "All"
databaseName = "All"
tableName = "All"

rows_processed = Rules_df.count()
print(rows_processed)

df_temp_for_scala = spark.createDataFrame(
    [(script, rows_processed, databaseName, tableName)],
    ("script", "rows_processed", "databaseName", "tableName")
)

df_temp_for_scala.createOrReplaceTempView("temp_table_name")

# COMMAND ----------

# #@@@@@@@@@ Rep
# // Access the temporary table created in Python using Scala 
# val df_scala = spark.table("temp_table_name")  
# // Convert the DataFrame to Scala variables  
# val script = df_scala.select("script").collect.map(_(0)).toList.head.toString  
# val tableName = df_scala.select("tableName").collect.map(_(0)).toList.head.toString  
# val databaseName = df_scala.select("databaseName").collect.map(_(0)).toList.head.toString  
# val rows_processed = df_scala.select("rows_processed").collect.map(_(0)).toList.head.toString.toInt  

# COMMAND ----------

Write_log_DQCE_server(
    source,
    databaseName,
    schema,
    tableName,
    Loaded_By,
    script,
    cluster_name
)

# COMMAND ----------

spk_df = Rules_df.withColumn(
    "Loaded_By",
    F.lit(Loaded_By)
).where(
    "Business_Rule_Description is not null"
)

display(spk_df)

# COMMAND ----------

Delete_stg_tbl_DQCE_server(schema,"stg_Dim_Rules")

# COMMAND ----------

write_to_DQCE_sql_server(
    spk_df,
    parameters_global["stg_Dim_Rules"],
    "Append"
)

# COMMAND ----------

Update_Table_DQCE_server(schema,"Merge_dimRules")

# COMMAND ----------

# #@@@@@@@ Rep
# %scala
# Update_log_DQCE_server(
#     databaseName,
#     schema,
#     tableName,
#     Loaded_By,
#     rows_processed,
#     script
# )

# COMMAND ----------

Update_log_DQCE_server(
    databaseName,
    schema,
    tableName,
    Loaded_By,
    rows_processed,
    script
)