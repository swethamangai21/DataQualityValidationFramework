# Databricks notebook source
# Databricks notebook source

dbutils.widgets.text("Job_User", "", "Loaded_By")
dbutils.widgets.text("Database_Name", "All", "Database_Name")

# COMMAND ----------

# Import required PySpark functions
import pyspark.sql.functions as F

# COMMAND ----------

# #@@@@@@@@@@@@@@@@@@ Rep

# %scala
# val Loaded_By = dbutils.widgets.get("Job_User").replace("@cummins.com","").toLowerCase()
# val database_name = dbutils.widgets.get("Database_Name").toLowerCase()

# COMMAND ----------

# #^^^^^^^^^^ Serverless
# ============================================================
# Serverless version - read notebook parameters
# ============================================================

Loaded_By = (
    dbutils.widgets.get("Job_User")
    .replace("@cummins.com", "")
    .lower()
)

database_name = (
    dbutils.widgets.get("Database_Name")
    .lower()
)

print("Loaded_By:", Loaded_By)
print("database_name:", database_name)

# COMMAND ----------

# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

# MAGIC %run ../Reference/03_FN_Function

# COMMAND ----------

# Read Critical Attribute metadata from MDS

Critical_df = (
    read_from_MDS_DQCOE_server("[mdm].[vw_Critical]")
    .select(
        F.col("Source_Code").alias("Source"),
        "Database_Name",
        "Table_Name",
        "Attribute_Name_Source",
        "Attribute_Name",
        F.col("Critical_Code").alias("Critical"),
        F.col("Active_code").alias("Active"),
        "Definition",
        "Level_1",
        "Level_2",
        F.col("CDE Owner").alias("CDE_Owner"),
        F.col("CDE Owning Function").alias("CDE_Owning_Function")
    )
)

# COMMAND ----------

Critical_df = Critical_df.where(
    'Source in ("Bolt","1OM","CIL","BZL","HHP","C360")'
)

# COMMAND ----------

# #@@@@@@@@@@@@@@@ Rep
# cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
# script = 'FN_Critical_Attributes'
# source = 'All'
# database_name = 'All'
# tableName = 'All'
# rows_processed = Critical_df.count()

# df_temp_for_scala = spark.createDataFrame(
#     [(source, database_name, tableName, script, rows_processed)],
#     ("source", "database_name", "tableName", "script", "rows_processed")
# )

# df_temp_for_scala.createOrReplaceTempView("temp_table_name")

# COMMAND ----------

# #^^^^^^^^^^^^^^ Serverless
# ============================================================
# Serverless equivalent - execution metadata
# ============================================================

try:
    cluster_name = spark.conf.get(
        "spark.databricks.clusterUsageTags.clusterName"
    )
except Exception:
    cluster_name = "serverless"

script = "FN_Critical_Attributes"
source = "All"
database_name = "All"
tableName = "All"

rows_processed = Critical_df.count()

print("script:", script)
print("source:", source)
print("database_name:", database_name)
print("tableName:", tableName)
print("rows_processed:", rows_processed)
print("cluster_name:", cluster_name)

# COMMAND ----------

# #@@@@@@@@@@@@@@@ Rep
# %scala

# // Access the temporary table created in Python
# val df_scala = spark.table("temp_table_name")

# val source =
#   df_scala.select("source").collect.map(_(0)).toList.head.toString

# val script =
#   df_scala.select("script").collect.map(_(0)).toList.head.toString

# val tableName =
#   df_scala.select("tableName").collect.map(_(0)).toList.head.toString

# val database_name =
#   df_scala.select("database_name").collect.map(_(0)).toList.head.toString

# val rows_processed =
#   df_scala.select("rows_processed").collect.map(_(0)).toList.head.toString.toInt

# COMMAND ----------

spk_df = Critical_df.withColumn(
    "Loaded_By",
    F.lit(Loaded_By)
)

# COMMAND ----------

# # @@@@@@@@@@@@@ Rep
# %scala

# Delete_stg_tbl_DQCE_server(schema, "stg_Critical")

# COMMAND ----------

# #^^^^^^^^^^^^^^ Serverless

Delete_stg_tbl_DQCE_server(
    schema,
    "stg_Critical"
)

# COMMAND ----------

write_to_DQCE_sql_server(
    spk_df,
    parameters_global["stg_Critical"],
    "Append"
)

# COMMAND ----------

# # @@@@@@@@@@@@@ Rep
# %scala

# // data from stg_Critical to Dim_Table
# Update_Table_DQCE_server(schema, "Merge_Critical")

# COMMAND ----------

# #^^^^^^^^^^^^^^ Serverless
Update_Table_DQCE_server(
    schema,
    "Merge_Critical"
)

# COMMAND ----------

# # @@@@@@@@@@@@@ Rep
# %scala

# // update log
# Update_log_DQCE_server(
#     database_name,
#     schema,
#     tableName,
#     Loaded_By,
#     rows_processed,
#     script
# )

# COMMAND ----------

# #^^^^^^^^^^^^^^ Serverless

Update_log_DQCE_server(
    database_name,
    schema,
    tableName,
    Loaded_By,
    rows_processed,
    script
)

# COMMAND ----------

# Validation 1 - check staging load

stg_critical_check = read_from_DQCE_server(
    "[lm_prd].[stg_Critical]"
)

print("Rows in stg_Critical:", stg_critical_check.count())

display(stg_critical_check)

# COMMAND ----------

dim_table_check = read_from_DQCE_server(
    "[lm_prd].[Dim_Table]"
)

print("Rows in Dim_Table:", dim_table_check.count())

display(dim_table_check)