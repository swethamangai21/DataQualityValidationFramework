# Databricks notebook source
dbutils.widgets.text("Job_User", "", "Loaded_By")
dbutils.widgets.text("Database_Name", "All", "Database_Name")

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

from pyspark.sql.types import (
    StringType,
    LongType,
    IntegerType,
    DecimalType
)

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP

import smtplib
import sys

from os.path import basename
from email.utils import COMMASPACE, formatdate

# COMMAND ----------

Loaded_By = (
    dbutils.widgets.get("Job_User")
    .replace("@cummins.com", "")
    .lower()
    .strip()
)

database_name = (
    dbutils.widgets.get("Database_Name")
    .strip()
)

# COMMAND ----------

# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

Source_Org_df_MDS = (
    read_from_MDS_DQCOE_server("[mdm].[vw_Source_Org]")
    .select(
        F.col("Code").alias("Org_Id"),
        F.col("Source_Name_Code").alias("Source_Name"),
        "Org_Name",
        "Source_Org_Name",
        F.col("Active_Code").alias("Active")
    )
    .distinct()
)

display(Source_Org_df_MDS)

# COMMAND ----------

# cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")  #rep
cluster_name = "serverless"  #serverless

script = "FN_Source_Org"
source = "All"
databaseName = "All"
tableName = "All"

rows_processed = Source_Org_df_MDS.count()
print(rows_processed)

df_temp_for_scala = spark.createDataFrame(
    [(script, rows_processed, databaseName, tableName)],
    ("script", "rows_processed", "databaseName", "tableName")
)

df_temp_for_scala.createOrReplaceTempView("temp_table_name")

# COMMAND ----------

df_final = (
    Source_Org_df_MDS
    .withColumn("Loaded_By", F.lit(Loaded_By))
    .select(
        "Source_Name",
        "Source_Org_Name",
        "Org_Id",
        "Org_Name",
        "Active",
        "Loaded_By"
    )
)

display(df_final)

# COMMAND ----------

Delete_stg_tbl_DQCE_server(
    schema,
    "stg_MDS_Source_Org"
)

# COMMAND ----------

write_to_DQCE_sql_server(
    df_final,
    parameters_global["stg_MDS_Source_Org"],
    "Append"
)

# COMMAND ----------

Update_Table_DQCE_server(
    schema,
    "Merge_dimSourceOrgID"
)

# COMMAND ----------

# Check for new Source Orgs missing in MDS

df_dim_Source_Org = read_from_DQCE_server(
    f"[{schema}].[Dim_Source_Org]"
)

df_email_to_send = (
    df_dim_Source_Org
    .where("Org_Id is NULL and Org_Name is NULL")
    .select(
        "Source_Name",
        "Source_Org_Name"
    )
    .distinct()
)

display(df_email_to_send)

# COMMAND ----------

# Original project sends an email through Cummins mail relay if new Source Orgs are missing in MDS  #rep
#
# if df_email_to_send.count() > 0:
#     header_text = (
#         "New sources in SQL Table [{0}].[Dim_Source_Org]. "
#         "Add these sources in MDS entity [13_Source_Org] and update "
#         "Org_Name, Active fields respectively"
#     ).format(schema)
#
#     send_mail(
#         send_from='G_DQCOE_Admin@cummins.com',
#         send_to=['aravind.reddy@cummins.com'],
#         subject="Finance | Dim_Source_Org SQL new sources",
#         header_text=header_text,
#         text=df_email_to_send.toPandas(),
#         server="mailrelay.cummins.com"
#     )


# Dev/serverless equivalent  #serverless
if df_email_to_send.count() > 0:
    print("New Source Orgs were found that are missing in MDS.")
    display(df_email_to_send)
else:
    print("No new Source Orgs are missing in MDS.")

# COMMAND ----------

# write source ORG table to ADLS

df_dim_Source_Org = df_dim_Source_Org.withColumn(
    "Source_Org_Name",
    F.col("Source_Org_Name").cast("string")
)

src_org_view_folderpath = (
    parameters_global["path"]
    + "Source_Org_Finance"
    + ".delta"
)

print(src_org_view_folderpath)

df_dim_Source_Org.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .save(src_org_view_folderpath)