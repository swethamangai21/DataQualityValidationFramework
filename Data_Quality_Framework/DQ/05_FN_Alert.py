# Databricks notebook source
# importing required libraries

import pyspark.sql.functions as F

from pyspark.sql.functions import (
    col,
    lit,
    length,
    regexp_extract,
    concat,
    concat_ws,
    count,
    collect_list,
    date_add,
    current_date,
    lower
)

from pyspark.sql.types import *
from pyspark.sql import Window

import pandas as pd
import pyspark.pandas as ps

import sys
import os

from dateutil.relativedelta import relativedelta

import datetime
import calendar

from datetime import (
    date,
    datetime as datetime1,
    timedelta
)

from joblib import Parallel, delayed


# Serverless adaptation:
# The original project explicitly set classic-compute Spark configs here.
# We are not setting them in the Serverless replica.


ps.set_option(
    "compute.ops_on_diff_frames",
    True
)


# Email libraries used later by the alert logic

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from smtplib import SMTP
import smtplib

from os.path import basename
from email.utils import COMMASPACE, formatdate


from pyspark.sql.functions import (
    regexp_replace,
    trim
)

from pyspark.sql.functions import (
    countDistinct,
    upper
)

from pyspark.sql.functions import sum as _sum

# COMMAND ----------

# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

database_name = 'null'

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

# MAGIC %run ../Reference/03_FN_Function

# COMMAND ----------

# Load dashboard names from MDS

df_MDS_dashboard = (
    read_from_MDS_DQCOE_server('[mdm].[vw_dashboard_ref]')
    .select("Code")
)

# COMMAND ----------

# creating dropdown widget which contains dashboard values from MDS table

dbutils.widgets.dropdown(
    name="Dashboard Name",
    defaultValue="Finance",
    choices=[row["Code"] for row in df_MDS_dashboard.collect()]
)

dashboard_name = dbutils.widgets.get("Dashboard Name")

# COMMAND ----------

# Load DQ Rules from MDS

df_MDS_dq_rules = (
    read_from_MDS_DQCOE_server('[mdm].[vw_DQ_Rules]')
    .select(
        F.col('Source Name_Code').alias('Source_Name'),
        F.col('Database_Name').alias('Database_Name'),
        F.col('Table Name').alias('Table_Name'),
        F.col('Column Name').alias('Column_Name_Source'),
        F.col('Dimension_Code').alias('Dimension'),
        F.col('Technical_Rule').alias('Rule_Description'),
        "X",
        "Y",
        F.col('Data Quality Rule').alias('Business_Rule_Description'),
        F.col('HealthScoreFlag_Code').alias('Health_Score_Flag'),
        "Dashboard_Name_Code",
        F.col('Development_Status_Code').alias('Active')
    )
)

df_MDS_dq_rules = df_MDS_dq_rules.filter(
    (trim(col('Active')) == 'Active') &
    (trim(col('Dashboard_Name_Code')) == 'Finance')
)

# COMMAND ----------

df_MDS_dq_rules.display()

# COMMAND ----------

# Get the count of rules from the MDS data frame

df_MDS_dq_rules = df_MDS_dq_rules.withColumn(
    'table_name',
    lower(col("Table_Name"))
)

df_MDS_dq_rules = df_MDS_dq_rules.groupby("Table_Name").agg(
    count("Business_Rule_Description").alias("MDS Rule Count")
)

df_MDS_dq_rules.display()

# COMMAND ----------

# Load Fact Rules from DQCE server for rule alert.
df_fact_rules = read_from_DQCE_server(
    "[" + schema + "].[vw_fact_rules]"
)

Latest_snapshot_date = (
    df_fact_rules
    .agg({"Snapshot_Date": "max"})
    .collect()[0][0]
)

# Load Dim table from DQCE server.
df_dim_table = read_from_DQCE_server(
    "[" + schema + "].[Dim_Table]"
)

df_vw_fact_rules = (
    df_fact_rules
    .join(df_dim_table, ['tablekey'])
    .where(df_fact_rules.Snapshot_Date == Latest_snapshot_date)
)

df_vw_fact_rules.display()

# COMMAND ----------

# Error count from fact rules

df_vw_fact_rules_error_count = (
    df_vw_fact_rules
    .withColumnRenamed("Fail_Count", "Fact Fail Count")
    .select(
        "Rule_ID",
        "TableKey",
        "Source_Name",
        "Table_Name",
        "Column_Name_Source",
        "Snapshot_Date",
        "Fact fail Count"
    )
    .groupby(
        "Rule_ID",
        "TableKey",
        "Source_Name",
        "Table_Name",
        "Column_Name_Source",
        "Snapshot_Date"
    )
    .agg(
        _sum("Fact fail count").alias("Fact fail count")
    )
)

df_vw_fact_rules_error_count.display()

# COMMAND ----------

# Rule count from fact rules

df_fact_rules_final = (
    df_vw_fact_rules
    .groupby("Table_Name")
    .agg(
        countDistinct("Rule_ID").alias("Dashboard Rule Count")
    )
)

df_fact_rules_final = df_fact_rules_final.withColumn(
    "Table_Name",
    upper(df_fact_rules_final["Table_Name"])
)

display(df_fact_rules_final)

# COMMAND ----------

# Rule count comparison

# Convert "Table_Name" to uppercase in both the dataframes to be joined.
df_MDS_dq_rules = df_MDS_dq_rules.withColumn(
    "Table_Name",
    upper(trim(df_MDS_dq_rules["Table_Name"]))
)

df_fact_rules_final = df_fact_rules_final.withColumn(
    "Table_Name",
    upper(trim(df_fact_rules_final["Table_Name"]))
)


# Join the Fact DQ Rules with MDS DQ Rules on "Table_Name"
final_join_df = df_MDS_dq_rules.join(
    df_fact_rules_final,
    ["Table_Name"],
    how="left"
)

rule_alert_df = final_join_df.withColumn(
    "Rules Diff",
    col("MDS Rule Count") - col("Dashboard Rule Count")
)

rule_alert_df.display()

# COMMAND ----------

gmail_user = "swethamangai.2004@gmail.com"

gmail_app_password = dbutils.secrets.get(
    scope="finance-dq-kv-scope",
    key="gmail-password"
)

# COMMAND ----------

# Receiver email
send_to = [
    "swethamangai.r@gmail.com"
]

# Email subject
subject = dashboard_name + " - Alert for MDS VS SQL Table Count"

send_count_alert_mail_DQCOE_Admin(
    send_from=gmail_user,
    send_to=send_to,
    subject=subject,
    header_text="Count comparison",
    latest_snapshot=Latest_snapshot_date,
    countalert=rule_alert_df.toPandas(),
    smtp_user=gmail_user,
    smtp_password=gmail_app_password,
    server="smtp.gmail.com",
    port=587
)

# COMMAND ----------

import smtplib

smtp = smtplib.SMTP(
    "smtp.gmail.com",
    587,
    timeout=20
)

smtp.starttls()

smtp.login(
    gmail_user,
    gmail_app_password
)

print("Gmail SMTP login successful")

smtp.quit()

# COMMAND ----------

# %sql
# CREATE SCHEMA IF NOT EXISTS dbw_finance_dq_dev.dqcoe_features;

# COMMAND ----------

error_folderpath = (
    parameters_global["error_report_path"]
    + "Finance_Fact_Error_v1.delta"
)

spark.sql(f"""
CREATE OR REPLACE VIEW dbw_finance_dq_dev.dqcoe_features.vwdl_fn_Fact_Error
AS
SELECT *
FROM delta.`{error_folderpath}`
""")

# COMMAND ----------

spark.sql("""
SELECT *
FROM dbw_finance_dq_dev.dqcoe_features.vwdl_fn_Fact_Error
--LIMIT 10
""").display()

# COMMAND ----------

# SQL statement on Error source
SQL_Error_Count_Stmt = F"""
SELECT
    SUM(fail_count) AS Datalake_error_count,
    Rule_ID,
    TableKey
FROM dbw_finance_dq_dev.dqcoe_features.vwdl_fn_Fact_Error
WHERE snapshot_date = '{Latest_snapshot_date}'
GROUP BY
    Rule_ID,
    TableKey
"""

# Read error records from data lake
dl_error_count_df = spark.sql(SQL_Error_Count_Stmt)

# COMMAND ----------

# Join Fact_Rules failure count with Fact_Error count

Error_count_df = (
    df_vw_fact_rules_error_count
    .join(
        dl_error_count_df,
        ["TableKey", "Rule_ID"],
        how="left"
    )
    .fillna(0)
)

Error_alert_df = Error_count_df.withColumn(
    "Error count Diff",
    col("Fact fail count") - col("Datalake_error_count")
)

Error_alert_df = Error_alert_df.withColumnRenamed(
    "Datalake_error_count",
    "Error report count"
)

Error_alert_df = Error_alert_df.orderBy(
    col("Source_Name").desc(),
    col("Table_Name").asc()
)

Error_alert_df.display()

# COMMAND ----------

# Alert email for error record count

subject = dashboard_name + " - Alert for DQ Rules and Error Report Count"

send_count_alert_mail_DQCOE_Admin(
    send_from=gmail_user,
    send_to=send_to,
    subject=subject,
    header_text="Count comparison",
    latest_snapshot=Latest_snapshot_date,
    countalert=Error_alert_df.toPandas(),
    smtp_user=gmail_user,
    smtp_password=gmail_app_password,
    server="smtp.gmail.com",
    port=587
)