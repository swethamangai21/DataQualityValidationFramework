# Databricks notebook source
# Importing required libraries

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
    lower,
    when,
    regexp_replace,
    month,
    year,
    trim,
    upper,
    countDistinct,
    to_date,
    date_format
)

from pyspark.sql.types import *
from pyspark.sql import Window

from delta.tables import DeltaTable

import pandas as pd

import sys
import os
import re

from dateutil.relativedelta import relativedelta

import datetime
import calendar

from datetime import (
    date,
    datetime as datetime1,
    timedelta
)

from joblib import Parallel, delayed


# Email libraries

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from smtplib import SMTP
import smtplib

from os.path import basename
from email.utils import COMMASPACE, formatdate

# COMMAND ----------

# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

database_name = 'null'
table_name = 'null'

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

# MAGIC %run ../Reference/03_FN_Function

# COMMAND ----------

# Creating dropdown widgets which contains only dashboard values from reference table

df_MDS_dashboard = (
    read_from_MDS_DQCOE_server('[mdm].[vw_dashboard_ref]')
    .select("Code")
)

dbutils.widgets.dropdown(
    name="Dashboard_Name",
    defaultValue="Finance",
    choices=[row["Code"] for row in df_MDS_dashboard.collect()]
)

dashboard_name = dbutils.widgets.get("Dashboard_Name")

# COMMAND ----------

snapshot_date = date.today()

print("snapshot_date:: ", snapshot_date)

# COMMAND ----------

# Reading latest error report

error_folderpath = (
    parameters_global["error_report_path"]
    + "Finance_Fact_Error_v1.delta"
)

print("error_folderpath:: ", error_folderpath)

df_error_report = (
    spark.read
    .format("delta")
    .load(error_folderpath)
)

initial_error_report_count = df_error_report.count()

print(
    "initial_error_report_count:: ",
    initial_error_report_count
)

# COMMAND ----------

# Receiver email for archive alerts
send_to = [
    "swethamangai.r@gmail.com"
]

# Email subject
subject = dashboard_name + " - Alert for Error Report Archive Count"

# COMMAND ----------

current_date = datetime1.now()

archive_threshold_date = (
    current_date - relativedelta(months=6)
).strftime('%Y-%m-%d')

print(
    "archive_threshold_date:: ",
    archive_threshold_date
)

archive_year = str(current_date).split("-")[0]

# COMMAND ----------

# Filter records to archive
df_FN_Fact_Error_archive = df_error_report.filter(
    col("Snapshot_Date") < archive_threshold_date
)

records_to_archive_cnt = df_FN_Fact_Error_archive.count()

print(
    "Archive_data_count:: ",
    records_to_archive_cnt
)

archive_error_folderpath = (
    parameters_global["error_report_archivepath"]
    + "Archive_"
    + archive_year
    + ".delta"
)

print(
    "archive_error_folderpath:: ",
    archive_error_folderpath
)

# COMMAND ----------

# No records are old enough to archive

df_final_error_report_schema = (
    df_error_report
    .groupBy("Snapshot_Date")
    .count()
    .withColumnRenamed(
        "count",
        "Error_Record_Count"
    )
    .limit(0)
)

df_final_error_report_schema.display()

# COMMAND ----------

# Gmail sender configuration for replica

gmail_user = "swethamangai.2004@gmail.com"

gmail_app_password = dbutils.secrets.get(
    scope="finance-dq-kv-scope",
    key="gmail-password"
)

# COMMAND ----------

send_count_alert_mail_DQCOE_Admin(
    send_from=gmail_user,
    send_to=send_to,
    subject=subject,
    header_text="Error Report Archive Count comparison. No Error record to be archived",
    latest_snapshot=snapshot_date,
    countalert=df_final_error_report_schema.toPandas(),
    smtp_user=gmail_user,
    smtp_password=gmail_app_password,
    server="smtp.gmail.com",
    port=587
)

# COMMAND ----------

df_FN_Fact_Error_archive = (
    df_FN_Fact_Error_archive
    .withColumn(
        "Snapshot_Year",
        year("Snapshot_Date")
    )
)

df_FN_Fact_Error_archive.printSchema()

# COMMAND ----------

error_schema = StructType([
    StructField("Value", StringType(), True),
    StructField("Supporting_CDE_Values", StringType(), True),
    StructField("Account_Number", StringType(), True),
    StructField("Source_Org_Name", StringType(), True),
    StructField("fail_count", IntegerType(), True),
    StructField("Source_Name", StringType(), True),
    StructField("Rule_ID", IntegerType(), True),
    StructField("Tablekey", StringType(), True),
    StructField("Snapshot_Date", DateType(), True),
    StructField("Loaded_By", StringType(), True),
    StructField("Snapshot_Year", IntegerType(), True)
])


# COMMAND ----------

if records_to_archive_cnt > 0:

    # Step 1: Write old records to archive
    df_FN_Fact_Error_archive.write \
        .mode("append") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .partitionBy("Snapshot_Year") \
        .save(archive_error_folderpath)

    # Step 2: Delete archived records from active Fact_Error
    delta_table = DeltaTable.forPath(
        spark,
        error_folderpath
    )

    delta_table.delete(
        f"Snapshot_Date < '{archive_threshold_date}'"
    )

    # Step 3: Read archive back
    archive_error_report = (
        spark.read
        .format("delta")
        .load(archive_error_folderpath)
    )

    archive_error_report_count = archive_error_report.count()

    print(
        "archive_error_report_count::",
        archive_error_report_count
    )

    # Active error record counts
    df_final_error_report = (
        df_error_report
        .groupBy("Snapshot_Date")
        .count()
        .withColumn(
            "Archival Status",
            lit("Active")
        )
    )

    # Archived error record counts
    df_final_archive_error_report = (
        archive_error_report
        .groupBy("Snapshot_Date")
        .count()
        .withColumn(
            "Archival Status",
            lit("Archived")
        )
    )

    # Combine active + archive counts
    df_final = (
        df_final_error_report
        .unionByName(df_final_archive_error_report)
        .withColumnRenamed(
            "count",
            "Error_Record_Count"
        )
        .orderBy(
            col("Snapshot_Date").desc()
        )
    )

    # Re-read active Fact_Error after delete
    df_error_report = (
        spark.read
        .format("delta")
        .load(error_folderpath)
    )

    final_error_report_count = df_error_report.count()

    print(
        "final_error_report_count::",
        final_error_report_count
    )

    # Validate archival counts
    if initial_error_report_count == (
        records_to_archive_cnt + final_error_report_count
    ):

        send_count_alert_mail_DQCOE_Admin(
            send_from=gmail_user,
            send_to=send_to,
            subject=subject,
            header_text="Error Report Archive Count comparison. Count is matching",
            latest_snapshot=snapshot_date,
            countalert=df_final.toPandas(),
            smtp_user=gmail_user,
            smtp_password=gmail_app_password,
            server="smtp.gmail.com",
            port=587
        )

    else:

        send_count_alert_mail_DQCOE_Admin(
            send_from=gmail_user,
            send_to=send_to,
            subject=subject,
            header_text="Error Report Archive Count comparison. Mismatch in count",
            latest_snapshot=snapshot_date,
            countalert=df_final.toPandas(),
            smtp_user=gmail_user,
            smtp_password=gmail_app_password,
            server="smtp.gmail.com",
            port=587
        )

else:

    # No records are old enough to archive
    df_final_error_report_schema = (
        df_error_report
        .groupBy("Snapshot_Date")
        .count()
        .withColumnRenamed(
            "count",
            "Error_Record_Count"
        )
        .limit(0)
    )

    send_count_alert_mail_DQCOE_Admin(
        send_from=gmail_user,
        send_to=send_to,
        subject=subject,
        header_text="Error Report Archive Count comparison. No Error record to be archived",
        latest_snapshot=snapshot_date,
        countalert=df_final_error_report_schema.toPandas(),
        smtp_user=gmail_user,
        smtp_password=gmail_app_password,
        server="smtp.gmail.com",
        port=587
    )