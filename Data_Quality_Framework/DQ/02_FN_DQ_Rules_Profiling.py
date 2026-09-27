# Databricks notebook source
dbutils.widgets.text("Job_User", "", "Loaded_By")
dbutils.widgets.text("Database_Name", "", "Enter_Database")
dbutils.widgets.text("Table_Name", "", "Enter_Table")
dbutils.widgets.text("active_value", "", "Active_Rules")

dbutils.widgets.dropdown(
    name="Load_Type",
    defaultValue="full load",
    choices=["full load", "incremental load", "daily load"]
)

# core_prd_rl_bolt_db -- hz_cust_accounts

# COMMAND ----------

import pyspark.sql.functions as F

from pyspark.sql.functions import (
    col, lit, length, regexp_extract, concat, concat_ws, count,
    collect_list, date_add, current_date, when, lower, upper,
    coalesce, trim, from_json, expr, explode, explode_outer,
    array_contains, size, regexp_replace, split, instr
)

import re

from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable

import pandas as pd

# import pyspark.pandas as ps  #rep

import sys, os

from dateutil.relativedelta import relativedelta

import datetime, calendar

from datetime import (
    date,
    datetime as datetime1,
    timedelta
)

from joblib import Parallel, delayed


# spark.conf.set("spark.databricks.io.cache.enabled", "true")  #rep


# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  #rep


# ps.set_option('compute.ops_on_diff_frames', True)  #rep


# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  #rep


from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from smtplib import SMTP

import smtplib
import sys

from os.path import basename
from email.utils import COMMASPACE, formatdate

# COMMAND ----------

# # @@@@@@@@@@@@ rep
# import io.delta.tables._
# import org.apache.spark.sql.DataFrame
# import java.sql.Date
# import java.text.SimpleDateFormat

# val active_value = dbutils.widgets.get("active_value").trim()
# val load_type = dbutils.widgets.get("Load_Type").toLowerCase().trim()
# val database_name = dbutils.widgets.get("Database_Name").toLowerCase().trim()
# val table_name = dbutils.widgets.get("Table_Name").toLowerCase().trim()
# val Loaded_By = dbutils.widgets.get("Job_User").replace("@cummins.com","").toLowerCase().trim()

# COMMAND ----------

active_value = int(dbutils.widgets.get("active_value"))
database_name = dbutils.widgets.get("Database_Name").lower().strip()
table_name = dbutils.widgets.get("Table_Name").lower().strip()
load_type = dbutils.widgets.get("Load_Type").lower().strip()
Loaded_By = dbutils.widgets.get("Job_User").replace("@cummins.com", "").lower().strip()

# COMMAND ----------

# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

# MAGIC %run ../Reference/03_FN_Function

# COMMAND ----------

Source_Stakeholder_df = read_from_MDS_DQCOE_server(
    '[mdm].[vw_Source_Stakeholder]'
).select(
    "Source_Name_Code",
    "Source_Group_Code",
    "Region_Code"
).distinct()

Source_Stakeholder_df = Source_Stakeholder_df.where(
    '''Source_Name_Code in ("Bolt","1OM","CIL","BZL","HHP","C360")'''
)

# COMMAND ----------

dbutils.widgets.dropdown(
    name="Source",
    defaultValue="Bolt",
    choices=[
        col["Source_Name_Code"]
        for col in Source_Stakeholder_df
        .select("Source_Name_Code")
        .distinct()
        .orderBy("Source_Name_Code")
        .collect()
    ]
)

# COMMAND ----------

# #@@@@@@@@@@@@@@@@@ rep
# val source = dbutils.widgets.get("Source").trim() 

# COMMAND ----------

source = dbutils.widgets.get("Source").strip()

# COMMAND ----------

# get_table() defined in Function notebook.
# Get customized table name if required, else return same table_name
table_name = get_table(database_name, table_name)

print(table_name)

# get_df() defined in Function notebook.
# Uses the custom query if one is defined; otherwise reads the table directly.
df = get_df(table_name)

df.count()

# COMMAND ----------

## Load DimTable - master for any new source, table, column details

df_tabletemp = pd.DataFrame(df.head(0), columns=df.columns)

df_tablemaster = df_tabletemp

df_table = pd.DataFrame(
    [df_tablemaster.columns]
).rename(
    index={0: 'Column_Name_Source'}
).T

df_table['Source_Name'] = source
df_table['Database_Name'] = database_name
df_table['Table_Name'] = table_name
df_table['Loaded_By'] = Loaded_By

df_table = spark.createDataFrame(df_table).withColumn(
    "Column_Name_Source",
    F.lower(F.col("Column_Name_Source"))
)

df_table.display()

# COMMAND ----------

# #@@@@@@@@@ rep
# Delete_tbl_DQCE_server(schema,"stg_Dim_Table",source)

# COMMAND ----------

# ^^^^^^^^^^^^^^^^^ Serverless
Delete_tbl_DQCE_server(schema, "stg_Dim_Table", source)

# COMMAND ----------

write_to_DQCE_sql_server(
    df_table,
    parameters_global['stg_Dim_Table'],
    "Append"
)

# COMMAND ----------

dbutils.notebook.run(
    "../DQ/01_FN_Critical_Attribute",
    0,
    {
        "Job_User": Loaded_By,
        "Database_Name": "All"
    }
)

# COMMAND ----------

Update_DimTable_DQCE_server(schema, "Merge_dimTable", source)

# COMMAND ----------

dim_check = (
    read_from_DQCE_server("[lm_prd].[Dim_Table]")
    .filter(
        (F.lower(F.col("Source_Name")) == source.lower()) &
        (F.lower(F.col("Database_Name")) == database_name.lower()) &
        (F.lower(F.col("Table_Name")) == table_name.lower())
    )
    .orderBy("Tablekey")
)

display(dim_check)

# COMMAND ----------

dbutils.notebook.run(
    "../DQ/03_FN_DQ_Rules",
    0,
    {
        "Job_User": Loaded_By,
        "Database_Name": "All"
    }
)

# COMMAND ----------

df_source = (
    df
    .withColumn("Source_Name", F.lit(source))
    .withColumn("Source_Org_Name", F.upper(col("Source_Org_Name")))
    .select("Source_Name", "Source_Org_Name")
    .distinct()
)

# COMMAND ----------

df_final = (
    df_source
    .withColumn("Loaded_By", F.lit(Loaded_By))
    .select(
        "Source_Name",
        "Source_Org_Name",
        "Loaded_By"
    )
)

display(df_final)

# COMMAND ----------

Delete_tbl_DQCE_server(
    schema,
    "stg_Dim_Source_Org",
    source
) 

# COMMAND ----------

write_to_DQCE_sql_server(
    df_final,
    parameters_global["stg_Dim_Source_Org"],
    "Append"
)

# COMMAND ----------

Update_Table_DQCE_server(
    schema,
    "Merge_dimSourceOrg"
)

# COMMAND ----------

# DBTITLE 1,Cell 28
# Fetch Source_Org details from MDS and update Org_Id, Org_Name, Active in Dim_Source_Org

dbutils.notebook.run(
    "../DQ/04_FN_Source_Org_New",
    0,
    {
        "Job_User": Loaded_By,
        "Database_Name": "All"
    }
)

# COMMAND ----------

# read SQL [vw_Dim_Rules]

active_value = int(dbutils.widgets.get("active_value"))
database_name = dbutils.widgets.get("Database_Name").lower()
table_name = dbutils.widgets.get("Table_Name").lower()
load_type = dbutils.widgets.get("Load_Type").lower()

Loaded_By = (
    dbutils.widgets.get("Job_User")
    .replace("@cummins.com", "")
    .lower()
)

# Original project  #rep
# if active_value == 1 and schema == "fn_expl":
#     active_value = ["Active", "Testing", "Yes"]
# elif active_value == 1 and schema == "fn_prd":
#     active_value = ["Active", "Yes"]

# Our lm_prd schema represents the production DQ layer  #serverless
if active_value == 1 and schema == "lm_prd":
    active_value = ["Active", "Yes"]

input_df = (
    read_from_DQCE_server(f"[{schema}].[vw_Dim_Rules]")
    .filter(F.col("Active").isin(active_value))
    .orderBy("Rule_ID")
    .toPandas()
)

display(input_df)

# COMMAND ----------

# read data based on load type

load_type = dbutils.widgets.get("Load_Type").lower().strip()

print("Load Type:", load_type)

if load_type == "full load":
    df1 = df
else:
    raise ValueError(
        f"Current replication flow is configured for full load. "
        f"Received Load_Type = {load_type}"
    )

print("Source rows:", df1.count())

# COMMAND ----------

# create the temporary Delta staging copy of the current source table

# Get temporary ADLS Delta path
folderpath = parameters_global["path"] + table_name + ".delta"
print(folderpath)

# Remove previous copy of this table
dbutils.fs.rm(folderpath, recurse=True)

# Replace spaces in column names with underscores
df1 = df1.select(
    [
        F.col(col).alias(col.replace(" ", "_"))
        for col in df1.columns
    ]
)

# Write current source data to ADLS as Delta
df1.write \
    .mode("overwrite") \
    .format("delta") \
    .save(folderpath)

# Read the Delta copy back
df = spark.read \
    .format("delta") \
    .load(folderpath)

print(df.count())

# COMMAND ----------

if df.count() == 0:
    dbutils.notebook.exit(" == NO RECORDS IN TABLE ==")

# COMMAND ----------

# fetch snapshot_date, rows_processed

# cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")  #rep
cluster_name = "serverless"  #serverless

script = "FN_DQ_Rules_Profiling"

snapshot_date = date.today()

rows_processed = df.count()

df_temp_for_scala = spark.createDataFrame(
    [(snapshot_date, script, rows_processed)],
    ("snapshot_date", "script", "rows_processed")
)

df_temp_for_scala.createOrReplaceTempView("temp_table_name")

print(snapshot_date)
print(rows_processed)

# COMMAND ----------

# Original project moved these Python values into Scala because later helpers ran in Scala  #rep
#
# %scala
# val df_scala = spark.table("temp_table_name")
# val snapshot_date = df_scala.select("snapshot_date").collect.map(_(0)).toList.head.toString
# val script = df_scala.select("script").collect.map(_(0)).toList.head.toString
# val rows_processed = df_scala.select("rows_processed").collect.map(_(0)).toList.head.toString.toLong

# Serverless version stays in Python, so these variables are already available directly.  #serverless

# COMMAND ----------

Write_log_DQCE_server(
    source,
    database_name,
    schema,
    table_name,
    Loaded_By,
    script,
    cluster_name
)

# COMMAND ----------

from pyspark.sql.functions import col, lower, month, year
from pyspark.sql import functions as F
from datetime import datetime

current_month = int(f"{datetime.now().month:02d}")
current_year = datetime.now().year

print("current_month:", current_month)
print("current_year:", current_year)

# COMMAND ----------

# Remove existing error records for this table/current month before fresh profiling

error_folderpath = (
    parameters_global["error_report_path"]
    + "Finance_Fact_Error_v1.delta"
)

print(error_folderpath)

# Find the TableKey(s) belonging to the current source + table
dim_table_df = (
    read_from_DQCE_server(f"[{schema}].[Dim_Table]")
    .filter(
        (F.lower(F.col("Source_Name")) == source.lower()) &
        (F.lower(F.col("Table_Name")) == table_name.lower())
    )
)

tablekey_df = dim_table_df.select("TableKey").distinct()

tablekey_list = [
    row["TableKey"]
    for row in tablekey_df.collect()
]

print("TableKeys:", tablekey_list)


# Original project directly opens and deletes from the existing Delta table  #rep
# delta_table = DeltaTable.forPath(spark, error_folderpath)
# delta_table.delete(
#     (col("TableKey").isin(tablekey_list)) &
#     (lower(col("Source_Name")) == source.lower()) &
#     (F.month(col("Snapshot_Date")) == current_month) &
#     (F.year(col("Snapshot_Date")) == current_year)
# )


# Our from-scratch/serverless-safe equivalent  #serverless
if DeltaTable.isDeltaTable(spark, error_folderpath):

    delta_table = DeltaTable.forPath(
        spark,
        error_folderpath
    )

    delta_table.delete(
        (F.col("TableKey").isin(tablekey_list)) &
        (F.lower(F.col("Source_Name")) == source.lower()) &
        (F.month(F.col("Snapshot_Date")) == current_month) &
        (F.year(F.col("Snapshot_Date")) == current_year)
    )

    print("Previous error records deleted for current table/month.")

else:
    print("Fact Error Delta table does not exist yet. Nothing to delete on first run.")

# COMMAND ----------

def DQ_Rule_Profiling(df):

    # Store rule-processing errors
    log_entries = []

    # Keep only rules belonging to the current Source + Database + Table
    df_filtered = input_df.loc[
        (
            (input_df["Source_Name"].str.lower() == source.lower())
            &
            (input_df["Database_Name"].str.lower() == database_name.lower())
            &
            (input_df["Table_Name"].str.lower() == table_name.lower())
        ),
        :
    ]

    # Execute one rule at a time
    for rule_id in df_filtered["Rule_ID"].to_list():

        print("===================================================================================================")
        print("rule_Id:" + str(rule_id))

        try:

            # Get metadata for this Rule_ID
            (
                c,
                tablekey,
                rule_description,
                x,
                y,
                dimension,
                attribute,
                supporting_cde_values,
                Business_Rule_Description
            ) = (
                df_filtered.loc[
                    df_filtered["Rule_ID"] == rule_id,
                    col
                ].values[0]
                for col in [
                    "Column_Name_Source",
                    "Tablekey",
                    "Rule_Description",
                    "X",
                    "Y",
                    "Dimension",
                    "Attribute",
                    "Supporting_CDE_Values",
                    "Business_Rule_Description"
                ]
            )

            # Apply null-handling logic before executing the rule
            df_sub = ignore_nulls_check(
                dimension,
                df,
                c,
                rule_description
            )

            if df_sub.count() == 0:
                print(
                    "Data after ignore_nulls_check() count is:: ",
                    df_sub.count()
                )

            # Execute actual DQ rule
            results, error_reports, log_entries = DQ_Rule_Count(
                rule_id,
                rule_description,
                x,
                y,
                c,
                df_sub,
                attribute,
                Business_Rule_Description,
                supporting_cde_values
            )

            # If rule produced no summary rows, create zero counts
            if results.count() == 0:

                distinct_org = next(
                    (
                        row["Source_Org_Name"]
                        for row in df
                        .select("Source_Org_Name")
                        .distinct()
                        .collect()
                    ),
                    None
                )

                results = spark.createDataFrame(
                    [{
                        "fail_count": "0",
                        "pass_count": "0",
                        "Source_Org_Name": distinct_org
                    }]
                )

            # Prepare summary result for Fact_Rules
            df_results_spark = (
                results
                .withColumn(
                    "database_name",
                    F.lit(database_name)
                )
                .withColumn(
                    "table_name",
                    F.lit(table_name)
                )
                .withColumn(
                    "Column_Name_Source",
                    F.lit(c)
                )
                .withColumn(
                    "Rule_ID",
                    F.lit(rule_id)
                )
                .withColumn(
                    "Tablekey",
                    F.lit(tablekey)
                )
                .withColumn(
                    "Source_Name",
                    F.lit(source)
                )
                .withColumn(
                    "Snapshot_Date",
                    F.lit(snapshot_date)
                )
                .withColumn(
                    "Loaded_By",
                    F.lit(Loaded_By)
                )
                .fillna({
                    "pass_count": 0,
                    "fail_count": 0
                })
                .filter(
                    (F.col("pass_count") >= 0)
                    |
                    (F.col("fail_count") >= 0)
                )
            )

            display(df_results_spark)

            # Write summary results into SQL staging
            if load_type == "full load":

                write_to_DQCE_sql_server(
                    df_results_spark,
                    parameters_global["stg_Fact_Rules"],
                    "Append"
                )

            # Prepare detailed failed-record report
            if error_reports.count() > 0:

                df_error_report = (
                    error_reports
                    .withColumn(
                        "Rule_ID",
                        F.lit(rule_id)
                    )
                    .withColumn(
                        "Tablekey",
                        F.lit(tablekey)
                    )
                    .withColumn(
                        "Snapshot_Date",
                        F.lit(snapshot_date)
                    )
                    .withColumn(
                        "Loaded_By",
                        F.lit(Loaded_By)
                    )
                    .withColumnRenamed(
                        "count",
                        "fail_count"
                    )
                    .withColumn(
                        "Value",
                        col("Value").cast(StringType())
                    )
                    .withColumn(
                        "Supporting_CDE_Values",
                        col("Supporting_CDE_Values").cast(StringType())
                    )
                    .withColumn(
                        "fail_count",
                        col("fail_count").cast(IntegerType())
                    )
                    .withColumn(
                        "Snapshot_Date",
                        col("Snapshot_Date").cast(DateType())
                    )
                    .withColumn(
                        "Source_Org_Name",
                        F.col("Source_Org_Name").cast("string")
                    )
                )

                error_schema = StructType([
                    StructField("Account_Number", StringType(), True),
                    StructField("Source_Org_Name", StringType(), True),
                    StructField("Supporting_CDE_Values", StringType(), True),
                    StructField("Value", StringType(), True),
                    StructField("fail_count", IntegerType(), True),
                    StructField("Source_Name", StringType(), True),
                    StructField("Rule_ID", IntegerType(), True),
                    StructField("Tablekey", StringType(), True),
                    StructField("Snapshot_Date", DateType(), True),
                    StructField("Loaded_By", StringType(), True)
                ])

                df_error_report = (
                    df_error_report
                    .withColumn(
                        "Account_Number",
                        col("Account_Number").cast(StringType())
                    )
                    .withColumn(
                        "Source_Org_Name",
                        col("Source_Org_Name").cast(StringType())
                    )
                    .withColumn(
                        "Supporting_CDE_Values",
                        col("Supporting_CDE_Values").cast(StringType())
                    )
                    .withColumn(
                        "Value",
                        col("Value").cast(StringType())
                    )
                    .withColumn(
                        "fail_count",
                        col("fail_count").cast(IntegerType())
                    )
                    .withColumn(
                        "Source_Name",
                        col("Source_Name").cast(StringType())
                    )
                    .withColumn(
                        "Rule_ID",
                        col("Rule_ID").cast(IntegerType())
                    )
                    .withColumn(
                        "Tablekey",
                        col("Tablekey").cast(StringType())
                    )
                    .withColumn(
                        "Snapshot_Date",
                        col("Snapshot_Date").cast(DateType())
                    )
                    .withColumn(
                        "Loaded_By",
                        col("Loaded_By").cast(StringType())
                    )
                )

                # Append detailed failed records to ADLS Fact_Error
                print(error_folderpath)

                (
                    df_error_report.write
                    .mode("append")
                    .format("delta")
                    .option("overwriteSchema", "true")
                    .option("schema", error_schema)
                    .option("mergeSchema", "true")
                    .partitionBy(
                        "Source_Name",
                        "Rule_ID",
                        "Snapshot_Date"
                    )
                    .save(error_folderpath)
                )

        except Exception as e:

            print(
                f"Error processing rule_id {rule_id}: {str(e)}"
            )

    # Write rule-processing errors to notebook error log
    if log_entries:

        log_entries_cols = (
            "Rule_id",
            "Source",
            "Database_Name",
            "table_name",
            "Column_Name_Source",
            "rule_description",
            "Error_Description",
            "ErrorMessage",
            "Notebook_name",
            "Loaded_By"
        )

        log_entries = [
            data + (script, Loaded_By)
            for data in log_entries
        ]

        log_entries_spark = (
            spark.createDataFrame(
                log_entries,
                log_entries_cols
            )
            .withColumn(
                "Notebook_name",
                lit("FN_DQ_Rules_Profiling")
            )
        )

        write_to_DQCE_sql_server(
            log_entries_spark,
            parameters_global["notebook_Error"],
            "Append"
        )

# COMMAND ----------

# delete data from staging tables

# Original project used Scala here  #rep
# if (load_type == "full load") {
#     Delete_tbl_DQCE_server(schema, "stg_Fact_Rules", source)
# }

# Serverless equivalent  #serverless
if load_type == "full load":
    Delete_tbl_DQCE_server(
        schema,
        "stg_Fact_Rules",
        source
    )

# COMMAND ----------

DQ_Rule_Profiling(df)

# COMMAND ----------

error_folderpath = (
    parameters_global["error_report_path"]
    + "Finance_Fact_Error_v1.delta"
)

df_fact_error = (
    spark.read
    .format("delta")
    .load(error_folderpath)
    .filter(F.col("Source_Name") == "Bolt")
)

display(df_fact_error)

# COMMAND ----------

# load staging results into final Fact_Rules

# Original project executed this through Scala  #rep
# if (load_type == "full load") {
#     Update_FactTable_DQCE_server(
#         schema,
#         "Merge_FactRules",
#         source,
#         database_name,
#         table_name,
#         snapshot_date
#     )
# }

# Serverless equivalent  #serverless
if load_type == "full load":
    Update_FactTable_DQCE_server(
        schema,
        "Merge_FactRules",
        source,
        database_name,
        table_name,
        snapshot_date
    )

print("Merge_FactRules completed")

# COMMAND ----------

# fact rules table
df_fact_rules = read_from_DQCE_server(f"[{schema}].[Fact_Rules]")

display(df_fact_rules)
