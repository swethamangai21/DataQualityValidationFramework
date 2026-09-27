# Databricks notebook source
# MAGIC %run ../Reference/01_FN_SQL_Connection

# COMMAND ----------

database_name = "null"
table_name = "null"

# COMMAND ----------

# MAGIC %run ../Reference/02_FN_Parameter

# COMMAND ----------

# MAGIC %run ../Reference/03_FN_Function

# COMMAND ----------

def run_notebook_for_each_row(spark_df):
    """
    Iterate through source/table combinations and run
    the DQ profiling notebook for each one.
    """

    for row in spark_df.collect():

        source = row["Source_Name"]
        table_name = row["Table_Name"]
        database_name = row["Database_Name"]

        try:

            dbutils.notebook.run(
                "../DQ/02_FN_DQ_Rules_Profiling",
                0,
                {
                    "Database_Name": database_name,
                    "Table_Name": table_name,
                    "active_value": "1",
                    "Load_Type": "full load",
                    "Source": source,
                    "Job_User": "FN_Master"
                }
            )

        except Exception as e:

            print(
                f"Error encountered for Source: {source}, "
                f"Table: {table_name}, "
                f"Database: {database_name}. "
                f"Skipping to the next row."
            )

            print(f"Error details: {e}")

            continue

# COMMAND ----------

from pyspark.sql import functions as F

def get_distinct_tables_by_source(schema, source_name):
    """
    Read active tables for one source from Dim_Table.
    """

    spark_df = (
        read_from_DQCE_server(f"[{schema}].[Dim_Table]")
        .filter(
            (F.col("Source_Name") == source_name) &
            (F.col("Active") == "Y")
        )
        .select(
            "Database_Name",
            "Table_Name",
            "Source_Name"
        )
        .distinct()
    )

    return spark_df

# COMMAND ----------

source_name = "Bolt"

print(schema, source_name)

spark_df = get_distinct_tables_by_source(
    schema,
    source_name
)

run_notebook_for_each_row(spark_df)

# COMMAND ----------

source_name = "1OM"

print(schema, source_name)

spark_df = get_distinct_tables_by_source(
    schema,
    source_name
)

run_notebook_for_each_row(spark_df)

# COMMAND ----------

source_name = "CIL"

print(schema, source_name)

spark_df = get_distinct_tables_by_source(
    schema,
    source_name
)

run_notebook_for_each_row(spark_df)

# COMMAND ----------

source_name = "BZL"

print(schema, source_name)

spark_df = get_distinct_tables_by_source(
    schema,
    source_name
)

run_notebook_for_each_row(spark_df)

# COMMAND ----------

source_name = "HHP"

print(schema, source_name)

spark_df = get_distinct_tables_by_source(
    schema,
    source_name
)

run_notebook_for_each_row(spark_df)

# COMMAND ----------

source_name = "C360"

print(schema, source_name)

spark_df = get_distinct_tables_by_source(
    schema,
    source_name
)

run_notebook_for_each_row(spark_df)

# COMMAND ----------

# the corresponding stored procedure is not presented in the sql object definition
# Actual project  #rep 
# %scala
# Update_Table_DQCE_server(schema, "[Merge_FactCleanse]")

# Serverless equivalent would be:
# Update_Table_DQCE_server(schema, "[Merge_FactCleanse]")

# Not executed in our replica because the supplied project
# does not contain the Merge_FactCleanse stored procedure.

# COMMAND ----------

dbutils.notebook.run("../DQ/05_FN_Alert", 0)

# COMMAND ----------

# MAGIC %run ../DQ/06_FN_Alert_Error_Report_Archive $Dashboard_Name="Finance"