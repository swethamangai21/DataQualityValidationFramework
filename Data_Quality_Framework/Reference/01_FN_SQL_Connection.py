# Databricks notebook source
# Retrieve the private key
private_key=dbutils.secrets.get(
    scope="finance-dq-kv-scope",
    key='snowflake-private-key'
)

print("Private key retrieved successfully")
#print(len(private_key))

# COMMAND ----------

private_key_clean=(
    private_key
    .replace("-----BEGIN PRIVATE KEY-----","")
    .replace("-----END PRIVATE KEY-----","")
    .replace("\n","")
    .replace("\r","")
)
print("Private key prepared successfully")

# COMMAND ----------

#Snowflake connection parameters 
sf_options={
    "sfURL": "WIMKOXW-JR05806.snowflakecomputing.com",
    "sfUser": "DQ_DATABRICKS_USER",
    "sfWarehouse": "DQ_DEV_WH",
    "sfDatabase": "FINANCE_DQ_SOURCE_DEV",
    "sfSchema": "BOLT",
    "sfRole": "DQ_DATABRICKS_ROLE",
    "pem_private_key": private_key_clean
}

print("Snowflake connection options configured")

# COMMAND ----------

# #Read the BOLT data as DataFrame (for testing whether the connection works)
# df_bolt_accounts=(
#     spark.read
#     .format("snowflake")
#     .options(**sf_options)
#     .option("dbtable","HZ_CUST_ACCOUNTS")
#     .load()
# )
# display(df_bolt_accounts)

# COMMAND ----------

#Create a reusable snowflake read function
def read_snowflake_table(schema_name,table_name):

    options=sf_options.copy()
    options["sfSchema"]=schema_name

    df=(
        spark.read
        .format("snowflake")
        .options(**options)
        .option("dbtable",table_name)
        .load()
    )

    return df

# COMMAND ----------

# #Testing the reusable function
# df_bolt_test=read_snowflake_table("BOLT","HZ_CUST_ACCOUNTS")
# display(df_bolt_test)

# COMMAND ----------

# #Testing if the schema changes dynamically
# df_cil_test=read_snowflake_table("CIL","HZ_CUST_ACCOUNTS")
# display(df_cil_test)

# COMMAND ----------

#Function for reading the snowflake query
def read_snowflake_query(schema_name,query):

    options=sf_options.copy()
    options["sfSchema"]=schema_name

    df=(
        spark.read
        .format("snowflake")
        .options(**options)
        .option("query",query)
        .load()
    )

    return df


# COMMAND ----------

#Azure SQL connection parameters

sql_HostName = "finance-dq-dev-sql.database.windows.net"
sql_Port = 1433
sql_DatabaseName = "finance-dq-dev-db"

sql_UserName = dbutils.secrets.get(
    scope="finance-dq-kv-scope",
    key="azure-sql-username"
)

sql_Password = dbutils.secrets.get(
    scope="finance-dq-kv-scope",
    key="azure-sql-password"
)

sql_JdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

sql_JdbcUrl = (
    f"jdbc:sqlserver://{sql_HostName}:{sql_Port};"
    f"database={sql_DatabaseName};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

sql_ConnectionProperties = {
    "user": sql_UserName,
    "password": sql_Password,
    "fetchsize": "10000",
    "driver": sql_JdbcDriver
}

print("Azure SQL JDBC configuration prepared successfully")

# COMMAND ----------

# @@@@@@@ rep
# connection_test_df = spark.read.jdbc(
#     url=sql_JdbcUrl,
#     table="""
#         (
#             SELECT
#                 DB_NAME() AS database_name,
#                 CURRENT_TIMESTAMP AS sql_server_time
#         ) AS connection_test
#     """,
#     properties=sql_ConnectionProperties
# )

# display(connection_test_df)

# COMMAND ----------

# # @@@@@@@@@@@@@ Rep

# #Function to read tables from Azure SQL (as a dataframe) (for lm_prod schema)

# def read_from_DQCE_server(table_name):

#     df = spark.read.jdbc(
#         url=sql_JdbcUrl,
#         table=table_name,
#         properties=sql_ConnectionProperties
#     )

#     print("read_from_DQCE_server function is executed")

#     return df

# COMMAND ----------

# # @@@@@@@@@@@@@ Rep

# #Function to write data to Azure SQL

# def write_to_DQCE_sql_server(
#     df,
#     table_name,
#     mode_of_writing="Overwrite"
# ):

#     if mode_of_writing == "Overwrite":

#         (
#             df.write
#             .option("batchsize", "10000")
#             .mode("overwrite")
#             .jdbc(
#                 url=sql_JdbcUrl,
#                 table=table_name,
#                 properties=sql_ConnectionProperties
#             )
#         )

#     elif mode_of_writing == "Append":

#         (
#             df.write
#             .option("batchsize", "10000")
#             .mode("append")
#             .jdbc(
#                 url=sql_JdbcUrl,
#                 table=table_name,
#                 properties=sql_ConnectionProperties
#             )
#         )

#     else:

#         raise ValueError(
#             "mode_of_writing must be 'Overwrite' or 'Append'"
#         )

#     print("write_to_DQCE_sql_server function is executed")

# COMMAND ----------

# # @@@@@@@@@@ Rep

# #Function to read tables from Azure SQL (for mdm schema)

# def read_from_MDS_DQCOE_server(table_name):

#     df = spark.read.jdbc(
#         url=sql_JdbcUrl,
#         table=table_name,
#         properties=sql_ConnectionProperties
#     )

#     print("read_from_MDS_DQCOE_server function is executed")

#     return df

# COMMAND ----------

# #@@@@@@@@@@@ Rep
# # Helper function- to create execution log entry in Azure SQL

# def Write_log_DQCE_server(
#     Source,
#     Database_Name,
#     Schema_Name,
#     Table_Name,
#     Loaded_By,
#     Script,
#     cluster_name
# ):

#     table_name = f"{Schema_Name}.log_Execution"

#     df_log_dqcoe = spark.createDataFrame(
#         [
#             (
#                 Source,
#                 Database_Name,
#                 Schema_Name,
#                 Table_Name,
#                 Loaded_By,
#                 Script,
#                 cluster_name
#             )
#         ],
#         (
#             "Source",
#             "Database_Name",
#             "Schema",
#             "Table_Name",
#             "Loaded_By",
#             "Script",
#             "Cluster_Name"
#         )
#     )

#     (
#         df_log_dqcoe.write
#         .option("batchsize", "10000")
#         .mode("Append")
#         .jdbc(
#             url=sql_JdbcUrl,
#             table=table_name,
#             properties=sql_ConnectionProperties
#         )
#     )

#     print("Write_log_DQCE_server function is executed")

# COMMAND ----------

# @@@@@@@ rep
# # Helper function- update log execution table

# %scala

# import java.sql.{CallableStatement, Connection, DriverManager}

# def Update_log_DQCE_server(
#     databaseName: String,
#     schemaName: String,
#     tableName: String,
#     loadedBy: String,
#     rowsProcessed: Long,
#     script: String
# ): Unit = {

#     val sqlHost = "finance-dq-dev-sql.database.windows.net"
#     val sqlPort = 1433
#     val sqlDatabase = "finance-dq-dev-db"

#     val sqlUser =
#       dbutils.secrets.get(
#         scope = "finance-dq-kv-scope",
#         key = "azure-sql-username"
#       )

#     val sqlPassword =
#       dbutils.secrets.get(
#         scope = "finance-dq-kv-scope",
#         key = "azure-sql-password"
#       )

#     val jdbcUrl =
#       s"jdbc:sqlserver://$sqlHost:$sqlPort;" +
#       s"database=$sqlDatabase;" +
#       "encrypt=true;" +
#       "trustServerCertificate=false;" +
#       "hostNameInCertificate=*.database.windows.net;" +
#       "loginTimeout=30;"

#     val conn: Connection =
#       DriverManager.getConnection(
#         jdbcUrl,
#         sqlUser,
#         sqlPassword
#       )

#     val call: CallableStatement =
#       conn.prepareCall(
#         "{call " +
#         schemaName +
#         ".log_UpdateExecution(?,?,?,?,?,?)}"
#       )

#     call.setString(1, databaseName)
#     call.setString(2, schemaName)
#     call.setString(3, tableName)
#     call.setString(4, loadedBy)
#     call.setLong(5, rowsProcessed)
#     call.setString(6, script)

#     call.execute()

#     call.close()
#     conn.close()

#     println("Update_log_DQCE_server function is executed")
# }

# COMMAND ----------

# @@@@@@@ rep
# Helper function- delete records for one specific Source_Name

# %scala

# import java.sql.{CallableStatement, Connection}

# def Delete_tbl_DQCE_server(
#     schema: String,
#     tableName: String,
#     source: String
# ): Unit = {

#     val sqlHost = "finance-dq-dev-sql.database.windows.net"
#     val sqlPort = 1433
#     val sqlDatabase = "finance-dq-dev-db"

#     val sqlUser =
#       dbutils.secrets.get(
#         scope = "finance-dq-kv-scope",
#         key = "azure-sql-username"
#       )

#     val sqlPassword =
#       dbutils.secrets.get(
#         scope = "finance-dq-kv-scope",
#         key = "azure-sql-password"
#       )

#     val jdbcUrl =
#       s"jdbc:sqlserver://$sqlHost:$sqlPort;" +
#       s"database=$sqlDatabase;" +
#       "encrypt=true;" +
#       "trustServerCertificate=false;" +
#       "hostNameInCertificate=*.database.windows.net;" +
#       "loginTimeout=30;"

#     val conn: Connection =
#       java.sql.DriverManager.getConnection(
#         jdbcUrl,
#         sqlUser,
#         sqlPassword
#       )

#     val call: CallableStatement =
#       conn.prepareCall(
#         "{call " +
#         schema +
#         ".Delete_tbl_DQCE_server(?, ?, ?)}"
#       )

#     call.setString(1, schema)
#     call.setString(2, tableName)
#     call.setString(3, source)

#     call.execute()

#     call.close()
#     conn.close()

#     println("Delete_tbl_DQCE_server function is executed")
# }

# COMMAND ----------

# @@@@@@@ rep
# # Helper function- delete entire staging table

# %scala

# import java.sql.{CallableStatement, Connection}

# def Delete_stg_tbl_DQCE_server(
#     schema: String,
#     tableName: String
# ): Unit = {

#     val sql_HostName = "finance-dq-dev-sql.database.windows.net"
#     val sql_Port = 1433
#     val sql_DatabaseName = "finance-dq-dev-db"

#     val sql_UserName =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-username"
#         )

#     val sql_Password =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-password"
#         )

#     val jdbcUrl =
#         s"jdbc:sqlserver://${sql_HostName}:${sql_Port};" +
#         s"database=${sql_DatabaseName};" +
#         "encrypt=true;" +
#         "trustServerCertificate=false;" +
#         "hostNameInCertificate=*.database.windows.net;" +
#         "loginTimeout=30;"

#     val conn: Connection =
#         java.sql.DriverManager.getConnection(
#             jdbcUrl,
#             sql_UserName,
#             sql_Password
#         )

#     val call: CallableStatement =
#         conn.prepareCall(
#             "{call " +
#             schema +
#             ".Delete_stg_tbl_DQCE_server(?, ?)}"
#         )

#     call.setString(1, schema)
#     call.setString(2, tableName)

#     call.execute()

#     call.close()
#     conn.close()

#     println("Delete_stg_tbl_DQCE_server function is executed")
# }

# COMMAND ----------

# @@@@@@@ rep
# # Helper function- move the profiling summary from the staging table into the final fact table

# %scala

# import java.sql.{CallableStatement, Connection}

# def Update_FactTable_DQCE_server(
#     schema: String,
#     sp_name: String,
#     source: String,
#     database_name: String,
#     table_name: String,
#     snapshot_date: String
# ): Unit = {

#     val sql_HostName = "finance-dq-dev-sql.database.windows.net"
#     val sql_Port = 1433
#     val sql_DatabaseName = "finance-dq-dev-db"

#     val sql_UserName =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-username"
#         )

#     val sql_Password =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-password"
#         )

#     val jdbcUrl =
#         s"jdbc:sqlserver://${sql_HostName}:${sql_Port};" +
#         s"database=${sql_DatabaseName};" +
#         "encrypt=true;" +
#         "trustServerCertificate=false;" +
#         "hostNameInCertificate=*.database.windows.net;" +
#         "loginTimeout=30;"

#     val conn: Connection =
#         java.sql.DriverManager.getConnection(
#             jdbcUrl,
#             sql_UserName,
#             sql_Password
#         )

#     val call: CallableStatement =
#         conn.prepareCall(
#             "{call " + schema + "." + sp_name + "(?,?,?,?)}"
#         )

#     call.setString(1, source)
#     call.setString(2, database_name)
#     call.setString(3, table_name)
#     call.setString(4, snapshot_date)

#     call.execute()

#     call.close()
#     conn.close()

#     println("Update_FactTable_DQCE_server function is executed")
# }

# COMMAND ----------

# @@@@@@@ rep
# # Helper function- part of producing the final dim table

# %scala

# import java.sql.{CallableStatement, Connection}

# def Update_DimTable_DQCE_server(
#     schema: String,
#     sp_name: String,
#     source: String
# ): Unit = {

#     val sql_HostName = "finance-dq-dev-sql.database.windows.net"
#     val sql_Port = 1433
#     val sql_DatabaseName = "finance-dq-dev-db"

#     val sql_UserName =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-username"
#         )

#     val sql_Password =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-password"
#         )

#     val jdbcUrl =
#         s"jdbc:sqlserver://${sql_HostName}:${sql_Port};" +
#         s"database=${sql_DatabaseName};" +
#         "encrypt=true;" +
#         "trustServerCertificate=false;" +
#         "hostNameInCertificate=*.database.windows.net;" +
#         "loginTimeout=30;"

#     val conn: Connection =
#         java.sql.DriverManager.getConnection(
#             jdbcUrl,
#             sql_UserName,
#             sql_Password
#         )

#     val call: CallableStatement =
#         conn.prepareCall(
#             "{call " + schema + "." + sp_name + "(?)}"
#         )

#     call.setString(1, source)

#     call.execute()

#     call.close()
#     conn.close()

#     println("Update_DimTable_DQCE_server function is executed")
# }

# COMMAND ----------

# @@@@@@@ rep
# # Helper function- used for stored procedures

# %scala

# import java.sql.{CallableStatement, Connection}

# def Update_Table_DQCE_server(
#     schema: String,
#     sp_name: String
# ): Unit = {

#     val sql_HostName = "finance-dq-dev-sql.database.windows.net"
#     val sql_Port = 1433
#     val sql_DatabaseName = "finance-dq-dev-db"

#     val sql_UserName =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-username"
#         )

#     val sql_Password =
#         dbutils.secrets.get(
#             scope = "finance-dq-kv-scope",
#             key = "azure-sql-password"
#         )

#     val jdbcUrl =
#         s"jdbc:sqlserver://${sql_HostName}:${sql_Port};" +
#         s"database=${sql_DatabaseName};" +
#         "encrypt=true;" +
#         "trustServerCertificate=false;" +
#         "hostNameInCertificate=*.database.windows.net;" +
#         "loginTimeout=30;"

#     val conn: Connection =
#         java.sql.DriverManager.getConnection(
#             jdbcUrl,
#             sql_UserName,
#             sql_Password
#         )

#     val call: CallableStatement =
#         conn.prepareCall(
#             "{call " + schema + "." + sp_name + "}"
#         )

#     call.execute()

#     call.close()
#     conn.close()

#     println("Update_Table_DQCE_server function is executed")
# }

# COMMAND ----------

# @@@@@@@ rep
# # ADLS Gen2 connection

# adls_storage_account = "financedqdevsa"

# adls_account_key = dbutils.secrets.get(
#     scope="finance-dq-kv-scope",
#     key="adls-account-key"
# )

# spark.conf.set(
#     f"fs.azure.account.key.{adls_storage_account}.dfs.core.windows.net",
#     adls_account_key
# )

# print("ADLS Gen2 configuration completed.")

# COMMAND ----------

# @@@@@@@ rep
# # Testing
# adls_test_path = (
#     "abfss://users@financedqdevsa.dfs.core.windows.net/"
#     "DQCE/lm_prd/"
# )

# display(dbutils.fs.ls(adls_test_path))

# COMMAND ----------

# ^^^^^^^^^^^^^ Serverless
# ============================================================
# SERVERLESS AZURE SQL READ / WRITE HELPERS
# ============================================================

def _sqlserver_reader():
    return (
        spark.read
        .format("sqlserver")
        .option("host", sql_HostName)
        .option("database", sql_DatabaseName)
        .option("user", sql_UserName)
        .option("password", sql_Password)
    )


def _sqlserver_writer(df):
    return (
        df.write
        .format("sqlserver")
        .option("host", sql_HostName)
        .option("database", sql_DatabaseName)
        .option("user", sql_UserName)
        .option("password", sql_Password)
    )


# ------------------------------------------------------------
# Serverless read - DQCE / lm_prd
# ------------------------------------------------------------

def read_from_DQCE_server(table_name):

    df = (
        _sqlserver_reader()
        .option("dbtable", table_name)
        .load()
    )

    print("read_from_DQCE_server executed - SERVERLESS")

    return df


# ------------------------------------------------------------
# Serverless read - MDS / mdm
# ------------------------------------------------------------

def read_from_MDS_DQCOE_server(table_name):

    df = (
        _sqlserver_reader()
        .option("dbtable", table_name)
        .load()
    )

    print("read_from_MDS_DQCOE_server executed - SERVERLESS")

    return df


# ------------------------------------------------------------
# Serverless write
# ------------------------------------------------------------

def write_to_DQCE_sql_server(
    df,
    table_name,
    mode_of_writing="Overwrite"
):

    mode = mode_of_writing.lower()

    if mode not in ["overwrite", "append"]:
        raise ValueError(
            "mode_of_writing must be 'Overwrite' or 'Append'"
        )

    (
        _sqlserver_writer(df)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )

    print("write_to_DQCE_sql_server executed - SERVERLESS")


# ------------------------------------------------------------
# Serverless execution-log insert
# ------------------------------------------------------------

def Write_log_DQCE_server(
    Source,
    Database_Name,
    Schema_Name,
    Table_Name,
    Loaded_By,
    Script,
    cluster_name
):

    table_name = f"{Schema_Name}.log_Execution"

    df_log_dqcoe = spark.createDataFrame(
        [
            (
                Source,
                Database_Name,
                Schema_Name,
                Table_Name,
                Loaded_By,
                Script,
                cluster_name
            )
        ],
        [
            "Source",
            "Database_Name",
            "Schema",
            "Table_Name",
            "Loaded_By",
            "Script",
            "Cluster_Name"
        ]
    )

    (
        _sqlserver_writer(df_log_dqcoe)
        .option("dbtable", table_name)
        .mode("append")
        .save()
    )

    print("Write_log_DQCE_server executed - SERVERLESS")


print("Serverless Azure SQL read/write helpers loaded successfully.")


# COMMAND ----------

# DBTITLE 1,Cell 23
# ^^^^^^^^^^^^^ Serverless

from mssql_python import connect

connection_string = (
    f"Server=tcp:{sql_HostName},{sql_Port};"
    f"Database={sql_DatabaseName};"
    f"UID={sql_UserName};"
    f"PWD={sql_Password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

# with connect(connection_string, timeout=30) as conn:
#     with conn.cursor() as cursor:
#         cursor.execute("SELECT DB_NAME()")
#         row = cursor.fetchone()

#         print("Connected successfully")
#         print("Database:", row[0])

# COMMAND ----------

# ^^^^^^^^^^^^^ Serverless

# ============================================================
# SERVERLESS STORED PROCEDURE HELPERS
# mssql-python
# ============================================================

from mssql_python import connect


def _get_sql_procedure_connection():

    connection_string = (
        f"Server=tcp:{sql_HostName},{sql_Port};"
        f"Database={sql_DatabaseName};"
        f"UID={sql_UserName};"
        f"PWD={sql_Password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    return connect(
        connection_string,
        timeout=30
    )


# ------------------------------------------------------------
# Update execution log
# ------------------------------------------------------------

def Update_log_DQCE_server(
    databaseName,
    schemaName,
    tableName,
    loadedBy,
    rowsProcessed,
    script
):

    with _get_sql_procedure_connection() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                EXEC lm_prd.log_UpdateExecution
                    %(databaseName)s,
                    %(schemaName)s,
                    %(tableName)s,
                    %(loadedBy)s,
                    %(rowsProcessed)s,
                    %(script)s
                """,
                {
                    "databaseName": databaseName,
                    "schemaName": schemaName,
                    "tableName": tableName,
                    "loadedBy": loadedBy,
                    "rowsProcessed": int(rowsProcessed),
                    "script": script
                }
            )

            conn.commit()


# ------------------------------------------------------------
# Delete source-specific records
# ------------------------------------------------------------

def Delete_tbl_DQCE_server(
    schema,
    tableName,
    source
):

    with _get_sql_procedure_connection() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                EXEC lm_prd.Delete_tbl_DQCE_server
                    %(schema)s,
                    %(tableName)s,
                    %(source)s
                """,
                {
                    "schema": schema,
                    "tableName": tableName,
                    "source": source
                }
            )

            conn.commit()


# ------------------------------------------------------------
# Clear staging table
# ------------------------------------------------------------

def Delete_stg_tbl_DQCE_server(
    schema,
    tableName
):

    with _get_sql_procedure_connection() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                EXEC lm_prd.Delete_stg_tbl_DQCE_server
                    %(schema)s,
                    %(tableName)s
                """,
                {
                    "schema": schema,
                    "tableName": tableName
                }
            )

            conn.commit()


# ------------------------------------------------------------
# Merge Fact_Rules
# ------------------------------------------------------------

def Update_FactTable_DQCE_server(
    schema,
    sp_name,
    source,
    database_name,
    table_name,
    snapshot_date
):

    with _get_sql_procedure_connection() as conn:
        with conn.cursor() as cursor:

            sql = f"""
                EXEC {schema}.{sp_name}
                    %(source)s,
                    %(database_name)s,
                    %(table_name)s,
                    %(snapshot_date)s
            """

            cursor.execute(
                sql,
                {
                    "source": source,
                    "database_name": database_name,
                    "table_name": table_name,
                    "snapshot_date": snapshot_date
                }
            )

            conn.commit()


# ------------------------------------------------------------
# Stored procedure with Source parameter
# ------------------------------------------------------------

def Update_DimTable_DQCE_server(
    schema,
    sp_name,
    source
):

    with _get_sql_procedure_connection() as conn:
        with conn.cursor() as cursor:

            sql = f"""
                EXEC {schema}.{sp_name}
                    %(source)s
            """

            cursor.execute(
                sql,
                {"source": source}
            )

            conn.commit()


# ------------------------------------------------------------
# Stored procedure without parameters
# ------------------------------------------------------------

def Update_Table_DQCE_server(
    schema,
    sp_name
):

    with _get_sql_procedure_connection() as conn:
        with conn.cursor() as cursor:

            sql = f"EXEC {schema}.{sp_name}"

            cursor.execute(sql)

            conn.commit()


print("Serverless Azure SQL stored procedure helpers loaded successfully.")

# COMMAND ----------

def send_count_alert_mail_DQCOE_Admin(
    send_from,
    send_to,
    subject,
    header_text,
    latest_snapshot,
    countalert,
    smtp_user,
    smtp_password,
    server="smtp.gmail.com",
    port=587
):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()

    msg["From"] = send_from
    msg["To"] = COMMASPACE.join(send_to)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject

    if type(countalert) == pd.DataFrame:

        html = """\
        <html>
            <head>{0}</head>
            <br><br>
            <head>
                <br>Latest Snapshot Date : {1}<br>
            </head>
            <body>
                <p>
                    <br>Here is count comparison data:<br>
                    {2}
                </p>
            </body>
        </html>
        """.format(
            header_text,
            latest_snapshot,
            countalert.to_html()
        )

        part1 = MIMEText(html, "html")

    else:
        part1 = MIMEText(str(countalert), "plain")

    msg.attach(part1)

    smtp = smtplib.SMTP(
        server,
        port
    )

    smtp.starttls()

    smtp.login(
        smtp_user,
        smtp_password
    )

    smtp.sendmail(
        send_from,
        send_to,
        msg.as_string()
    )

    smtp.quit()