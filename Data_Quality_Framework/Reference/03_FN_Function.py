# Databricks notebook source
# #Duplicate
# import pyspark.sql.functions as F
# from pyspark.sql.types import ArrayType,StringType

# COMMAND ----------

# def get_table(database_name,table_name):
#     return table_name

# COMMAND ----------

# #Duplicate
# table_name=get_table("FINANCE_DQ_SOURCE_DEV","HZ_LOCATION")
# print(table_name)

# COMMAND ----------

# #Duplicate
# def get_df(table_name):
#     #BOLT
#     if(database_name.lower()=="finance_dq_source_dev"
#        and source.lower()=="bolt"
#        and table_name.lower()=="hz_cust_accounts"):
#         df=read_snowflake_query("BOLT",bolt_cust_accounts_query)
    
#     elif(database_name.lower()=="finance_dq_source_dev"
#          and source.lower()=="bolt"
#          and table_name.lower()=="hz_parties"):
#         df=read_snowflake_query("BOLT",bolt_parties_query)
    
#     elif(database_name.lower()=="finance_dq_source_dev"
#          and source.lower()=="bolt"
#          and table_name.lower()=="hz_contact_points"):
#         df=read_snowflake_query("BOLT",bolt_contact_points_query)
    
#     elif(database_name.lower()=="finance_dq_source_dev"
#          and source.lower()=="bolt"
#          and table_name.lower()=="hz_locations"):
#         df=read_snowflake_query("BOLT",bolt_locations_query)
    
#     #ONEOM
#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="1om"
#         and table_name.lower()=="hz_cust_accounts"):
#         df=read_snowflake_query("ONEOM",oneom_cust_accounts_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="1om"
#         and table_name.lower()=="hz_locations"):
#         df=read_snowflake_query("ONEOM",oneom_locations_query)
        
#     #CIL
#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="cil"
#         and table_name.lower()=="hz_cust_accounts"):
#         df=read_snowflake_query("CIL",cil_cust_accounts_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="cil"
#         and table_name.lower()=="hz_parties"):
#         df=read_snowflake_query("CIL",cil_parties_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="cil"
#         and table_name.lower()=="hz_contact_points"):
#         df=read_snowflake_query("CIL",cil_contact_points_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="cil"
#         and table_name.lower()=="hz_locations"):
#         df=read_snowflake_query("CIL",cil_locations_query)

#     #BZL
#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="bzl"
#         and table_name.lower()=="hz_cust_accounts"):
#         df=read_snowflake_query("BZL",bzl_cust_accounts_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="bzl"
#         and table_name.lower()=="hz_parties"):
#         df=read_snowflake_query("BZL",bzl_parties_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="bzl"
#         and table_name.lower()=="hz_locations"):
#         df=read_snowflake_query("BZL",bzl_locations_query)

#     #HHP
#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="hhp"
#         and table_name.lower()=="hz_cust_accounts"):
#         df=read_snowflake_query("HHP",hhp_cust_accounts_query)

#     elif (database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="hhp"
#         and table_name.lower()=="hz_parties"):
#         df=read_snowflake_query("HHP",hhp_parties_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="hhp"
#         and table_name.lower()=="hz_locations"):
#         df=read_snowflake_query("HHP",hhp_locations_query)

#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="hhp"
#         and table_name.lower()=="hz_contact_points"):
#         df=read_snowflake_query("HHP",hhp_contact_points_query)
    
#     #C360
#     elif(database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="c360"
#         and table_name.lower()=="c_bo_pstl_addr"):
#         df=read_snowflake_query("C360",c360_locations_query)

#     elif (database_name.lower()=="finance_dq_source_dev"
#         and source.lower()=="c360"
#         and table_name.lower()=="c_bo_prty"):
#         df=read_snowflake_query("C360",c360_phone_query)

#     else:
#        schema_name="ONEOM" if source.lower()=="1om" else source.upper()
#        df=read_snowflake_table(schema_name,table_name)

#     return df

# COMMAND ----------

# #Duplicate
# pass_fact=["Source_Org_Name"]
# fail_rec_req=["Account_Number","Source_Org_Name"]

# COMMAND ----------

# #Duplicate
# def lastupdateon_friday():
#     lastFriday = date.today()
#     oneday = datetime.timedelta(days=1)

#     if lastFriday.weekday() == calendar.FRIDAY:
#         lastFriday -= timedelta(days=14)
#     else:
#         while lastFriday.weekday() != calendar.FRIDAY:
#             lastFriday -= oneday
#         lastFriday -= timedelta(days=14)

#     lastFriday = lastFriday + datetime.timedelta(hours=24)
    
#     return lastFriday

# def lastoccurance_monday():
#     lastMonday = date.today()
#     oneday = datetime.timedelta(days=1)

#     if lastMonday.weekday() == calendar.MONDAY:
#         lastMonday -= timedelta(days=14)
#     else:
#         while lastMonday.weekday() != calendar.MONDAY:
#             lastMonday -= oneday
#         lastMonday -= timedelta(days=14)

#     return lastMonday

# COMMAND ----------

# #Duplicate
# #Completeness-keep NULL and blank rows
# #Any other dimension-remove NULL and blank rows first

# ignore_null_exception=[]

# def ignore_nulls_check(dimension, df, c, rule_description):

#     if rule_description in ignore_null_exception:
#         return df
    
#     elif dimension != 'Completeness':
#         return df.filter(
#             (F.col(c).isNotNull()) & (F.trim(F.col(c))!='')
#         )

#     else:
#         return df

# COMMAND ----------

# #Duplicate
# log_entries=[]
# array_schema=ArrayType(StringType())

# COMMAND ----------

# Databricks notebook source
import calendar
import datetime
import re

import pyspark.sql.functions as F
from datetime import date, timedelta
from pyspark.sql import Window
from pyspark.sql.functions import (
    array_contains,
    col,
    concat,
    concat_ws,
    count,
    explode,
    explode_outer,
    expr,
    from_json,
    instr,
    length,
    lit,
    lower,
    regexp_replace,
    size,
    split,
    trim,
    when,
)
from pyspark.sql.types import ArrayType, StringType

# COMMAND ----------

def get_table(database_name,table_name):
    return table_name

# COMMAND ----------

# table_name=get_table("FINANCE_DQ_SOURCE_DEV","HZ_LOCATION")
# print(table_name)

# COMMAND ----------

def get_df(table_name):
    #BOLT
    if(database_name.lower()=="finance_dq_source_dev"
       and source.lower()=="bolt"
       and table_name.lower()=="hz_cust_accounts"):
        df=read_snowflake_query("BOLT",bolt_cust_accounts_query)
    
    elif(database_name.lower()=="finance_dq_source_dev"
         and source.lower()=="bolt"
         and table_name.lower()=="hz_parties"):
        df=read_snowflake_query("BOLT",bolt_parties_query)
    
    elif(database_name.lower()=="finance_dq_source_dev"
         and source.lower()=="bolt"
         and table_name.lower()=="hz_contact_points"):
        df=read_snowflake_query("BOLT",bolt_contact_points_query)
    
    elif(database_name.lower()=="finance_dq_source_dev"
         and source.lower()=="bolt"
         and table_name.lower()=="hz_locations"):
        df=read_snowflake_query("BOLT",bolt_locations_query)
    
    #ONEOM
    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="1om"
        and table_name.lower()=="hz_cust_accounts"):
        df=read_snowflake_query("ONEOM",oneom_cust_accounts_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="1om"
        and table_name.lower()=="hz_locations"):
        df=read_snowflake_query("ONEOM",oneom_locations_query)
        
    #CIL
    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="cil"
        and table_name.lower()=="hz_cust_accounts"):
        df=read_snowflake_query("CIL",cil_cust_accounts_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="cil"
        and table_name.lower()=="hz_parties"):
        df=read_snowflake_query("CIL",cil_parties_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="cil"
        and table_name.lower()=="hz_contact_points"):
        df=read_snowflake_query("CIL",cil_contact_points_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="cil"
        and table_name.lower()=="hz_locations"):
        df=read_snowflake_query("CIL",cil_locations_query)

    #BZL
    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="bzl"
        and table_name.lower()=="hz_cust_accounts"):
        df=read_snowflake_query("BZL",bzl_cust_accounts_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="bzl"
        and table_name.lower()=="hz_parties"):
        df=read_snowflake_query("BZL",bzl_parties_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="bzl"
        and table_name.lower()=="hz_locations"):
        df=read_snowflake_query("BZL",bzl_locations_query)

    #HHP
    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="hhp"
        and table_name.lower()=="hz_cust_accounts"):
        df=read_snowflake_query("HHP",hhp_cust_accounts_query)

    elif (database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="hhp"
        and table_name.lower()=="hz_parties"):
        df=read_snowflake_query("HHP",hhp_parties_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="hhp"
        and table_name.lower()=="hz_locations"):
        df=read_snowflake_query("HHP",hhp_locations_query)

    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="hhp"
        and table_name.lower()=="hz_contact_points"):
        df=read_snowflake_query("HHP",hhp_contact_points_query)
    
    #C360
    elif(database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="c360"
        and table_name.lower()=="c_bo_pstl_addr"):
        df=read_snowflake_query("C360",c360_locations_query)

    elif (database_name.lower()=="finance_dq_source_dev"
        and source.lower()=="c360"
        and table_name.lower()=="c_bo_prty"):
        df=read_snowflake_query("C360",c360_phone_query)

    else:
       schema_name="ONEOM" if source.lower()=="1om" else source.upper()
       df=read_snowflake_table(schema_name,table_name)

    return df

# COMMAND ----------

pass_fact = ["Source_Org_Name"]
fail_rec_req = ["Account_Number", "Source_Org_Name"]

# COMMAND ----------

def lastupdateon_friday():
    lastFriday = date.today()
    oneday = datetime.timedelta(days=1)

    if lastFriday.weekday() == calendar.FRIDAY:
        lastFriday -= timedelta(days=14)
    else:
        while lastFriday.weekday() != calendar.FRIDAY:
            lastFriday -= oneday
        lastFriday -= timedelta(days=14)

    lastFriday = lastFriday + datetime.timedelta(hours=24)
    
    return lastFriday

def lastoccurance_monday():
    lastMonday = date.today()
    oneday = datetime.timedelta(days=1)

    if lastMonday.weekday() == calendar.MONDAY:
        lastMonday -= timedelta(days=14)
    else:
        while lastMonday.weekday() != calendar.MONDAY:
            lastMonday -= oneday
        lastMonday -= timedelta(days=14)

    return lastMonday


def snapshot_date_friday():
    lastFriday = datetime.date.today()
    oneday = datetime.timedelta(days=1)

    while lastFriday.weekday() != calendar.FRIDAY:
        lastFriday -= oneday

    return lastFriday

# COMMAND ----------

#Completeness-keep NULL and blank rows
#Any other dimension-remove NULL and blank rows first

ignore_null_exception = []

def ignore_nulls_check(dimension, df, c, rule_description):

    if rule_description in ignore_null_exception:
        return df
    
    elif dimension != 'Completeness':
        return df.filter(
            (F.col(c).isNotNull()) & (F.trim(F.col(c))!='')
        )

    else:
        return df

# COMMAND ----------

log_entries = []
array_schema = ArrayType(StringType())

# COMMAND ----------

# DBTITLE 1,Cell 9
def DQ_Rule_Count(
    rule_id,
    rule_description,
    x_value,
    y_value,
    column_name,
    input_data,
    attribute,
    Business_Rule_Description,
    supporting_cde_values
):
    error_description = ""
    results = []
    error_reports = []

    valid_country_codes=[
        'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS',
        'AT', 'AU', 'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG',
        'BH', 'BI', 'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT',
        'BV', 'BW', 'BY', 'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI',
        'CK', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CW', 'CX', 'CY',
        'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ', 'EC', 'EE', 'EG', 'EH',
        'ER', 'ES', 'ET', 'FI', 'FJ', 'FK', 'FM', 'FO', 'FR', 'GA', 'GB',
        'GD', 'GE', 'GF', 'GG', 'GH', 'GI', 'GL', 'GM', 'GN', 'GP', 'GQ',
        'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM', 'HN', 'HR', 'HT',
        'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR', 'IS', 'IT',
        'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP',
        'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS',
        'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH',
        'MK', 'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU',
        'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NC', 'NE', 'NF', 'NG', 'NI',
        'NL', 'NO', 'NP', 'NR', 'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG',
        'PH', 'PK', 'PL', 'PM', 'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA',
        'RE', 'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 'SE', 'SG',
        'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS', 'ST',
        'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF', 'TG', 'TH', 'TJ', 'TK',
        'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG',
        'UM', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU',
        'WF', 'WS', 'YE', 'YT', 'ZA', 'ZM', 'ZW'
    ]

    invalid_values=['N/A', 'n/a', 'none', 'None', 'NONE', '', 'null']

    try:

        if attribute.lower().startswith('email') and table_name.lower()=='hz_contact_points':
            input_data = input_data.where("lower(contact_point_type) = 'email'")

        if attribute.lower().startswith('phone') and table_name.lower()=='hz_contact_points':
            input_data = input_data.where("lower(contact_point_type) = 'phone'")

#################################################################

        if rule_description == "Must have a value":
            pass_condition=(
                F.col(column_name).isNotNull() 
                & 
                ~F.col(column_name).isin(invalid_values)
            )

            fail_condition=(
                F.col(column_name).isNull()
                |
                F.col(column_name).isin(invalid_values)
            )

#################################################################

        elif "Must be a value from valid country codes" == rule_description:

            input_data=input_data.filter(~trim(col(column_name)).isin(invalid_values))

            pass_condition=(
                col(column_name).isNotNull()
                &
                trim(col(column_name)).isin(valid_country_codes)
            )

            fail_condition=(
                col(column_name).isNull()
                |
                ~trim(col(column_name)).isin(valid_country_codes)
            )

#################################################################

        elif "Must follow standard format" == rule_description:

            cleaned_col = trim(col(column_name))

            valid_company_suffix_regex = (
                r"(?i).*(?<![A-Za-z0-9])"
                r"(co\.?|c\.o\.?|ltd\.?|l\.t\.d\.?|inc\.?|i\.n\.c\.?|"
                r"corp\.?|c\.o\.r\.p\.?|llc\.?|l\.l\.c\.?|lp\.?|l\.p\.?|"
                r"llp\.?|l\.l\.p\.?|pc\.?|p\.c\.?|ulc\.?|u\.l\.c\.?|"
                r"lca\.?|l\.c\.a\.?)"
                r"(?![A-Za-z0-9]).*"
            )

            pass_condition = (
                col(column_name).isNotNull()
                & cleaned_col.rlike(valid_company_suffix_regex)
            )

            fail_condition = (
                col(column_name).isNull()
                | ~cleaned_col.rlike(valid_company_suffix_regex)
            )

#################################################################

        elif "Must not be in X" == rule_description:

            input_data=input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            if x_value is not None:
                x_value=[
                    x_value.strip()
                    for x_value in x_value.replace('or', ',').split(',')
                ]

                pass_condition=(
                    col(column_name).isNotNull()
                    &
                    ~(col(column_name).isin(x_value))
                )

                fail_condition=(
                    col(column_name).isNull()
                    |
                    col(column_name).isin(x_value)
                )

            else:
                error_description="X value is missing for the rule"

#################################################################

        elif "Must be in X" == rule_description:

            input_data=input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            if x_value is not None:
                x_value=[
                    x_value.strip()
                    for x_value in x_value.replace('or', ',').split(',')
                ]

                pass_condition=(
                    col(column_name).isNotNull()
                    &
                    col(column_name).isin(x_value)
                )

                fail_condition=(
                    col(column_name).isNull()
                    |
                    ~col(column_name).isin(x_value)
                )

            else:
                error_description="X value is missing for the rule"

#################################################################

        elif "Must not start with X" == rule_description:

            input_data=input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            if x_value is not None:

                if Business_Rule_Description == "Bill To Customer Name must not start with special characters or blank spaces":

                    x_value=[
                        x.strip()
                        for x in x_value.replace('or', ',').split(',')
                    ]

                    x_value.append(',')

                    pattern="^(" + "|".join(
                        [re.escape(x) for x in x_value]
                    ) + ")"

                    print(pattern)

                    combined_condition=col(column_name).rlike(pattern)

                    pass_condition=(
                        col(column_name).isNotNull()
                        &
                        ~combined_condition
                    )

                    fail_condition=(
                        col(column_name).isNull()
                        |
                        combined_condition
                    )

                else:

                    x_value=[
                        x_value.strip()
                        for x_value in x_value.replace('or', ',').split(',')
                    ]

                    pass_condition=(
                        col(column_name).isNotNull()
                        &
                        ~col(column_name).isin(x_value)
                    )

                    fail_condition=(
                        col(column_name).isNull()
                        |
                        col(column_name).isin(x_value)
                    )

            else:
                error_description="X value is missing for the rule"

#################################################################

        elif "Must be an unique value" == rule_description:

            window_spec=Window.partitionBy(
                column_name,
                "Source_Org_Name"
            )

            input_data=input_data.withColumn(
                "count",
                count("*").over(window_spec)
            )

            pass_condition=col("count")==1

            fail_condition=col("count")>1

#################################################################

        elif "Must be <= X" == rule_description:

            input_data=input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            pass_condition=length(col(column_name)) <= x_value

            fail_condition=length(col(column_name)) > x_value

#################################################################

        elif "Must have a suffix when customer type is equal to Y" == rule_description:

            input_data=input_data.filter(
                (~col(column_name).isin(invalid_values))
                &
                (lower(col("customer_type"))==y_value.lower())
            )

            bc_code_pattern=r"(?i)\(?\s*BC[-\s]?[A-Z0-9]+\)?$"

            pass_condition=col(column_name).rlike(bc_code_pattern)

            fail_condition=(
                col(column_name).isNull()
                |
                (~col(column_name).rlike(bc_code_pattern))
            )

#################################################################

        elif "Must be a suffix from X" == rule_description:

            input_data=input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            if isinstance(x_value,str):
                suffixes=[
                    s.strip().strip("',\"")
                    for s in x_value.split(',')
                    if s.strip()
                ]

            elif isinstance(x_value,(list,tuple)):
                suffixes=[
                    str(s).strip()
                    for s in x_value
                    if str(s).strip()
                ]

            else:
                suffixes=[]

            suffixes=[
                s for s in suffixes
                if len(re.sub(r"\W","",s)) > 1
            ]

            seen=set()
            clean_suffixes=[]

            for s in suffixes:
                key=s.upper()

                if key not in seen:
                    seen.add(key)
                    clean_suffixes.append(s)

            regex_parts=[
                re.escape(s).replace(r"\.", r"\.?")
                for s in clean_suffixes
            ]

            pattern=(
                r"(?i)("
                + "|".join(regex_parts)
                + r")\.?$"
            )

            pass_condition=col(column_name).rlike(pattern)
            
            fail_condition=(
                col(column_name).isNull()
                |
                ~col(column_name).rlike(pattern)
            )



#################################################################           
        elif "Must be equal to column X" == rule_description:

            input_data = input_data.filter(
                (~col(column_name).isin(invalid_values)) &
                (~col(x_value).isin(invalid_values))
            )

            pass_condition = (
                trim(
                    regexp_replace(
                        lower(col(column_name)),
                        r'\s+',
                        ' '
                    )
                ).eqNullSafe(
                    trim(
                        regexp_replace(
                            lower(col(x_value)),
                            r'\s+',
                            ' '
                        )
                    )
                )
            )

            fail_condition = (
                trim(
                    regexp_replace(
                        lower(col(column_name)),
                        r'\s+',
                        ' '
                    )
                ).isNotNull()
                &
                trim(
                    regexp_replace(
                        lower(col(x_value)),
                        r'\s+',
                        ' '
                    )
                ).isNotNull()
                &
                (
                    ~trim(
                        regexp_replace(
                            lower(col(column_name)),
                            r'\s+',
                            ' '
                        )
                    ).eqNullSafe(
                        trim(
                            regexp_replace(
                                lower(col(x_value)),
                                r'\s+',
                                ' '
                            )
                        )
                    )
                )
            )

#################################################################           
        elif "Must be seperated by X value" == rule_description:
        
            input_data=input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            has_multiple_emails=col(column_name).rlike(r"[,;]")

            pass_condition=(
                col(column_name).isNotNull()
                &
                (
                    (~has_multiple_emails)
                    |
                    (instr(col(column_name), x_value)>0)
                )
            )

            fail_condition=(
                col(column_name).isNull()
                |
                (
                    has_multiple_emails
                    &
                    (instr(col(column_name), x_value)==0)
                )
            )
      
#################################################################      

        elif "Must contain X value" == rule_description:

            input_data = input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            input_data = input_data.withColumn(
                "email_array",
                split(
                    regexp_replace(col(column_name), ",", ";"),
                    ";"
                )
            )

            pass_condition = (
                col(column_name).isNotNull()
                &
                expr(
                    f"""
                    forall(
                        filter(
                            transform(email_array, x -> trim(x)),
                            x -> x != ''
                        ),
                        x -> x like concat('%', '{x_value}', '%')
                    )
                    """
                )
            )

            fail_condition = (
                col(column_name).isNull()
                |
                expr(
                    f"""
                    exists(
                        filter(
                            transform(email_array, x -> trim(x)),
                            x -> x != ''
                        ),
                        x -> x not like concat('%', '{x_value}', '%')
                    )
                    """
                )
            )

################################################################# 

        elif "Must not have X value" == rule_description:

            input_data = input_data.filter(
                ~col(column_name).isin(invalid_values)
            )

            input_data = input_data.withColumn(
                "phone_array",
                split(
                    regexp_replace(col(column_name), ",", ";"),
                    ";"
                )
            )

            pass_condition = (
                col(column_name).isNotNull()
                &
                expr(
                    f"""
                    forall(
                        transform(phone_array, x -> trim(x)),
                        x -> NOT (
                            lower(x) like concat('%', '{x_value}', '%')
                        )
                    )
                    """
                )
            )

            fail_condition = (
                col(column_name).isNull()
                |
                expr(
                    f"""
                    exists(
                        transform(phone_array, x -> trim(x)),
                        x -> lower(x) like concat('%', '{x_value}', '%')
                    )
                    """
                )
            )

#################################################################

        elif "Must be in a value from column X" == rule_description:

            invalid_values_norm = ["", "na", "null", "none", "n/a"]

            # This variable exists in the source project but is not used later.
            has_valid_ref = size(col("norm_x_value_arr")) > 0

            # Parse JSON arrays from the current column and reference column.
            input_data = input_data.withColumn(
                "column_arr",
                from_json(col(column_name), array_schema),
            )

            input_data = input_data.withColumn(
                "x_value_arr",
                from_json(col(x_value), array_schema),
            )

            # Remove null and invalid values from both arrays.
            input_data = input_data.withColumn(
                "column_arr",
                expr(
                    """
                    filter(
                        column_arr,
                        x -> x is not null
                        AND lower(trim(x)) NOT IN ('', 'na', 'null', 'none', 'n/a')
                    )
                    """
                ),
            )

            input_data = input_data.withColumn(
                "x_value_arr",
                expr(
                    """
                    filter(
                        x_value_arr,
                        x -> x is not null
                        AND lower(trim(x)) NOT IN ('', 'na', 'null', 'none', 'n/a')
                    )
                    """
                ),
            )

            # Explode only the current-source side.
            input_data = input_data.withColumn(
                "column_value",
                explode(col("column_arr")),
            )

            # Normalize both sides before comparing.
            input_data = input_data.withColumn(
                column_name,
                lower(trim(col("column_value"))),
            )

            input_data = input_data.withColumn(
                "norm_x_value_arr",
                expr("transform(x_value_arr, x -> lower(trim(x)))"),
            )

            pass_condition = (
                array_contains(col("norm_x_value_arr"), col(column_name))
                & (size(col("norm_x_value_arr")) > 0)
            )

            fail_condition = (
                ~array_contains(col("norm_x_value_arr"), col(column_name))
                & (size(col("norm_x_value_arr")) > 0)
            )

        elif "Must be a non null value" == rule_description:

            invalid_values_norm = list(
                set(v.strip().lower() for v in invalid_values)
            )

            input_data = input_data.withColumn(
                "column_arr",
                from_json(col(column_name), array_schema)
            )

            input_data = input_data.withColumn(
                "column_value",
                explode_outer(col("column_arr"))
            )

            input_data = input_data.withColumn(
                "norm_value",
                lower(trim(col("column_value")))
            )

            pass_condition = (
                col("column_value").isNotNull()
                &
                (~col("norm_value").isin(invalid_values_norm))
            )

            fail_condition = (
                col("column_value").isNull()
                |
                col("norm_value").isin(invalid_values_norm)
            )

#################################################################

        else:
            error_description="Rule Logic needs to be defined for this rule"

            # log_entries.append(
            #     (
            #         source,
            #         table_name,
            #         column_name,
            #         rule_description,
            #         database_name,
            #         "Rule Logic needs to be defined for this rule",
            #         Loaded_By
            #     )
            # )

#################################################################

        total_count=input_data.count()

        invalid_count=input_data.filter(
            col(column_name).isNull()
            |
            col(column_name).isin(invalid_values)
        ).count()

        pass_count=input_data.filter(pass_condition).count()
        fail_count=input_data.filter(fail_condition).count()

#################################################################

        pass_results=(
            input_data
            .filter(pass_condition)
            .groupBy(pass_fact)
            .count()
            .withColumnRenamed("count","pass_count")
        )

        fail_results=(
            input_data
            .filter(fail_condition)
            .groupBy(pass_fact)
            .count()
            .withColumnRenamed("count","fail_count")
        )

        pass_fail_results=pass_results.join(
            fail_results,
            ['Source_Org_Name'],
            'full'
        )

#################################################################

        errors_dataset = input_data.filter(fail_condition)

#################################################################
# Build detailed error records
# Fix for rules where Account_Number itself is being validated
#################################################################

        if supporting_cde_values is not None and errors_dataset.count() > 0:

            supporting_cols = (
                supporting_cde_values
                .replace(", ", ",")
                .replace(" ", "_")
                .split(",")
            )

            concatenated_column = concat_ws(
                " | ",
                *[
                    concat(
                        lit(col_name),
                        lit(" = "),
                        when(
                            col(col_name).isNull(),
                            lit("null")
                        ).otherwise(col(col_name))
                    )
                    for col_name in supporting_cols
                ]
            )

            errors_dataset = errors_dataset.withColumn(
                "Supporting_CDE_Values",
                concatenated_column
            )

        else:

            errors_dataset = errors_dataset.withColumn(
                "Supporting_CDE_Values",
                lit(None).cast("string")
            )

#################################################################
# Columns required in the error report
#################################################################

        error_columns = fail_rec_req + ["Supporting_CDE_Values"]

        # Add the checked column only if it is not already included.
        # This avoids duplicate/ambiguous Account_Number columns when
        # Account_Number itself is the column being validated.
        if column_name.lower() not in [c.lower() for c in error_columns]:
            error_columns.append(column_name)

#################################################################
# Select and group failed records
#################################################################

        errors = errors_dataset.select(*error_columns)

        error_column_names = errors.columns

        errors = errors.groupBy(*error_column_names).count()

#################################################################
# Add source metadata
#################################################################

        errors = errors.withColumn(
            "Source_Name",
            lit(source)
        )

#################################################################
# Create Value column
#################################################################

        matching_identifier = next(
            (
                c
                for c in fail_rec_req
                if c.lower() == column_name.lower()
            ),
            None
        )

        # If Account_Number itself is being validated, keep the
        # Account_Number identifier and copy its value into Value.
        if matching_identifier is not None:

            errors = errors.withColumn(
                "Value",
                col(matching_identifier).cast("string")
            )

        # For other checked columns, preserve the original project
        # behavior by renaming the checked column to Value.
        else:

            errors = errors.withColumnRenamed(
                column_name,
                "Value"
            )

        error_reports = errors

#################################################################

        return pass_fail_results, error_reports, log_entries

#################################################################

    except Exception as e:
        log_entries.append(
            (
                rule_id,
                source,
                database_name,
                table_name,
                column_name,
                rule_description,
                error_description,
                "Failed :: " + str(e)
            )
        )