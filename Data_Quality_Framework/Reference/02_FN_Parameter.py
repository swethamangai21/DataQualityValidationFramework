# Databricks notebook source
# DBTITLE 1,Run SQL connection notebook
# MAGIC %run ./01_FN_SQL_Connection

# COMMAND ----------

# DQ SQL schema used throughout the framework

schema = "lm_prd"

parameters_global = {}

# Logging tables
parameters_global["notebook_Error"] = schema + ".log_notebook_Error"
parameters_global["ExecutionLog"] = schema + ".log_Execution"

# Staging tables
parameters_global["stg_Critical"] = schema + ".stg_Critical"
parameters_global["stg_Dim_Table"] = schema + ".stg_Dim_Table"
parameters_global["stg_Dim_Rules"] = schema + ".stg_Dim_Rules"
parameters_global["stg_Dim_Source_Org"] = schema + ".stg_Dim_Source_Org"
parameters_global["stg_MDS_Source_Org"] = schema + ".stg_MDS_Source_Org"
parameters_global["stg_Fact_Rules"] = schema + ".stg_Fact_Rules"

# COMMAND ----------

# ^^^^^^^Serverless

# ============================================================
# Finance DQ Data Lake paths
# ============================================================

folder = "lm_prd/"

adls_base_path = (
    "abfss://users@financedqdevsa.dfs.core.windows.net/"
    "DQCE/" + folder
)

parameters_global["path"] = (
    adls_base_path + "profiling_tables/"
)

parameters_global["error_report_path"] = (
    adls_base_path + "error_tables/"
)

parameters_global["error_report_archivepath"] = (
    adls_base_path + "error_tables_archive/"
)

print(parameters_global["path"])
print(parameters_global["error_report_path"])
print(parameters_global["error_report_archivepath"])

# COMMAND ----------

#BOLT customer query
bolt_cust_accounts_query="""
select
    account_name,
    cust_account_id,
    customer_type,
    'all' as source_org_name,
    account_number,
    account_name as bill_to_customer_name
from hz_cust_accounts
where status='A'
"""

# COMMAND ----------

# #Testing
# df_bolt_cust_accounts=read_snowflake_query("BOLT",bolt_cust_accounts_query)

# display(df_bolt_cust_accounts)

# COMMAND ----------

#BOLT parties query
bolt_parties_query="""
select
    p.party_id,
    p.party_number,
    p.party_name,
    p.status,
    a.cust_account_id,
    a.account_number,
    a.customer_type,
    'all' as source_org_name
from hz_parties p
left join hz_cust_accounts a
    on p.party_id=a.party_id
where p.status='A' and a.status='A'
"""

# COMMAND ----------

bolt_locations_query = """
select distinct
    loc.address1,
    loc.address2,
    loc.address3,
    loc.address4,
    loc.city,
    loc.country,
    loc.county,
    loc.postal_code,
    loc.state,
    loc.province,
    hca.account_number,
    hca.cust_account_id,
    hps.party_site_number,
    hps.orig_system_reference,
    'all' as source_org_name
from bolt.hz_locations loc
left join bolt.hz_party_sites hps
    on hps.location_id = loc.location_id
left join bolt.hz_cust_acct_sites_all cas
    on cas.party_site_id = hps.party_site_id
left join bolt.hz_cust_accounts hca
    on hca.cust_account_id = cas.cust_account_id
where hca.status = 'A'
  and hps.status = 'A'
  and cas.status = 'A'
"""

# COMMAND ----------

#BOLT contact points query
bolt_contact_points_query="""
select distinct
    cnp.email_address,
    cnp.phone_number,
    con.account_number,
    cnp.contact_point_type,
    con.cust_account_id,
    'all' as source_org_name
from bolt.hz_contact_points cnp
left join bolt.hz_relationships rel
    on cnp.owner_table_id=rel.party_id
left join bolt.hz_cust_accounts con
    on con.party_id=rel.object_id
left join bolt.hz_parties sub
    on sub.party_id=cnp.owner_table_id
where cnp.owner_table_name='HZ_PARTIES'
    and rel.directional_flag='F'
    and rel.relationship_type='CONTACT'
    and con.hdisdeletedrecord!=0
    and con.status='A'
    and sub.status='A'
"""

# COMMAND ----------

#ONEOM customer accounts query
oneom_cust_accounts_query="""
with bolt_data as(
    select distinct
        hca.account_number as bolt_account_number,
        hca.account_name as bolt_account_name,
        hca.customer_type as bolt_customer_type,
        hps.party_site_number as bolt_party_site_number
    from bolt.hz_cust_accounts hca
    join bolt.hz_cust_acct_sites_all cas
        on hca.cust_account_id=cas.cust_account_id
    join bolt.hz_party_sites hps
        on cas.party_site_id=hps.party_site_id
    where hca.status='A' and cas.status='A' and hps.status='A'
)

select distinct
    hca.account_name,
    hp.party_name,
    hca.cust_account_id,
    hca.customer_type,
    'all' as source_org_name,
    blt.bolt_account_name,
    blt.bolt_customer_type,
    blt.bolt_account_number,
    hca.account_number,
    hps.attribute10 as bolt_site_reference
from oneom.hz_cust_accounts hca
join oneom.hz_parties hp
    on hp.party_id = hca.party_id

join oneom.hz_cust_acct_sites_all hcas
    on hcas.cust_account_id = hca.cust_account_id

join oneom.hz_party_sites hps
    on hps.party_id = hp.party_id
    and hcas.party_site_id = hps.party_site_id

left join bolt_data blt
    on hca.account_number = blt.bolt_account_number
    and hps.attribute10 = blt.bolt_party_site_number

where hca.status = 'A'
  and hp.status = 'A'
"""

# COMMAND ----------

#ONEOM locations query
oneom_locations_query="""
with bolt_data as(
    select distinct
        hca.account_number as bolt_account_number,
        hca.account_name as bolt_account_name,
        hp.party_name as bolt_party_name,
        hps.party_site_number as bolt_party_site_number,
        loc.address1 as bolt_address1,
        loc.address2 as bolt_address2,
        loc.address3 as bolt_address3,
        loc.address4 as bolt_address4,
        loc.city as bolt_city,
        loc.county as bolt_county,
        loc.state as bolt_state,
        loc.country as bolt_country,
        loc.postal_code as bolt_postal_code
    from bolt.hz_cust_accounts hca
join bolt.hz_parties hp
    on hp.party_id = hca.party_id

join bolt.hz_cust_acct_sites_all hcas
    on hcas.cust_account_id = hca.cust_account_id

join bolt.hz_party_sites hps
    on hps.party_id = hp.party_id
    and hcas.party_site_id = hps.party_site_id

join bolt.hz_locations loc
    on loc.location_id = hps.location_id

where hca.status = 'A'
  and hcas.status = 'A'
  and hps.status = 'A'
)
select distinct
    hca.account_number,
    hp.party_name as customer_name,
    hps.attribute10 as party_site_number,
    hl.address1,
    hl.address2,
    hl.address3,
    hl.address4,
    hl.city,
    hl.state,
    hl.country,
    hl.postal_code,
    hl.county,
    hl.province,
    'all' as source_org_name,
    blt.bolt_account_number,
    blt.bolt_party_site_number,
    blt.bolt_account_name,
    blt.bolt_party_name,
    blt.bolt_address1,
    blt.bolt_address2,
    blt.bolt_address3,
    blt.bolt_address4,
    blt.bolt_city,
    blt.bolt_county,
    blt.bolt_state,
    blt.bolt_country,
    blt.bolt_postal_code
from oneom.hz_cust_accounts hca
join oneom.hz_parties hp
    on hp.party_id = hca.party_id
join oneom.hz_cust_acct_sites_all hcas
    on hcas.cust_account_id = hca.cust_account_id
join oneom.hz_party_sites hps
    on hps.party_id = hp.party_id
    and hcas.party_site_id = hps.party_site_id
join oneom.hz_cust_site_uses_all hcsu
    on hcsu.cust_acct_site_id = hcas.cust_acct_site_id
join oneom.hz_locations hl
    on hl.location_id = hps.location_id
join bolt_data blt
    on hca.account_number = blt.bolt_account_number
    and hps.attribute10 = blt.bolt_party_site_number
where hca.status = 'A'
  and hps.status = 'A'
"""

# COMMAND ----------

#CILparties query
cil_parties_query = """
with bolt_data as (
    select
        p.party_name as bolt_party_name,
        hca.account_name as bolt_account_name,
        hca.account_number as bolt_account_number
    from bolt.hz_parties p
    join bolt.hz_cust_accounts hca
        on hca.party_id = p.party_id
    where p.status = 'A'
      and hca.status = 'A'
)
select distinct
    p.party_id,
    p.party_name,
    blt.bolt_party_name,
    c.account_name as bill_to_customer_name,
    blt.bolt_account_name,
    c.account_number,
    'all' as source_org_name
from cil.hz_parties p
left join cil.hz_cust_accounts c
    on p.party_id = c.party_id
left join bolt_data blt
    on c.account_number = blt.bolt_account_number
where p.status = 'A'
  and c.status = 'A'
  and blt.bolt_account_number is not null
"""

# COMMAND ----------

#CIL customer accounts query
cil_cust_accounts_query = """
select
    a.account_name,
    a.cust_account_id,
    a.customer_type,
    'all' as source_org_name,
    b.account_name as bolt_account_name,
    b.cust_account_id as bolt_cust_account_id,
    b.customer_type as bolt_customer_type,
    a.account_number,
    b.account_number as bolt_account_number,
    a.account_name as bill_to_customer_name
from cil.hz_cust_accounts a
left join bolt.hz_cust_accounts b
    on a.account_number = b.account_number
where a.hdisdeletedrecord != 0
  and a.status = 'A'
  and b.status = 'A'
"""

# COMMAND ----------

#CIL locations query
cil_locations_query = """
with bolt_data as (
    select distinct
        loc.address1 as bolt_address1,
        loc.address2 as bolt_address2,
        loc.address3 as bolt_address3,
        loc.address4 as bolt_address4,
        loc.city as bolt_city,
        loc.country as bolt_country,
        loc.county as bolt_county,
        loc.postal_code as bolt_postal_code,
        loc.state as bolt_state,
        hca.account_number as bolt_account_number,
        hca.cust_account_id as bolt_cust_account_id,
        hps.orig_system_reference,
        hps.party_site_number as bolt_party_site_number
    from bolt.hz_cust_accounts hca
    left join bolt.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    left join bolt.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    left join bolt.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
),
cil_data as (
    select distinct
        loc.address1,
        loc.address2,
        loc.address3,
        loc.address4,
        loc.city,
        loc.country,
        loc.county,
        loc.postal_code,
        loc.state,
        hca.account_number,
        hca.cust_account_id,
        cas.attribute10,
        hps.party_site_number,
        'all' as source_org_name
    from cil.hz_cust_accounts hca
    left join cil.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    left join cil.hz_cust_site_uses_all usa
        on cas.cust_acct_site_id = usa.cust_acct_site_id
    left join cil.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    left join cil.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
      and hps.status = 'A'
      and usa.site_use_code = 'BILL_TO'
)
select distinct
    c.address1,
    c.address2,
    c.address3,
    c.address4,
    c.city,
    c.country,
    c.county,
    c.postal_code,
    c.state,
    c.account_number,
    c.cust_account_id,
    c.source_org_name,
    c.attribute10 as attribute10_cil_identifier,
    c.party_site_number,
    b.bolt_account_number,
    b.bolt_cust_account_id,
    b.bolt_address1,
    b.bolt_address2,
    b.bolt_address3,
    b.bolt_address4,
    b.bolt_city,
    b.bolt_country,
    b.bolt_county,
    b.bolt_postal_code,
    b.bolt_state,
    b.orig_system_reference as orig_system_reference_bolt_identifier,
    b.bolt_party_site_number
from cil_data c
left join bolt_data b
    on b.bolt_account_number = c.account_number
    and b.orig_system_reference = c.attribute10
where b.bolt_party_site_number is not null
"""

# COMMAND ----------

#BZL parties query
bzl_parties_query = """
with bolt_data as (
    select
        p.party_name as bolt_party_name,
        hca.account_name as bolt_account_name,
        hca.account_number as bolt_account_number
    from bolt.hz_parties p
    join bolt.hz_cust_accounts hca
        on hca.party_id = p.party_id
    where p.status = 'A' and hca.status = 'A'
)
select distinct
    p.party_id,
    p.party_name,
    blt.bolt_party_name,
    a.account_name as bill_to_customer_name,
    blt.bolt_account_name,
    a.account_number,
    'all' as source_org_name
from bzl.hz_parties p
left join bzl.hz_cust_accounts a
    on p.party_id = a.party_id
left join bolt_data blt
    on a.account_number = blt.bolt_account_number
where p.status = 'A'
  and a.status = 'A'
  and blt.bolt_account_number is not null
"""

# COMMAND ----------

#BZL customer accounts query
bzl_cust_accounts_query = """
select
    a.account_name,
    a.cust_account_id,
    a.customer_type,
    'all' as source_org_name,
    b.account_name as bolt_account_name,
    b.cust_account_id as bolt_cust_account_id,
    b.customer_type as bolt_customer_type,
    a.account_number,
    b.account_number as bolt_account_number
from bzl.hz_cust_accounts a
left join bolt.hz_cust_accounts b
    on a.account_number = b.account_number
where a.hdisdeletedrecord != 0
  and a.status = 'A'
  and b.status = 'A'
"""

# COMMAND ----------

#BZL locations query
bzl_locations_query = """
with bolt_data as (
    select distinct
        loc.address1 as bolt_address1,
        loc.address2 as bolt_address2,
        loc.address3 as bolt_address3,
        loc.address4 as bolt_address4,
        loc.city as bolt_city,
        loc.country as bolt_country,
        loc.county as bolt_county,
        loc.postal_code as bolt_postal_code,
        loc.state as bolt_state,
        hca.account_number as bolt_account_number,
        hca.cust_account_id as bolt_cust_account_id,
        hps.orig_system_reference,
        hps.party_site_number as bolt_party_site_number
    from bolt.hz_cust_accounts hca
    left join bolt.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    left join bolt.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    left join bolt.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
),
bzl_data as (
    select distinct
        loc.address1,
        loc.address2,
        loc.address3,
        loc.address4,
        loc.city,
        loc.country,
        loc.county,
        loc.postal_code,
        loc.state,
        hca.account_number,
        hca.cust_account_id,
        cas.attribute10,
        hps.party_site_number,
        'all' as source_org_name
    from bzl.hz_cust_accounts hca
    left join bzl.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    left join bzl.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    left join bzl.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
      and hps.status = 'A'
)
select distinct
    bzl.address1,
    bzl.address2,
    bzl.address3,
    bzl.address4,
    bzl.city,
    bzl.country,
    bzl.county,
    bzl.postal_code,
    bzl.state,
    bzl.account_number,
    bzl.cust_account_id,
    bzl.source_org_name,
    bzl.attribute10,
    bzl.party_site_number,
    blt.bolt_account_number,
    blt.bolt_cust_account_id,
    blt.bolt_address1,
    blt.bolt_address2,
    blt.bolt_address3,
    blt.bolt_address4,
    blt.bolt_city,
    blt.bolt_country,
    blt.bolt_county,
    blt.bolt_postal_code,
    blt.bolt_state,
    blt.orig_system_reference,
    blt.bolt_party_site_number
from bzl_data bzl
left join bolt_data blt
    on blt.bolt_account_number = bzl.account_number
    and blt.orig_system_reference = bzl.attribute10
where blt.bolt_party_site_number is not null
"""

# COMMAND ----------

#HHP parties query
hhp_parties_query = """
with bolt_data as (
    select
        p.party_name as bolt_party_name,
        hca.account_name as bolt_account_name,
        hca.account_number as bolt_account_number
    from bolt.hz_parties p
    join bolt.hz_cust_accounts hca
        on hca.party_id = p.party_id
    where p.status = 'A'
)
select distinct
    p.party_id,
    p.party_name,
    blt.bolt_party_name,
    a.account_name as bill_to_customer_name,
    blt.bolt_account_name,
    a.account_number,
    'all' as source_org_name
from hhp.hz_parties p
left join hhp.hz_cust_accounts a
    on p.party_id = a.party_id
left join bolt_data blt
    on a.account_number = blt.bolt_account_number
where p.status = 'A'
  and a.account_number is not null
"""

# COMMAND ----------

#HHP customer accounts query
hhp_cust_accounts_query = """
select
    a.account_name,
    a.cust_account_id,
    a.customer_type,
    'all' as source_org_name,
    b.account_name as bolt_account_name,
    b.cust_account_id as bolt_cust_account_id,
    b.customer_type as bolt_customer_type,
    a.account_number,
    b.account_number as bolt_account_number
from hhp.hz_cust_accounts a
left join bolt.hz_cust_accounts b
    on a.account_number = b.account_number
where a.hdisdeletedrecord != 0
  and a.status = 'A'
  and b.status = 'A'
"""

# COMMAND ----------

#HHP locations query
hhp_locations_query = """
with bolt_data as (
    select distinct
        loc.address1 as bolt_address1,
        loc.address2 as bolt_address2,
        loc.address3 as bolt_address3,
        loc.address4 as bolt_address4,
        loc.city as bolt_city,
        loc.country as bolt_country,
        loc.county as bolt_county,
        loc.postal_code as bolt_postal_code,
        loc.state as bolt_state,
        hca.account_number as bolt_account_number,
        hca.cust_account_id as bolt_cust_account_id,
        hps.orig_system_reference,
        hps.party_site_number as bolt_party_site_number
    from bolt.hz_cust_accounts hca
    left join bolt.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    left join bolt.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    left join bolt.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
),
hhp_data as (
    select distinct
        loc.address1,
        loc.address2,
        loc.address3,
        loc.address4,
        loc.city,
        loc.country,
        loc.county,
        loc.postal_code,
        loc.state,
        hca.account_number,
        hca.cust_account_id,
        cas.attribute10,
        hps.party_site_number,
        'all' as source_org_name
    from hhp.hz_cust_accounts hca
    left join hhp.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    left join hhp.hz_cust_site_uses_all usa
        on cas.cust_acct_site_id = usa.cust_acct_site_id
    left join hhp.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    left join hhp.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
      and hps.status = 'A'
      and usa.site_use_code = 'BILL_TO'
)
select distinct
    hhp.address1,
    hhp.address2,
    hhp.address3,
    hhp.address4,
    hhp.city,
    hhp.country,
    hhp.county,
    hhp.postal_code,
    hhp.state,
    hhp.account_number,
    hhp.cust_account_id,
    hhp.source_org_name,
    hhp.attribute10,
    hhp.party_site_number,
    blt.bolt_account_number,
    blt.bolt_cust_account_id,
    blt.bolt_address1,
    blt.bolt_address2,
    blt.bolt_address3,
    blt.bolt_address4,
    blt.bolt_city,
    blt.bolt_country,
    blt.bolt_county,
    blt.bolt_postal_code,
    blt.bolt_state,
    blt.orig_system_reference,
    blt.bolt_party_site_number
from hhp_data hhp
left join bolt_data blt
    on blt.bolt_account_number = hhp.account_number
    and blt.orig_system_reference = hhp.attribute10
where blt.bolt_party_site_number is not null
"""

# COMMAND ----------

#C360 locations query
c360_locations_query = """
with bolt_data as (
    select distinct
        hp.party_id as bolt_party_id,
        hp.party_number as bolt_party_number,
        hca.account_number as bolt_account_number,
        hca.account_name as bolt_account_name,
        hca.customer_type as bolt_customer_type,
        hp.party_name as bolt_party_name,
        hps.party_site_number as bolt_party_site_number,
        loc.address1 as bolt_address1,
        loc.address2 as bolt_address2,
        loc.address3 as bolt_address3,
        loc.address4 as bolt_address4,
        loc.city as bolt_city,
        loc.county as bolt_county,
        loc.state as bolt_state,
        loc.province as bolt_province,
        loc.country as bolt_country,
        loc.postal_code as bolt_postal_code
    from bolt.hz_parties hp
    join bolt.hz_cust_accounts hca
        on hp.party_id = hca.party_id
    join bolt.hz_cust_acct_sites_all cas
        on hca.cust_account_id = cas.cust_account_id
    join bolt.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    join bolt.hz_locations loc
        on hps.location_id = loc.location_id
    where hca.status = 'A'
      and cas.status = 'A'
      and hps.status = 'A'
),
acc_cte as (
    select
        split_part(pkey_src_object, '|', 1) as usage_type,
        split_part(pkey_src_object, '|', 2) as party_site_number,
        split_part(pkey_src_object, '|', 3) as account_number,
        pkey_src_object,
        x_prty_fk,
        x_pstl_addr_fk,
        x_addr_usg_typ
    from c360.c_xo_prty_addr_chld_xref
    where upper(trim(x_addr_usg_typ)) = 'BILL TO'
),
x_ref as (
    select
        rowid_system,
        pstl_addr_fk,
        prty_fk,
        phn_num
    from c360.c_br_prty_rel_pstl_addr_xref
    where lower(trim(rowid_system)) = 'bolt'
),
bo_prty as (
    select
        rowid_object,
        full_nm,
        prty_typ,
        x_entity_code,
        x_customer_number
    from c360.c_bo_prty
    where hub_state_ind = 1
)
select
    d.account_number,
    d.party_site_number,
    a.addr_ln_1,
    a.addr_ln_2,
    a.addr_ln_3,
    a.addr_ln_4,
    a.addr_ln_5,
    a.city,
    a.state,
    a.cntry_cd,
    a.pstl_cd,
    a.county,
    a.x_province,
    a.x_country_name,
    c.full_nm as party_name,
    c.prty_typ,
    c.x_entity_code,
    c.x_customer_number,
    b.phn_num,
    blt.bolt_account_number,
    blt.bolt_party_site_number,
    blt.bolt_address1,
    blt.bolt_address2,
    blt.bolt_address3,
    blt.bolt_address4,
    blt.bolt_account_name,
    blt.bolt_party_name,
    blt.bolt_city,
    blt.bolt_state,
    blt.bolt_province,
    blt.bolt_country,
    blt.bolt_county,
    blt.bolt_postal_code,
    'all' as source_org_name
from c360.c_bo_pstl_addr a
left join x_ref b
    on b.pstl_addr_fk = a.rowid_object
left join bo_prty c
    on c.rowid_object = b.prty_fk
left join acc_cte d
    on d.x_prty_fk = b.prty_fk
    and d.x_pstl_addr_fk = b.pstl_addr_fk
left join bolt_data blt
    on d.account_number = blt.bolt_account_number
    and d.party_site_number = blt.bolt_party_site_number
where lower(trim(a.last_rowid_system)) = 'bolt'
  and lower(trim(d.x_addr_usg_typ)) = 'bill to'
"""

# COMMAND ----------

#C360 phone query
c360_phone_query = """
with acc_cte as (
    select
        split_part(pkey_src_object, '|', 1) as usage_type,
        split_part(pkey_src_object, '|', 2) as party_site_number,
        split_part(pkey_src_object, '|', 3) as account_number,
        x_prty_fk,
        x_pstl_addr_fk,
        x_addr_usg_typ
    from c360.c_xo_prty_addr_chld_xref
    where upper(trim(x_addr_usg_typ)) = 'BILL TO'
),
x_ref as (
    select
        rowid_system,
        pstl_addr_fk,
        prty_fk
    from c360.c_br_prty_rel_pstl_addr_xref
    where lower(trim(rowid_system)) = 'bolt'
),
c360_party as (
    select
        rowid_object,
        full_nm,
        x_cust_type,
        x_main_ph_num,
        x_ownership_type
    from c360.c_bo_prty
    where hub_state_ind = 1
),
bolt_contact_data as (
    select distinct
        cnp.phone_number as bolt_phone_number,
        con.account_number as bolt_account_number,
        con.cust_account_id as bolt_cust_account_id,
        con.customer_type as bolt_customer_type,
        cnp.contact_point_type as bolt_contact_point_type,
        hps.party_site_number as bolt_party_site_number,
        sub.party_name as bolt_party_name
    from bolt.hz_contact_points cnp
    left join bolt.hz_relationships rel
        on cnp.owner_table_id = rel.party_id
    left join bolt.hz_cust_accounts con
        on con.party_id = rel.object_id
    left join bolt.hz_parties sub
        on sub.party_id = cnp.owner_table_id
    left join bolt.hz_cust_acct_sites_all cas
        on con.cust_account_id = cas.cust_account_id
    left join bolt.hz_party_sites hps
        on cas.party_site_id = hps.party_site_id
    where cnp.owner_table_name = 'HZ_PARTIES'
      and rel.directional_flag = 'F'
      and rel.relationship_type = 'CONTACT'
      and con.status = 'A'
      and sub.status = 'A'
      and cnp.contact_point_type = 'PHONE'
)
select
    d.account_number,
    d.party_site_number,
    c.x_cust_type,
    case
        when upper(c.x_ownership_type) = 'E' then 'R'
        else c.x_ownership_type
    end as customer_type,
    c.x_main_ph_num,
    blt.bolt_phone_number,
    blt.bolt_account_number,
    blt.bolt_party_site_number,
    blt.bolt_customer_type,
    c.full_nm as party_name,
    blt.bolt_party_name,
    'all' as source_org_name
from c360.c_bo_pstl_addr a
left join x_ref b
    on b.pstl_addr_fk = a.rowid_object
left join c360_party c
    on c.rowid_object = b.prty_fk
left join acc_cte d
    on d.x_prty_fk = b.prty_fk
    and d.x_pstl_addr_fk = b.pstl_addr_fk
left join bolt_contact_data blt
    on d.account_number = blt.bolt_account_number
    and d.party_site_number = blt.bolt_party_site_number
where lower(trim(a.last_rowid_system)) = 'bolt'
  and lower(trim(d.x_addr_usg_typ)) = 'bill to'
  and blt.bolt_account_number is not null
"""

# COMMAND ----------

#CIL contact points query
cil_contact_points_query = """
with bolt_data as (
    select
        array_agg(coalesce(cnp.email_address, 'NULL')) as bolt_email_address,
        array_agg(coalesce(cnp.phone_number, 'NULL')) as bolt_phone_number,
        con.account_number as bolt_account_number,
        cnp.contact_point_type as bolt_contact_point_type,
        con.cust_account_id as bolt_cust_account_id
    from bolt.hz_contact_points cnp
    left join bolt.hz_relationships rel
        on cnp.owner_table_id = rel.party_id
    left join bolt.hz_cust_accounts con
        on con.party_id = rel.object_id
    where cnp.owner_table_name = 'HZ_PARTIES'
      and rel.directional_flag = 'F'
      and rel.relationship_type = 'CONTACT'
      and con.hdisdeletedrecord != 0
      and con.status = 'A'
    group by
        con.account_number,
        cnp.contact_point_type,
        con.cust_account_id
)
select
    array_agg(coalesce(cnp.email_address, 'NULL')) as email_address,
    array_agg(coalesce(cnp.phone_number, 'NULL')) as phone_number,
    blt.bolt_email_address,
    blt.bolt_phone_number,
    con.account_number,
    cnp.contact_point_type,
    con.cust_account_id,
    'all' as source_org_name,
    blt.bolt_account_number
from cil.hz_contact_points cnp
left join cil.hz_relationships rel
    on cnp.owner_table_id = rel.party_id
left join cil.hz_cust_accounts con
    on con.party_id = rel.object_id
left join bolt_data blt
    on blt.bolt_account_number = con.account_number
    and blt.bolt_contact_point_type = cnp.contact_point_type
where cnp.owner_table_name = 'HZ_PARTIES'
  and rel.directional_flag = 'F'
  and rel.relationship_type = 'CONTACT'
  and con.hdisdeletedrecord != 0
  and con.status = 'A'
group by
    con.account_number,
    cnp.contact_point_type,
    con.cust_account_id,
    blt.bolt_phone_number,
    blt.bolt_email_address,
    blt.bolt_account_number
"""

# COMMAND ----------

#HHP contact points query
hhp_contact_points_query = """
with bolt_data as (
    select
        array_agg(coalesce(cnp.email_address, 'NULL')) as bolt_email_address,
        array_agg(coalesce(cnp.phone_number, 'NULL')) as bolt_phone_number,
        con.account_number as bolt_account_number,
        cnp.contact_point_type as bolt_contact_point_type,
        con.cust_account_id as bolt_cust_account_id
    from bolt.hz_contact_points cnp
    left join bolt.hz_relationships rel
        on cnp.owner_table_id = rel.party_id
    left join bolt.hz_cust_accounts con
        on con.party_id = rel.object_id
    left join bolt.hz_parties sub
        on sub.party_id = cnp.owner_table_id
    where cnp.owner_table_name = 'HZ_PARTIES'
      and rel.directional_flag = 'F'
      and rel.relationship_type = 'CONTACT'
      and con.status = 'A'
      and sub.status = 'A'
    group by
        con.account_number,
        cnp.contact_point_type,
        con.cust_account_id
),
hhp_data as (
    select
        cnp.email_address,
        cnp.phone_number,
        con.account_number,
        cnp.contact_point_type,
        con.cust_account_id,
        'all' as source_org_name
    from hhp.hz_contact_points cnp
    left join hhp.hz_relationships rel
        on cnp.owner_table_id = rel.party_id
    left join hhp.hz_cust_accounts con
        on con.party_id = rel.object_id
    left join hhp.hz_parties sub
        on sub.party_id = cnp.owner_table_id
    where cnp.owner_table_name = 'HZ_PARTIES'
      and rel.directional_flag = 'F'
      and rel.relationship_type = 'CONTACT'
      and con.status = 'A'
)
select
    array_agg(coalesce(hhp.email_address, 'NULL')) as email_address,
    array_agg(coalesce(hhp.phone_number, 'NULL')) as phone_number,
    blt.bolt_email_address,
    blt.bolt_phone_number,
    hhp.account_number,
    hhp.contact_point_type,
    hhp.cust_account_id,
    hhp.source_org_name,
    blt.bolt_account_number
from hhp_data hhp
left join bolt_data blt
    on blt.bolt_account_number = hhp.account_number
    and blt.bolt_contact_point_type = hhp.contact_point_type
group by
    hhp.account_number,
    hhp.contact_point_type,
    hhp.cust_account_id,
    hhp.source_org_name,
    blt.bolt_phone_number,
    blt.bolt_email_address,
    blt.bolt_account_number
"""