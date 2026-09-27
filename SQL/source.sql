--database
create database if not exists finance_dq_source_dev;

--check if the database exists
show databases like 'finance_dq_source_dev';

--warehouse
create warehouse if not exists dq_dev_wh
warehouse_size='xsmall'
auto_suspend=60
auto_resume=true
initially_suspended=true;

-------------------------- BOLT ---------------------------
--schema: BOLT
create schema if not exists finance_dq_source_dev.bolt;

use warehouse dq_dev_wh;
use database finance_dq_source_dev;
use schema bolt;

select 
current_warehouse(),
current_database(),
current_schema();

--Table: HZ_CUST_ACCOUNTS
create table if not exists hz_cust_accounts(
cust_account_id number,
party_id number,
account_number varchar(30),
account_name varchar(200),
customer_type varchar(10),
status varchar(1)
)

describe table hz_cust_accounts;

INSERT INTO HZ_CUST_ACCOUNTS
(
    CUST_ACCOUNT_ID,
    PARTY_ID,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME,
    CUSTOMER_TYPE,
    STATUS
)
VALUES
    (1001, 501, 'B100001', 'Tata Motors',        'R', 'A'),
    (1002, 502, 'B100002', 'ABC Industries',     'R', 'A'),
    (1003, 503, 'B100003', 'Global Auto Parts',  'D', 'A'),
    (1004, 504, NULL,      'Metro Components',   'R', 'A'),
    (1005, 505, 'B100005', NULL,                 'R', 'A'),
    (1006, 506, 'B100006', 'Prime Engineering',  'X', 'A'),
    (1007, 507, 'B100002', 'Zenith Motors',       'R', 'A'),
    (1008, 508, 'B100008', 'Delta Equipment',     'D', 'I');

select * from hz_cust_accounts;

select * from hz_cust_accounts where status='A';

--Table: HZ_PARTIES
create table if not exists hz_parties(
party_id number,
party_number varchar(30),
party_name varchar(200),
status varchar(1)
);

INSERT INTO HZ_PARTIES
(
    PARTY_ID,
    PARTY_NUMBER,
    PARTY_NAME,
    STATUS
)
VALUES
    (501, 'P000501', 'Tata Motors',         'A'),
    (502, 'P000502', 'ABC Industries',      'A'),
    (503, 'P000503', 'Global Auto Parts',   'A'),
    (504, 'P000504', 'Metro Components',    'A'),
    (505, 'P000505', 'Nova Manufacturing',  'A'),
    (506, 'P000506', 'Prime Engineering',   'A'),
    (507, 'P000507', 'Zenith Motors',        'A'),
    (508, 'P000508', 'Delta Equipment',      'A');

--Table: HZ_PARTY_SITES
create table if not exists hz_party_sites(
party_site_id number,
party_id number,
location_id number,
party_site_number varchar(30),
status varchar(1),
orig_system_reference varchar(100)
)

INSERT INTO HZ_PARTY_SITES
(
    PARTY_SITE_ID,
    PARTY_ID,
    LOCATION_ID,
    PARTY_SITE_NUMBER,
    STATUS,
    ORIG_SYSTEM_REFERENCE
)
VALUES
    (7001, 501, 9001, 'SITE0001', 'A', 'BOLT_SITE_7001'),
    (7002, 502, 9002, 'SITE0002', 'A', 'BOLT_SITE_7002'),
    (7003, 503, 9003, 'SITE0003', 'A', 'BOLT_SITE_7003'),
    (7004, 504, 9004, 'SITE0004', 'A', 'BOLT_SITE_7004'),
    (7005, 505, 9005, 'SITE0005', 'A', 'BOLT_SITE_7005'),
    (7006, 506, 9006, 'SITE0006', 'A', 'BOLT_SITE_7006'),
    (7007, 507, 9007, 'SITE0007', 'A', 'BOLT_SITE_7007'),
    (7008, 508, 9008, 'SITE0008', 'A', 'BOLT_SITE_7008');

select * from hz_party_sites;

--Table: HZ_CUST_ACCT_SITES_ALL
create table if not exists hz_cust_acct_sites_all(
cust_account_id number,
party_site_id number,
status varchar(1)
)

INSERT INTO HZ_CUST_ACCT_SITES_ALL
(
    CUST_ACCOUNT_ID,
    PARTY_SITE_ID,
    STATUS
)
VALUES
    (1001, 7001, 'A'),
    (1002, 7002, 'A'),
    (1003, 7003, 'A'),
    (1004, 7004, 'A'),
    (1005, 7005, 'A'),
    (1006, 7006, 'A'),
    (1007, 7007, 'A'),
    (1008, 7008, 'A');

select * from hz_cust_acct_sites_all;

--Table: HZ_LOCATIONS
create table if not exists hz_locations(
location_id number,
address1 varchar(200),
address2 varchar(200),
address3 varchar(200),
address4 varchar(200),
city varchar(100),
county varchar(100),
state varchar(100),
province varchar(100),
country varchar(10),
postal_code varchar(30),
hdisdeletedrecord number
);

INSERT INTO HZ_LOCATIONS
(
    LOCATION_ID,
    ADDRESS1,
    ADDRESS2,
    ADDRESS3,
    ADDRESS4,
    CITY,
    COUNTY,
    STATE,
    PROVINCE,
    COUNTRY,
    POSTAL_CODE
)
VALUES
    (9001, '123 Industrial Road', NULL, NULL, NULL, 'Bengaluru', NULL, 'Karnataka', NULL, 'IN', '560001'),
    (9002, '45 Manufacturing Park', NULL, NULL, NULL, 'Pune', NULL, 'Maharashtra', NULL, 'IN', '411001'),
    (9003, '78 Auto Components Street', NULL, NULL, NULL, 'Chennai', NULL, 'Tamil Nadu', NULL, 'IN', '600001'),
    (9004, '12 Metro Industrial Estate', NULL, NULL, NULL, 'Hyderabad', NULL, 'Telangana', NULL, 'IN', '500001'),
    (9005, '56 Production Avenue', NULL, NULL, NULL, 'Mumbai', NULL, 'Maharashtra', NULL, 'IN', '400001'),
    (9006, '89 Engineering Layout', NULL, NULL, NULL, 'Bengaluru', NULL, 'Karnataka', NULL, 'IN', '560002'),
    (9007, '34 Motors Park', NULL, NULL, NULL, 'Delhi', NULL, 'Delhi', NULL, 'IN', '110001'),
    (9008, '67 Equipment Road', NULL, NULL, NULL, 'Ahmedabad', NULL, 'Gujarat', NULL, 'IN', '380001');

select * from hz_locations;

------------------------- 1OM ----------------------------
create schema if not exists finance_dq_source_dev.oneom;

use schema finance_dq_source_dev.oneom;

--Table: HZ_CUST_ACCOUNTS
create table if not exists hz_cust_accounts(
cust_account_id number,
party_id number,
account_number varchar(30),
account_name varchar(200),
customer_type varchar(10),
status varchar(1)
)

INSERT INTO HZ_CUST_ACCOUNTS
(
    CUST_ACCOUNT_ID,
    PARTY_ID,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME,
    CUSTOMER_TYPE,
    STATUS
)
VALUES
    (2001, 1501, 'B100001', 'Tata Motors',                'R', 'A'),
    (2002, 1502, 'B100002', 'ABC Industries Pvt Ltd',    'R', 'A'),
    (2003, 1503, 'B100003', 'Global Auto Parts',         'D', 'A'),
    (2004, 1504, 'B100004', 'Metro Components',          'R', 'A'),
    (2005, 1505, 'B100005', 'Nova Manufacturing',        'R', 'A'),
    (2006, 1506, 'B100006', 'Prime Engineering',         'R', 'A'),
    (2007, 1507, 'B100007', 'Zenith Motors',             'R', 'A'),
    (2008, 1508, 'B100008', 'Delta Equipment',           'D', 'I');

select * from hz_cust_accounts;

--Table: HZ_PARTIES
create table if not exists hz_parties(
party_id number,
party_number varchar(30),
party_name varchar(200),
status varchar(1)
);

INSERT INTO HZ_PARTIES
(
    PARTY_ID,
    PARTY_NUMBER,
    PARTY_NAME,
    STATUS
)
VALUES
    (1501, 'P001501', 'Tata Motors',                'A'),
    (1502, 'P001502', 'ABC Industries Pvt Ltd',    'A'),
    (1503, 'P001503', 'Global Auto Parts',         'A'),
    (1504, 'P001504', 'Metro Components',          'A'),
    (1505, 'P001505', 'Nova Manufacturing',        'A'),
    (1506, 'P001506', 'Prime Engineering',         'A'),
    (1507, 'P001507', 'Zenith Motors',              'A'),
    (1508, 'P001508', 'Delta Equipment',            'A');

select * from hz_parties;

--Table: HZ_CUST_ACCT_SITES_ALL
create table if not exists hz_cust_acct_sites_all(
cust_acct_site_id number,
cust_account_id number,
party_site_id number,
status varchar(1)
);

INSERT INTO HZ_CUST_ACCT_SITES_ALL
(
    CUST_ACCT_SITE_ID,
    CUST_ACCOUNT_ID,
    PARTY_SITE_ID,
    STATUS
)
VALUES
    (3001, 2001, 1701, 'A'),
    (3002, 2002, 1702, 'A'),
    (3003, 2003, 1703, 'A'),
    (3004, 2004, 1704, 'A'),
    (3005, 2005, 1705, 'A'),
    (3006, 2006, 1706, 'A'),
    (3007, 2007, 1707, 'A'),
    (3008, 2008, 1708, 'A');

select * from hz_cust_acct_sites_all;

--Table: HZ_PARTY_SITES
create table if not exists hz_party_sites(
party_site_id number,
party_id number,
location_id number,
party_site_number varchar(30),
status varchar(1),
orig_system_reference varchar(100)
);

INSERT INTO HZ_PARTY_SITES
(
    PARTY_SITE_ID,
    PARTY_ID,
    LOCATION_ID,
    PARTY_SITE_NUMBER,
    STATUS,
    ORIG_SYSTEM_REFERENCE
)
VALUES
    (1701, 1501, 1901, '1OM_SITE_001', 'A', '1OM_REF_1701'),
    (1702, 1502, 1902, '1OM_SITE_002', 'A', '1OM_REF_1702'),
    (1703, 1503, 1903, '1OM_SITE_003', 'A', '1OM_REF_1703'),
    (1704, 1504, 1904, '1OM_SITE_004', 'A', '1OM_REF_1704'),
    (1705, 1505, 1905, '1OM_SITE_005', 'A', '1OM_REF_1705'),
    (1706, 1506, 1906, '1OM_SITE_006', 'A', '1OM_REF_1706'),
    (1707, 1507, 1907, '1OM_SITE_007', 'A', '1OM_REF_1707'),
    (1708, 1508, 1908, '1OM_SITE_008', 'A', '1OM_REF_1708');

select * from hz_party_sites;

alter table hz_party_sites
add column attribute10 varchar(100);

update hz_party_sites
set attribute10=
    case party_site_id
        when 1701 then 'SITE0001'
        WHEN 1702 THEN 'SITE0002'
        WHEN 1703 THEN 'SITE0003'
        WHEN 1704 THEN 'SITE0004'
        WHEN 1705 THEN 'SITE0005'
        WHEN 1706 THEN 'SITE0006'
        WHEN 1707 THEN 'SITE0007'
        WHEN 1708 THEN 'SITE0008'
    end;

--Table: HZ_CUST_SITE_USES_ALL
create table if not exists hz_cust_site_uses_all(
cust_acct_site_id number
);

INSERT INTO HZ_CUST_SITE_USES_ALL
(
    CUST_ACCT_SITE_ID
)
VALUES
    (3001),
    (3002),
    (3003),
    (3004),
    (3005),
    (3006),
    (3007),
    (3008);

select * from hz_cust_site_uses_all;

--Table: HZ_LOCATIONS
create table if not exists hz_locations(
location_id number,
address1 varchar(200),
address2 varchar(200),
address3 varchar(200),
address4 varchar(200),
city varchar(100),
state varchar(100),
country varchar(100),
postal_code varchar(30),
county varchar(100),
province varchar(100)
)

INSERT INTO HZ_LOCATIONS
(
    LOCATION_ID,
    ADDRESS1,
    ADDRESS2,
    ADDRESS3,
    ADDRESS4,
    CITY,
    STATE,
    COUNTRY,
    POSTAL_CODE,
    COUNTY,
    PROVINCE
)
VALUES
    (1901, '123 Industrial Road',       NULL, NULL, NULL, 'Bengaluru', 'Karnataka',    'IN', '560001', NULL, NULL),
    (1902, '47 Manufacturing Park',     NULL, NULL, NULL, 'Pune',      'Maharashtra',  'IN', '411001', NULL, NULL),
    (1903, '78 Auto Components Street', NULL, NULL, NULL, 'Chennai',   'Tamil Nadu',   'IN', '600001', NULL, NULL),
    (1904, '12 Metro Industrial Estate',NULL, NULL, NULL, 'Hyderabad', 'Telangana',    'IN', '500001', NULL, NULL),
    (1905, '56 Production Avenue',      NULL, NULL, NULL, 'Mumbai',    'Maharashtra',  'IN', '400001', NULL, NULL),
    (1906, '89 Engineering Layout',     NULL, NULL, NULL, 'Bengaluru', 'Karnataka',    'IN', NULL,     NULL, NULL),
    (1907, '34 Motors Park',            NULL, NULL, NULL, 'Delhi',     'Delhi',        'IN', '110001', NULL, NULL),
    (1908, '67 Equipment Road',         NULL, NULL, NULL, 'Ahmedabad', 'Gujarat',      'IN', '380001', NULL, NULL);

select * from hz_locations;

------------------------ CIL --------------------------
create schema if not exists finance_dq_source_dev.cil;

use schema finance_dq_source_dev.cil;

--Table: HZ_CUST_ACCOUNTS
CREATE TABLE IF NOT EXISTS HZ_CUST_ACCOUNTS (
    CUST_ACCOUNT_ID       NUMBER,
    PARTY_ID              NUMBER,
    ACCOUNT_NUMBER        VARCHAR(30),
    ACCOUNT_NAME          VARCHAR(200),
    CUSTOMER_TYPE         VARCHAR(10),
    STATUS                VARCHAR(1),
    HDISDELETEDRECORD     NUMBER
);

INSERT INTO HZ_CUST_ACCOUNTS
(
    CUST_ACCOUNT_ID,
    PARTY_ID,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME,
    CUSTOMER_TYPE,
    STATUS,
    HDISDELETEDRECORD
)
VALUES
    (3001, 2501, 'B100001', 'Tata Motors',              'R', 'A', 1),
    (3002, 2502, 'B100002', 'ABC Industries',           'R', 'A', 1),
    (3003, 2503, 'B100003', 'Global Auto Parts Ltd',    'D', 'A', 1),
    (3004, 2504, 'B100004', 'Metro Components',         'R', 'A', 1),
    (3005, 2505, 'B100005', 'Nova Manufacturing',       'R', 'A', 1),
    (3006, 2506, 'B100006', 'Prime Engineering',        'R', 'A', 1),
    (3007, 2507, 'B100007', 'Zenith Motors',             'R', 'A', 0),
    (3008, 2508, 'B100008', 'Delta Equipment',           'D', 'I', 1);

select * from hz_cust_accounts;

--Table: HZ_PARTIES
CREATE TABLE IF NOT EXISTS HZ_PARTIES (
    PARTY_ID     NUMBER,
    PARTY_NAME   VARCHAR(200),
    STATUS       VARCHAR(1)
);

INSERT INTO HZ_PARTIES
(
    PARTY_ID,
    PARTY_NAME,
    STATUS
)
VALUES
    (2501, 'Tata Motors',             'A'),
    (2502, 'ABC Industries',          'A'),
    (2503, 'Global Auto Parts Ltd',   'A'),
    (2504, 'Metro Components',        'A'),
    (2505, 'Nova Manufacturing',      'A'),
    (2506, 'Prime Engineering',       'A'),
    (2507, 'Zenith Motors',           'A'),
    (2508, 'Delta Equipment',         'A');
    
select * from hz_parties;

--Table: HZ_CUST_ACCT_SITES_ALL
CREATE TABLE IF NOT EXISTS HZ_CUST_ACCT_SITES_ALL (
    CUST_ACCT_SITE_ID   NUMBER,
    CUST_ACCOUNT_ID     NUMBER,
    PARTY_SITE_ID       NUMBER,
    STATUS              VARCHAR(1),
    ATTRIBUTE10         VARCHAR(100)
);

INSERT INTO HZ_CUST_ACCT_SITES_ALL
(
    CUST_ACCT_SITE_ID,
    CUST_ACCOUNT_ID,
    PARTY_SITE_ID,
    STATUS,
    ATTRIBUTE10
)
VALUES
    (4001, 3001, 2701, 'A', 'BOLT_SITE_7001'),
    (4002, 3002, 2702, 'A', 'BOLT_SITE_7002'),
    (4003, 3003, 2703, 'A', 'BOLT_SITE_7003'),
    (4004, 3004, 2704, 'A', 'BOLT_SITE_7004'),
    (4005, 3005, 2705, 'A', 'BOLT_SITE_7005'),
    (4006, 3006, 2706, 'A', 'BOLT_SITE_7006'),
    (4007, 3007, 2707, 'A', 'BOLT_SITE_7007'),
    (4008, 3008, 2708, 'A', 'BOLT_SITE_7008');

select * from hz_cust_acct_sites_all;

--Table: HZ_PARTY_SITES
CREATE TABLE IF NOT EXISTS HZ_PARTY_SITES (
    PARTY_SITE_ID       NUMBER,
    LOCATION_ID         NUMBER,
    PARTY_SITE_NUMBER   VARCHAR(30),
    STATUS              VARCHAR(1)
);

INSERT INTO HZ_PARTY_SITES
(
    PARTY_SITE_ID,
    LOCATION_ID,
    PARTY_SITE_NUMBER,
    STATUS
)
VALUES
    (2701, 2901, 'CIL_SITE_001', 'A'),
    (2702, 2902, 'CIL_SITE_002', 'A'),
    (2703, 2903, 'CIL_SITE_003', 'A'),
    (2704, 2904, 'CIL_SITE_004', 'A'),
    (2705, 2905, 'CIL_SITE_005', 'A'),
    (2706, 2906, 'CIL_SITE_006', 'A'),
    (2707, 2907, 'CIL_SITE_007', 'A'),
    (2708, 2908, 'CIL_SITE_008', 'A');

select * from hz_party_sites;

--Table: HZ_CUST_SITE_USES_ALL
CREATE TABLE IF NOT EXISTS HZ_CUST_SITE_USES_ALL (
    CUST_ACCT_SITE_ID   NUMBER,
    SITE_USE_CODE       VARCHAR(30)
);

INSERT INTO HZ_CUST_SITE_USES_ALL
(
    CUST_ACCT_SITE_ID,
    SITE_USE_CODE
)
VALUES
    (4001, 'BILL_TO'),
    (4002, 'BILL_TO'),
    (4003, 'BILL_TO'),
    (4004, 'BILL_TO'),
    (4005, 'BILL_TO'),
    (4006, 'SHIP_TO'),
    (4007, 'BILL_TO'),
    (4008, 'BILL_TO');

select * from hz_cust_site_uses_all;

--Table: HZ_LOCATIONS
CREATE TABLE IF NOT EXISTS HZ_LOCATIONS (
    LOCATION_ID   NUMBER,
    ADDRESS1      VARCHAR(200),
    ADDRESS2      VARCHAR(200),
    ADDRESS3      VARCHAR(200),
    ADDRESS4      VARCHAR(200),
    CITY          VARCHAR(100),
    COUNTRY       VARCHAR(10),
    COUNTY        VARCHAR(100),
    POSTAL_CODE   VARCHAR(30),
    STATE         VARCHAR(100)
);

INSERT INTO HZ_LOCATIONS
(
    LOCATION_ID,
    ADDRESS1,
    ADDRESS2,
    ADDRESS3,
    ADDRESS4,
    CITY,
    COUNTRY,
    COUNTY,
    POSTAL_CODE,
    STATE
)
VALUES
    (2901, '123 Industrial Road',        NULL, NULL, NULL, 'Bengaluru', 'IN', NULL, '560001', 'Karnataka'),
    (2902, '46 Manufacturing Park',      NULL, NULL, NULL, 'Pune',      'IN', NULL, '411001', 'Maharashtra'),
    (2903, '78 Auto Components Street',  NULL, NULL, NULL, 'Chennai',   'IN', NULL, '600001', 'Tamil Nadu'),
    (2904, '12 Metro Industrial Estate', NULL, NULL, NULL, 'Hyderabad', 'IN', NULL, NULL,     'Telangana'),
    (2905, '56 Production Avenue',       NULL, NULL, NULL, 'Mumbai',    'IN', NULL, '400001', 'Maharashtra'),
    (2906, '89 Engineering Layout',      NULL, NULL, NULL, 'Bengaluru', 'IN', NULL, '560002', 'Karnataka'),
    (2907, '34 Motors Park',             NULL, NULL, NULL, 'Delhi',     'IN', NULL, '110001', 'Delhi'),
    (2908, '67 Equipment Road',          NULL, NULL, NULL, 'Ahmedabad', 'IN', NULL, '380001', 'Gujarat');

select * from hz_locations;

----------------------------- BZL -------------------------
CREATE SCHEMA IF NOT EXISTS FINANCE_DQ_SOURCE_DEV.BZL;

USE SCHEMA FINANCE_DQ_SOURCE_DEV.BZL;

--Table: HZ_CUST_ACCOUNTS
CREATE TABLE IF NOT EXISTS HZ_CUST_ACCOUNTS (
    CUST_ACCOUNT_ID       NUMBER,
    PARTY_ID              NUMBER,
    ACCOUNT_NUMBER        VARCHAR(30),
    ACCOUNT_NAME          VARCHAR(200),
    CUSTOMER_TYPE         VARCHAR(10),
    STATUS                VARCHAR(1),
    HDISDELETEDRECORD     NUMBER
);

INSERT INTO HZ_CUST_ACCOUNTS
(
    CUST_ACCOUNT_ID,
    PARTY_ID,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME,
    CUSTOMER_TYPE,
    STATUS,
    HDISDELETEDRECORD
)
VALUES
    (4001, 3501, 'B100001', 'Tata Motors',              'R', 'A', 1),
    (4002, 3502, 'B100002', 'ABC Industries',           'R', 'A', 1),
    (4003, 3503, 'B100003', 'Global Auto Parts',        'D', 'A', 1),
    (4004, 3504, 'B100004', 'Metro Components',         'R', 'A', 1),
    (4005, 3505, 'B100005', 'Nova Manufacturing Ltd',  'R', 'A', 1),
    (4006, 3506, 'B100006', 'Prime Engg',               'R', 'A', 1),
    (4007, 3507, 'B100007', 'Zenith Motors',             'R', 'A', 1),
    (4008, 3508, 'B100008', 'Delta Equipment',           'D', 'I', 1);

SELECT * FROM HZ_CUST_ACCOUNTS;

--Table: HZ_PARTIES
CREATE TABLE IF NOT EXISTS HZ_PARTIES (
    PARTY_ID       NUMBER,
    PARTY_NAME     VARCHAR(200),
    STATUS         VARCHAR(1)
);

INSERT INTO HZ_PARTIES
(
    PARTY_ID,
    PARTY_NAME,
    STATUS
)
VALUES
    (3501, 'Tata Motors',              'A'),
    (3502, 'ABC Industries',           'A'),
    (3503, 'Global Auto Parts',        'A'),
    (3504, 'Metro Components',         'A'),
    (3505, 'Nova Manufacturing Ltd',   'A'),
    (3506, 'Prime Engg',               'A'),
    (3507, 'Zenith Motors',             'A'),
    (3508, 'Delta Equipment',           'A');

SELECT * FROM HZ_PARTIES;

--Table: HZ_CUST_ACCT_SITES_ALL
CREATE TABLE IF NOT EXISTS HZ_CUST_ACCT_SITES_ALL (
    CUST_ACCOUNT_ID     NUMBER,
    PARTY_SITE_ID       NUMBER,
    STATUS              VARCHAR(1),
    ATTRIBUTE10         VARCHAR(100)
);

INSERT INTO HZ_CUST_ACCT_SITES_ALL
(
    CUST_ACCOUNT_ID,
    PARTY_SITE_ID,
    STATUS,
    ATTRIBUTE10
)
VALUES
    (4001, 3701, 'A', 'BOLT_SITE_7001'),
    (4002, 3702, 'A', 'BOLT_SITE_7002'),
    (4003, 3703, 'A', 'BOLT_SITE_7003'),
    (4004, 3704, 'A', 'BOLT_SITE_7004'),
    (4005, 3705, 'A', 'BOLT_SITE_7005'),
    (4006, 3706, 'A', 'BOLT_SITE_7006'),
    (4007, 3707, 'A', 'BOLT_SITE_7007'),
    (4008, 3708, 'A', 'BOLT_SITE_7008');

SELECT * FROM HZ_CUST_ACCT_SITES_ALL;

--Table: HZ_PARTY_SITES
CREATE TABLE IF NOT EXISTS HZ_PARTY_SITES (
    PARTY_SITE_ID       NUMBER,
    LOCATION_ID         NUMBER,
    PARTY_SITE_NUMBER   VARCHAR(30),
    STATUS              VARCHAR(1)
);

INSERT INTO HZ_PARTY_SITES
(
    PARTY_SITE_ID,
    LOCATION_ID,
    PARTY_SITE_NUMBER,
    STATUS
)
VALUES
    (3701, 3901, 'BZL_SITE_001', 'A'),
    (3702, 3902, 'BZL_SITE_002', 'A'),
    (3703, 3903, 'BZL_SITE_003', 'A'),
    (3704, 3904, 'BZL_SITE_004', 'A'),
    (3705, 3905, 'BZL_SITE_005', 'A'),
    (3706, 3906, 'BZL_SITE_006', 'A'),
    (3707, 3907, 'BZL_SITE_007', 'A'),
    (3708, 3908, 'BZL_SITE_008', 'A');

SELECT * FROM HZ_PARTY_SITES;

--Table: HZ_LOCATIONS
CREATE TABLE IF NOT EXISTS HZ_LOCATIONS (
    LOCATION_ID   NUMBER,
    ADDRESS1      VARCHAR(200),
    ADDRESS2      VARCHAR(200),
    ADDRESS3      VARCHAR(200),
    ADDRESS4      VARCHAR(200),
    CITY          VARCHAR(100),
    COUNTRY       VARCHAR(10),
    COUNTY        VARCHAR(100),
    POSTAL_CODE   VARCHAR(30),
    STATE         VARCHAR(100)
);

INSERT INTO HZ_LOCATIONS
(
    LOCATION_ID,
    ADDRESS1,
    ADDRESS2,
    ADDRESS3,
    ADDRESS4,
    CITY,
    COUNTRY,
    COUNTY,
    POSTAL_CODE,
    STATE
)
VALUES
    (3901, '123 Industrial Road',        NULL, NULL, NULL, 'Bengaluru', 'IN', NULL, '560001', 'Karnataka'),
    (3902, '48 Manufacturing Park',      NULL, NULL, NULL, 'Pune',      'IN', NULL, '411001', 'Maharashtra'),
    (3903, '78 Auto Components Street',  NULL, NULL, NULL, 'Chennai',   'IN', NULL, '600001', 'Tamil Nadu'),
    (3904, '12 Metro Industrial Estate', NULL, NULL, NULL, 'Hyderabad', 'IN', NULL, '500001', 'Telangana'),
    (3905, '56 Production Avenue',       NULL, NULL, NULL, 'Mumbai',    'IN', NULL, NULL,     'Maharashtra'),
    (3906, '90 Engineering Layout',      NULL, NULL, NULL, 'Bengaluru', 'IN', NULL, '560002', 'Karnataka'),
    (3907, '34 Motors Park',             NULL, NULL, NULL, 'Delhi',     'IN', NULL, '110001', 'Delhi'),
    (3908, '67 Equipment Road',          NULL, NULL, NULL, 'Ahmedabad', 'IN', NULL, '380001', 'Gujarat');

SELECT * FROM HZ_LOCATIONS;

------------------------ HHP --------------------------
CREATE SCHEMA IF NOT EXISTS FINANCE_DQ_SOURCE_DEV.HHP;

USE SCHEMA FINANCE_DQ_SOURCE_DEV.HHP;

--Table: HZ_CUST_ACCOUNTS
CREATE TABLE IF NOT EXISTS HZ_CUST_ACCOUNTS (
    CUST_ACCOUNT_ID       NUMBER,
    PARTY_ID              NUMBER,
    ACCOUNT_NUMBER        VARCHAR(30),
    ACCOUNT_NAME          VARCHAR(200),
    CUSTOMER_TYPE         VARCHAR(10),
    STATUS                VARCHAR(1),
    HDISDELETEDRECORD     NUMBER
);

INSERT INTO HZ_CUST_ACCOUNTS
(
    CUST_ACCOUNT_ID,
    PARTY_ID,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME,
    CUSTOMER_TYPE,
    STATUS,
    HDISDELETEDRECORD
)
VALUES
    (5001, 4501, 'B100001', 'Tata Motors',             'R', 'A', 1),
    (5002, 4502, 'B100002', 'ABC Industries',          'R', 'A', 1),
    (5003, 4503, 'B100003', 'Global Auto Parts',       'D', 'A', 1),
    (5004, 4504, 'B100004', 'Metro Components Ltd',    'R', 'A', 1),
    (5005, 4505, 'B100005', 'Nova Manufacturing',      'R', 'A', 1),
    (5006, 4506, 'B100006', 'Prime Engineering',       'R', 'A', 1),
    (5007, 4507, 'B100007', 'Zenith Motors',            'R', 'A', 0),
    (5008, 4508, 'B100008', 'Delta Equipment',          'D', 'I', 1);

SELECT * FROM HZ_CUST_ACCOUNTS;

--Table: HZ_PARTIES 
CREATE TABLE IF NOT EXISTS HZ_PARTIES (
    PARTY_ID       NUMBER,
    PARTY_NAME     VARCHAR(200),
    STATUS         VARCHAR(1)
);

INSERT INTO HZ_PARTIES
(
    PARTY_ID,
    PARTY_NAME,
    STATUS
)
VALUES
    (4501, 'Tata Motors',            'A'),
    (4502, 'ABC Industries',         'A'),
    (4503, 'Global Auto Parts',      'A'),
    (4504, 'Metro Components Ltd',   'A'),
    (4505, 'Nova Manufacturing',     'A'),
    (4506, 'Prime Engineering',      'A'),
    (4507, 'Zenith Motors',           'A'),
    (4508, 'Delta Equipment',         'A');

SELECT * FROM HZ_PARTIES;

--Table: HZ_CUST_ACCT_SITES_ALL
CREATE TABLE IF NOT EXISTS HZ_CUST_ACCT_SITES_ALL (
    CUST_ACCT_SITE_ID   NUMBER,
    CUST_ACCOUNT_ID     NUMBER,
    PARTY_SITE_ID       NUMBER,
    STATUS              VARCHAR(1),
    ATTRIBUTE10         VARCHAR(100)
);

INSERT INTO HZ_CUST_ACCT_SITES_ALL
(
    CUST_ACCT_SITE_ID,
    CUST_ACCOUNT_ID,
    PARTY_SITE_ID,
    STATUS,
    ATTRIBUTE10
)
VALUES
    (6001, 5001, 4701, 'A', 'BOLT_SITE_7001'),
    (6002, 5002, 4702, 'A', 'BOLT_SITE_7002'),
    (6003, 5003, 4703, 'A', 'BOLT_SITE_7003'),
    (6004, 5004, 4704, 'A', 'BOLT_SITE_7004'),
    (6005, 5005, 4705, 'A', 'BOLT_SITE_7005'),
    (6006, 5006, 4706, 'A', 'BOLT_SITE_7006'),
    (6007, 5007, 4707, 'A', 'BOLT_SITE_7007'),
    (6008, 5008, 4708, 'A', 'BOLT_SITE_7008');

SELECT * FROM HZ_CUST_ACCT_SITES_ALL;


--Table: HZ_CUST_SITE_USES_ALL
CREATE TABLE IF NOT EXISTS HZ_CUST_SITE_USES_ALL (
    CUST_ACCT_SITE_ID   NUMBER,
    SITE_USE_CODE       VARCHAR(30)
);

INSERT INTO HZ_CUST_SITE_USES_ALL
(
    CUST_ACCT_SITE_ID,
    SITE_USE_CODE
)
VALUES
    (6001, 'BILL_TO'),
    (6002, 'BILL_TO'),
    (6003, 'BILL_TO'),
    (6004, 'BILL_TO'),
    (6005, 'BILL_TO'),
    (6006, 'SHIP_TO'),
    (6007, 'BILL_TO'),
    (6008, 'BILL_TO');

SELECT * FROM HZ_CUST_SITE_USES_ALL;

--HZ_PARTY_SITES
CREATE TABLE IF NOT EXISTS HZ_PARTY_SITES (
    PARTY_SITE_ID       NUMBER,
    LOCATION_ID         NUMBER,
    PARTY_SITE_NUMBER   VARCHAR(30),
    STATUS              VARCHAR(1)
);

INSERT INTO HZ_PARTY_SITES
(
    PARTY_SITE_ID,
    LOCATION_ID,
    PARTY_SITE_NUMBER,
    STATUS
)
VALUES
    (4701, 4901, 'HHP_SITE_001', 'A'),
    (4702, 4902, 'HHP_SITE_002', 'A'),
    (4703, 4903, 'HHP_SITE_003', 'A'),
    (4704, 4904, 'HHP_SITE_004', 'A'),
    (4705, 4905, 'HHP_SITE_005', 'A'),
    (4706, 4906, 'HHP_SITE_006', 'A'),
    (4707, 4907, 'HHP_SITE_007', 'A'),
    (4708, 4908, 'HHP_SITE_008', 'A');

SELECT * FROM HZ_PARTY_SITES;

--Table: HZ_LOCATIONS
CREATE TABLE IF NOT EXISTS HZ_LOCATIONS (
    LOCATION_ID   NUMBER,
    ADDRESS1      VARCHAR(200),
    ADDRESS2      VARCHAR(200),
    ADDRESS3      VARCHAR(200),
    ADDRESS4      VARCHAR(200),
    CITY          VARCHAR(100),
    COUNTRY       VARCHAR(10),
    COUNTY        VARCHAR(100),
    POSTAL_CODE   VARCHAR(30),
    STATE         VARCHAR(100)
);

INSERT INTO HZ_LOCATIONS
(
    LOCATION_ID,
    ADDRESS1,
    ADDRESS2,
    ADDRESS3,
    ADDRESS4,
    CITY,
    COUNTRY,
    COUNTY,
    POSTAL_CODE,
    STATE
)
VALUES
    (4901, '123 Industrial Road',        NULL, NULL, NULL, 'Bengaluru', 'IN', NULL, '560001', 'Karnataka'),
    (4902, '45 Manufacturing Park',      NULL, NULL, NULL, 'Pune',      'IN', NULL, '411001', 'Maharashtra'),
    (4903, '79 Auto Components Street',  NULL, NULL, NULL, 'Chennai',   'IN', NULL, '600001', 'Tamil Nadu'),
    (4904, '12 Metro Industrial Estate', NULL, NULL, NULL, 'Hyderabad', 'IN', NULL, '500001', 'Telangana'),
    (4905, '56 Production Avenue',       NULL, NULL, NULL, 'Mumbai',    'IN', NULL, NULL,     'Maharashtra'),
    (4906, '89 Engineering Layout',      NULL, NULL, NULL, 'Bengaluru', 'IN', NULL, '560002', 'Karnataka'),
    (4907, '34 Motors Park',             NULL, NULL, NULL, 'Delhi',     'IN', NULL, '110001', 'Delhi'),
    (4908, '67 Equipment Road',          NULL, NULL, NULL, 'Ahmedabad', 'IN', NULL, '380001', 'Gujarat');

SELECT * FROM HZ_LOCATIONS;

----------------------- C360 PREREQUISITE: BOLT PHONE / CONTACT REFERENCE DATA -------------------------------

USE SCHEMA FINANCE_DQ_SOURCE_DEV.BOLT;

ALTER TABLE HZ_CUST_ACCOUNTS
ADD COLUMN IF NOT EXISTS HDISDELETEDRECORD NUMBER;

UPDATE HZ_CUST_ACCOUNTS
SET HDISDELETEDRECORD = 1;

--Table: HZ_RELATIONSHIPS
CREATE TABLE IF NOT EXISTS HZ_RELATIONSHIPS (
    RELATIONSHIP_ID      NUMBER,
    PARTY_ID             NUMBER,
    OBJECT_ID            NUMBER,
    DIRECTIONAL_FLAG     VARCHAR(5),
    RELATIONSHIP_TYPE    VARCHAR(30)
);

INSERT INTO HZ_RELATIONSHIPS
(
    RELATIONSHIP_ID,
    PARTY_ID,
    OBJECT_ID,
    DIRECTIONAL_FLAG,
    RELATIONSHIP_TYPE
)
VALUES
    (8001, 501, 501, 'F', 'CONTACT'),
    (8002, 502, 502, 'F', 'CONTACT'),
    (8003, 503, 503, 'F', 'CONTACT'),
    (8004, 504, 504, 'F', 'CONTACT'),
    (8005, 505, 505, 'F', 'CONTACT'),
    (8006, 506, 506, 'F', 'CONTACT'),
    (8007, 507, 507, 'F', 'CONTACT'),
    (8008, 508, 508, 'F', 'CONTACT');

SELECT * FROM HZ_RELATIONSHIPS;

--Table: HZ_CONTACT_POINTS
CREATE TABLE IF NOT EXISTS HZ_CONTACT_POINTS (
    CONTACT_POINT_ID     NUMBER,
    OWNER_TABLE_ID       NUMBER,
    OWNER_TABLE_NAME     VARCHAR(50),
    PHONE_NUMBER         VARCHAR(30),
    CONTACT_POINT_TYPE   VARCHAR(30)
);

INSERT INTO HZ_CONTACT_POINTS
(
    CONTACT_POINT_ID,
    OWNER_TABLE_ID,
    OWNER_TABLE_NAME,
    PHONE_NUMBER,
    CONTACT_POINT_TYPE
)
VALUES
    (8101, 501, 'HZ_PARTIES', '9876500001', 'PHONE'),
    (8102, 502, 'HZ_PARTIES', '9876500002', 'PHONE'),
    (8103, 503, 'HZ_PARTIES', '9876500003', 'PHONE'),
    (8104, 504, 'HZ_PARTIES', '9876500004', 'PHONE'),
    (8105, 505, 'HZ_PARTIES', '9876500005', 'PHONE'),
    (8106, 506, 'HZ_PARTIES', '9876500006', 'PHONE'),
    (8107, 507, 'HZ_PARTIES', '9876500007', 'PHONE'),
    (8108, 508, 'HZ_PARTIES', '9876500008', 'PHONE');

SELECT * FROM HZ_CONTACT_POINTS;

------------------------ C360 -------------------------

CREATE SCHEMA IF NOT EXISTS FINANCE_DQ_SOURCE_DEV.C360;

USE SCHEMA FINANCE_DQ_SOURCE_DEV.C360;

--Table: C_XO_PRTY_ADDR_CHLD_XREF
create table if not exists c_xo_prty_addr_chld_xref(
pkey_src_object varchar(200),
x_prty_fk varchar(50),
x_pstl_addr_fk varchar(50),
x_addr_usg_typ varchar(30)
)

INSERT INTO C_XO_PRTY_ADDR_CHLD_XREF
(
    PKEY_SRC_OBJECT,
    X_PRTY_FK,
    X_PSTL_ADDR_FK,
    X_ADDR_USG_TYP
)
VALUES
    ('BILL TO|SITE0001|B100001', 'PRTY_001', 'ADDR_001', 'BILL TO'),
    ('BILL TO|SITE0002|B100002', 'PRTY_002', 'ADDR_002', 'BILL TO'),
    ('BILL TO|SITE0003|B100003', 'PRTY_003', 'ADDR_003', 'BILL TO'),
    ('BILL TO|SITE0004|B100004', 'PRTY_004', 'ADDR_004', 'BILL TO'),
    ('BILL TO|SITE0005|B100005', 'PRTY_005', 'ADDR_005', 'BILL TO'),
    ('BILL TO|SITE0006|B100006', 'PRTY_006', 'ADDR_006', 'BILL TO'),
    ('BILL TO|SITE0007|B100007', 'PRTY_007', 'ADDR_007', 'BILL TO'),
    ('BILL TO|SITE0008|B100008', 'PRTY_008', 'ADDR_008', 'BILL TO');

select * from c_xo_prty_addr_chld_xref;

--Table: C_BR_PRTY_REL_PSTL_ADDR_XREF

create table if not exists c_br_prty_rel_pstl_addr_xref(
rowid_system varchar(30),
pstl_addr_fk varchar(50),
prty_fk varchar(50),
phn_num varchar(30)
)

INSERT INTO C_BR_PRTY_REL_PSTL_ADDR_XREF
(
    ROWID_SYSTEM,
    PSTL_ADDR_FK,
    PRTY_FK,
    PHN_NUM
)
VALUES
    ('bolt', 'ADDR_001', 'PRTY_001', '9876500001'),
    ('bolt', 'ADDR_002', 'PRTY_002', '9876500002'),
    ('bolt', 'ADDR_003', 'PRTY_003', '9876500003'),
    ('bolt', 'ADDR_004', 'PRTY_004', '9876500004'),
    ('bolt', 'ADDR_005', 'PRTY_005', '9876500005'),
    ('bolt', 'ADDR_006', 'PRTY_006', '9876500099'),
    ('bolt', 'ADDR_007', 'PRTY_007', '9876500007'),
    ('bolt', 'ADDR_008', 'PRTY_008', '9876500008');

select * from c_br_prty_rel_pstl_addr_xref;

--Table: C_BO_PARTY
create table if not exists c_bo_prty(
rowid_object varchar(50),
full_nm varchar(200),
prty_typ varchar(30),
x_entity_code varchar(30),
x_customer_number varchar(30),
x_cust_type varchar(30),
x_main_ph_num varchar(30),
x_ownership_type varchar(10),
hub_state_ind number
);

INSERT INTO C_BO_PRTY
(
    ROWID_OBJECT,
    FULL_NM,
    PRTY_TYP,
    X_ENTITY_CODE,
    X_CUSTOMER_NUMBER,
    X_CUST_TYPE,
    X_MAIN_PH_NUM,
    X_OWNERSHIP_TYPE,
    HUB_STATE_IND
)
VALUES
    ('PRTY_001', 'Tata Motors',              'ORGANIZATION', 'ENT001', 'B100001', 'RETAIL',      '9876500001', 'E', 1),
    ('PRTY_002', 'ABC Industries',           'ORGANIZATION', 'ENT002', 'B100002', 'RETAIL',      '9876500002', 'E', 1),
    ('PRTY_003', 'Global Auto Parts',        'ORGANIZATION', 'ENT003', 'B100003', 'DISTRIBUTOR', '9876500003', 'D', 1),
    ('PRTY_004', 'Metro Components',         'ORGANIZATION', 'ENT004', 'B100004', 'RETAIL',      '9876500004', 'E', 1),
    ('PRTY_005', 'Nova Manufacturing',       'ORGANIZATION', 'ENT005', 'B100005', 'RETAIL',      '9876500005', 'E', 1),
    ('PRTY_006', 'Prime Engineering',        'ORGANIZATION', 'ENT006', 'B100006', 'RETAIL',      '9876500099', 'E', 1),
    ('PRTY_007', 'Zenith Motors',            'ORGANIZATION', 'ENT007', 'B100007', 'RETAIL',      '9876500007', 'E', 1),
    ('PRTY_008', 'Delta Equipment',          'ORGANIZATION', 'ENT008', 'B100008', 'DISTRIBUTOR', '9876500008', 'D', 1);

select * from c_bo_prty;

--Table: C_BO_PSTL_ADDR
create table if not exists c_bo_pstl_addr(
rowid_object varchar(50),
addr_ln_1 varchar(200),
addr_ln_2 varchar(200),
addr_ln_3 varchar(200),
addr_ln_4 varchar(200),
addr_ln_5 varchar(200),
city varchar(100),
state varchar(100),
cntry_cd varchar(10),
pstl_cd varchar(30),
county varchar(100),
x_province varchar(100),
x_country_name varchar(100),
last_rowid_system varchar(30)
);

INSERT INTO C_BO_PSTL_ADDR
(
    ROWID_OBJECT,
    ADDR_LN_1,
    ADDR_LN_2,
    ADDR_LN_3,
    ADDR_LN_4,
    ADDR_LN_5,
    CITY,
    STATE,
    CNTRY_CD,
    PSTL_CD,
    COUNTY,
    X_PROVINCE,
    X_COUNTRY_NAME,
    LAST_ROWID_SYSTEM
)
VALUES
    ('ADDR_001', '123 Industrial Road',        NULL, NULL, NULL, NULL, 'Bengaluru', 'Karnataka',   'IN', '560001', NULL, NULL, 'India', 'bolt'),
    ('ADDR_002', '45 Manufacturing Park',      NULL, NULL, NULL, NULL, 'Pune',      'Maharashtra', 'IN', '411001', NULL, NULL, 'India', 'bolt'),
    ('ADDR_003', '78 Auto Components Street',  NULL, NULL, NULL, NULL, 'Chennai',   'Tamil Nadu',  'IN', '600001', NULL, NULL, 'India', 'bolt'),
    ('ADDR_004', '15 Metro Industrial Estate', NULL, NULL, NULL, NULL, 'Hyderabad', 'Telangana',   'IN', '500001', NULL, NULL, 'India', 'bolt'),
    ('ADDR_005', '56 Production Avenue',       NULL, NULL, NULL, NULL, 'Mumbai',    'Maharashtra', 'IN', '400001', NULL, NULL, 'India', 'bolt'),
    ('ADDR_006', '89 Engineering Layout',      NULL, NULL, NULL, NULL, 'Bengaluru', 'Karnataka',   'IN', '560002', NULL, NULL, 'India', 'bolt'),
    ('ADDR_007', '34 Motors Park',             NULL, NULL, NULL, NULL, 'Delhi',     'Delhi',       'IN', '110001', NULL, NULL, 'India', 'bolt'),
    ('ADDR_008', '67 Equipment Road',          NULL, NULL, NULL, NULL, 'Ahmedabad', 'Gujarat',     'IN', '380001', NULL, NULL, 'India', 'bolt');

select * from c_bo_pstl_addr;

----------------------- CREATE ROLE & GRANT USAGE ----------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS DQ_DATABRICKS_ROLE;

GRANT USAGE 
ON WAREHOUSE DQ_DEV_WH
TO ROLE DQ_DATABRICKS_ROLE;

GRANT USAGE
ON DATABASE FINANCE_DQ_SOURCE_DEV
TO ROLE DQ_DATABRICKS_ROLE;

GRANT USAGE 
ON ALL SCHEMAS IN DATABASE FINANCE_Dq_SOURCE_DEV
TO ROLE DQ_DATABRICKS_ROLE;

GRANT SELECT
ON ALL TABLES IN DATABASE FINANCE_DQ_SOURCE_DEV
TO ROLE DQ_DATABRICKS_ROLE;

GRANT SELECT
ON FUTURE TABLES IN DATABASE FINANCE_DQ_SOURCE_DEV
TO ROLE DQ_DATABRICKS_ROLE;

GRANT USAGE
ON FUTURE SCHEMAS IN DATABASE FINANCE_DQ_SOURCE_DEV
TO ROLE DQ_DATABRICKS_ROLE;

SHOW GRANTS TO ROLE DQ_DATABRICKS_ROLE;

----------------- KEYS ----------------------
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS DQ_DATABRICKS_USER
TYPE=SERVICE
DEFAULT_ROLE=DQ_DATABRICKS_ROLE
DEFAULT_WAREHOUSE=DQ_DEV_WH
COMMENT='Service user for Databricks Finance DQ connection';

GRANT ROLE DQ_DATABRICKS_ROLE
TO USER DQ_DATABRICKS_USER;

ALTER USER DQ_DATABRICKS_USER
SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt5NXzbPS4hhS34fXgmAi
rFWsmkW+oOtkTsQZ003S29cnN0dlAW8gB838FQ0ksQNdfcF7EiG27ITx7Ir0MYs3
9WeqlFc7PzhlsiE5RtjIU8rqzxm3QljPkEln0BaALDMsMEFcbwWUWiUFUOjDO/91
iejSPhSusZ/EzqZ7414Z1y2TpNfRADPRDk8Ma1u/ZCL2s3OvbckFIhc/kzz2DhgP
CzYUb7Fyi1C6zP0f2iJ/OKMXb4Re73nXnUkAneAsy9/72p8b99HcZJzcg9adRm67
Mqr/x1aXkKUJ9byWcagQSpd0gdnGjTZ96B8asj/YbtTkQ6lRnZ6gMNqciki5LuP0
CQIDAQAB';

DESC USER DQ_DATABRICKS_USER;

------------------------------------------------------------
create table cil.hz_relationships (
    relationship_id number,
    party_id number,
    object_id number,
    directional_flag varchar(1),
    relationship_type varchar(50)
);

insert into cil.hz_relationships values
(1, 3501, 2501, 'F', 'CONTACT'),
(2, 3502, 2502, 'F', 'CONTACT'),
(3, 3503, 2503, 'F', 'CONTACT'),
(4, 3504, 2504, 'F', 'CONTACT'),
(5, 3505, 2505, 'F', 'CONTACT'),
(6, 3506, 2506, 'F', 'CONTACT'),
(7, 3507, 2507, 'F', 'CONTACT'),
(8, 3508, 2508, 'F', 'CONTACT');

select * from cil.hz_relationships;

---------------------------------------------
create table cil.hz_contact_points (
    contact_point_id number,
    owner_table_id number,
    owner_table_name varchar(50),
    contact_point_type varchar(20),
    phone_number varchar(30),
    email_address varchar(100)
);

insert into cil.hz_contact_points values
(1, 3501, 'HZ_PARTIES', 'PHONE', '9876500001', null),
(2, 3501, 'HZ_PARTIES', 'EMAIL', null, 'contact1@cil.com'),

(3, 3502, 'HZ_PARTIES', 'PHONE', '9876500002', null),
(4, 3502, 'HZ_PARTIES', 'EMAIL', null, 'contact2@cil.com'),

(5, 3503, 'HZ_PARTIES', 'PHONE', '9876500003', null),
(6, 3503, 'HZ_PARTIES', 'EMAIL', null, 'contact3@cil.com'),

(7, 3504, 'HZ_PARTIES', 'PHONE', '9876500004', null),
(8, 3504, 'HZ_PARTIES', 'EMAIL', null, 'contact4@cil.com'),

(9, 3505, 'HZ_PARTIES', 'PHONE', '9876500005', null),
(10, 3505, 'HZ_PARTIES', 'EMAIL', null, 'contact5@cil.com'),

(11, 3506, 'HZ_PARTIES', 'PHONE', '9876500006', null),
(12, 3506, 'HZ_PARTIES', 'EMAIL', null, 'contact6@cil.com'),

(13, 3507, 'HZ_PARTIES', 'PHONE', '9876500007', null),
(14, 3507, 'HZ_PARTIES', 'EMAIL', null, 'contact7@cil.com'),

(15, 3508, 'HZ_PARTIES', 'PHONE', '9876500008', null),
(16, 3508, 'HZ_PARTIES', 'EMAIL', null, 'contact8@cil.com');

select *
from cil.hz_contact_points
order by contact_point_id;

--------------------------------------------
create table hhp.hz_relationships (
    relationship_id number,
    party_id number,
    object_id number,
    directional_flag varchar(1),
    relationship_type varchar(50)
);

insert into hhp.hz_relationships values
(1, 5501, 4501, 'F', 'CONTACT'),
(2, 5502, 4502, 'F', 'CONTACT'),
(3, 5503, 4503, 'F', 'CONTACT'),
(4, 5504, 4504, 'F', 'CONTACT'),
(5, 5505, 4505, 'F', 'CONTACT'),
(6, 5506, 4506, 'F', 'CONTACT'),
(7, 5507, 4507, 'F', 'CONTACT'),
(8, 5508, 4508, 'F', 'CONTACT');

select *
from hhp.hz_relationships
order by relationship_id;

-----------------------------------------
create table hhp.hz_contact_points (
    contact_point_id number,
    owner_table_id number,
    owner_table_name varchar(50),
    contact_point_type varchar(20),
    phone_number varchar(30),
    email_address varchar(100)
);

insert into hhp.hz_contact_points values
(1, 5501, 'HZ_PARTIES', 'PHONE', '9866500001', null),
(2, 5501, 'HZ_PARTIES', 'EMAIL', null, 'contact1@hhp.com'),

(3, 5502, 'HZ_PARTIES', 'PHONE', '9866500002', null),
(4, 5502, 'HZ_PARTIES', 'EMAIL', null, 'contact2@hhp.com'),

(5, 5503, 'HZ_PARTIES', 'PHONE', '9866500003', null),
(6, 5503, 'HZ_PARTIES', 'EMAIL', null, 'contact3@hhp.com'),

(7, 5504, 'HZ_PARTIES', 'PHONE', '9866500004', null),
(8, 5504, 'HZ_PARTIES', 'EMAIL', null, 'contact4@hhp.com'),

(9, 5505, 'HZ_PARTIES', 'PHONE', '9866500005', null),
(10, 5505, 'HZ_PARTIES', 'EMAIL', null, 'contact5@hhp.com'),

(11, 5506, 'HZ_PARTIES', 'PHONE', '9866500006', null),
(12, 5506, 'HZ_PARTIES', 'EMAIL', null, 'contact6@hhp.com'),

(13, 5507, 'HZ_PARTIES', 'PHONE', '9866500007', null),
(14, 5507, 'HZ_PARTIES', 'EMAIL', null, 'contact7@hhp.com'),

(15, 5508, 'HZ_PARTIES', 'PHONE', '9866500008', null),
(16, 5508, 'HZ_PARTIES', 'EMAIL', null, 'contact8@hhp.com');

select *
from hhp.hz_contact_points
order by contact_point_id;

---------------------------------
alter table bolt.hz_contact_points
add column email_address varchar(100);

select *
from bolt.hz_contact_points
order by contact_point_id;





