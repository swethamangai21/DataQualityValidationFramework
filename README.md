# Finance Data Quality Validation Framework

An end-to-end data quality framework designed to profile, validate, monitor, and report data quality across multiple finance data sources.

The framework supports metadata-driven data quality rules, automated profiling, detailed error capture, alerting, historical tracking, and Power BI reporting.

## Overview

The solution evaluates data quality across multiple source systems using configurable rules and critical data elements.

The framework performs:

- Critical Data Element identification
- Metadata-driven data quality validation
- Completeness and validity checks
- Pass and fail record profiling
- Detailed error record capture
- Health score calculation
- Automated alert notifications
- Historical error archiving
- Scheduled execution
- Power BI reporting and monitoring

## Architecture

The solution uses the following components:

- **Snowflake** - Source data platform
- **Azure Databricks** - Data quality processing and orchestration
- **Azure SQL Database** - Metadata, dimensions, rule definitions, and aggregated profiling results
- **Azure Data Lake Storage** - Detailed data quality error records
- **Azure Key Vault** - Secure credential management
- **Power BI** - Data quality dashboards and reporting
- **Gmail SMTP** - Automated job and data quality notifications

## High-Level Data Flow

```text
Snowflake Source Data
        |
        v
Azure Databricks
        |
        |-- Load DQ Metadata
        |-- Identify Critical Attributes
        |-- Execute Data Quality Rules
        |-- Calculate Pass / Fail Counts
        |-- Capture Failed Records
        |
        +-----------------------+
        |                       |
        v                       v
Azure SQL                  ADLS / Delta
Fact_Rules                 Fact_Error
        |                       |
        +-----------+-----------+
                    |
                    v
                 Power BI
