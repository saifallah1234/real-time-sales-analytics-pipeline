/*
  CDC project — Snowflake infrastructure and landing tables

  Run this file first as a user that has ACCOUNTADMIN.

  Responsibilities:
    1. Create the warehouse, database, roles, and service user.
    2. Create the project schemas.
    3. Grant the Kafka connector only the privileges it needs.
    4. Create the append-only CDC landing tables.

  Important:
    - RSA_PUBLIC_KEY is a public key, not the private key.
    - Keep the matching private key outside Git under
      cdc_infrastructure/secrets/.
    - On a clean deployment, run this file before starting Kafka Connect.
*/

-- ---------------------------------------------------------------------------
-- 1. Account-level objects
-- ---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS CDC_WAREHOUSE
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS CDC_DATABASE;

CREATE ROLE IF NOT EXISTS CDC_ROLE;
CREATE ROLE IF NOT EXISTS KAFKA_CONNECTOR_ROLE_1;

-- CDC_ROLE owns the project schemas and is used by the application/dbt user.
GRANT USAGE ON WAREHOUSE CDC_WAREHOUSE TO ROLE CDC_ROLE;
GRANT USAGE, CREATE SCHEMA ON DATABASE CDC_DATABASE TO ROLE CDC_ROLE;
GRANT ROLE CDC_ROLE TO USER SAIF;

-- ---------------------------------------------------------------------------
-- 2. Project schemas
-- ---------------------------------------------------------------------------

USE ROLE CDC_ROLE;
USE DATABASE CDC_DATABASE;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
CREATE SCHEMA IF NOT EXISTS STAGING_DATA;
CREATE SCHEMA IF NOT EXISTS DATA_MART;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS CDC_LANDING;

-- ---------------------------------------------------------------------------
-- 3. Kafka connector role and service user
-- ---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

GRANT USAGE ON DATABASE CDC_DATABASE
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

GRANT USAGE ON SCHEMA CDC_DATABASE.CDC_LANDING
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

GRANT CREATE TABLE, CREATE STAGE, CREATE PIPE,
      CREATE STREAM, CREATE TASK
    ON SCHEMA CDC_DATABASE.CDC_LANDING
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

-- The merge tasks run on this warehouse.
GRANT USAGE ON WAREHOUSE CDC_WAREHOUSE
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

GRANT EXECUTE TASK ON ACCOUNT
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

-- The tasks merge CDC changes into existing RAW_DATA tables. DML privileges
-- are sufficient; ownership of the RAW_DATA tables must remain with CDC_ROLE.
GRANT USAGE ON SCHEMA CDC_DATABASE.RAW_DATA
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA CDC_DATABASE.RAW_DATA
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON FUTURE TABLES IN SCHEMA CDC_DATABASE.RAW_DATA
    TO ROLE KAFKA_CONNECTOR_ROLE_1;

CREATE USER IF NOT EXISTS KAFKA_USER
    TYPE = SERVICE
    DEFAULT_ROLE = KAFKA_CONNECTOR_ROLE_1
    RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsfu1ba4FQGB98nPnWlnizLsbqZg1cLy205ELAD8D+xa2qVKl20P/dWjcMvg6bY7UDufWiSjf1s7Wbbgfm94CmYhDM+84OnAZ3ADyjRNPVo8Y3vrbYiCOqhQqyukQryDw85xj3X+ManAyb8xJQgHrIBXHM0K7B9ppksiPBEvpidpm7bRDgxnyIyfOL9MTs/4bs3p1MbZW0SD4PgU8qy127VTQZTQmNS5p+ocHow+FMPhkCusWMJinIue1IYlENM+pHumsLaRYI/M9bJcBwKvTJG0iNSNdHapM6obu9b2TSGpBtpaMbG9/DZmxNGnRg/1J9M/OelHF1cvLblZtiV0DmwIDAQAB';

-- CREATE USER IF NOT EXISTS does not update an existing user, so enforce the
-- expected properties for both new and existing project installations.
ALTER USER KAFKA_USER SET
    TYPE = SERVICE
    DEFAULT_ROLE = KAFKA_CONNECTOR_ROLE_1
    RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsfu1ba4FQGB98nPnWlnizLsbqZg1cLy205ELAD8D+xa2qVKl20P/dWjcMvg6bY7UDufWiSjf1s7Wbbgfm94CmYhDM+84OnAZ3ADyjRNPVo8Y3vrbYiCOqhQqyukQryDw85xj3X+ManAyb8xJQgHrIBXHM0K7B9ppksiPBEvpidpm7bRDgxnyIyfOL9MTs/4bs3p1MbZW0SD4PgU8qy127VTQZTQmNS5p+ocHow+FMPhkCusWMJinIue1IYlENM+pHumsLaRYI/M9bJcBwKvTJG0iNSNdHapM6obu9b2TSGpBtpaMbG9/DZmxNGnRg/1J9M/OelHF1cvLblZtiV0DmwIDAQAB';

GRANT ROLE KAFKA_CONNECTOR_ROLE_1 TO USER KAFKA_USER;

-- ---------------------------------------------------------------------------
-- 4. Append-only CDC landing tables
-- ---------------------------------------------------------------------------

USE ROLE KAFKA_CONNECTOR_ROLE_1;
USE DATABASE CDC_DATABASE;
USE SCHEMA CDC_LANDING;

CREATE TABLE IF NOT EXISTS CUSTOMER_CHANGES (
    CUSTOMER_ID       NUMBER(38, 0),
    FIRST_NAME        VARCHAR,
    LAST_NAME         VARCHAR,
    EMAIL             VARCHAR,
    PHONE_NUMBER      VARCHAR,
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS SUPPLIER_CHANGES (
    SUPPLIER_ID       NUMBER(38, 0),
    NAME              VARCHAR,
    CONTACT_NAME      VARCHAR,
    CONTACT_EMAIL     VARCHAR,
    CONTACT_PHONE     VARCHAR,
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS PRODUCT_CHANGES (
    PRODUCT_ID        NUMBER(38, 0),
    NAME              VARCHAR,
    DESCRIPTION       VARCHAR,
    UNIT_PRICE        NUMBER(10, 2),
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS SALES_INVOICE_CHANGES (
    INVOICE_ID        NUMBER(38, 0),
    CUSTOMER_ID       NUMBER(38, 0),
    INVOICE_DATE      TIMESTAMP_NTZ(3),
    TOTAL_SUM         NUMBER(10, 2),
    TAX_AMOUNT        NUMBER(10, 2),
    STATUS            VARCHAR,
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS SALES_INVOICE_ITEM_CHANGES (
    ITEM_ID           NUMBER(38, 0),
    INVOICE_ID        NUMBER(38, 0),
    PRODUCT_ID        NUMBER(38, 0),
    QUANTITY          NUMBER(38, 0),
    UNIT_PRICE        NUMBER(10, 2),
    DISCOUNT          NUMBER(10, 2),
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS PURCHASE_BILL_CHANGES (
    BILL_ID           NUMBER(38, 0),
    SUPPLIER_ID       NUMBER(38, 0),
    BILL_DATE         TIMESTAMP_NTZ(3),
    TOTAL_SUM         NUMBER(10, 2),
    TAX_AMOUNT        NUMBER(10, 2),
    PAID_AMOUNT       NUMBER(10, 2),
    STATUS            VARCHAR,
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS PURCHASE_BILL_ITEM_CHANGES (
    ITEM_ID           NUMBER(38, 0),
    BILL_ID           NUMBER(38, 0),
    PRODUCT_ID        NUMBER(38, 0),
    QUANTITY          NUMBER(38, 0),
    UNIT_PRICE        NUMBER(10, 2),
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

CREATE TABLE IF NOT EXISTS INVENTORY_MOVEMENT_CHANGES (
    MOVEMENT_ID       NUMBER(38, 0),
    PRODUCT_ID        NUMBER(38, 0),
    MOVEMENT_TYPE     VARCHAR,
    QUANTITY          NUMBER(38, 0),
    MOVEMENT_DATE     TIMESTAMP_NTZ(3),
    CREATED_AT        TIMESTAMP_NTZ(3),
    UPDATED_AT        TIMESTAMP_TZ(3),
    __OP              VARCHAR,
    __TABLE           VARCHAR,
    __LSN             NUMBER(38, 0),
    __SOURCE_TS_MS    NUMBER(38, 0),
    __DELETED         VARCHAR,
    RECORD_METADATA   VARIANT
);

-- Required when the Kafka connector is configured for schematization/schema
-- evolution. The table owner executes these statements.
ALTER TABLE CUSTOMER_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE SUPPLIER_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE PRODUCT_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE SALES_INVOICE_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE SALES_INVOICE_ITEM_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE PURCHASE_BILL_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE PURCHASE_BILL_ITEM_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE INVENTORY_MOVEMENT_CHANGES SET ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Optional verification commands.
SHOW TABLES IN SCHEMA CDC_DATABASE.CDC_LANDING;
SHOW GRANTS TO ROLE KAFKA_CONNECTOR_ROLE_1;
