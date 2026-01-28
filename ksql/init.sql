SET 'auto.offset.reset' = 'earliest';

-- 1. Stream for Mailpieces
CREATE STREAM IF NOT EXISTS mailpieces_src (
    "mailpiece_id" INT,
    "cust_id" INT,
    "name" STRING,
    "address" STRING,
    "imb" STRING,
    "statement_gen_date" BIGINT
) WITH (
    KAFKA_TOPIC='sqlserver-mailpieces',
    VALUE_FORMAT='JSON',
    PARTITIONS=1
);

-- 2. Stream for Delivery Scans
CREATE STREAM IF NOT EXISTS scans_src (
    "delivery_scan_id" INT,
    "imb" STRING,
    "scan_datetime" BIGINT,
    "scan_zipcode" STRING,
    "delivery_code" INT,
    "is_returned" INT,
    "is_forwarded" INT,
    "forwarded_address" STRING
) WITH (
    KAFKA_TOPIC='sqlserver-delivery_scans',
    VALUE_FORMAT='JSON',
    PARTITIONS=1
);

-- 3. Aggregate Scans by IMB
CREATE TABLE IF NOT EXISTS scans_agg_table AS
    SELECT 
        "imb", 
        COLLECT_LIST(STRUCT(
            "delivery_scan_id" := "delivery_scan_id",
            "scan_datetime" := "scan_datetime",
            "scan_zipcode" := "scan_zipcode",
            "delivery_code" := "delivery_code",
            "is_returned" := CASE WHEN "is_returned" = 1 THEN TRUE ELSE FALSE END,
            "is_forwarded" := CASE WHEN "is_forwarded" = 1 THEN TRUE ELSE FALSE END,
            "forwarded_address" := "forwarded_address"
        )) AS "scans"
    FROM scans_src
    GROUP BY "imb"
    EMIT CHANGES;

-- 4. Create a Table for Mailpieces to allow Joining
CREATE TABLE IF NOT EXISTS mailpieces_table AS
    SELECT 
        "imb",
        LATEST_BY_OFFSET("mailpiece_id") AS "mailpiece_id",
        LATEST_BY_OFFSET("cust_id") AS "cust_id",
        LATEST_BY_OFFSET("name") AS "name",
        LATEST_BY_OFFSET("address") AS "address",
        LATEST_BY_OFFSET("statement_gen_date") AS "statement_gen_date"
    FROM mailpieces_src
    GROUP BY "imb";

-- 5. Final Denormalized Table with LEFT JOIN
CREATE TABLE IF NOT EXISTS denormalized_mailpieces WITH (KAFKA_TOPIC='denormalized_mailpieces') AS
    SELECT 
        m."imb" AS "imb_key",
        AS_VALUE(m."imb") AS "imb",
        m."mailpiece_id",
        m."cust_id",
        m."name",
        m."address",
        m."statement_gen_date",
        s."scans"
    FROM mailpieces_table m
    LEFT JOIN scans_agg_table s ON m."imb" = s."imb"
    EMIT CHANGES;