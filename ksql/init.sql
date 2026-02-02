SET 'auto.offset.reset' = 'earliest';

-- 1. Stream for Mailpieces
CREATE STREAM IF NOT EXISTS mailpieces_src (
    "mailpiece_id" INT,
    "cust_id" INT,
    "name" STRING,
    "address_line_1" STRING,
    "address_line_2" STRING,
    "city" STRING,
    "state" STRING,
    "zip_code" STRING,
    "imb" STRING,
    "statement_gen_date" BIGINT,
    "print_sla_date" BIGINT,
    "delivery_sla_date" BIGINT
)
WITH (
        KAFKA_TOPIC = 'sqlserver-mailpieces',
        VALUE_FORMAT = 'JSON',
        PARTITIONS = 1
    );

-- 2. Stream for Delivery Scans
CREATE STREAM IF NOT EXISTS scans_src (
    "delivery_scan_id" INT,
    "imb" STRING,
    "scan_datetime" BIGINT,
    "scan_zipcode" STRING,
    "delivery_status" STRING,
    "is_returned" INT,
    "is_forwarded" INT,
    "forwarded_address" STRING,
    "return_start_date" BIGINT,
    "forward_start_date" BIGINT
)
WITH (
        KAFKA_TOPIC = 'sqlserver-delivery_scans',
        VALUE_FORMAT = 'JSON',
        PARTITIONS = 1
    );

-- 3. Aggregate Scans by IMB
CREATE TABLE IF NOT EXISTS scans_agg_table AS
SELECT
    "imb",
    LATEST_BY_OFFSET ("delivery_status") AS "delivery_status",
    MAX("is_returned") AS "is_returned",
    MAX("is_forwarded") AS "is_forwarded",
    LATEST_BY_OFFSET ("forwarded_address") AS "forwarded_address",
    MAX("return_start_date") AS "return_start_date",
    MAX("forward_start_date") AS "forward_start_date",
    COLLECT_LIST (
        STRUCT (
            "delivery_scan_id" := "delivery_scan_id",
            -- Use FROM_UNIXTIME to convert BIGINT to TIMESTAMP
            "scan_datetime" := "scan_datetime",
            "scan_zipcode" := "scan_zipcode",
            "delivery_status" := "delivery_status",
            "is_returned" := CASE
                WHEN "is_returned" = 1 THEN TRUE
                ELSE FALSE
            END,
            "is_forwarded" := CASE
                WHEN "is_forwarded" = 1 THEN TRUE
                ELSE FALSE
            END,
            "forwarded_address" := "forwarded_address",
            "return_start_date" := "return_start_date",
            "forward_start_date" := "forward_start_date"
        )
    ) AS "scans"
FROM scans_src
GROUP BY
    "imb" EMIT CHANGES;

-- 4. Create a Table for Mailpieces to allow Joining
CREATE TABLE IF NOT EXISTS mailpieces_table AS
SELECT
    "imb",
    LATEST_BY_OFFSET ("mailpiece_id") AS "mailpiece_id",
    LATEST_BY_OFFSET ("cust_id") AS "cust_id",
    LATEST_BY_OFFSET ("name") AS "name",
    LATEST_BY_OFFSET ("address_line_1") AS "address_line_1",
    LATEST_BY_OFFSET ("address_line_2") AS "address_line_2",
    LATEST_BY_OFFSET ("city") AS "city",
    LATEST_BY_OFFSET ("state") AS "state",
    LATEST_BY_OFFSET ("zip_code") AS "zip_code",
    LATEST_BY_OFFSET ("statement_gen_date") AS "statement_gen_date",
    LATEST_BY_OFFSET ("print_sla_date") AS "print_sla_date",
    LATEST_BY_OFFSET ("delivery_sla_date") AS "delivery_sla_date"
FROM mailpieces_src
GROUP BY
    "imb";

-- 5. Final Denormalized Table with LEFT JOIN
CREATE TABLE IF NOT EXISTS denormalized_mailpieces
WITH (
        KAFKA_TOPIC = 'denormalized_mailpieces'
    ) AS
SELECT
    m."imb" AS "imb_key",
    AS_VALUE (m."imb") AS "imb",
    m."mailpiece_id",
    m."cust_id",
    m."name",
    STRUCT (
        "address_line_1" := m."address_line_1",
        "address_line_2" := m."address_line_2",
        "city" := m."city",
        "state" := m."state",
        "zip_code" := m."zip_code"
    ) AS "address",
    m."statement_gen_date",
    m."print_sla_date",
    m."delivery_sla_date",
    s."scans"
FROM
    mailpieces_table m
    LEFT JOIN scans_agg_table s ON m."imb" = s."imb" EMIT CHANGES;