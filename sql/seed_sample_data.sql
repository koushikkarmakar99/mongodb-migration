USE mailtracking;
GO

-- Sample data: 100 mailpieces, 600 delivery_scans rows (6 per mailpiece)
-- Assumes dbo.mailpieces(mailpiece_id identity, cust_id, name, address, imb, statement_gen_date)
-- Assumes dbo.delivery_scans(delivery_scan_id identity, imb, scan_datetime, scan_zipcode, delivery_code, is_returned, is_forwarded, forwarded_address)

-- 1) Insert 100 mailpieces
;WITH nums AS (
    SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects
)
INSERT INTO dbo.mailpieces (cust_id, name, address_line_1, address_line_2, city, state, zip_code, imb, statement_gen_date, print_sla_date, delivery_sla_date)
SELECT
    100000 + n AS cust_id,
    CONCAT('Customer ', n) AS name,
    CONCAT(n, ' Main St') AS address_line_1,
    CASE WHEN n % 2 = 0 THEN CONCAT('Suite ', n % 100) ELSE NULL END AS address_line_2,
    'Springfield' AS city,
    'IL' AS state,
    '62704' + RIGHT(CONCAT('000000', n), 6) AS zip_code,
    -- 31-digit IMB: BarcodeID(2) + ServiceType(3) + MailerID(6) + Serial(9) + Routing(11)
    CONCAT('00700123456', RIGHT(CONCAT('000000000', n), 9), '06202109999') AS imb,
    DATEADD(day, -n, SYSUTCDATETIME()) AS statement_gen_date,
    DATEADD(day, 2, DATEADD(day, -n, SYSUTCDATETIME())) AS print_sla_date,
    DATEADD(day, 7, DATEADD(day, -n, SYSUTCDATETIME())) AS delivery_sla_date
FROM nums;

-- 2) Insert 600 delivery_scans rows (6 per mailpiece)
;WITH mp AS (
    SELECT TOP (100)
        mailpiece_id,
        imb,
        statement_gen_date
    FROM dbo.mailpieces
    ORDER BY mailpiece_id DESC
),
scans AS (
    SELECT v.scan_idx
    FROM (VALUES (1),(2),(3),(4),(5),(6)) AS v(scan_idx)
)
INSERT INTO dbo.delivery_scans (imb, scan_datetime, scan_zipcode, delivery_status, is_returned, is_forwarded, forwarded_address, return_start_date, forward_start_date)
SELECT
    mp.imb,
    DATEADD(hour, s.scan_idx * 4, mp.statement_gen_date) AS scan_datetime,
    '60601' + RIGHT(CONCAT('000000', mp.mailpiece_id), 6) AS scan_zipcode,
    CASE 
        WHEN s.scan_idx < 4 THEN 'IN_TRANSIT'
        WHEN s.scan_idx BETWEEN 4 AND 5 THEN
            CASE 
                WHEN (mp.mailpiece_id % 100) BETWEEN 60 AND 74 THEN 'RETURN_IN_TRANSIT'
                WHEN (mp.mailpiece_id % 100) BETWEEN 75 AND 84 THEN 'RETURN_IN_TRANSIT'
                WHEN (mp.mailpiece_id % 100) BETWEEN 85 AND 94 THEN 'FORWARD_IN_TRANSIT'
                WHEN (mp.mailpiece_id % 100) >= 95 THEN 'FORWARD_IN_TRANSIT'
                ELSE 'IN_TRANSIT'
            END
        ELSE -- Final scan (6)
            CASE 
                WHEN (mp.mailpiece_id % 100) < 50 THEN 'DELIVERED'
                WHEN (mp.mailpiece_id % 100) < 60 THEN 'IN_TRANSIT'
                WHEN (mp.mailpiece_id % 100) < 75 THEN 'RETURN_DELIVERED'
                WHEN (mp.mailpiece_id % 100) < 85 THEN 'RETURN_IN_TRANSIT'
                WHEN (mp.mailpiece_id % 100) < 95 THEN 'FORWARD_DELIVERED'
                ELSE 'FORWARD_IN_TRANSIT'
            END
    END AS delivery_status,
    CASE 
        WHEN (mp.mailpiece_id % 100) BETWEEN 60 AND 84 AND s.scan_idx >= 4 THEN 1
        ELSE 0 
    END AS is_returned,
    CASE 
        WHEN (mp.mailpiece_id % 100) >= 85 AND s.scan_idx >= 4 THEN 1
        ELSE 0 
    END AS is_forwarded,
    CASE 
        WHEN (mp.mailpiece_id % 100) >= 85 AND s.scan_idx >= 4 THEN 
            CONCAT(mp.mailpiece_id + 500, ' Forwarded St', 
                   CASE WHEN mp.mailpiece_id % 2 = 0 THEN CONCAT(', Apt ', mp.mailpiece_id % 10) ELSE '' END,
                   ', Chicago, IL, ',
                   CASE 
                      WHEN mp.mailpiece_id % 3 = 0 THEN '60601' 
                      WHEN mp.mailpiece_id % 3 = 1 THEN '60601-1234' 
                      ELSE '60601123456' 
                   END)
        ELSE NULL 
    END AS forwarded_address,
    CASE 
        WHEN (mp.mailpiece_id % 100) BETWEEN 60 AND 84 AND s.scan_idx >= 4 THEN DATEADD(hour, 4 * 4, mp.statement_gen_date)
        ELSE NULL 
    END AS return_start_date,
    CASE 
        WHEN (mp.mailpiece_id % 100) >= 85 AND s.scan_idx >= 4 THEN DATEADD(hour, 4 * 4, mp.statement_gen_date)
        ELSE NULL 
    END AS forward_start_date
FROM mp
CROSS JOIN scans s;
