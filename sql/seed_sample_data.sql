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
    CASE WHEN n % 3 = 0 THEN '62704-1234' ELSE '62704' END AS zip_code,
    -- 31-digit IMB: BarcodeID(2) + ServiceType(3) + MailerID(6) + Serial(9) + Routing(11)
    CONCAT('00700123456', RIGHT(CONCAT('000000000', n), 9), '06202109999') AS imb,
    DATEADD(day, -n, SYSUTCDATETIME()) AS statement_gen_date,
    DATEADD(hour, 2, DATEADD(day, -n, SYSUTCDATETIME())) AS print_sla_date,
    DATEADD(day, 2, DATEADD(day, -n, SYSUTCDATETIME())) AS delivery_sla_date
FROM nums;

-- 2) Insert delivery_scans rows
-- Distribution: 70% Delivered (3-7 scans), 15% Returned (5-9 scans), 10% Forwarded (5-9 scans), 5% No scans
;WITH mp AS (
    SELECT 
        mailpiece_id,
        imb,
        statement_gen_date,
        (ROW_NUMBER() OVER (ORDER BY mailpiece_id) - 1) % 100 AS piece_idx,
        CASE 
            WHEN (ROW_NUMBER() OVER (ORDER BY mailpiece_id) - 1) % 100 < 70 THEN 3 + (mailpiece_id % 5)
            WHEN (ROW_NUMBER() OVER (ORDER BY mailpiece_id) - 1) % 100 < 95 THEN 5 + (mailpiece_id % 5)
            ELSE 0 
        END AS max_scans
    FROM dbo.mailpieces
),
scans AS (
    SELECT v.scan_idx
    FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9)) AS v(scan_idx)
)
INSERT INTO dbo.delivery_scans (imb, scan_datetime, scan_zipcode, delivery_status, is_returned, is_forwarded, forwarded_address, return_start_date, forward_start_date)
SELECT
    mp.imb,
    DATEADD(hour, s.scan_idx * 4, mp.statement_gen_date) AS scan_datetime,
    RIGHT('00000' + CAST((10000 + (mp.mailpiece_id % 90000)) AS varchar(5)), 5) AS scan_zipcode,
    CASE 
        WHEN s.scan_idx = mp.max_scans THEN 
            CASE 
                WHEN mp.piece_idx >= 70 AND mp.piece_idx < 85 THEN 'RETURN_DELIVERED'
                WHEN mp.piece_idx >= 85 AND mp.piece_idx < 95 THEN 'FORWARD_DELIVERED'
                ELSE 'DELIVERED'
            END
        WHEN s.scan_idx = (mp.max_scans - 1) THEN 
             CASE 
                WHEN mp.piece_idx >= 70 AND mp.piece_idx < 85 THEN 'RETURN_IN_TRANSIT'
                WHEN mp.piece_idx >= 85 AND mp.piece_idx < 95 THEN 'FORWARD_IN_TRANSIT'
                ELSE 'IN_TRANSIT'
            END
        ELSE 'IN_TRANSIT'
    END AS delivery_status,
    CASE WHEN s.scan_idx = mp.max_scans AND mp.piece_idx >= 70 AND mp.piece_idx < 85 THEN 1 ELSE 0 END AS is_returned,
    CASE WHEN s.scan_idx >= 4 AND mp.piece_idx >= 85 AND mp.piece_idx < 95 THEN 1 ELSE 0 END AS is_forwarded,
    CASE WHEN s.scan_idx >= 4 AND mp.piece_idx >= 85 AND mp.piece_idx < 95 THEN CONCAT('Forwarded ', mp.mailpiece_id, ' Oak Ave') ELSE NULL END AS forwarded_address,
    CASE WHEN mp.piece_idx >= 70 AND mp.piece_idx < 85 THEN DATEADD(hour, 1, mp.statement_gen_date) ELSE NULL END AS return_start_date,
    CASE WHEN mp.piece_idx >= 85 AND mp.piece_idx < 95 THEN DATEADD(hour, 1, mp.statement_gen_date) ELSE NULL END AS forward_start_date
FROM mp
CROSS JOIN scans s
WHERE s.scan_idx <= mp.max_scans;
