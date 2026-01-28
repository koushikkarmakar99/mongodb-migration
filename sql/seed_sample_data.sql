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
INSERT INTO dbo.mailpieces (cust_id, name, address, imb, statement_gen_date)
SELECT
    100000 + n AS cust_id,
    CONCAT('Customer ', n) AS name,
    CONCAT(n, ' Main St', 
           CASE WHEN n % 2 = 0 THEN CONCAT(', Suite ', n % 100) ELSE '' END, 
           ', Springfield, IL, 62704',
           CASE WHEN n % 3 = 0 THEN '-1234' ELSE '' END,
           CASE WHEN n % 5 = 0 THEN '55' ELSE '' END) AS address,
    -- 31-digit IMB: BarcodeID(2) + ServiceType(3) + MailerID(6) + Serial(9) + Routing(11)
    CONCAT('00700123456', RIGHT(CONCAT('000000000', n), 9), '06202109999') AS imb,
    DATEADD(day, -n, SYSUTCDATETIME()) AS statement_gen_date
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
INSERT INTO dbo.delivery_scans (imb, scan_datetime, scan_zipcode, delivery_code, is_returned, is_forwarded, forwarded_address)
SELECT
    mp.imb,
    DATEADD(hour, s.scan_idx * 4, mp.statement_gen_date) AS scan_datetime,
    RIGHT('00000' + CAST((10000 + (mp.mailpiece_id % 90000)) AS varchar(5)), 5) AS scan_zipcode,
    10 + s.scan_idx AS delivery_code,
    CASE WHEN s.scan_idx = 6 AND (mp.mailpiece_id % 10 = 0) THEN 1 ELSE 0 END AS is_returned,
    CASE WHEN s.scan_idx = 4 AND (mp.mailpiece_id % 15 = 0) THEN 1 ELSE 0 END AS is_forwarded,
    CASE WHEN s.scan_idx = 4 AND (mp.mailpiece_id % 15 = 0) THEN CONCAT('Forwarded ', mp.mailpiece_id, ' Oak Ave') ELSE NULL END AS forwarded_address
FROM mp
CROSS JOIN scans s;
