USE mailtracking;
GO

-- Schema for sample data
IF OBJECT_ID('dbo.mailpieces', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.mailpieces (
        mailpiece_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        cust_id INT NOT NULL,
        name NVARCHAR(100) NOT NULL,
        address NVARCHAR(200) NOT NULL,
        imb VARCHAR(50) NOT NULL,
        statement_gen_date DATETIME2(0) NOT NULL
    );
END;

IF OBJECT_ID('dbo.delivery_scans', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.delivery_scans (
        delivery_scan_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        imb VARCHAR(50) NOT NULL,
        scan_datetime DATETIME2(0) NOT NULL,
        scan_zipcode VARCHAR(10) NOT NULL,
        delivery_code INT NOT NULL,
        is_returned BIT NOT NULL,
        is_forwarded BIT NOT NULL,
        forwarded_address NVARCHAR(200) NULL
    );
END;
