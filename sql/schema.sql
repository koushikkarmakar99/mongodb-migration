USE mailtracking;
GO

-- Schema for sample data
IF OBJECT_ID('dbo.mailpieces', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.mailpieces (
        mailpiece_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        cust_id INT NOT NULL,
        name NVARCHAR(100) NOT NULL,
        address_line_1 NVARCHAR(100) NOT NULL,
        address_line_2 NVARCHAR(100) NULL,
        city NVARCHAR(50) NOT NULL,
        state NVARCHAR(2) NOT NULL,
        zip_code VARCHAR(11) NOT NULL,
        imb VARCHAR(31) NOT NULL UNIQUE,
        statement_gen_date DATETIME2(0) NOT NULL,
        print_sla_date DATETIME2(0) NOT NULL,
        delivery_sla_date DATETIME2(0) NOT NULL
    );
END;

IF OBJECT_ID('dbo.delivery_scans', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.delivery_scans (
        delivery_scan_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        imb VARCHAR(31) NOT NULL FOREIGN KEY REFERENCES dbo.mailpieces(imb),
        scan_datetime DATETIME2(0) NOT NULL,
        scan_zipcode VARCHAR(11) NOT NULL,
        delivery_status VARCHAR(20) NOT NULL,
        is_returned BIT NOT NULL,
        is_forwarded BIT NOT NULL,
        forwarded_address NVARCHAR(200) NULL,
        return_start_date DATETIME2(0) NULL,
        forward_start_date DATETIME2(0) NULL
    );
END;
