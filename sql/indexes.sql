USE mailtracking;
GO

-- SQL Server indexes to support Kafka incremental queries
CREATE INDEX IX_mailpieces_statement_gen_date
ON dbo.mailpieces(statement_gen_date, mailpiece_id);

CREATE INDEX IX_delivery_scans_scan_datetime
ON dbo.delivery_scans(scan_datetime, delivery_scan_id);
