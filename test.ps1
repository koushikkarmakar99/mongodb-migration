# Test and Debug Script for SQL Server to MongoDB Migration

Write-Host "`n--- 1. Container Status Check ---" -ForegroundColor Cyan
podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`n--- 2. Database Connectivity Check ---" -ForegroundColor Cyan
# SQL Server
$sqlCommand = "SELECT count(*) FROM mailtracking.dbo.mailpieces"
$sqlCheck = podman exec mongodb-migration-sqlserver-1 sqlcmd -S localhost -U sa -P "$env:MSSQL_PASSWORD" -C -Q "$sqlCommand" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "SQL Server Connection: OK" -ForegroundColor Green
    $sqlCount = ($sqlCheck | Where-Object { $_ -match '\d+' } | Select-Object -Last 1).Trim()
    if ($null -eq $sqlCount) { $sqlCount = "0 (or no response)" }
    Write-Host "Mailpieces in SQL: $sqlCount"
} else {
    Write-Host "SQL Server Connection: FAILED" -ForegroundColor Red
    Write-Host "Error details: $sqlCheck" -ForegroundColor Gray
}

# MongoDB
$mongoCheck = podman exec mongodb-migration-mongodb-1 mongosh mailtracking --eval "db.mailpieces_with_scans.countDocuments()" --quiet 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "MongoDB Connection: OK" -ForegroundColor Green
    $mongoCount = ($mongoCheck | Where-Object { $_ -match '^\d+$' } | Select-Object -Last 1)
    if ($null -ne $mongoCount) { $mongoCount = $mongoCount.Trim() } else { $mongoCount = "0" }
    Write-Host "Documents in MongoDB: $mongoCount"
} else {
    Write-Host "MongoDB Connection: FAILED" -ForegroundColor Red
    Write-Host "Error details: $mongoCheck" -ForegroundColor Gray
}

Write-Host "`n--- 3. Kafka Connect Connector Status ---" -ForegroundColor Cyan
$connectors = @('sqlserver-mailtracking-source', 'sqlserver-delivery-scans-source', 'mongodb-denormalized-sink')
foreach ($name in $connectors) {
    try {
        $status = Invoke-RestMethod -Uri "http://localhost:8083/connectors/$name/status" -ErrorAction Stop
        $state = $status.connector.state
        $taskStates = $status.tasks | ForEach-Object { $_.state }
        
        $color = if ($state -eq 'RUNNING') { 'Green' } else { 'Yellow' }
        Write-Host "Connector '$name': $state (Tasks: $($taskStates -join ', '))" -ForegroundColor $color
        
        if ($state -ne 'RUNNING' -or ($taskStates -contains 'FAILED')) {
            Write-Host "  DEBUG: Getting failure details for '$name'..." -ForegroundColor Gray
            $status.tasks | Where-Object { $_.state -eq 'FAILED' } | ForEach-Object {
                Write-Host "  Task ID $($_.id): $($_.trace)" -ForegroundColor Red
            }
        }
    } catch {
        Write-Host "Connector '$name': NOT FOUND or Connect API unreachable" -ForegroundColor Red
    }
}

Write-Host "`n--- 4. Kafka Topics Check ---" -ForegroundColor Cyan
podman exec mongodb-migration-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 | Select-String "sqlserver|denormalized"

Write-Host "`n--- 5. ksqlDB Streams/Tables Check ---" -ForegroundColor Cyan
$ksqlCheck = "SHOW STREAMS; SHOW TABLES;" | podman exec -i ksqldb-cli ksql http://ksqldb-server:8088 2>$null
$ksqlCheck | Select-String "MAILPIECES|SCANS|DENORMALIZED"

Write-Host "`n--- Test Complete ---" -ForegroundColor Green
