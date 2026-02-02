# Setup Script for SQL Server to MongoDB Migration

# 1. Pre-requisite Folders
Write-Host '--- Checking/Creating Data Folders ---' -ForegroundColor Cyan
$folders = @('mongo-data', 'sqlserver-data', 'plugins')
foreach ($folder in $folders) {
    if (-not (Test-Path $folder)) {
        New-Item -ItemType Directory -Path $folder | Out-Null
        Write-Host "Created folder: $folder" -ForegroundColor Gray
    }
}

# 2. Load Environment Variables
if (-not (Test-Path .env)) {
    if (Test-Path .env.example) {
        Copy-Item .env.example .env
        Write-Host 'Created .env from .env.example. PLEASE UPDATE THE PASSWORD IN .env AND RE-RUN.' -ForegroundColor Yellow
        exit
    } else {
        Write-Error '.env file not found!'
        exit 1
    }
}

Write-Host '--- Loading Environment Variables ---' -ForegroundColor Cyan
Get-Content .env | Foreach-Object {
    if ($_ -match '^(?<name>[^#=]+)=(?<value>.*)$') {
        $name = $Matches['name'].Trim()
        $value = $Matches['value'].Trim()
        $value = $value -replace "^['""]|['""]$", ""
        [System.Environment]::SetEnvironmentVariable($name, $value, 'Process')
    }
}

# 3. Start Infrastructure
Write-Host '--- Starting Containers ---' -ForegroundColor Cyan
podman compose up -d

# 4. Wait for SQL Server Service
Write-Host '--- Waiting for SQL Server Service ---' -ForegroundColor Cyan
$maxRetries = 30
$sqlReady = $false
for ($retry = 1; $retry -le $maxRetries; $retry++) {
    podman exec mongodb-migration-sqlserver-1 sqlcmd -S localhost -U sa -P "$env:MSSQL_PASSWORD" -C -Q 'SELECT 1' 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host 'SQL Server is ready!' -ForegroundColor Green
        $sqlReady = $true
        break
    }
    Write-Host '.' -NoNewline
    Start-Sleep -Seconds 3
}

if (-not $sqlReady) {
    Write-Error 'SQL Server failed to become ready.'
    exit 1
}

# 5. Initialize Database
Write-Host '--- Initializing Database ---' -ForegroundColor Cyan

Write-Host 'Creating Database...'
Get-Content -Raw .\sql\init-db.sql | podman exec -i mongodb-migration-sqlserver-1 sqlcmd -S localhost -U sa -P "$env:MSSQL_PASSWORD" -C -d master

Write-Host 'Applying Schema...'
Get-Content -Raw .\sql\schema.sql | podman exec -i mongodb-migration-sqlserver-1 sqlcmd -S localhost -U sa -P "$env:MSSQL_PASSWORD" -C -d mailtracking

Write-Host 'Creating Indexes...'
Get-Content -Raw .\sql\indexes.sql | podman exec -i mongodb-migration-sqlserver-1 sqlcmd -S localhost -U sa -P "$env:MSSQL_PASSWORD" -C -d mailtracking

# 6. Wait for Kafka Connect API
Write-Host '--- Waiting for Kafka Connect ---' -ForegroundColor Cyan
$connectReady = $false
for ($retry = 1; $retry -le $maxRetries; $retry++) {
    try {
        $response = Invoke-RestMethod -Uri 'http://localhost:8083/connectors' -ErrorAction SilentlyContinue
        if ($null -ne $response) {
            Write-Host 'Kafka Connect is ready!' -ForegroundColor Green
            $connectReady = $true
            break
        }
    } catch { }
    Write-Host '.' -NoNewline
    Start-Sleep -Seconds 5
}

if (-not $connectReady) {
    Write-Error 'Kafka Connect failed to start.'
    exit 1
}

# 7. Register Connectors
Write-Host '--- Registering Connectors ---' -ForegroundColor Cyan

function Register-Connector {
    param([string]$filePath)
    $connectorJson = Get-Content $filePath | ConvertFrom-Json
    $connectorName = $connectorJson.name
    
    try {
        $existingConnectors = Invoke-RestMethod -Uri 'http://localhost:8083/connectors' -ErrorAction SilentlyContinue
        if ($existingConnectors -contains $connectorName) {
            Write-Host "Connector '$connectorName' already exists. Skipping." -ForegroundColor Yellow
            return
        }
    } catch { }

    Write-Host "Registering '$connectorName'..."
    $configContent = Get-Content $filePath -Raw
    $parsedConfig = $configContent.Replace('${env:MSSQL_PASSWORD}', $env:MSSQL_PASSWORD)
    
    try {
        Invoke-RestMethod -Uri 'http://localhost:8083/connectors' -Method Post -ContentType 'application/json' -Body $parsedConfig -ErrorAction Stop | Out-Null
        Write-Host 'Success!' -ForegroundColor Green
    } catch {
        Write-Error "Failed: $connectorName"
    }
}

Register-Connector 'connectors/sqlserver-jdbc-source-mailpieces.json'
Register-Connector 'connectors/sqlserver-jdbc-source-delivery_scans.json'
Register-Connector 'connectors/mongodb-sink-denormalized.json'

Write-Host "`n--- Setup Complete! ---" -ForegroundColor Green
Write-Host "Insert test data into SQL Server to begin migration." -ForegroundColor Green
Write-Host "Access MongoDB at mongodb://localhost:27017" -ForegroundColor Green
Write-Host "Access KSQLDB at http://localhost:8088" -ForegroundColor Green
# End of Script