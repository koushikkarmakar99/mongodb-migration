# Cleanup Script for SQL Server to MongoDB Migration

Write-Host '--- Stopping and Removing Containers, Networks, and Volumes ---' -ForegroundColor Cyan
podman compose down -v 2>&1 | Where-Object { $_ -notmatch "Executing external compose provider" -and $_ -notmatch "please see podman-compose" }

Write-Host '--- Cleanup Complete ---' -ForegroundColor Green
