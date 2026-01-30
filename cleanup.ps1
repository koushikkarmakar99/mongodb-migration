# Cleanup Script for SQL Server to MongoDB Migration

Write-Host '--- Stopping and Removing Containers, Networks, and Volumes ---' -ForegroundColor Cyan
podman compose down -v

Write-Host '--- Cleanup Complete ---' -ForegroundColor Green
