#!/bin/bash

# Test and Debug Script for SQL Server to MongoDB Migration (Bash)

# Load Environment Variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Colors for output
CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

echo -e "\n${CYAN}--- 1. Container Status Check ---${NC}"
podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n${CYAN}--- 2. Database Connectivity Check ---${NC}"

# SQL Server Check
echo -n "SQL Server Connection: "
SQL_COMMAND="SELECT count(*) FROM mailtracking.dbo.mailpieces"
SQL_CHECK=$(podman exec mongodb-migration-sqlserver-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_PASSWORD" -C -Q "$SQL_COMMAND" 2>/dev/null)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}OK${NC}"
    # Extract the number from sqlcmd output
    SQL_COUNT=$(echo "$SQL_CHECK" | grep -E '^[0-9]+' | tail -n 1 | xargs)
    echo "Mailpieces in SQL: ${SQL_COUNT:-0}"
else
    echo -e "${RED}FAILED${NC}"
    echo -e "${GRAY}Check if SQL Server is still initializing or password is correct.${NC}"
fi

# MongoDB Check
echo -n "MongoDB Connection: "
MONGO_CHECK=$(podman exec mongodb-migration-mongodb-1 mongosh mailtracking --eval "db.mailpieces_with_scans.countDocuments()" --quiet 2>/dev/null)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}OK${NC}"
    MONGO_COUNT=$(echo "$MONGO_CHECK" | tr -d '\r\n')
    echo "Documents in MongoDB: ${MONGO_COUNT:-0}"
else
    echo -e "${RED}FAILED${NC}"
fi

echo -e "\n${CYAN}--- 3. Kafka Connect Connector Status ---${NC}"
CONNECTORS=("sqlserver-mailtracking-source" "sqlserver-delivery-scans-source" "mongodb-denormalized-sink")

for name in "${CONNECTORS[@]}"; do
    STATUS_JSON=$(curl -s "http://localhost:8083/connectors/$name/status")
    if [ -z "$STATUS_JSON" ] || [[ "$STATUS_JSON" == *"Connector not found"* ]]; then
        echo -e "Connector '$name': ${RED}NOT FOUND or Connect API unreachable${NC}"
        continue
    fi

    STATE=$(echo "$STATUS_JSON" | jq -r '.connector.state // "UNKNOWN"')
    TASK_STATES=$(echo "$STATUS_JSON" | jq -r '.tasks[].state' 2>/dev/null | tr '\n' ',' | sed 's/,$//')
    
    COLOR=$YELLOW
    if [ "$STATE" == "RUNNING" ]; then COLOR=$GREEN; fi
    
    echo -e "Connector '$name': ${COLOR}${STATE}${NC} (Tasks: ${TASK_STATES:-NONE})"
    
    if [ "$STATE" != "RUNNING" ] || [[ "$TASK_STATES" == *"FAILED"* ]]; then
        echo -e "${GRAY}  DEBUG: Getting failure details for '$name'...${NC}"
        # Print up to 5 lines of the error trace
        echo "$STATUS_JSON" | jq -r '.tasks[] | select(.state=="FAILED") | "  Task ID \(.id): \(.trace)"' | head -n 5 | sed 's/^/  /' | echo -e "${RED}$(cat)${NC}"
    fi
done

echo -e "\n${CYAN}--- 4. Kafka Topics Check ---${NC}"
podman exec mongodb-migration-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -E "sqlserver|denormalized" || echo "No relevant topics found."

echo -e "\n${CYAN}--- 5. ksqlDB Streams/Tables Check ---${NC}"
KSQL_CHECK=$(echo "SHOW STREAMS; SHOW TABLES;" | podman exec -i ksqldb-cli ksql http://ksqldb-server:8088 2>/dev/null)
echo "$KSQL_CHECK" | grep -E "MAILPIECES|SCANS|DENORMALIZED" || echo "No relevant streams/tables found."

echo -e "\n${GREEN}--- Test Complete ---${NC}"