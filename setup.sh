#!/bin/bash

# Setup Script for SQL Server to MongoDB Migration (Bash)

# 1. Pre-requisite Folders
echo "--- Checking/Creating Data Folders ---"
mkdir -p mongo-data sqlserver-data plugins

# 2. Load Environment Variables
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "Created .env from .env.example. PLEASE UPDATE THE PASSWORD IN .env AND RE-RUN."
        exit 0
    else
        echo ".env file not found!"
        exit 1
    fi
fi

echo "--- Loading Environment Variables ---"
export $(grep -v '^#' .env | xargs)

if [ "$MSSQL_PASSWORD" == "YourStrong!Passw0rd" ]; then
    echo "Warning: You are still using the default password from .env.example."
fi

# 3. Start Infrastructure
echo "--- Starting Containers ---"
podman compose up -d

# 4. Wait for SQL Server
echo "--- Waiting for SQL Server ---"
SQL_PATH="/opt/mssql-tools18/bin/sqlcmd"
until podman exec mongodb-migration-sqlserver-1 $SQL_PATH -S localhost -U sa -P "$MSSQL_PASSWORD" -C -Q "SELECT 1" &> /dev/null; do
    echo -n "."
    sleep 2
done
echo "SQL Server is up!"

# 5. Initialize Database
echo "--- Initializing Database ---"
SQL_CMD="podman exec -i mongodb-migration-sqlserver-1 $SQL_PATH -S localhost -U sa -P $MSSQL_PASSWORD -C"

cat ./sql/init-db.sql | $SQL_CMD -d master > /dev/null
cat ./sql/schema.sql | $SQL_CMD -d mailtracking > /dev/null
cat ./sql/indexes.sql | $SQL_CMD -d mailtracking > /dev/null

# 6. Wait for Kafka Connect
echo "--- Waiting for Kafka Connect ---"
until curl -s -f http://localhost:8083/connectors > /dev/null; do
    echo -n "."
    sleep 2
done
echo "Kafka Connect is ready!"

# 7. Register Connectors
echo "--- Registering Connectors ---"

register_connector() {
    local file=$1
    local name=$(grep -oP '"name":\s*"\K[^"]+' "$file")
    
    if curl -s -f "http://localhost:8083/connectors/$name" > /dev/null; then
        echo "Connector '$name' already exists. Skipping."
        return
    fi

    echo "Registering '$name'..."
    cat "$file" | sed 's/\${env:/\${/g' | envsubst | curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @-
    echo "Done."
}

register_connector "connectors/sqlserver-jdbc-source-mailpieces.json"
register_connector "connectors/sqlserver-jdbc-source-delivery_scans.json"
register_connector "connectors/mongodb-sink-denormalized.json"

echo -e "\n--- Setup Complete! ---"
echo "Check MongoDB: podman exec -it mongodb-migration-mongodb-1 mongosh mailtracking --eval 'db.mailpieces_with_scans.countDocuments()'"
