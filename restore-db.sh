#!/bin/bash
echo "Restoring AdventureWorks2022 database..."

# Chờ SQL Server khởi động
sleep 20

# Kiểm tra nếu database đã tồn tại
DB_EXIST=$(/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "YourStrong!Passw0rd" -C -Q "
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'AdventureWorks2022') 
    PRINT 'EXISTS' 
ELSE 
    PRINT 'NOT EXISTS'" -h -1 -W)

if [[ "$DB_EXIST" == "EXISTS" ]]; then
    echo "Database AdventureWorks2022 already exists. Skipping restore."
else
    echo "Restoring AdventureWorks2022 database..."

    /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "YourStrong!Passw0rd" -C -Q "
    USE master;
    RESTORE DATABASE AdventureWorks2022
    FROM DISK = '/var/opt/mssql/backup/AdventureWorks2022.bak'
    WITH
        MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022_Data.mdf',
        MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022_log.ldf',
        FILE = 1,
        NOUNLOAD,
        REPLACE,
        RECOVERY,
        STATS = 5;"

    echo "Database AdventureWorks2022 restored successfully!"
fi
