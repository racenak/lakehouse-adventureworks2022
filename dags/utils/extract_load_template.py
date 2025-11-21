from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, max as spark_max, to_timestamp
from pyspark.sql.types import TimestampType
from typing import Dict, Optional
from datetime import datetime
import json
from pyspark.sql import DataFrame


class ExtractLoadTemplate:
    """
    Reusable template for extracting data from source systems 
    and loading to Delta Lake Bronze layer
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    def extract_from_jdbc(
        self,
        source_table: str,
        primary_key: str,
        table_lakehouse: str,
        jdbc_url: str = "jdbc:sqlserver://source-mssql:1433;databaseName=AdventureWorks2022;encrypt=false",
        jdbc_user: str = "sa",
        jdbc_password: str = "YourStrong!Passw0rd",
        jdbc_driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        event_column: str = "ModifiedDate",
        checkpoint_path: str = "s3a://lake/checkpoints"
    ) -> Dict:
        """
        Generic JDBC extraction with incremental loading support using checkpoint files
        
        Args:
            jdbc_url: JDBC connection URL
            jdbc_user: Database username
            jdbc_password: Database password
            jdbc_driver: JDBC driver class name
            source_table: Source table name (e.g., dbo.users)
            primary_key: Primary key column name
            table_lakehouse: Target table name in lakehouse
            event_column: Column used for incremental loading
            checkpoint_path: Base path for checkpoint files (e.g., s3a://lake/checkpoints)
        
        Returns:
            Dictionary with extraction statistics
        """
        
        jdbc_props = {
            "user": jdbc_user,
            "password": jdbc_password,
            "driver": jdbc_driver,
            "useUnicode": "true",
            "characterEncoding": "UTF-8"
        }
        
        print(f"🚀 Starting extraction: {source_table} → bronze.{table_lakehouse}")
        
        # Get last watermark from checkpoint file (Spark writes JSON as directories)
        checkpoint_dir = f"{checkpoint_path}/{table_lakehouse}/checkpoint"
        last_event_time = self._get_checkpoint(checkpoint_dir)
        
        # Perform extraction
        if last_event_time is None:
            print("🔄 Full load")
            df = self._full_load(jdbc_url, source_table, jdbc_props)
        else:
            print(f"🔄 Incremental load since {last_event_time}")
            df = self._incremental_load(
                jdbc_url, source_table, event_column, 
                last_event_time, jdbc_props
            )
        
        df = df.withColumn(event_column, col(event_column).cast(TimestampType()))

        # Load to Bronze if data exists
        if df.count() > 0:
            record_count = self._load_to_bronze(df, table_lakehouse)
            self._save_checkpoint(
                df, checkpoint_dir, table_lakehouse, 
                primary_key, event_column
            )
            
            return {
                "table_name": table_lakehouse,
                "status": "success",
                "records_ingested": record_count,
                "load_type": "full" if last_event_time is None else "incremental",
                "timestamp": datetime.now().isoformat()
            }
        else:
            print("ℹ️ No new data to ingest")
            return {
                "table_name": table_lakehouse,
                "status": "no_data",
                "records_ingested": 0,
                "load_type": "incremental",
                "timestamp": datetime.now().isoformat()
            }
    
    def _get_checkpoint(self, checkpoint_dir: str) -> Optional[str]:
        """Get last watermark from checkpoint directory (Spark writes JSON as directories)"""
        try:
            # Read checkpoint directory as JSON using Spark
            checkpoint_df = self.spark.read.json(checkpoint_dir)
            
            if checkpoint_df.count() > 0:
                checkpoint_data = checkpoint_df.collect()[0].asDict()
                last_event_time = checkpoint_data.get("last_event_time")
                print(f"📖 Loaded checkpoint: last_event_time = {last_event_time}")
                return last_event_time
            else:
                print("ℹ️ No checkpoint found, starting full load")
                return None
        except Exception as e:
            # Directory doesn't exist or error reading - start fresh
            print(f"ℹ️ No checkpoint found (or error reading): {e}")
            return None
    
    def _full_load(self, jdbc_url: str, table: str, props: Dict):
        """Perform full data load"""
        query = f"(SELECT * FROM {table}) AS src"
        return self.spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    
    def _incremental_load(
        self, jdbc_url: str, table: str, 
        event_col: str, last_time: str, props: Dict
    ):
        """Perform incremental data load with properly formatted timestamp for SQL Server"""
        # Parse and format the timestamp string for SQL Server compatibility
        # SQL Server DATETIME2 can handle microseconds, but we need proper format
        try:
            if last_time:
                # Handle timestamp with microseconds (format: "2014-02-08 10:17:21.587000")
                if '.' in last_time:
                    # Split date/time and microseconds
                    date_part, microsecond_part = last_time.split('.', 1)
                    # Parse the main datetime part
                    dt = datetime.strptime(date_part, "%Y-%m-%d %H:%M:%S")
                    # Format with microseconds (limit to 3 digits for SQL Server compatibility)
                    # SQL Server DATETIME2 supports up to 7 digits, but we'll use 3 for compatibility
                    microseconds = microsecond_part[:3] if len(microsecond_part) >= 3 else microsecond_part.ljust(3, '0')
                    formatted_time = f"{dt.strftime('%Y-%m-%d %H:%M:%S')}.{microseconds}"
                else:
                    # No microseconds, just format the datetime
                    dt = datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                formatted_time = last_time
        except Exception as e:
            # If parsing fails, use the original string and let SQL Server CAST handle it
            print(f"⚠️ Warning: Could not parse timestamp '{last_time}', using as-is: {e}")
            formatted_time = last_time
        
        # Use CAST with DATETIME2 to handle microseconds properly
        # DATETIME2 is more flexible than DATETIME for modern SQL Server
        query = f"(SELECT * FROM {table} WHERE {event_col} > CAST('{formatted_time}' AS DATETIME2)) AS src"
        return self.spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    
    def _load_to_bronze(self, df, table_name: str) -> int:
        """Load data to Bronze layer with metadata"""
        bronze_table = f"bronze.{table_name}"
        
        # Count before adding metadata columns
        record_count = df.count()
        
        df_with_metadata = df \
            .withColumn("ingestion_date", to_date(current_timestamp()))
        
        df_with_metadata.write.format("delta") \
            .mode("append") \
            .partitionBy("ingestion_date") \
            .option("mergeSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✅ Loaded {record_count} records to {bronze_table}")
        
        return record_count
    
    def _save_checkpoint(
        self, df: DataFrame, checkpoint_dir: str, table_name: str, 
        primary_key: str, event_col: str
    ):
        """Save checkpoint directory with watermark information (Spark writes JSON as directories)"""
        # Add metadata columns first before aggregation
        df_with_metadata = df \
            .withColumn("ingestion_date", to_date(current_timestamp()))
        
        # Get max values
        max_values = df_with_metadata.select(
            spark_max(primary_key).alias("last_id"),
            spark_max(event_col).cast("timestamp").alias("last_event_time"),
            spark_max("ingestion_date").alias("last_ingestion_date")
        ).collect()[0]
        
        # Create checkpoint data as a single-row DataFrame
        checkpoint_data = {
            "table_name": table_name,
            "last_id": str(max_values["last_id"]) if max_values["last_id"] is not None else None,
            "last_event_time": str(max_values["last_event_time"]) if max_values["last_event_time"] is not None else None,
            "last_ingestion_date": str(max_values["last_ingestion_date"]) if max_values["last_ingestion_date"] is not None else None,
            "processed_at": datetime.now().isoformat()
        }
        
        # Create DataFrame from checkpoint data and write as JSON directory
        # Spark will create a directory with part files, which is fine for reading
        checkpoint_df = self.spark.createDataFrame([checkpoint_data])
        checkpoint_df.coalesce(1).write.mode("overwrite").json(checkpoint_dir)
        
        print(f"💾 Saved checkpoint for '{table_name}': {checkpoint_data['last_event_time']}")