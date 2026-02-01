"""
Enterprise Cloud Migration Pipeline
Migrates data from on-prem SQL Server to Azure Synapse Analytics
Implements incremental load with watermark pattern
"""

import pyodbc
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
import logging
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CloudMigrationPipeline:
    """
    Handles migration from legacy SQL Server to Azure Synapse
    Supports full load and incremental (CDC) patterns
    """
    
    def __init__(self):
        self.watermark_file = "migration_watermark.txt"
        self.batch_size = 100000  # Process 100k rows at a time
        
    def connect_onprem_sql(self, server: str, database: str, username: str, password: str):
        """Connect to source SQL Server (on-premise)"""
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password}"
            )
            conn = pyodbc.connect(conn_str)
            logger.info(f"Connected to source: {server}.{database}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to source: {e}")
            raise
    
    def get_watermark(self, table_name: str) -> datetime:
        """Get last successful migration timestamp for incremental load"""
        try:
            with open(self.watermark_file, 'r') as f:
                watermarks = dict(line.strip().split('=') for line in f)
                return datetime.fromisoformat(watermarks.get(table_name, '1900-01-01'))
        except FileNotFoundError:
            return datetime(1900, 1, 1)
    
    def update_watermark(self, table_name: str, new_watermark: datetime):
        """Update watermark after successful migration"""
        try:
            with open(self.watermark_file, 'a+') as f:
                f.write(f"{table_name}={new_watermark.isoformat()}\n")
            logger.info(f"Updated watermark for {table_name}: {new_watermark}")
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")
    
    def extract_data(self, conn, table_name: str, incremental: bool = True) -> pd.DataFrame:
        """
        Extract data from source with optional incremental logic
        Uses ROWVERSION (timestamp) for change data capture
        """
        if incremental:
            watermark = self.get_watermark(table_name)
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE LastModified > ?
            ORDER BY LastModified
            """
            logger.info(f"Incremental load from {table_name} since {watermark}")
            df = pd.read_sql(query, conn, params=[watermark])
        else:
            logger.info(f"Full load from {table_name}")
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
        
        logger.info(f"Extracted {len(df)} rows from {table_name}")
        return df
    
    def transform_for_synapse(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform data for Azure Synapse
        - Standardize column names
        - Handle nulls
        - Optimize data types
        """
        # Convert column names to snake_case (Synapse best practice)
        df.columns = (
            df.columns.str.lower()
            .str.replace(' ', '_')
            .str.replace(r'[^\w]', '', regex=True)
        )
        
        # Optimize data types
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if it's actually a date
                try:
                    df[col] = pd.to_datetime(df[col], errors='ignore')
                except:
                    pass
        
        # Add metadata columns
        df['_ingestion_timestamp'] = datetime.utcnow()
        df['_source_system'] = 'legacy_sql_server'
        df['_migration_batch_id'] = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        
        logger.info(f"Transformed data: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    
    def load_to_adls(self, df: pd.DataFrame, table_name: str, layer: str = 'bronze'):
        """
        Load data to Azure Data Lake (Bronze layer initially)
        Parquet format for optimal performance
        """
        try:
            # In production, use Azure SDK to write to ADLS Gen2
            # Simulating here with local file for demonstration
            file_path = f"{layer}/{table_name}/{datetime.now().strftime('%Y/%m/%d')}/{table_name}.parquet"
            
            # Create directory if not exists
            import os
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Write as Parquet (compressed, columnar format)
            df.to_parquet(file_path, index=False, compression='snappy')
            
            logger.info(f"Loaded {len(df)} rows to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def migrate_table(self, source_config: dict, table_name: str, incremental: bool = True):
        """
        End-to-end migration for a single table
        1. Extract from SQL Server
        2. Transform
        3. Load to Bronze
        4. Update watermark
        """
        logger.info(f"Starting migration for {table_name}")
        
        conn = None
        try:
            # Connect to source
            conn = self.connect_onprem_sql(
                source_config['server'],
                source_config['database'],
                source_config['username'],
                source_config['password']
            )
            
            # Extract
            df = self.extract_data(conn, table_name, incremental)
            
            if len(df) == 0:
                logger.info(f"No new data for {table_name}")
                return
            
            # Transform
            df_transformed = self.transform_for_synapse(df)
            
            # Load to Bronze
            file_path = self.load_to_adls(df_transformed, table_name, 'bronze')
            
            # Update watermark (use max LastModified from source)
            if incremental and 'lastmodified' in df.columns:
                new_watermark = df['lastmodified'].max()
                self.update_watermark(table_name, new_watermark)
            
            logger.info(f"Migration completed for {table_name}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Migration failed for {table_name}: {e}")
            raise
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    # Configuration (in production, use Key Vault)
    source_config = {
        'server': 'onprem-sql-server.company.local',
        'database': 'LegacyDataWarehouse',
        'username': 'migration_user',
        'password': 'secure_password'  # Use Key Vault in production!
    }
    
    pipeline = CloudMigrationPipeline()
    
    # Migrate dimension tables (full load)
    dimensions = ['dim_customer', 'dim_product', 'dim_date']
    for table in dimensions:
        pipeline.migrate_table(source_config, table, incremental=False)
    
    # Migrate fact tables (incremental)
    facts = ['fact_sales', 'fact_inventory']
    for table in facts:
        pipeline.migrate_table(source_config, table, incremental=True)
    
    logger.info("Migration batch completed successfully!")
