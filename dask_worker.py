import dask.dataframe as dd
import time
import polars as pl
from sqlalchemy import create_engine,text
from vault_util import get_db_credentials  # <--- NEW IMPORT

def process_partition(pandas_df, db_conn_str):
    """
    Worker Function: Converts Pandas chunk to Polars and writes to DB.
    """
    if pandas_df.empty:
        return 0

    try:
        # 1. Zero-Copy Convert to Polars
        df = pl.from_pandas(pandas_df)

        # 2. Transform (Example: Clean timestamps & add metrics)
        processed_df = (
            df.lazy()
            .with_columns(
                pl.col("timestamp").str.to_datetime(),
                (pl.col("amount") * 1.1).alias("adjusted_amount")
            )
            .collect()
        )

        # 3. Write to TimescaleDB using the Secure Connection String
        processed_df.write_database(
            table_name="sales_metrics",
            connection=db_conn_str,
            if_table_exists="append",
            engine="sqlalchemy"
        )
        return len(processed_df)

    except Exception as e:
        print(f"⚠️ Worker Error: {e}")
        return 0

async def process_heavy_data(file_path):
    print(f"🔐 Fetching credentials from Vault...")
    
    # 1. GET SECURE CONNECTION STRING
    db_conn_str = await get_db_credentials()
    if not db_conn_str:
        return {"status": "failed", "error": "Could not retrieve DB credentials"}

    print(f"⚡ Dask+Polars: Starting ingestion for {file_path}...")
    
    # 2. Setup Database (Create table if missing)
    try:
        
        print(f"⚡ Starting DB Setup...")
        engine = await create_engine(db_conn_str)
        with engine.connect() as conn:
            print("🛠️ Setting up database...")
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            query="""
                CREATE TABLE IF NOT EXISTS sales_metrics (
                    timestamp TIMESTAMPTZ NOT NULL,
                    category TEXT,
                    region TEXT,
                    amount DOUBLE PRECISION,
                    adjusted_amount DOUBLE PRECISION
                );
            """
            await conn.execute(text(query))
           

            try:
                # Convert to Hypertable (Timescale Magic)
                print("🟡 Kuch to Batawo kya huwa he ?.") 
                await conn.execute("SELECT create_hypertable('sales_metrics', 'timestamp', if_not_exists => TRUE);")
            except:
                print("🟡 Hypertable already exists.") 

    except Exception as e:
        return {"status": "failed", "error": f"DB Setup Error: {str(e)}"}

    # 3. Dask Orchestration
    try:
        print("Started Dask Service Chunking.") 
        ddf = dd.read_csv(file_path)
        
        # Pass the connection string to every worker partition
        # meta handles the return type expectation (an integer count)
        print("Storing Data on Timescale.") 
        result = await ddf.map_partitions(
            process_partition, 
            db_conn_str=db_conn_str, 
            meta=('rows', 'int')
        )
        
        total_rows = result.compute().sum()
        print(f"✅ Dask+Polars: Completed ingestion. Total Rows Processed: {int(total_rows)}")
        return {"status": "success", "rows_processed": int(total_rows)}

    except Exception as e:
        return {"status": "failed", "error": str(e)}