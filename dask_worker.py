import polars as pl
from sqlalchemy import create_engine, text
from vault_util import get_db_credentials 
import traceback  # <--- Essential for debugging
import time

def process_partition(pandas_df, db_conn_str):
    """
    Worker Function: Converts Pandas chunk to Polars and writes to DB.
    """
    if pandas_df.empty:
        return 0

    try:
        # 1. Zero-Copy Convert to Polars
        df = pl.from_pandas(pandas_df)
        # 2. Transform 
        processed_df = (
            df.lazy()
            .with_columns(
                pl.col("timestamp").str.to_datetime(),
                (pl.col("amount") * 1.1).alias("adjusted_amount")                
            )
            .collect()
        )

        print(f"Writing {processed_df.height} rows to DB...")

        # 3. Write to TimescaleDB 
        # Polars handles the connection internally via SQLAlchemy or connectorx
        processed_df.write_database(
            table_name="sales_metrics",
            connection=db_conn_str,
            if_table_exists="append",
            engine="sqlalchemy"
        )
        return len(processed_df)

    except Exception as e:
        # 🛑 DO NOT SWALLOW EXCEPTIONS. PRINT THEM!
        print(f"❌ Worker FAILED: {str(e)}")
        traceback.print_exc()
        raise e  # Raise so Dask knows this partition failed

def process_heavy_data(file_path):
    """
    Synchronous version of the heavy processing task.
    """
    start_time=time.time()
    print(f"🔐 Fetching credentials from Vault...")
    
    # 1. GET SECURE CONNECTION STRING (Sync call)
    db_conn_str = get_db_credentials()
    if not db_conn_str:
        print("❌ Failed to get DB credentials")
        return {"status": "failed", "error": "No DB Creds"}

    print(f"⚡ Dask+Polars: Starting ingestion for {file_path}...")
    
    # 2. Setup Database (Create table if missing)
    try:
        print(f"⚡ Starting DB Setup...")
        # Create synchronous engine
        engine = create_engine(db_conn_str)
        
        with engine.begin() as conn:
            print("🛠️ Setting up database...")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sales_metrics (
                    timestamp TIMESTAMPTZ NOT NULL,
                    category TEXT,
                    region TEXT,
                    amount DOUBLE PRECISION,
                    adjusted_amount DOUBLE PRECISION
                );
            """))
            print("🛠️ database setup completed...")

            try:
                # Convert to Hypertable
                conn.execute(text("SELECT create_hypertable('sales_metrics', 'timestamp', if_not_exists => TRUE);"))
                print("🛠️ hypertable setup completed...")
            except Exception as e:
                print(f"🟡 Hypertable info: {e}") 

    except Exception as e:
        print(f"❌ DB Setup Error: {e}")
        return {"status": "failed", "error": str(e)}

    # 3. Dask Orchestration
    try:
        # 2. Lazy Load & Transform (Polars is instantaneous here)
        # We use scan_csv for lazy loading, but for 1M rows read_csv is also fine.
        q = (
            pl.scan_csv(file_path)
            .with_columns(
                pl.col("timestamp").str.to_datetime(),
                (pl.col("amount") * 1.1).alias("adjusted_amount")
            )
        )

        # 3. Collect (Execute the plan)
        df = q.collect()
        print(f"⚡ Data Processed in {time.time() - start_time:.2f}s. Rows: {len(df)}")

        # 4. Fast Write using ADBC (The Secret Sauce)
        # SQLAlchemy insert() does row-by-row or batched inserts (slow).
        # ADBC writes the Arrow memory buffer directly to Postgres (fast).
        
        write_start = time.time()
        
        # Ensure the table exists before writing (optional if you are sure)
        # Note: ADBC creates tables, but for complex schemas (Hypertable), 
        # keep your existing setup logic or run it once separately.
        
        df.write_database(
            table_name="sales_metrics",
            connection=db_conn_str,
            if_table_exists="append",
            engine="adbc"  # <--- THIS IS THE KEY CHANGE
        )
        
        total_time = time.time() - start_time
        print(f"✅ Write Complete in {time.time() - write_start:.2f}s.")
        print(f"🏁 Total Pipeline Time: {total_time:.2f}s")
        
        return {"status": "success", "rows_processed": len(df), "time_sec": total_time}

    except Exception as e:
        print(f"❌ Dask Execution Error: {e}")
        traceback.print_exc()
        return {"status": "failed", "error": str(e)}