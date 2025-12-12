import dask.dataframe as dd
import polars as pl
from sqlalchemy import create_engine, text
from vault_util import get_db_credentials 

def process_partition(pandas_df, db_conn_str):
    """
    Worker Function: Converts Pandas chunk to Polars and writes to DB.
    """
    if pandas_df.empty:
        return 0

    try:
        # 1. Zero-Copy Convert to Polars
        df = pl.from_pandas(pandas_df)
        df["transaction_id"].drop(in_place=True)  # Drop unnecessary column
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
        print(f"❌ CRITICAL WORKER ERROR: {e}")
        traceback.print_exc()
        raise e

def process_heavy_data(file_path):
    """
    Synchronous version of the heavy processing task.
    """
    print(f"🔐 Fetching credentials from Vault...")
    
    # 1. GET SECURE CONNECTION STRING (Sync call)
    db_conn_str = get_db_credentials()
    if not db_conn_str:
        return {"status": "failed", "error": "Could not retrieve DB credentials"}

    print(f"⚡ Dask+Polars: Starting ingestion for {file_path}...")
    
    # 2. Setup Database (Create table if missing)
    try:
        print(f"⚡ Starting DB Setup...")
        # Create synchronous engine
        engine = create_engine(db_conn_str)
        
        with engine.connect() as conn:
            print("🛠️ Setting up database...")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sales_metrics (
                    timestamp TIMESTAMPTZ NOT NULL,
                    category TEXT,
                    region TEXT,
                    amount DOUBLE PRECISION,
                    adjusted_amount DOUBLE PRECISION
                );
            """)
            conn.commit()  # Ensure changes are committed
            print("🛠️ database setup completed...")

            try:
                # Convert to Hypertable
                conn.execute(text("SELECT create_hypertable('sales_metrics', 'timestamp', if_not_exists => TRUE);"))
                conn.commit()
                print("🛠️ hypertable setup completed...")
            except Exception as e:
                print(f"🟡 Hypertable info: {e}") 

    except Exception as e:
        return {"status": "failed", "error": f"DB Setup Error: {str(e)}"}

    # 3. Dask Orchestration
    try:
        print("Started Dask Service Chunking.") 
        ddf = dd.read_csv(file_path)
        
        print("Storing Data on Timescale.") 
        # map_partitions is lazy; compute() triggers the execution
        result = ddf.map_partitions(
            process_partition, 
            db_conn_str=db_conn_str, 
            meta=('rows', 'int')
        )
        
        # compute() returns a pandas Series of row counts, sum() aggregates them
        total_rows = result.compute().sum()
        
        print(f"✅ Dask+Polars: Completed ingestion. Total Rows Processed: {int(total_rows)}")
        return {"status": "success", "rows_processed": int(total_rows)}

    except Exception as e:
        return {"status": "failed", "error": str(e)}