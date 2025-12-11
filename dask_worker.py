import dask.dataframe as dd
import time

def process_heavy_data(file_path):
    """
    Uses Dask to process large files without consuming all RAM.
    """
    print(f"⚡ Dask: Starting processing on {file_path}...")
    start_time = time.time()

    # 1. Read CSV (Lazy Load)
    # Dask doesn't read the whole file yet, it just scans the structure.
    ddf = dd.read_csv(file_path)

    # 2. Define the Computation
    # We want: Average sales amount per Category per Region
    # This is a complex grouping operation.
    result_op = ddf.groupby(['region', 'category'])['amount'].mean()

    # 3. Compute (The Heavy Lifting)
    # .compute() triggers the actual parallel processing
    final_result = result_op.compute()

    duration = time.time() - start_time
    print(f"✅ Dask: Finished in {duration:.2f} seconds.")

    # Convert to standard dictionary for JSON serialization
    # Then export as a list of records (which is perfect for JSON)
    flat_result = final_result.reset_index()
    return flat_result.to_dict(orient='records')