import pandas as pd
import numpy as np
import os

def generate_large_csv(filename="large_sales_data.csv", rows=1_000_000):
    print(f"Generating {rows} rows of data... this might take a moment.")
    
    # Create random data
    df = pd.DataFrame({
        'transaction_id': np.arange(rows),
        'date': pd.date_range(start='1/1/2020', periods=rows, freq='S'),
        'category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Toys'], rows),
        'amount': np.random.uniform(10, 1000, rows).round(2),
        'region': np.random.choice(['North', 'South', 'East', 'West'], rows)
    })
    
    # Save to CSV
    df.to_csv(filename, index=False)
    file_size = os.path.getsize(filename) / (1024 * 1024)
    print(f"✅ Created '{filename}' ({file_size:.2f} MB)")

if __name__ == "__main__":
    generate_large_csv()