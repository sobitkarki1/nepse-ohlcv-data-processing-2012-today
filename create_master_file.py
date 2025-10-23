import pandas as pd
import os
from pathlib import Path

def combine_all_stocks_to_single_file(input_folder='stock_data_combined', 
                                      output_file='nepse_stock_data_master.csv'):
    """
    Combine all individual stock CSV files into one master CSV file.
    
    Args:
        input_folder: Folder containing individual stock CSV files
        output_file: Output master CSV filename
    """
    
    print(f"{'='*70}")
    print(f"Combining All Stocks into Single Master File")
    print(f"{'='*70}\n")
    print(f"Input folder: {input_folder}")
    print(f"Output file: {output_file}\n")
    
    input_path = Path(input_folder)
    csv_files = list(input_path.glob('*.csv'))
    
    if not csv_files:
        print(f"❌ No CSV files found in {input_folder}")
        return
    
    print(f"Found {len(csv_files)} CSV files to combine\n")
    print(f"Reading and combining files...")
    
    all_data = []
    processed_count = 0
    total_rows = 0
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            
            # Ensure Symbol column exists (should already be there)
            if 'Symbol' not in df.columns:
                print(f"⚠️  Warning: {csv_file.name} missing Symbol column, skipping...")
                continue
            
            all_data.append(df)
            processed_count += 1
            total_rows += len(df)
            
            # Progress indicator
            if processed_count % 50 == 0:
                print(f"  Processed {processed_count}/{len(csv_files)} files...")
            
        except Exception as e:
            print(f"❌ Error reading {csv_file.name}: {str(e)}")
    
    print(f"  Processed {processed_count}/{len(csv_files)} files.\n")
    
    if not all_data:
        print("❌ No data to combine!")
        return
    
    print(f"Combining all dataframes...")
    master_df = pd.concat(all_data, ignore_index=True)
    
    print(f"Sorting by Symbol and Date...")
    # Convert date to datetime for proper sorting
    master_df['Date_sort'] = pd.to_datetime(master_df['Date'])
    master_df = master_df.sort_values(['Symbol', 'Date_sort'])
    master_df = master_df.drop('Date_sort', axis=1)
    master_df.reset_index(drop=True, inplace=True)
    
    print(f"Saving to {output_file}...")
    master_df.to_csv(output_file, index=False)
    
    # Get some statistics
    unique_symbols = master_df['Symbol'].nunique()
    date_range = f"{master_df['Date'].min()} to {master_df['Date'].max()}"
    
    print(f"\n{'='*70}")
    print(f"Master File Created Successfully!")
    print(f"{'='*70}")
    print(f"Output file: {output_file}")
    print(f"Total rows: {len(master_df):,}")
    print(f"Unique stocks: {unique_symbols}")
    print(f"Date range: {date_range}")
    print(f"File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
    print(f"{'='*70}\n")
    
    # Show sample data
    print("Sample data (first 10 rows):")
    print(master_df.head(10).to_string(index=False))
    print("\n...")
    print(f"\n✅ All {unique_symbols} stocks combined into one master file!")
    print(f"   Use '{output_file}' for your price prediction model.")
    print(f"   Example: df = pd.read_csv('{output_file}')")
    print(f"   Filter by stock: df[df['Symbol'] == 'NABIL']")

if __name__ == "__main__":
    combine_all_stocks_to_single_file()
