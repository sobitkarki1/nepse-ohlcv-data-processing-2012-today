import pandas as pd
import os
from pathlib import Path

def convert_nepse_csv_to_clean_format(input_dir='nepse-stock-data-2012-2025', output_dir='stock_data_clean'):
    """
    Convert NEPSE CSV files with technical indicators to clean OHLCV format.
    
    Args:
        input_dir: Directory containing the source CSV files
        output_dir: Directory to save the cleaned CSV files
    """
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"Reading CSV files from: {input_dir}")
    print(f"Output directory: {output_dir}\n")
    
    # Get all CSV files in the input directory
    input_path = Path(input_dir)
    csv_files = list(input_path.glob('*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process\n")
    print(f"{'='*60}")
    
    # Columns we want to keep (in order)
    columns_to_keep = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
    
    processed_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        try:
            stock_symbol = csv_file.stem  # Get filename without extension
            print(f"Processing: {stock_symbol}.csv", end=" ... ")
            
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Check if all required columns exist
            missing_cols = [col for col in columns_to_keep if col not in df.columns]
            if missing_cols:
                print(f"❌ Missing columns: {missing_cols}")
                error_count += 1
                continue
            
            # Select only the columns we need
            df_clean = df[columns_to_keep].copy()
            
            # Sort by date (oldest first)
            df_clean.sort_values('Date', inplace=True)
            
            # Reset index
            df_clean.reset_index(drop=True, inplace=True)
            
            # Handle any NaN values
            valid_records = df_clean['Close'].notna().sum()
            
            # Save to new CSV file in output directory
            output_file = Path(output_dir) / f"{stock_symbol}.csv"
            df_clean.to_csv(output_file, index=False)
            
            print(f"✓ {len(df_clean)} days, {valid_records} with valid data")
            processed_count += 1
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            error_count += 1
    
    print(f"{'='*60}")
    print(f"\nConversion complete!")
    print(f"  • {processed_count} files successfully processed")
    if error_count > 0:
        print(f"  • {error_count} files had errors")
    print(f"  • Files saved in '{output_dir}/' directory")
    print(f"{'='*60}")

if __name__ == "__main__":
    convert_nepse_csv_to_clean_format()
