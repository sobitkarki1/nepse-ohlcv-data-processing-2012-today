import pandas as pd
import os
from pathlib import Path

def clean_numeric_column(series):
    """
    Clean numeric columns that may contain commas or be stored as strings.
    """
    if series.dtype == 'object':
        # Remove commas and convert to float
        return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce')
    return series

def convert_nepse_excel_to_csv(excel_file='combined_excel.xlsx', output_dir='stock_data'):
    """
    Convert NEPSE Excel file with multiple sheets (one per day) into 
    separate CSV files for each stock symbol with OHLCV data.
    
    Args:
        excel_file: Path to the Excel file
        output_dir: Directory to save the CSV files
    """
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)
    
    print(f"Reading Excel file: {excel_file}")
    
    # Read all sheets from the Excel file
    excel_data = pd.ExcelFile(excel_file)
    sheet_names = excel_data.sheet_names
    
    print(f"Found {len(sheet_names)} sheets (trading days)")
    
    # Dictionary to store data for each stock symbol
    stock_data = {}
    
    # Define columns we want to keep (in order)
    columns_to_extract = {
        'Symbol': 'Symbol',
        'Open': 'Open',
        'High': 'High', 
        'Low': 'Low',
        'Close': 'Close',
        'Vol': 'Volume'  # Vol is the Volume column in the source data
    }
    
    # Process each sheet (each trading day)
    for idx, sheet_name in enumerate(sheet_names):
        print(f"Processing sheet {idx+1}/{len(sheet_names)}: {sheet_name}")
        
        # Read the sheet
        df = pd.read_excel(excel_data, sheet_name=sheet_name)
        
        # Display column names from first sheet
        if idx == 0:
            print(f"\nColumns in source data: {list(df.columns)}")
            print(f"Extracting: {list(columns_to_extract.keys())}\n")
        
        # Check if all required columns exist
        missing_cols = [col for col in columns_to_extract.keys() if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing columns in {sheet_name}: {missing_cols}")
            continue
        
        # Select and rename columns
        df_filtered = df[list(columns_to_extract.keys())].copy()
        df_filtered.rename(columns=columns_to_extract, inplace=True)
        
        # Add date column from sheet name
        df_filtered.insert(0, 'Date', sheet_name)
        
        # Clean numeric columns (remove commas, convert to float)
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_cols:
            if col in df_filtered.columns:
                df_filtered[col] = clean_numeric_column(df_filtered[col])
        
        # Group by stock symbol
        for symbol in df_filtered['Symbol'].unique():
            if pd.notna(symbol) and symbol != '':  # Skip empty symbols
                symbol_data = df_filtered[df_filtered['Symbol'] == symbol].copy()
                
                if symbol not in stock_data:
                    stock_data[symbol] = []
                
                stock_data[symbol].append(symbol_data)
    
    # Save each stock's data to a separate CSV file
    print(f"\n{'='*60}")
    print(f"Creating CSV files for {len(stock_data)} stocks...")
    print(f"{'='*60}\n")
    
    for symbol, data_list in stock_data.items():
        # Combine all data for this stock
        combined_df = pd.concat(data_list, ignore_index=True)
        
        # Sort by date (oldest first)
        combined_df.sort_values('Date', inplace=True)
        
        # Reset index
        combined_df.reset_index(drop=True, inplace=True)
        
        # Save to CSV
        safe_symbol = str(symbol).replace('/', '_').replace('\\', '_')  # Handle special characters
        csv_filename = os.path.join(output_dir, f"{safe_symbol}.csv")
        combined_df.to_csv(csv_filename, index=False)
        
        # Calculate some stats
        valid_records = combined_df['Close'].notna().sum()
        print(f"✓ {safe_symbol}.csv: {len(combined_df)} days, {valid_records} with valid data")
    
    print(f"\n{'='*60}")
    print(f"Conversion complete!")
    print(f"  • {len(stock_data)} CSV files created")
    print(f"  • {len(sheet_names)} trading days processed")
    print(f"  • Files saved in '{output_dir}/' directory")
    print(f"{'='*60}")

if __name__ == "__main__":
    convert_nepse_excel_to_csv()
