import pandas as pd
import os
from pathlib import Path

def combine_data_folders(folder1='stock_data_clean', folder2='stock_data_unique', 
                        output_folder='stock_data_combined'):
    """
    Combine data from two folders into one unified dataset.
    For common stocks, merge both datasets chronologically.
    For unique stocks, just copy them.
    
    Args:
        folder1: First data folder (typically the one with more historical data)
        folder2: Second data folder (typically with newer or unique data)
        output_folder: Output folder for combined data
    """
    
    print(f"{'='*70}")
    print(f"Combining Data from Two Folders")
    print(f"{'='*70}\n")
    print(f"Folder 1: {folder1}")
    print(f"Folder 2: {folder2}")
    print(f"Output: {output_folder}\n")
    
    # Create output directory if it doesn't exist
    Path(output_folder).mkdir(exist_ok=True)
    
    folder1_path = Path(folder1)
    folder2_path = Path(folder2)
    
    # Get list of CSV files in both folders
    files1 = {f.stem: f for f in folder1_path.glob('*.csv')}
    files2 = {f.stem: f for f in folder2_path.glob('*.csv')}
    
    # Find common and unique stocks
    common_stocks = set(files1.keys()).intersection(set(files2.keys()))
    unique_to_folder1 = set(files1.keys()) - set(files2.keys())
    unique_to_folder2 = set(files2.keys()) - set(files1.keys())
    
    print(f"Stocks in {folder1}: {len(files1)}")
    print(f"Stocks in {folder2}: {len(files2)}")
    print(f"Common stocks (will be merged): {len(common_stocks)}")
    print(f"Unique to {folder1}: {len(unique_to_folder1)}")
    print(f"Unique to {folder2}: {len(unique_to_folder2)}\n")
    
    total_rows = 0
    merged_count = 0
    copied_count = 0
    
    # Process common stocks - merge them
    if common_stocks:
        print(f"{'='*70}")
        print(f"Merging common stocks...")
        print(f"{'='*70}\n")
        
        for stock in sorted(common_stocks):
            df1 = pd.read_csv(files1[stock])
            df2 = pd.read_csv(files2[stock])
            
            original1_count = len(df1)
            original2_count = len(df2)
            
            # Normalize dates for both dataframes
            if 'Date' in df1.columns:
                df1['Date_normalized'] = pd.to_datetime(df1['Date'].str.replace('_', '-') if df1['Date'].dtype == object and '_' in str(df1['Date'].iloc[0]) else df1['Date'])
            
            if 'Date' in df2.columns:
                df2['Date_normalized'] = pd.to_datetime(df2['Date'].str.replace('_', '-') if df2['Date'].dtype == object and '_' in str(df2['Date'].iloc[0]) else df2['Date'])
            
            # Ensure both have same date format (use YYYY-MM-DD)
            df1['Date'] = df1['Date_normalized'].dt.strftime('%Y-%m-%d')
            df2['Date'] = df2['Date_normalized'].dt.strftime('%Y-%m-%d')
            
            # Drop the temporary normalized column
            df1 = df1.drop('Date_normalized', axis=1)
            df2 = df2.drop('Date_normalized', axis=1)
            
            # Combine both dataframes
            combined_df = pd.concat([df1, df2], ignore_index=True)
            
            # Remove duplicates (keep first occurrence based on date)
            combined_df = combined_df.drop_duplicates(subset=['Date'], keep='first')
            
            # Sort by date
            combined_df['Date_sort'] = pd.to_datetime(combined_df['Date'])
            combined_df = combined_df.sort_values('Date_sort')
            combined_df = combined_df.drop('Date_sort', axis=1)
            combined_df.reset_index(drop=True, inplace=True)
            
            # Save combined data
            output_file = Path(output_folder) / f"{stock}.csv"
            combined_df.to_csv(output_file, index=False)
            
            print(f"✓ {stock}: {original1_count} + {original2_count} = {len(combined_df)} total rows")
            total_rows += len(combined_df)
            merged_count += 1
    
    # Copy unique stocks from folder1
    if unique_to_folder1:
        print(f"\n{'='*70}")
        print(f"Copying stocks unique to {folder1}...")
        print(f"{'='*70}\n")
        
        for stock in sorted(unique_to_folder1):
            df = pd.read_csv(files1[stock])
            
            # Normalize date format if needed
            if 'Date' in df.columns and df['Date'].dtype == object:
                if '_' in str(df['Date'].iloc[0]):
                    df['Date'] = pd.to_datetime(df['Date'].str.replace('_', '-')).dt.strftime('%Y-%m-%d')
            
            output_file = Path(output_folder) / f"{stock}.csv"
            df.to_csv(output_file, index=False)
            
            print(f"✓ {stock}: {len(df)} rows")
            total_rows += len(df)
            copied_count += 1
    
    # Copy unique stocks from folder2
    if unique_to_folder2:
        print(f"\n{'='*70}")
        print(f"Copying stocks unique to {folder2}...")
        print(f"{'='*70}\n")
        
        for stock in sorted(unique_to_folder2):
            df = pd.read_csv(files2[stock])
            
            # Normalize date format if needed
            if 'Date' in df.columns and df['Date'].dtype == object:
                if '_' in str(df['Date'].iloc[0]):
                    df['Date'] = pd.to_datetime(df['Date'].str.replace('_', '-')).dt.strftime('%Y-%m-%d')
            
            output_file = Path(output_folder) / f"{stock}.csv"
            df.to_csv(output_file, index=False)
            
            print(f"✓ {stock}: {len(df)} rows")
            total_rows += len(df)
            copied_count += 1
    
    print(f"\n{'='*70}")
    print(f"Combination Complete!")
    print(f"{'='*70}")
    print(f"Merged stocks: {merged_count}")
    print(f"Copied stocks: {copied_count}")
    print(f"Total stocks: {merged_count + copied_count}")
    print(f"Total data rows: {total_rows:,}")
    print(f"Output saved to: {output_folder}/")
    print(f"{'='*70}\n")
    
    print("✅ All data successfully combined into one unified dataset!")
    print(f"   Use '{output_folder}/' for your price prediction model.")

if __name__ == "__main__":
    combine_data_folders(
        folder1='stock_data_clean',
        folder2='stock_data_unique',
        output_folder='stock_data_combined'
    )
