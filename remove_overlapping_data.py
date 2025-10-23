import pandas as pd
import os
from pathlib import Path

def remove_overlapping_data(folder_to_clean='stock_data', reference_folder='stock_data_clean', 
                           output_folder='stock_data_unique'):
    """
    Remove overlapping data from folder_to_clean based on dates present in reference_folder.
    Only keeps data that doesn't exist in the reference folder.
    
    Args:
        folder_to_clean: Folder containing data to clean
        reference_folder: Reference folder with data to compare against
        output_folder: Output folder for cleaned data
    """
    
    print(f"{'='*70}")
    print(f"Removing Overlapping Data")
    print(f"{'='*70}\n")
    print(f"Cleaning folder: {folder_to_clean}")
    print(f"Reference folder: {reference_folder}")
    print(f"Output folder: {output_folder}\n")
    
    # Create output directory if it doesn't exist
    Path(output_folder).mkdir(exist_ok=True)
    
    folder_to_clean_path = Path(folder_to_clean)
    reference_folder_path = Path(reference_folder)
    
    # Get list of CSV files in both folders
    files_to_clean = {f.stem: f for f in folder_to_clean_path.glob('*.csv')}
    reference_files = {f.stem for f in reference_folder_path.glob('*.csv')}
    
    # Find common stocks
    common_stocks = set(files_to_clean.keys()).intersection(reference_files)
    unique_stocks = set(files_to_clean.keys()) - reference_files
    
    print(f"Total stocks in {folder_to_clean}: {len(files_to_clean)}")
    print(f"Common stocks: {len(common_stocks)}")
    print(f"Unique stocks (no overlap): {len(unique_stocks)}\n")
    
    print(f"{'='*70}")
    print("Processing common stocks (removing overlapping dates)...")
    print(f"{'='*70}\n")
    
    total_removed = 0
    total_kept = 0
    files_deleted = 0
    files_updated = 0
    
    for stock in common_stocks:
        # Read data from both folders
        df_to_clean = pd.read_csv(files_to_clean[stock])
        df_reference = pd.read_csv(reference_folder_path / f"{stock}.csv")
        
        original_count = len(df_to_clean)
        
        # Normalize dates for comparison
        df_to_clean['Date_normalized'] = pd.to_datetime(df_to_clean['Date'].str.replace('_', '-'))
        df_reference['Date_normalized'] = pd.to_datetime(df_reference['Date'])
        
        # Find dates that are NOT in reference folder (unique dates)
        reference_dates = set(df_reference['Date_normalized'])
        df_unique = df_to_clean[~df_to_clean['Date_normalized'].isin(reference_dates)].copy()
        
        # Drop the temporary normalized date column
        df_unique = df_unique.drop('Date_normalized', axis=1)
        
        unique_count = len(df_unique)
        removed_count = original_count - unique_count
        
        if unique_count > 0:
            # Save file with unique data only
            output_file = Path(output_folder) / f"{stock}.csv"
            df_unique.to_csv(output_file, index=False)
            print(f"✓ {stock}: Kept {unique_count} unique rows, removed {removed_count} overlapping")
            total_kept += unique_count
            files_updated += 1
        else:
            # All data overlaps, no need to save
            print(f"✗ {stock}: All {original_count} rows overlap with reference - file not saved")
            files_deleted += 1
        
        total_removed += removed_count
    
    # Copy unique stocks (those not in reference folder)
    print(f"\n{'='*70}")
    print(f"Copying unique stocks (no overlap with reference)...")
    print(f"{'='*70}\n")
    
    for stock in unique_stocks:
        source_file = files_to_clean[stock]
        output_file = Path(output_folder) / f"{stock}.csv"
        
        # Just copy the file as-is
        df = pd.read_csv(source_file)
        df.to_csv(output_file, index=False)
        print(f"✓ {stock}: Copied {len(df)} rows (unique stock)")
        total_kept += len(df)
        files_updated += 1
    
    print(f"\n{'='*70}")
    print(f"Summary")
    print(f"{'='*70}")
    print(f"Total rows removed (overlapping): {total_removed}")
    print(f"Total rows kept (unique): {total_kept}")
    print(f"Files updated/copied: {files_updated}")
    print(f"Files deleted (fully overlapping): {files_deleted}")
    print(f"Output saved to: {output_folder}/")
    print(f"{'='*70}\n")
    
    if files_deleted > 0:
        print(f"Note: {files_deleted} stocks had complete overlap and were not saved.")
        print(f"All their data exists in '{reference_folder}'.")

if __name__ == "__main__":
    # Remove overlapping data from stock_data, keeping stock_data_clean as reference
    remove_overlapping_data(
        folder_to_clean='stock_data',
        reference_folder='stock_data_clean',
        output_folder='stock_data_unique'
    )
