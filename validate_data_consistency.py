import pandas as pd
import os
from pathlib import Path
import random

def validate_data_consistency(folder1='stock_data', folder2='stock_data_clean', num_samples=5):
    """
    Validate that OHLCV data is consistent between two folders for overlapping stocks and dates.
    
    Args:
        folder1: First data folder
        folder2: Second data folder
        num_samples: Number of stocks to randomly sample for validation
    """
    
    print(f"{'='*70}")
    print(f"Validating Data Consistency")
    print(f"{'='*70}\n")
    
    # Get list of CSV files in both folders
    folder1_path = Path(folder1)
    folder2_path = Path(folder2)
    
    files1 = {f.stem for f in folder1_path.glob('*.csv')}
    files2 = {f.stem for f in folder2_path.glob('*.csv')}
    
    # Find common stocks
    common_stocks = files1.intersection(files2)
    
    if not common_stocks:
        print("No common stocks found between the two folders!")
        return
    
    print(f"Found {len(common_stocks)} common stocks between folders")
    print(f"Folder 1 ({folder1}): {len(files1)} files")
    print(f"Folder 2 ({folder2}): {len(files2)} files")
    print(f"\nRandomly sampling {min(num_samples, len(common_stocks))} stocks for validation...\n")
    
    # Randomly sample stocks
    sample_stocks = random.sample(list(common_stocks), min(num_samples, len(common_stocks)))
    
    total_mismatches = 0
    total_comparisons = 0
    
    for stock in sample_stocks:
        print(f"{'='*70}")
        print(f"Validating: {stock}")
        print(f"{'='*70}")
        
        # Read data from both folders
        df1 = pd.read_csv(folder1_path / f"{stock}.csv")
        df2 = pd.read_csv(folder2_path / f"{stock}.csv")
        
        print(f"  {folder1}: {len(df1)} rows")
        print(f"  {folder2}: {len(df2)} rows")
        
        # Convert date columns to same format for comparison
        # Folder1 has dates like "2024_03_04", Folder2 has "2012-01-01"
        df1['Date_normalized'] = pd.to_datetime(df1['Date'].str.replace('_', '-'))
        df2['Date_normalized'] = pd.to_datetime(df2['Date'])
        
        # Find common dates
        dates1 = set(df1['Date_normalized'])
        dates2 = set(df2['Date_normalized'])
        common_dates = dates1.intersection(dates2)
        
        print(f"  Common dates: {len(common_dates)}")
        
        if not common_dates:
            print(f"  ⚠️  No overlapping dates found!\n")
            continue
        
        # Sample a few dates to check in detail
        num_date_samples = min(10, len(common_dates))
        sample_dates = random.sample(list(common_dates), num_date_samples)
        
        print(f"  Checking {num_date_samples} random dates...\n")
        
        mismatches = []
        
        for date in sample_dates:
            row1 = df1[df1['Date_normalized'] == date].iloc[0]
            row2 = df2[df2['Date_normalized'] == date].iloc[0]
            
            # Compare OHLCV values
            ohlcv_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            date_str = date.strftime('%Y-%m-%d')
            
            date_mismatch = False
            for col in ohlcv_columns:
                val1 = row1[col]
                val2 = row2[col]
                
                # Handle potential NaN values
                if pd.isna(val1) and pd.isna(val2):
                    continue
                
                # Compare values (allow small floating point differences)
                if pd.notna(val1) and pd.notna(val2):
                    diff = abs(float(val1) - float(val2))
                    if diff > 0.01:  # Allow 0.01 difference for rounding
                        mismatches.append({
                            'date': date_str,
                            'column': col,
                            'folder1_value': val1,
                            'folder2_value': val2,
                            'difference': diff
                        })
                        date_mismatch = True
                elif pd.notna(val1) or pd.notna(val2):
                    mismatches.append({
                        'date': date_str,
                        'column': col,
                        'folder1_value': val1,
                        'folder2_value': val2,
                        'difference': 'One is NaN'
                    })
                    date_mismatch = True
            
            total_comparisons += 1
            
            if not date_mismatch:
                print(f"    ✓ {date_str}: All OHLCV values match")
            else:
                print(f"    ✗ {date_str}: Mismatches found!")
                total_mismatches += 1
        
        if mismatches:
            print(f"\n  ❌ Found {len(mismatches)} value mismatches:")
            for m in mismatches[:5]:  # Show first 5 mismatches
                print(f"    - {m['date']} {m['column']}: {m['folder1_value']} vs {m['folder2_value']} (diff: {m['difference']})")
            if len(mismatches) > 5:
                print(f"    ... and {len(mismatches) - 5} more")
        else:
            print(f"\n  ✅ All sampled dates have matching OHLCV values!")
        
        print()
    
    print(f"{'='*70}")
    print(f"Validation Summary")
    print(f"{'='*70}")
    print(f"Total date comparisons: {total_comparisons}")
    print(f"Dates with mismatches: {total_mismatches}")
    print(f"Match rate: {((total_comparisons - total_mismatches) / total_comparisons * 100):.2f}%" if total_comparisons > 0 else "N/A")
    
    if total_mismatches == 0:
        print(f"\n✅ SUCCESS: All sampled data is consistent between folders!")
    else:
        print(f"\n⚠️  WARNING: Some mismatches found. Review the data sources.")
    print(f"{'='*70}")

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    
    # Validate with 5 random stocks
    validate_data_consistency(num_samples=5)
