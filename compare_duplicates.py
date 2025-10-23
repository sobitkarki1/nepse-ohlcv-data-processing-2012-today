import pandas as pd

print("Loading files...")
# Read both CSV files
df_master = pd.read_csv('nepse_stock_data_master.csv')
df_older = pd.read_csv('master_stock_daily-older-data.csv')

print(f"\nFile Statistics:")
print(f"  nepse_stock_data_master.csv: {len(df_master):,} rows")
print(f"  master_stock_daily-older-data.csv: {len(df_older):,} rows")

# Check for duplicates within each file
print("\n" + "="*70)
print("WITHIN-FILE DUPLICATES (Date-Symbol pairs)")
print("="*70)

# Check duplicates in master file
master_dupes = df_master[df_master.duplicated(subset=['Date', 'Symbol'], keep=False)]
master_unique_dupes = master_dupes.groupby(['Date', 'Symbol']).size().reset_index(name='count')
print(f"\nnepse_stock_data_master.csv:")
print(f"  Total duplicate rows: {len(master_dupes):,}")
print(f"  Unique Date-Symbol pairs with duplicates: {len(master_unique_dupes):,}")

if len(master_unique_dupes) > 0:
    print(f"\nTop 10 duplicate Date-Symbol pairs in master file:")
    print(master_unique_dupes.nlargest(10, 'count').to_string(index=False))

# Check duplicates in older file
older_dupes = df_older[df_older.duplicated(subset=['Date', 'Symbol'], keep=False)]
older_unique_dupes = older_dupes.groupby(['Date', 'Symbol']).size().reset_index(name='count')
print(f"\nmaster_stock_daily-older-data.csv:")
print(f"  Total duplicate rows: {len(older_dupes):,}")
print(f"  Unique Date-Symbol pairs with duplicates: {len(older_unique_dupes):,}")

if len(older_unique_dupes) > 0:
    print(f"\nTop 10 duplicate Date-Symbol pairs in older file:")
    print(older_unique_dupes.nlargest(10, 'count').to_string(index=False))

# Check for overlapping Date-Symbol pairs between files
print("\n" + "="*70)
print("BETWEEN-FILE OVERLAPS (Date-Symbol pairs in both files)")
print("="*70)

# Create composite key
df_master['key'] = df_master['Date'] + '|' + df_master['Symbol']
df_older['key'] = df_older['Date'] + '|' + df_older['Symbol']

# Find common keys
master_keys = set(df_master['key'])
older_keys = set(df_older['key'])
common_keys = master_keys.intersection(older_keys)

print(f"\nCommon Date-Symbol pairs: {len(common_keys):,}")

if len(common_keys) > 0:
    # Get rows for common keys from both files
    master_common = df_master[df_master['key'].isin(common_keys)].copy()
    older_common = df_older[df_older['key'].isin(common_keys)].copy()
    
    # Merge to compare OHLCV data
    merged = master_common.merge(
        older_common,
        on=['Date', 'Symbol'],
        suffixes=('_master', '_older')
    )
    
    print(f"Total overlapping rows to compare: {len(merged):,}")
    
    # Check where HLCV data doesn't match (ignoring Open)
    ohlcv_cols = ['High', 'Low', 'Close', 'Volume']
    mismatches = pd.DataFrame()
    
    for col in ohlcv_cols:
        col_master = f'{col}_master'
        col_older = f'{col}_older'
        merged[f'{col}_mismatch'] = merged[col_master] != merged[col_older]
    
    # Any row with at least one mismatch
    merged['any_mismatch'] = merged[[f'{col}_mismatch' for col in ohlcv_cols]].any(axis=1)
    mismatches = merged[merged['any_mismatch']]
    
    print(f"\nDate-Symbol pairs with MISMATCHED OHLCV data: {len(mismatches):,}")
    print(f"Date-Symbol pairs with MATCHING OHLCV data: {len(merged) - len(mismatches):,}")
    
    if len(mismatches) > 0:
        print(f"\nBreakdown of mismatches by column:")
        for col in ohlcv_cols:
            mismatch_count = merged[f'{col}_mismatch'].sum()
            print(f"  {col}: {mismatch_count:,} mismatches")
        
        print(f"\nFirst 10 rows with mismatched data:")
        display_cols = ['Date', 'Symbol'] + [f'{col}_master' for col in ohlcv_cols] + [f'{col}_older' for col in ohlcv_cols]
        print(mismatches[display_cols].head(10).to_string(index=False))
        
        # Save mismatches to file for detailed review
        mismatches[display_cols].to_csv('ohlcv_mismatches.csv', index=False)
        print(f"\n✓ Full mismatch details saved to: ohlcv_mismatches.csv")

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Total common Date-Symbol pairs: {len(common_keys):,}")
if len(common_keys) > 0:
    print(f"  - Matching OHLCV: {len(merged) - len(mismatches):,} ({100*(len(merged) - len(mismatches))/len(merged):.2f}%)")
    print(f"  - Mismatched OHLCV: {len(mismatches):,} ({100*len(mismatches)/len(merged):.2f}%)")
