import pandas as pd

print("Loading files...")
# Read both CSV files
df_master = pd.read_csv('nepse_stock_data_master.csv')
df_older = pd.read_csv('master_stock_daily-older-data.csv')

print(f"\nInitial Statistics:")
print(f"  nepse_stock_data_master.csv: {len(df_master):,} rows")
print(f"  master_stock_daily-older-data.csv: {len(df_older):,} rows")
print(f"  Total before merge: {len(df_master) + len(df_older):,} rows")

# Create composite key for both dataframes
df_master['key'] = df_master['Date'] + '|' + df_master['Symbol']
df_older['key'] = df_older['Date'] + '|' + df_older['Symbol']

# Find overlapping keys
master_keys = set(df_master['key'])
older_keys = set(df_older['key'])
common_keys = master_keys.intersection(older_keys)

print(f"\nOverlap Analysis:")
print(f"  Common Date-Symbol pairs: {len(common_keys):,}")
print(f"  Unique to master: {len(master_keys - older_keys):,}")
print(f"  Unique to older: {len(older_keys - master_keys):,}")

# Remove overlapping records from older file (keep master data)
df_older_unique = df_older[~df_older['key'].isin(common_keys)].copy()

print(f"\nAfter removing duplicates from older file:")
print(f"  Rows kept from older file: {len(df_older_unique):,}")
print(f"  Rows removed from older file: {len(df_older) - len(df_older_unique):,}")

# Drop the temporary 'key' column
df_master = df_master.drop('key', axis=1)
df_older_unique = df_older_unique.drop('key', axis=1)

# Combine the dataframes
df_merged = pd.concat([df_master, df_older_unique], ignore_index=True)

print(f"\nMerged Statistics:")
print(f"  Total rows in merged file: {len(df_merged):,}")
print(f"  Expected rows (no duplicates): {len(master_keys) + len(older_keys - master_keys):,}")

# Sort by Date and Symbol
df_merged['Date'] = pd.to_datetime(df_merged['Date'])
df_merged = df_merged.sort_values(['Date', 'Symbol'])
df_merged['Date'] = df_merged['Date'].dt.strftime('%Y-%m-%d')

# Verify no duplicates in merged file
duplicates_check = df_merged.duplicated(subset=['Date', 'Symbol'], keep=False)
num_duplicates = duplicates_check.sum()

print(f"\nDuplicate Check:")
print(f"  Duplicates in merged file: {num_duplicates}")

if num_duplicates == 0:
    print("  ✓ No duplicates found - merge successful!")
else:
    print("  ⚠ Warning: Duplicates still exist in merged file!")

# Get date range
df_merged['Date_temp'] = pd.to_datetime(df_merged['Date'])
min_date = df_merged['Date_temp'].min()
max_date = df_merged['Date_temp'].max()
df_merged = df_merged.drop('Date_temp', axis=1)

print(f"\nDate Range:")
print(f"  From: {min_date.strftime('%Y-%m-%d')}")
print(f"  To: {max_date.strftime('%Y-%m-%d')}")

# Get unique symbols
unique_symbols = df_merged['Symbol'].nunique()
print(f"\nUnique Symbols: {unique_symbols:,}")

# Save merged file
output_file = 'nepse_stock_data_merged_deduped.csv'
df_merged.to_csv(output_file, index=False)

print(f"\n{'='*70}")
print(f"✓ Merged and deduplicated data saved to: {output_file}")
print(f"{'='*70}")

# Show sample of merged data
print(f"\nFirst 10 rows of merged data:")
print(df_merged.head(10).to_string(index=False))

print(f"\nLast 10 rows of merged data:")
print(df_merged.tail(10).to_string(index=False))
