import pandas as pd

# Read the older data file
df = pd.read_csv('master_stock_daily-older-data.csv')

# Rename 'Stock Symbol' to 'Symbol'
df.rename(columns={'Stock Symbol': 'Symbol'}, inplace=True)

# Reorder columns to match: Date, Symbol, Open, High, Low, Close, Volume
df = df[['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]

# Save the reformatted data back to the same file
df.to_csv('master_stock_daily-older-data.csv', index=False)

print("✓ Successfully reformatted master_stock_daily-older-data.csv")
print(f"  Columns: {', '.join(df.columns)}")
print(f"  Total rows: {len(df):,}")
print(f"\nFirst few rows:")
print(df.head(10))
