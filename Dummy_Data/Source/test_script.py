
import pandas as pd

# === Load source and target data ===
source_df = pd.read_csv("dim_account.csv")
target_df = pd.read_csv(r"C:\Users\Tcala\Downloads\Dummy_Data\Target\target_dim_account.csv")

# === 1. Row Count Check ===
print(f"Row Count - Source: {len(source_df)}, Target: {len(target_df)}")
print("PASS" if len(source_df) == len(target_df) else "FAIL")

# === 2. Null Check on Critical Columns ===
key_cols = ['account_id']

for col in key_cols:
    nulls = target_df[col].isnull().sum()
    print(f"Nulls in {col}: {nulls}")
    print("PASS" if nulls == 0 else "FAIL")

# === 3. Duplicate Key Check ===
dup_keys = target_df.duplicated(subset=['account_id']).sum()
print(f"Duplicate Account ID entries: {dup_keys}")
print("PASS" if dup_keys == 0 else "FAIL")

# === 4. Format Check (account_id should be numeric) ===
non_numeric = target_df[~target_df['account_id'].astype(str).str.isnumeric()]
print(f"Non-numeric account_id: {len(non_numeric)}")
print("PASS" if len(non_numeric) == 0 else "FAIL")

# === 5. Value Check: Valid Account Types ===
allowed_types = {'Cash', 'Accounts Receivable', 'Inventory', 'Fixed Assets', 'Accounts Payable'}
invalid_types = target_df[~target_df['account_name'].isin(allowed_types)]
print(f"Invalid Account Name values: {len(invalid_types)}")
print("PASS" if len(invalid_types) == 0 else "FAIL")
