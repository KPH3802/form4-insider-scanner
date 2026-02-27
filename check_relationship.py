#!/usr/bin/env python3
"""Check RPTOWNER_RELATIONSHIP values"""
import os, pandas as pd

DATA_DIR = os.path.expanduser(
    "~/Desktop/Claude_Programs/Trading_Programs/Form4_Scanner/SEC_Form4_Data"
)

folder = "2020q1_form345"
owner_file = os.path.join(DATA_DIR, folder, "REPORTINGOWNER.tsv")
owners = pd.read_csv(owner_file, sep='\t', dtype=str, low_memory=False)

print("RPTOWNER_RELATIONSHIP unique values:")
vals = owners['RPTOWNER_RELATIONSHIP'].dropna().value_counts()
for v, c in vals.head(30).items():
    print(f"  {v:40s} : {c:>6,}")

print(f"\nTotal non-null: {owners['RPTOWNER_RELATIONSHIP'].notna().sum()}/{len(owners)}")

# Also check RPTOWNER_TXT
print(f"\nRPTOWNER_TXT sample values:")
txt_vals = owners['RPTOWNER_TXT'].dropna().unique()
print(f"  Non-null count: {owners['RPTOWNER_TXT'].notna().sum()}/{len(owners)}")
for v in txt_vals[:20]:
    print(f"  {v}")