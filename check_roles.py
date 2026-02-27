#!/usr/bin/env python3
"""
Quick diagnostic: What do the REPORTINGOWNER columns actually look like?
Run from Form4_Scanner directory.
"""
import os
import pandas as pd

DATA_DIR = os.path.expanduser(
    "~/Desktop/Claude_Programs/Trading_Programs/Form4_Scanner/SEC_Form4_Data"
)

# Just check one quarter to see column names and sample values
folders = sorted([
    f for f in os.listdir(DATA_DIR)
    if os.path.isdir(os.path.join(DATA_DIR, f)) and 'form' in f.lower()
])

# Check first and last quarter
for folder in [folders[0], folders[-1]]:
    print(f"\n{'='*60}")
    print(f"  {folder}")
    print(f"{'='*60}")
    
    owner_file = os.path.join(DATA_DIR, folder, "REPORTINGOWNER.tsv")
    owners = pd.read_csv(owner_file, sep='\t', dtype=str, low_memory=False)
    
    print(f"\n  ALL COLUMNS: {list(owners.columns)}")
    
    # Check for any column containing 'OFFICER', 'TITLE', 'DIRECTOR', 'ROLE'
    print(f"\n  Columns containing 'OFFICER':")
    for col in owners.columns:
        if 'OFFICER' in col.upper():
            print(f"    {col}")
            # Show unique values (first 30)
            vals = owners[col].dropna().unique()
            print(f"    Non-null count: {owners[col].notna().sum()}/{len(owners)}")
            print(f"    Sample values: {list(vals[:30])}")
    
    print(f"\n  Columns containing 'DIRECTOR':")
    for col in owners.columns:
        if 'DIRECTOR' in col.upper():
            print(f"    {col}")
            vals = owners[col].dropna().value_counts()
            print(f"    Value counts: {dict(vals)}")
    
    print(f"\n  Columns containing 'TITLE':")
    for col in owners.columns:
        if 'TITLE' in col.upper():
            print(f"    {col}")
            vals = owners[col].dropna().unique()
            print(f"    Non-null count: {owners[col].notna().sum()}/{len(owners)}")
            print(f"    Sample values: {list(vals[:30])}")
    
    print(f"\n  Columns containing 'TEN' or 'PERCENT':")
    for col in owners.columns:
        if 'TEN' in col.upper() or 'PERCENT' in col.upper():
            print(f"    {col}")
            vals = owners[col].dropna().value_counts()
            print(f"    Value counts: {dict(vals)}")

    # Also check: what does the IS_OFFICER column look like?
    print(f"\n  Columns containing 'IS_':")
    for col in owners.columns:
        if col.upper().startswith('IS_') or 'IS_' in col.upper():
            print(f"    {col}")
            vals = owners[col].dropna().value_counts()
            print(f"    Value counts: {dict(vals)}")
