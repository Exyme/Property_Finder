#!/usr/bin/env python3
"""Verification script to check output files meet requirements"""

import pandas as pd
import os
from Email_Fetcher import extract_finnkode

print("="*70)
print("OUTPUT FILES VERIFICATION")
print("="*70)

# Check Sales output
sales_file = 'output/sales/sales_property_listings_with_distances.csv'
print(f"\n📊 SALES OUTPUT: {sales_file}")
print("-" * 70)

if os.path.exists(sales_file):
    df_sales = pd.read_csv(sales_file)
    print(f"✅ File exists")
    print(f"   Properties: {len(df_sales)}")
    
    if len(df_sales) > 0:
        print(f"   Columns: {len(df_sales.columns)} columns")
        print(f"   Key columns: {', '.join(df_sales.columns[:8])}")
        
        # Check URL format
        sample_link = df_sales['link'].iloc[0] if 'link' in df_sales.columns else None
        if sample_link:
            print(f"   Sample link format: {sample_link[:100]}...")
            finnkode = extract_finnkode(sample_link)
            if finnkode:
                print(f"   Extracted finnkode: {finnkode}")
                # Check if it's sales format (short) or rental format
                if 'realestate/lettings' in sample_link:
                    print(f"   ⚠️  WARNING: Sales property has rental URL format!")
                else:
                    print(f"   ✅ Sales URL format correct (short format)")
        
        # Check date_read
        if 'date_read' in df_sales.columns:
            sample_date = df_sales['date_read'].iloc[0]
            print(f"   Sample date_read: {sample_date}")
            print(f"   ✅ date_read column present")
        else:
            print(f"   ⚠️  WARNING: date_read column missing")
        
        # Check for rental properties (should not be here)
        if 'realestate/lettings' in str(df_sales['link'].iloc[0]):
            rental_count = df_sales['link'].str.contains('realestate/lettings', na=False).sum()
            if rental_count > 0:
                print(f"   ⚠️  WARNING: Found {rental_count} rental properties in sales output!")
            else:
                print(f"   ✅ No rental properties in sales output")
    else:
        print(f"   ⚠️  File is empty")
else:
    print(f"❌ File does not exist")

# Check Rental output
rental_file = 'output/rental/property_listings_with_distances.csv'
print(f"\n📊 RENTAL OUTPUT: {rental_file}")
print("-" * 70)

if os.path.exists(rental_file):
    df_rental = pd.read_csv(rental_file)
    print(f"✅ File exists")
    print(f"   Properties: {len(df_rental)}")
    
    if len(df_rental) > 0:
        print(f"   Columns: {len(df_rental.columns)} columns")
        print(f"   Key columns: {', '.join(df_rental.columns[:8])}")
        
        # Check URL format
        sample_link = df_rental['link'].iloc[0] if 'link' in df_rental.columns else None
        if sample_link:
            print(f"   Sample link format: {sample_link[:100]}...")
            finnkode = extract_finnkode(sample_link)
            if finnkode:
                print(f"   Extracted finnkode: {finnkode}")
                # Check if it's rental format
                if 'realestate/lettings' in sample_link:
                    print(f"   ✅ Rental URL format correct (lettings format)")
                else:
                    print(f"   ⚠️  WARNING: Rental property has non-standard URL format!")
        
        # Check date_read
        if 'date_read' in df_rental.columns:
            sample_date = df_rental['date_read'].iloc[0]
            print(f"   Sample date_read: {sample_date}")
            print(f"   ✅ date_read column present")
        else:
            print(f"   ⚠️  WARNING: date_read column missing")
    else:
        print(f"   ⚠️  File is empty")
else:
    print(f"❌ File does not exist")

# Summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)

sales_exists = os.path.exists(sales_file) and os.path.getsize(sales_file) > 0
rental_exists = os.path.exists(rental_file) and os.path.getsize(rental_file) > 0

print(f"✅ Separate outputs: {'YES' if (sales_exists and rental_exists) else 'PARTIAL'}")
print(f"   - Sales output: {'✅' if sales_exists else '❌'}")
print(f"   - Rental output: {'✅' if rental_exists else '❌'}")

if sales_exists and rental_exists:
    print(f"\n✅ Both output files created successfully!")
    print(f"   Ready for manual verification")

