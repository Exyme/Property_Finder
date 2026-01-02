#!/usr/bin/env python3
"""Simple verification script to check output files"""

import pandas as pd
import os
import re

def extract_finnkode_simple(url):
    """Extract finnkode from URL without importing Email_Fetcher"""
    if not url or not isinstance(url, str):
        return None
    # Try to extract from finnkode= parameter
    match = re.search(r'finnkode=(\d+)', url)
    if match:
        return match.group(1)
    # Try to extract from short format /{finnkode}
    match = re.search(r'finn\.no/(\d+)', url)
    if match:
        return match.group(1)
    return None

print("="*70)
print("OUTPUT FILES VERIFICATION")
print("="*70)

# Check Sales output
sales_file = 'output/sales/sales_property_listings_with_distances.csv'
print(f"\n📊 SALES OUTPUT: {sales_file}")
print("-" * 70)

if os.path.exists(sales_file):
    size = os.path.getsize(sales_file)
    print(f"✅ File exists ({size} bytes)")
    
    if size > 0:
        try:
            df_sales = pd.read_csv(sales_file)
            print(f"   Properties: {len(df_sales)}")
            print(f"   Columns: {len(df_sales.columns)}")
            
            if len(df_sales) > 0:
                print(f"   Key columns: {', '.join(df_sales.columns[:8])}")
                
                # Check URL format
                if 'link' in df_sales.columns:
                    sample_link = str(df_sales['link'].iloc[0])
                    print(f"   Sample link: {sample_link[:100]}...")
                    finnkode = extract_finnkode_simple(sample_link)
                    if finnkode:
                        print(f"   Extracted finnkode: {finnkode}")
                    if 'realestate/lettings' in sample_link:
                        print(f"   ⚠️  WARNING: Sales property has rental URL format!")
                    else:
                        print(f"   ✅ Sales URL format appears correct")
                
                # Check date_read
                if 'date_read' in df_sales.columns:
                    sample_date = df_sales['date_read'].iloc[0]
                    print(f"   Sample date_read: {sample_date}")
                    print(f"   ✅ date_read column present")
                else:
                    print(f"   ⚠️  WARNING: date_read column missing")
        except Exception as e:
            print(f"   ⚠️  Error reading file: {e}")
    else:
        print(f"   ⚠️  File is empty (no header)")
else:
    print(f"❌ File does not exist")

# Check Rental output
rental_file = 'output/rental/property_listings_with_distances.csv'
print(f"\n📊 RENTAL OUTPUT: {rental_file}")
print("-" * 70)

if os.path.exists(rental_file):
    size = os.path.getsize(rental_file)
    print(f"✅ File exists ({size} bytes)")
    
    if size > 0:
        try:
            df_rental = pd.read_csv(rental_file)
            print(f"   Properties: {len(df_rental)}")
            print(f"   Columns: {len(df_rental.columns)}")
            
            if len(df_rental) > 0:
                print(f"   Key columns: {', '.join(df_rental.columns[:8])}")
                
                # Check URL format
                if 'link' in df_rental.columns:
                    sample_link = str(df_rental['link'].iloc[0])
                    print(f"   Sample link: {sample_link[:100]}...")
                    finnkode = extract_finnkode_simple(sample_link)
                    if finnkode:
                        print(f"   Extracted finnkode: {finnkode}")
                    if 'realestate/lettings' in sample_link:
                        print(f"   ✅ Rental URL format correct (lettings format)")
                    else:
                        print(f"   ⚠️  WARNING: Rental property has non-standard URL format")
                
                # Check date_read
                if 'date_read' in df_rental.columns:
                    sample_date = df_rental['date_read'].iloc[0]
                    print(f"   Sample date_read: {sample_date}")
                    print(f"   ✅ date_read column present")
                else:
                    print(f"   ⚠️  WARNING: date_read column missing")
        except Exception as e:
            print(f"   ⚠️  Error reading file: {e}")
    else:
        print(f"   ⚠️  File is empty (no header)")
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
else:
    print(f"\n⚠️  Pipeline may still be running or needs to be executed")
    print(f"   Run: python3 property_finder.py")

