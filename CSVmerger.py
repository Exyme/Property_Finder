import pandas as pd
import glob
import os

# Get the directory of this script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Function to merge finn_page_*.csv files (existing functionality)
def merge_finn_pages():
    files = glob.glob(os.path.join(script_dir, 'finn_page_*.csv'))
    print('Found files:', files)  # Debug: List matching files

    if not files:
        print('No CSV files found! Check directory and names.')
    else:
        dfs = []
        for f in files:
            try:
                df = pd.read_csv(f, on_bad_lines='skip')  # Skip bad lines
                dfs.append(df)
                print(f'Successfully read {f} with {len(df)} rows')
            except pd.errors.ParserError as e:
                print(f'Error reading {f}: {e} - Skipping bad lines')
                try:
                    df = pd.read_csv(f, on_bad_lines='skip')
                    dfs.append(df)
                    print(f'Read {f} with skips: {len(df)} rows')
                except Exception as ex:
                    print(f'Failed to read {f} even with skips: {ex} - Skipping file')
            except Exception as e:
                print(f'Unexpected error on {f}: {e} - Skipping file')
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df.to_csv(os.path.join(script_dir, 'master_listings.csv'), index=False)
            print(f'Combined {len(combined_df)} listings into master_listings.csv')
        else:
            print('No data to combine!')

# New function to merge enhanced_listing_*.csv files
def merge_enhanced_listings():
    enhanced_dir = os.path.join(script_dir, 'enhanced_listings')
    
    # Create directory if it doesn't exist
    os.makedirs(enhanced_dir, exist_ok=True)
    
    # Get all enhanced_listing_*.csv files
    files = glob.glob(os.path.join(enhanced_dir, 'enhanced_listing_*.csv'))
    
    print('='*70)
    print('MERGING ENHANCED LISTINGS')
    print('='*70)
    print(f'Looking in: {enhanced_dir}')
    print(f'Found {len(files)} file(s)')
    
    if not files:
        print('⚠️  No enhanced_listing_*.csv files found!')
        print(f'   Make sure CSV files are in: {enhanced_dir}')
        print(f'   Files should be named: enhanced_listing_[ID].csv')
        return None
    
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, on_bad_lines='skip')
            dfs.append(df)
            print(f'✅ Read {os.path.basename(f)}: {len(df)} rows')
        except Exception as e:
            print(f'❌ Error reading {os.path.basename(f)}: {e} - Skipping')
    
    if dfs:
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates based on link (same property listed multiple times)
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['link'], keep='first')
        after_dedup = len(combined_df)
        
        if before_dedup != after_dedup:
            print(f'   Removed {before_dedup - after_dedup} duplicate(s)')
        
        # Save merged file
        output_file = os.path.join(enhanced_dir, 'enhanced_listings_merged.csv')
        combined_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print('='*70)
        print(f'✅ Merged {len(combined_df)} listings into:')
        print(f'   {output_file}')
        print('='*70)
        return combined_df
    else:
        print('❌ No data to combine!')
        return None

# Helper function to detect CSV delimiter
def detect_delimiter(file_path):
    """Detect the delimiter used in a CSV file by reading the first line."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            # Count occurrences of common delimiters
            semicolon_count = first_line.count(';')
            comma_count = first_line.count(',')
            
            # If semicolon appears more than comma, use semicolon
            if semicolon_count > comma_count:
                return ';'
            else:
                return ','
    except Exception:
        # Default to semicolon if detection fails
        return ';'

# New function to merge sales listings from Initial_listings/Sales/
def merge_sales_listings():
    sales_dir = os.path.join(script_dir, 'Initial_listings', 'Sales')
    
    # Get all finn_sales_page_*.csv files
    files = glob.glob(os.path.join(sales_dir, 'finn_sales_page_*.csv'))
    # Sort files to ensure consistent order
    files.sort()
    
    print('='*70)
    print('MERGING SALES LISTINGS')
    print('='*70)
    print(f'Looking in: {sales_dir}')
    print(f'Found {len(files)} file(s)')
    
    if not files:
        print('⚠️  No finn_sales_page_*.csv files found!')
        print(f'   Make sure CSV files are in: {sales_dir}')
        print(f'   Files should be named: finn_sales_page_[NUMBER].csv')
        return None
    
    dfs = []
    for f in files:
        try:
            # Detect delimiter for each file
            delimiter = detect_delimiter(f)
            df = pd.read_csv(f, sep=delimiter, on_bad_lines='skip', encoding='utf-8')
            
            # Normalize column names (handle case differences)
            df.columns = df.columns.str.strip()
            
            # Ensure column names match expected format
            # Some files might have different casing or spacing
            column_mapping = {}
            for col in df.columns:
                col_upper = col.upper()
                if col_upper == 'URL':
                    column_mapping[col] = 'URL'
                elif col_upper in ['TITLE', 'ADDRESS', 'SIZE', 'PRICE']:
                    column_mapping[col] = col_upper.capitalize()
            
            if column_mapping:
                df = df.rename(columns=column_mapping)
            
            dfs.append(df)
            print(f'✅ Read {os.path.basename(f)}: {len(df)} rows (delimiter: {delimiter})')
        except Exception as e:
            print(f'❌ Error reading {os.path.basename(f)}: {e} - Skipping')
            import traceback
            traceback.print_exc()
    
    if dfs:
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates based on URL (same property listed multiple times)
        before_dedup = len(combined_df)
        # Try different possible column names for URL
        url_column = None
        for col in combined_df.columns:
            if col.upper() == 'URL':
                url_column = col
                break
        
        if url_column:
            combined_df = combined_df.drop_duplicates(subset=[url_column], keep='first')
        after_dedup = len(combined_df)
        
        if before_dedup != after_dedup:
            print(f'   Removed {before_dedup - after_dedup} duplicate(s)')
        
        # Save merged file to script directory (same location as master_listings.csv)
        output_file = os.path.join(script_dir, 'master_listings_sales.csv')
        # Use semicolon as delimiter for output (consistent with original sales format)
        combined_df.to_csv(output_file, index=False, sep=';', encoding='utf-8')
        
        print('='*70)
        print(f'✅ Merged {len(combined_df)} listings into:')
        print(f'   {output_file}')
        print('='*70)
        return combined_df
    else:
        print('❌ No data to combine!')
        return None

if __name__ == "__main__":
    print('CSV MERGER - Choose an option:')
    print('1. Merge finn_page_*.csv files (original functionality)')
    print('2. Merge enhanced_listing_*.csv files (new functionality)')
    print('3. Merge sales listings (finn_sales_page_*.csv)')
    print('4. Merge both finn_page and enhanced_listings')
    print('5. Merge all (finn_page, enhanced_listings, and sales)')
    print()
    
    choice = input('Enter choice (1/2/3/4/5) or press Enter for option 3: ').strip()
    
    if choice == '1':
        merge_finn_pages()
    elif choice == '2':
        merge_enhanced_listings()
    elif choice == '3' or choice == '':
        merge_sales_listings()
    elif choice == '4':
        merge_finn_pages()
        print()
        merge_enhanced_listings()
    elif choice == '5':
        merge_finn_pages()
        print()
        merge_enhanced_listings()
        print()
        merge_sales_listings()
    else:
        print('Invalid choice. Defaulting to option 3 (sales listings)')
        merge_sales_listings()
