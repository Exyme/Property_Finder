import pandas as pd
import os
import time
import googlemaps
from dotenv import load_dotenv

# Get script directory for relative paths
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load environment variables
env_path = os.path.join(script_dir, '.env')
if not os.path.exists(env_path):
    parent_dir = os.path.dirname(script_dir)
    env_path = os.path.join(parent_dir, '.env')

load_dotenv(dotenv_path=env_path)

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

# Initialize Google Maps client (will be None if no API key)
gmaps = None
if GOOGLE_API_KEY:
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)


# ================================================
# GEOCODING FUNCTION
# ================================================

def geocode_address(address, gmaps_client=None):
    """
    Converts an address string to latitude and longitude coordinates.
    
    Args:
        address (str): The address to geocode (e.g., "DUGGVEIEN 5 B, Oslo")
        gmaps_client: Optional Google Maps client (uses global gmaps if not provided)
    
    Returns:
        tuple: (latitude, longitude) if successful, None if failed
    """
    # Use provided client or global client
    client = gmaps_client if gmaps_client else gmaps
    
    if not client:
        raise ValueError("Google Maps client not initialized. Check GOOGLE_API_KEY.")
    
    try:
        # Call the Google Maps API to geocode the address
        geocode_result = client.geocode(address)
        
        # Check if we got any results
        if not geocode_result or len(geocode_result) == 0:
            return None

        # Get the first (most relevant) result
        first_result = geocode_result[0]

        # Extract the location coordinates
        location = first_result['geometry']['location']
        latitude = location['lat']
        longitude = location['lng']

        return (latitude, longitude)
    except Exception as e:
        # Handle any errors (network issues, API errors, etc.)
        return None


# ============================================
# HELPER FUNCTIONS
# ============================================

def has_valid_coordinates(row):
    """
    Check if a property row has valid geocoded coordinates.
    
    Args:
        row: A pandas Series representing a property row
    
    Returns:
        bool: True if the row has valid latitude, longitude, and geocode_status == 'Success'
    """
    try:
        # Check if latitude and longitude exist and are valid numbers
        lat = row.get('latitude')
        lng = row.get('longitude')
        status = row.get('geocode_status', '')
        
        # Must have latitude and longitude as valid floats
        if pd.isna(lat) or pd.isna(lng):
            return False
        
        # Convert to float to ensure they're valid numbers
        float(lat)
        float(lng)
        
        # Status must be 'Success'
        if str(status).strip() != 'Success':
            return False
        
        return True
    except (ValueError, TypeError):
        return False


def load_existing_coordinates(output_dir='output', file_suffix=''):
    """
    Load existing coordinates from the coordinates CSV file.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix appended to filename (e.g., '_test')
    
    Returns:
        dict: Dictionary mapping property links to their coordinate data
    """
    # Check both test and production CSVs
    coords_csv = os.path.join(output_dir, f'property_listings_with_coordinates{file_suffix}.csv')
    prod_coords_csv = os.path.join('output', 'property_listings_with_coordinates.csv')
    
    existing_coords = {}
    
    for csv_path in [coords_csv, prod_coords_csv]:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    link = row.get('link')
                    if link and has_valid_coordinates(row):
                        existing_coords[link] = {
                            'latitude': row['latitude'],
                            'longitude': row['longitude'],
                            'geocode_status': row['geocode_status']
                        }
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing coordinates from {csv_path}: {e}")
    
    return existing_coords


# ============================================
# MAIN WORKFLOW FUNCTION (for use by property_finder.py)
# ============================================

def geocode_properties(args, input_csv_path=None):
    """
    Geocode property addresses from CSV file.
    
    This function:
    1. Reads property listings from CSV
    2. Checks for existing valid coordinates and skips those properties
    3. Geocodes only new/failed addresses using Google Maps API
    4. Saves results with latitude/longitude to a new CSV
    
    Args:
        args: Argument object with output_dir, file_suffix attributes
        input_csv_path: Path to input CSV (defaults to property_listings_latest.csv in output_dir)
    
    Returns:
        str: Path to output CSV file with coordinates
    """
    # Get output directory and file suffix from args
    output_dir = getattr(args, 'output_dir', 'output')
    file_suffix = getattr(args, 'file_suffix', '')
    
    # Ensure output_dir is an absolute path
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(script_dir, output_dir)
    
    # Determine input CSV path
    if input_csv_path is None:
        input_csv_path = os.path.join(output_dir, f'property_listings_latest{file_suffix}.csv')
    
    # Initialize Google Maps client
    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY is not set in .env!")
    
    gmaps_client = googlemaps.Client(key=GOOGLE_API_KEY)
    
    # Debug info
    print(f"🔍 Looking for .env at: {env_path}")
    print(f"📁 Script directory: {script_dir}")
    print(f"🔑 API Key found: {GOOGLE_API_KEY is not None}")
    if GOOGLE_API_KEY:
        print(f"   Key length: {len(GOOGLE_API_KEY)} characters")
        print(f"   Key starts with: {GOOGLE_API_KEY[:10]}...")
    
    # Load the CSV file
    print(f"\n📂 Loading CSV: {input_csv_path}")
    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"Input CSV not found: {input_csv_path}")
    
    df = pd.read_csv(input_csv_path)
    print(f"   Loaded {len(df)} properties")
    
    # ================================================
    # LOAD EXISTING COORDINATES
    # ================================================
    existing_coords = load_existing_coordinates(output_dir, file_suffix)
    if existing_coords:
        print(f"📍 Found {len(existing_coords)} properties with existing valid coordinates")
    
    # Separate properties that need geocoding vs those that don't
    needs_geocoding = []
    already_geocoded = []
    
    for idx, row in df.iterrows():
        link = row.get('link')
        
        # Check if we have existing valid coordinates for this property
        if link and link in existing_coords:
            already_geocoded.append(idx)
        elif has_valid_coordinates(row):
            # Already has valid coordinates in the current data
            already_geocoded.append(idx)
        else:
            needs_geocoding.append(idx)
    
    print(f"✅ Already geocoded: {len(already_geocoded)} properties (will skip)")
    print(f"📍 Need geocoding: {len(needs_geocoding)} properties")
    
    # Apply existing coordinates to the DataFrame
    for idx, row in df.iterrows():
        link = row.get('link')
        if link and link in existing_coords:
            df.at[idx, 'latitude'] = existing_coords[link]['latitude']
            df.at[idx, 'longitude'] = existing_coords[link]['longitude']
            df.at[idx, 'geocode_status'] = existing_coords[link]['geocode_status']
    
    # If no properties need geocoding, skip the API calls
    if not needs_geocoding:
        print("\n✅ All properties already have valid coordinates! Skipping geocoding.")
    else:
        # Get addresses that need geocoding
        addresses_to_geocode = [(idx, df.at[idx, 'address']) for idx in needs_geocoding]
        
        # Print the addresses to be geocoded
        print(f"\n📍 {len(addresses_to_geocode)} addresses to be geocoded:")
        for i, (idx, addr) in enumerate(addresses_to_geocode[:5], 1):  # Show first 5
            print(f"   {i}. {addr}")
        if len(addresses_to_geocode) > 5:
            print(f"   ... and {len(addresses_to_geocode) - 5} more")
        
        # ================================================
        # TEST GEOCODING FUNCTION
        # ================================================
        print("\n" + "="*70)
        print("TESTING GEOCODING FUNCTION")
        print("="*70)
        test_address = addresses_to_geocode[0][1]
        print(f"\nTesting with: {test_address}")
        result = geocode_address(test_address, gmaps_client)
        
        if result:
            lat, lng = result
            print(f"✅ Success! Coordinates: {lat}, {lng}")
        else:
            print("❌ Failed to geocode address")
        
        # ================================================
        # GEOCODE ADDRESSES THAT NEED IT
        # ================================================
        print("\n" + "="*70)
        print("GEOCODING NEW ADDRESSES")
        print("="*70)
        
        # Track results for summary
        successful_count = 0
        failed_count = 0
        
        # Loop through addresses that need geocoding
        for i, (idx, address) in enumerate(addresses_to_geocode, 1):
            print(f"\n[{i}/{len(addresses_to_geocode)}] Geocoding: {address}")
            
            result = geocode_address(address, gmaps_client)
            
            if result:
                lat, lng = result
                df.at[idx, 'latitude'] = lat
                df.at[idx, 'longitude'] = lng
                df.at[idx, 'geocode_status'] = "Success"
                successful_count += 1
                print(f"  ✅ Success: ({lat:.6f}, {lng:.6f})")
            else:
                df.at[idx, 'latitude'] = None
                df.at[idx, 'longitude'] = None
                df.at[idx, 'geocode_status'] = "Failed"
                failed_count += 1
                print(f"  ❌ Failed to geocode")
            
            # Add a small delay to be polite to the API
            time.sleep(0.1)
        
        # Print summary
        print("\n" + "="*70)
        print("GEOCODING SUMMARY")
        print("="*70)
        print(f"✅ Newly geocoded: {successful_count}/{len(addresses_to_geocode)}")
        print(f"❌ Failed: {failed_count}/{len(addresses_to_geocode)}")
        print(f"⏭️  Skipped (already had coordinates): {len(already_geocoded)}")
    
    # Show which ones failed (including from previous runs)
    failed_df = df[df['geocode_status'] == 'Failed']
    if len(failed_df) > 0:
        print("\n❌ Addresses that failed:")
        for idx, row in failed_df[['address', 'title']].iterrows():
            title_preview = row['title'][:50] + '...' if len(str(row['title'])) > 50 else row['title']
            print(f"  - {row['address']} ({title_preview})")
    
    # Display the results preview
    print("\n" + "="*70)
    print("RESULTS PREVIEW")
    print("="*70)
    display_cols = ['title', 'address', 'latitude', 'longitude', 'geocode_status']
    display_cols = [c for c in display_cols if c in df.columns]
    print(df[display_cols].head(10).to_string(index=False))
    if len(df) > 10:
        print(f"... and {len(df) - 10} more rows")
    
    # Save the results to a new CSV file (with suffix)
    output_file = os.path.join(output_dir, f'property_listings_with_coordinates{file_suffix}.csv')
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n💾 Saved results to: {output_file}")
    
    return output_file


# For standalone execution
if __name__ == "__main__":
    # Create a mock args object for standalone execution
    class MockArgs:
        output_dir = 'output'
    
    args = MockArgs()
    
    # Run the geocoding workflow
    output_path = geocode_properties(args)
    print(f"\n✅ Geocoding complete. Output saved to: {output_path}")
