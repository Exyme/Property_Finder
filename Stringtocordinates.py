import pandas as pd
import os
import time
import googlemaps
import logging
from datetime import datetime
from dotenv import load_dotenv
from tracking_summary import tracker
from config import get_type_aware_filename, load_api_safety_config
from Email_Fetcher import extract_finnkode
from distance_calculator import load_too_far_properties

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


def load_existing_coordinates(output_dir='output', file_suffix='', property_type='rental'):
    """
    Load existing coordinates from the coordinates CSV file.
    
    Uses finnkode (unique property ID) as the key instead of link to ensure
    properties are recognized even if link format changes between runs.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix appended to filename (e.g., '_test')
        property_type: 'rental' or 'sales' (default: 'rental' for backward compat)
    
    Returns:
        dict: Dictionary mapping finnkode (str) to their coordinate data (dict)
    """
    # Use type-aware filename in the correct output_dir
    coords_filename = get_type_aware_filename('property_listings_with_coordinates', property_type, file_suffix)
    coords_csv = os.path.join(output_dir, coords_filename)
    
    # Also check old filename format within the same output_dir for backward compatibility (rental only)
    paths_to_check = [coords_csv]
    if property_type == 'rental' and file_suffix == '':
        # For rental properties, also check old filename format (without type prefix) in the same directory
        old_coords_csv = os.path.join(output_dir, 'property_listings_with_coordinates.csv')
        if old_coords_csv != coords_csv:
            paths_to_check.append(old_coords_csv)
    
    existing_coords = {}
    
    # Check all possible file locations
    for csv_path in paths_to_check:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    link = row.get('link')
                    if link and has_valid_coordinates(row):
                        # Extract finnkode from link and use it as the key
                        finnkode = extract_finnkode(link)
                        if finnkode:
                            existing_coords[finnkode] = {
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
        args: Argument object with output_dir, file_suffix, property_type attributes
        input_csv_path: Path to input CSV (defaults to type-aware property_listings_latest.csv in output_dir)
    
    Returns:
        str: Path to output CSV file with coordinates
    """
    # Get output directory and file suffix from args
    output_dir = getattr(args, 'output_dir', 'output')
    file_suffix = getattr(args, 'file_suffix', '')
    property_type = getattr(args, 'property_type', 'rental')  # Get property_type from args
    
    # Ensure output_dir is an absolute path
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(script_dir, output_dir)
    
    # Determine input CSV path (type-aware, with backward compatibility)
    if input_csv_path is None:
        # Try type-aware filename first
        input_filename = get_type_aware_filename('property_listings_latest', property_type, file_suffix)
        input_csv_path = os.path.join(output_dir, input_filename)
        # If not found, try old naming for backward compatibility
        if not os.path.exists(input_csv_path) and property_type == 'rental':
            old_input_csv_path = os.path.join(output_dir, f'property_listings_latest{file_suffix}.csv')
            if os.path.exists(old_input_csv_path):
                input_csv_path = old_input_csv_path
    
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
    existing_coords = load_existing_coordinates(output_dir, file_suffix, property_type)
    if existing_coords:
        print(f"📍 Found {len(existing_coords)} properties with existing valid coordinates")
    
    # ================================================
    # LOAD PROPERTIES THAT WERE PREVIOUSLY TOO FAR AWAY
    # ================================================
    # Get work location and max travel time from args (with defaults)
    work_lat = getattr(args, 'work_lat', None)
    work_lng = getattr(args, 'work_lng', None)
    max_transit_time_work = getattr(args, 'max_transit_time_work', None)
    
    too_far_finnkodes = set()
    if work_lat is not None and work_lng is not None and max_transit_time_work is not None:
        too_far_finnkodes = load_too_far_properties(
            output_dir, file_suffix, property_type,
            work_lat, work_lng, max_transit_time_work
        )
        if too_far_finnkodes:
            print(f"⏭️  Found {len(too_far_finnkodes)} properties that were previously too far away (will skip geocoding)")
    
    # Initialize API safety and logging
    api_safety = load_api_safety_config()
    geocoding_calls = 0
    max_geocoding_calls = api_safety['max_geocoding_calls_per_run']
    warning_threshold = int(max_geocoding_calls * api_safety['warning_threshold_percent'] / 100)
    
    # Setup logging
    logger = logging.getLogger('geocoding')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Separate properties that need geocoding vs those that don't
    needs_geocoding = []
    already_geocoded = []
    skipped_too_far = 0
    
    for idx, row in df.iterrows():
        link = row.get('link')
        
        # Extract finnkode from link and use it to match existing coordinates
        finnkode = None
        if link:
            finnkode = extract_finnkode(link)
        
        # Check if property was previously too far away (skip geocoding if so)
        if finnkode and finnkode in too_far_finnkodes:
            already_geocoded.append(idx)
            skipped_too_far += 1
            logger.info(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: SKIPPED (previously too far away, no API call)")
        # Check if we have existing valid coordinates for this property (match by finnkode)
        elif finnkode and finnkode in existing_coords:
            already_geocoded.append(idx)
            logger.info(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: SKIPPED (already geocoded, no API call)")
        elif has_valid_coordinates(row):
            # Already has valid coordinates in the current data
            already_geocoded.append(idx)
            if finnkode:
                logger.info(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: SKIPPED (has valid coordinates, no API call)")
        else:
            needs_geocoding.append(idx)
            if finnkode:
                logger.info(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: NEEDS geocoding (will make API call)")
    
    print(f"✅ Already geocoded: {len(already_geocoded)} properties (will skip)")
    if skipped_too_far > 0:
        print(f"   (Including {skipped_too_far} properties that were previously too far away)")
    print(f"📍 Need geocoding: {len(needs_geocoding)} properties")
    
    # Apply existing coordinates to the DataFrame (match by finnkode)
    for idx, row in df.iterrows():
        link = row.get('link')
        if link:
            finnkode = extract_finnkode(link)
            if finnkode and finnkode in existing_coords:
                df.at[idx, 'latitude'] = existing_coords[finnkode]['latitude']
                df.at[idx, 'longitude'] = existing_coords[finnkode]['longitude']
                df.at[idx, 'geocode_status'] = existing_coords[finnkode]['geocode_status']
    
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
            # Check API safety limits before making call
            if geocoding_calls >= max_geocoding_calls:
                if api_safety['hard_stop_on_limit']:
                    logger.error(f"[{property_type.upper()}] [GEOCODING] API LIMIT REACHED: {geocoding_calls}/{max_geocoding_calls} calls. STOPPING to prevent credit exhaustion.")
                    print(f"\n⚠️  API LIMIT REACHED: {geocoding_calls}/{max_geocoding_calls} geocoding calls")
                    print("   Stopping to prevent API credit exhaustion.")
                    break
                else:
                    logger.warning(f"[{property_type.upper()}] [GEOCODING] API LIMIT REACHED but hard_stop_on_limit is False. Continuing...")
            
            # Check warning threshold
            if geocoding_calls >= warning_threshold and geocoding_calls < max_geocoding_calls:
                logger.warning(f"[{property_type.upper()}] [GEOCODING] Approaching API limit: {geocoding_calls}/{max_geocoding_calls} calls ({int(geocoding_calls*100/max_geocoding_calls)}%)")
            
            # Extract finnkode for logging
            link = df.at[idx, 'link']
            finnkode = extract_finnkode(link) if link else None
            
            print(f"\n[{i}/{len(addresses_to_geocode)}] Geocoding: {address}")
            if finnkode:
                logger.info(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: Making API call for address '{address}'")
            
            result = geocode_address(address, gmaps_client)
            geocoding_calls += 1  # Track API call
            
            if result:
                lat, lng = result
                df.at[idx, 'latitude'] = lat
                df.at[idx, 'longitude'] = lng
                df.at[idx, 'geocode_status'] = "Success"
                if finnkode:
                    logger.info(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: SUCCESS - Coordinates: {lat}, {lng}")
                successful_count += 1
                print(f"  ✅ Success: ({lat:.6f}, {lng:.6f})")
            else:
                df.at[idx, 'latitude'] = None
                df.at[idx, 'longitude'] = None
                df.at[idx, 'geocode_status'] = "Failed"
                failed_count += 1
                if finnkode:
                    logger.warning(f"[{property_type.upper()}] [GEOCODING] Property {finnkode}: FAILED to geocode address '{address}'")
                print(f"  ❌ Failed to geocode")
            
            # Add a small delay to be polite to the API
            time.sleep(0.1)
        
        # Track geocoding results
        tracker.stats['step4_geocoding']['geocoding_success'] = successful_count + len(already_geocoded)
        tracker.stats['step4_geocoding']['geocoding_failed'] = failed_count
        
        # Print API usage summary
        print(f"\n📊 API Usage: {geocoding_calls}/{max_geocoding_calls} geocoding calls ({int(geocoding_calls*100/max_geocoding_calls)}%)")
        logger.info(f"[{property_type.upper()}] [GEOCODING] API Usage Summary: {geocoding_calls}/{max_geocoding_calls} calls made")
        
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
    
    # Save the results to a new CSV file (type-aware, with suffix)
    output_filename = get_type_aware_filename('property_listings_with_coordinates', property_type, file_suffix)
    output_file = os.path.join(output_dir, output_filename)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n💾 Saved results to: {output_file}")
    
    # Track final count after geocoding
    valid_coords = df[df['geocode_status'] == 'Success']
    tracker.stats['step4_geocoding']['after_count'] = len(valid_coords)
    
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
