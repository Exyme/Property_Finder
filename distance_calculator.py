# distance_calculator.py
# Implement distance calculation using Distance Matrix API

import pandas as pd
import os
import googlemaps
from dotenv import load_dotenv
import time
from math import radians, cos, sin, asin, sqrt

import logging
from datetime import datetime
from tracking_summary import tracker
from config import CONFIG, get_type_aware_filename, load_property_type_config, load_api_safety_config
from Email_Fetcher import extract_finnkode

# ================================================
# PRICE CLEANING UTILITY
# ================================================

def clean_price(price_str):
    """
    Clean price string to integer. E.g., '13 000 kr' -> 13000
    
    Args:
        price_str: Price value (can be string like '13 000 kr' or integer)
    
    Returns:
        int: Cleaned price as integer, or None if invalid
    """
    if pd.isna(price_str) or str(price_str).lower() == 'unknown':
        return None
    # Remove 'kr', non-breaking spaces, and regular spaces
    price_str = str(price_str).replace('kr', '').replace('\xa0', '').replace(' ', '').strip()
    try:
        return int(float(price_str))  # Use float first to handle decimal strings
    except ValueError:
        return None

# ================================================
# STEP 1: SET UP ENVIRONMENT
# ================================================

# Get the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load environment variables (API key)
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
# LOGGING SETUP
# ================================================

def setup_logging(output_dir=None):
    """
    Setup logging for the distance calculator.
    
    Args:
        output_dir: Directory for log file (defaults to script_dir/output)
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger('distance_calculator')
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate log messages if logger already configured
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        if output_dir is None:
            output_dir = os.path.join(script_dir, 'output')
        log_file = os.path.join(output_dir, 'distance_calculator.log')
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info("="*70)
        logger.info("LOGGING INITIALIZED")
        logger.info(f"Log file: {log_file}")
        logger.info("="*70)
    
    return logger

# Initialize logger with default output directory
logger = setup_logging()

# ================================================
# RATE LIMITING AND API ERROR HANDLING
# ================================================

MAX_REQUESTS_PER_WINDOW = 90
TIME_WINDOW_SECONDS = 100

# Track API calls
api_call_tracker = {
    'distance_matrix': [],
    'places': [],
    'total_calls': 0
}

def check_rate_limit(api_type='distance_matrix'):
    """
    Checks if we're approaching rate limits and waits if necessary.
    """
    current_time = time.time()
    
    if api_type in api_call_tracker:
        api_call_tracker[api_type] = [
            ts for ts in api_call_tracker[api_type] 
            if current_time - ts < TIME_WINDOW_SECONDS
        ]
        
        calls_in_window = len(api_call_tracker[api_type])
        usage_percentage = calls_in_window / MAX_REQUESTS_PER_WINDOW
        
        if usage_percentage >= 0.95:
            oldest_call = min(api_call_tracker[api_type])
            wait_time = TIME_WINDOW_SECONDS - (current_time - oldest_call) + 1
            logger.debug(f"Rate limit near limit for {api_type} ({calls_in_window}/{MAX_REQUESTS_PER_WINDOW}). Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            
            current_time = time.time()
            api_call_tracker[api_type] = [
                ts for ts in api_call_tracker[api_type] 
                if current_time - ts < TIME_WINDOW_SECONDS
            ]
            
        elif usage_percentage >= 0.90:
            wait_time = 2.0
            logger.debug(f"Rate limit high for {api_type} ({calls_in_window}/{MAX_REQUESTS_PER_WINDOW}). Adding {wait_time}s delay...")
            time.sleep(wait_time)
            
        elif usage_percentage >= 0.80:
            wait_time = 0.5
            logger.debug(f"Rate limit moderate for {api_type} ({calls_in_window}/{MAX_REQUESTS_PER_WINDOW}). Adding {wait_time}s delay...")
            time.sleep(wait_time)


def handle_api_error(error, api_type='distance_matrix', retry_count=0, max_retries=3):
    """
    Handles API errors with exponential backoff retry logic.
    """
    error_str = str(error).lower()
    
    is_rate_limit = (
        '429' in error_str or 
        'rate limit' in error_str or 
        'quota exceeded' in error_str or
        'over_query_limit' in error_str
    )
    
    is_temporary = (
        '500' in error_str or 
        '503' in error_str or 
        'service unavailable' in error_str
    )
    
    if is_rate_limit:
        wait_time = (2 ** retry_count) * 5
        logger.warning(f"Rate limit error for {api_type} (attempt {retry_count + 1}/{max_retries + 1}). Waiting {wait_time}s...")
        time.sleep(wait_time)
        
        if api_type in api_call_tracker:
            api_call_tracker[api_type] = []
        
        return True
        
    elif is_temporary and retry_count < max_retries:
        wait_time = 2 ** retry_count
        logger.warning(f"Temporary error for {api_type} (attempt {retry_count + 1}/{max_retries + 1}): {str(error)}. Retrying in {wait_time}s...")
        time.sleep(wait_time)
        return True
    
    else:
        logger.error(f"API error for {api_type} (after {retry_count} retries): {str(error)}")
        return False


def make_api_call_with_retry(api_func, *args, api_type='distance_matrix', **kwargs):
    """
    Wrapper function that makes an API call with rate limiting and retry logic.
    """
    max_retries = 3
    
    for attempt in range(max_retries + 1):
        try:
            check_rate_limit(api_type)
            result = api_func(*args, **kwargs)
            
            # Track successful API call
            if result is not None:
                current_time = time.time()
                if api_type in api_call_tracker:
                    api_call_tracker[api_type].append(current_time)
                api_call_tracker['total_calls'] += 1
                
                # Update tracker stats
                if api_type == 'distance_matrix':
                    tracker.stats['step5_distance_calculation']['api_calls_distance_matrix'] += 1
                elif api_type == 'places':
                    tracker.stats['step5_distance_calculation']['api_calls_places'] += 1
            
            return result
            
        except Exception as e:
            should_retry = handle_api_error(e, api_type, attempt, max_retries)
            
            if not should_retry:
                return None
    
    logger.error(f"API call failed after {max_retries} retries")
    return None


def get_api_stats():
    """
    Returns statistics about API usage.
    """
    current_time = time.time()
    
    stats = {
        'total_calls': api_call_tracker.get('total_calls', 0),
        'distance_matrix_calls_in_window': 0,
        'places_calls_in_window': 0
    }
    
    for api_type in ['distance_matrix', 'places']:
        if api_type in api_call_tracker:
            stats[f'{api_type}_calls_in_window'] = len([
                ts for ts in api_call_tracker[api_type]
                if current_time - ts < TIME_WINDOW_SECONDS
            ])
    
    return stats


# ================================================
# HAVERSINE DISTANCE FUNCTION (NO API CALL)
# ================================================

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the straight-line distance between two coordinates in kilometers.
    """
    R = 6371.0
    
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    lon1_rad = radians(lon1)
    lon2_rad = radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    
    return R * c


# ================================================
# DEFAULT CONFIGURATION VALUES
# ================================================

DEFAULT_WORK_LAT = 59.899
DEFAULT_WORK_LNG = 10.627
DEFAULT_WORK_ADDRESS = "Snarøyveien 20, 1364 Lysaker"
DEFAULT_MAX_TRAVEL_TIME_MINUTES = 60
DEFAULT_TEST_MODE = True
DEFAULT_TEST_LIMIT = 20
DEFAULT_PLACE_SEARCH_RADIUS_METERS = 10000
DEFAULT_MIN_PLACES_REQUIRED = 1

DEFAULT_PLACE_SEARCH_KEYWORDS = [
    'EVO',
    'SATS',
    'Evo Fitness',
    'SATS Fitness',
    'martial arts gym',
    'boxing gym',
    'jiu jitsu',
    'karate',
    'muay thai',
    'bjj'
]

DEFAULT_PLACE_SEARCH_TYPES = [
    'gym',
    'shopping_mall',
    'grocery_or_supermarket'
]

# Place categories from config.py - users can edit config.py to add/modify categories
# The column_prefix defaults to the category name if not specified
def get_place_categories():
    """Get place categories from config.py with defaults for missing fields."""
    categories = {}
    for name, settings in CONFIG['place_categories'].items():
        categories[name] = {
            'keywords': settings.get('keywords', []),
            'calculate_transit': settings.get('calculate_transit', False),
            'column_prefix': settings.get('column_prefix', name)  # Default to category name
        }
    return categories

DEFAULT_PLACE_CATEGORIES = get_place_categories()

# In-memory cache for place searches (to avoid duplicate API calls)
place_search_cache = {}


# ================================================
# COMPLETION STATUS HELPERS
# ================================================

def check_property_completion_status(row, place_categories):
    """
    Check if a property has all required data fields completed based on the configured place categories.
    
    A property is 'completed' if it has:
    - Valid distance_to_work_km and transit_time_work_minutes
    - For each place category (EVO, SATS, martial_arts):
      - walking_time_{prefix}_minutes must exist
      - If calculate_transit=True for category, transit_time_{prefix}_minutes must also exist
    
    Args:
        row: A pandas Series representing a property row
        place_categories: Dictionary of place categories with their configuration
    
    Returns:
        str: 'completed' if all required fields are present, 'incomplete' otherwise
    """
    try:
        # Check work distance fields (always required)
        distance_km = row.get('distance_to_work_km')
        transit_time = row.get('transit_time_work_minutes')
        
        if pd.isna(distance_km) or pd.isna(transit_time):
            return 'incomplete'
        
        # Check each place category
        for cat_name, cat_config in place_categories.items():
            prefix = cat_config['column_prefix']
            
            # Walking time is always required for each category
            walking_col = f'walking_time_{prefix}_minutes'
            walking_time = row.get(walking_col)
            
            if pd.isna(walking_time):
                return 'incomplete'
            
            # Transit time required only if calculate_transit is True
            if cat_config.get('calculate_transit', False):
                transit_col = f'transit_time_{prefix}_minutes'
                transit_time_place = row.get(transit_col)
                
                if pd.isna(transit_time_place):
                    return 'incomplete'
        
        return 'completed'
    except Exception:
        return 'incomplete'


def load_existing_distance_data(output_dir='output', file_suffix='', property_type='rental'):
    """
    Load existing distance data from the distances CSV file.
    
    Uses finnkode (unique property ID) as the key instead of link to ensure
    properties are recognized even if link format changes between runs.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix appended to filename (e.g., '_test')
        property_type: 'rental' or 'sales' (default: 'rental' for backward compat)
    
    Returns:
        dict: Dictionary mapping finnkode (str) to their distance/place data (dict)
    """
    # Use type-aware filename in the correct output_dir
    distances_filename = get_type_aware_filename('property_listings_with_distances', property_type, file_suffix)
    distances_csv = os.path.join(output_dir, distances_filename)
    
    # Also check old filename format within the same output_dir for backward compatibility (rental only)
    paths_to_check = [distances_csv]
    if property_type == 'rental' and file_suffix == '':
        # For rental properties, also check old filename format (without type prefix) in the same directory
        old_distances_csv = os.path.join(output_dir, 'property_listings_with_distances.csv')
        if old_distances_csv != distances_csv:
            paths_to_check.append(old_distances_csv)
    
    existing_data = {}
    
    # Check all possible file locations
    for csv_path in paths_to_check:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    link = row.get('link')
                    if link:
                        # Extract finnkode from link and use it as the key
                        finnkode = extract_finnkode(link)
                        if finnkode:
                            # Store all relevant columns for this property, keyed by finnkode
                            existing_data[finnkode] = row.to_dict()
            except Exception as e:
                logger.warning(f"Could not load existing distance data from {csv_path}: {e}")
    
    return existing_data


def work_location_matches(prev_lat, prev_lng, current_lat, current_lng, tolerance=0.001):
    """
    Check if previous work location matches current work location within tolerance.
    
    Args:
        prev_lat: Previous work latitude
        prev_lng: Previous work longitude
        current_lat: Current work latitude
        current_lng: Current work longitude
        tolerance: Tolerance in degrees (default 0.001 ~100m)
    
    Returns:
        bool: True if locations match within tolerance
    """
    if pd.isna(prev_lat) or pd.isna(prev_lng):
        return False  # No previous location stored
    return (abs(prev_lat - current_lat) < tolerance and 
            abs(prev_lng - current_lng) < tolerance)


def load_too_far_properties(output_dir='output', file_suffix='', property_type='rental', 
                            current_work_lat=None, current_work_lng=None, current_max_travel_time=None):
    """
    Load properties that were previously determined to be too far away.
    
    These properties exceeded max_transit_time_work_minutes and should be skipped
    for geocoding and distance matrix API calls if the work location hasn't changed.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix appended to filename (e.g., '_test')
        property_type: 'rental' or 'sales' (default: 'rental')
        current_work_lat: Current work location latitude
        current_work_lng: Current work location longitude
        current_max_travel_time: Current maximum travel time in minutes
    
    Returns:
        set: Set of finnkodes (str) for properties that were too far away
    """
    if current_work_lat is None or current_work_lng is None or current_max_travel_time is None:
        return set()  # Can't determine too far properties without current config
    
    # Use type-aware filename (with backward compatibility)
    distances_filename = get_type_aware_filename('property_listings_with_distances', property_type, file_suffix)
    distances_csv = os.path.join(output_dir, distances_filename)
    
    # Try type-aware filename first, then old naming for backward compatibility
    if not os.path.exists(distances_csv) and property_type == 'rental':
        old_distances_csv = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
        if os.path.exists(old_distances_csv):
            distances_csv = old_distances_csv
    
    too_far_finnkodes = set()
    
    if not os.path.exists(distances_csv):
        return too_far_finnkodes  # No existing data
    
    try:
        df = pd.read_csv(distances_csv)
        
        # Check if work_lat/work_lng columns exist (backward compatibility)
        has_work_location = 'work_lat' in df.columns and 'work_lng' in df.columns
        
        for _, row in df.iterrows():
            link = row.get('link')
            if not link:
                continue
            
            finnkode = extract_finnkode(link)
            if not finnkode:
                continue
            
            # Check if property was too far away
            transit_time = row.get('transit_time_work_minutes')
            if pd.isna(transit_time):
                continue  # No transit time data, can't determine if too far
            
            if transit_time <= current_max_travel_time:
                continue  # Property was within limit, don't skip
            
            # Property exceeded limit - check if work location matches
            if has_work_location:
                prev_work_lat = row.get('work_lat')
                prev_work_lng = row.get('work_lng')
                
                if work_location_matches(prev_work_lat, prev_work_lng, current_work_lat, current_work_lng):
                    # Work location matches - this property was too far and should be skipped
                    too_far_finnkodes.add(finnkode)
            else:
                # No work location stored - can't verify if location matches
                # For backward compatibility, don't skip (process the property)
                pass
        
    except Exception as e:
        logger.warning(f"Could not load too far properties from {distances_csv}: {e}")
    
    return too_far_finnkodes


# ================================================
# DISTANCE CALCULATION FUNCTION
# ================================================

def calculate_distance_to_work(property_lat, property_lng, work_lat, work_lng, mode='transit', gmaps_client=None):
    """
    Calculates travel distance and time from a property to the work location using Google Distance Matrix API.
    """
    client = gmaps_client if gmaps_client else gmaps
    
    if not client:
        raise ValueError("Google Maps client not initialized. Check GOOGLE_API_KEY.")
    
    start_time = time.time()
    
    logger.debug(f"Distance Matrix API call - Mode: {mode}, From: ({property_lat}, {property_lng}), To: ({work_lat}, {work_lng})")
    
    try:
        result = make_api_call_with_retry(
            client.distance_matrix,
            origins=[(property_lat, property_lng)],
            destinations=[(work_lat, work_lng)],
            mode=mode,
            units='metric',
            api_type='distance_matrix'
        )
        
        if result is None:
            elapsed_time = time.time() - start_time
            logger.error(f"Distance Matrix API call failed after retries (elapsed: {elapsed_time:.2f}s)")
            return {
                'distance_km': None,
                'duration_minutes': None,
                'status': 'ERROR'
            }
        
        row = result['rows'][0]
        element = row['elements'][0]
        status = element.get('status', 'UNKNOWN')
        
        if status == 'OK':
            distance_value = element['distance']['value']
            duration_value = element['duration']['value']
            
            distance_km = distance_value / 1000.0
            duration_minutes = duration_value / 60.0
            
            logger.info(f"Distance Matrix API - Status: OK - Distance: {distance_km:.2f} km, Time: {duration_minutes:.1f} min ({mode})")
            
            return {
                'distance_km': distance_km,
                'duration_minutes': duration_minutes,
                'status': 'OK'
            }
        else:
            logger.warning(f"Distance Matrix API - Status: {status} (route calculation failed)")
            return {
                'distance_km': None,
                'duration_minutes': None,
                'status': status
            }
            
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Distance Matrix API call failed after {elapsed_time:.2f}s: {str(e)}", exc_info=True)
        return {
            'distance_km': None,
            'duration_minutes': None,
            'status': 'ERROR'
        }


# ================================================
# PLACE SEARCH FUNCTIONS
# ================================================

def find_nearby_places(property_lat, property_lng, 
                       search_keywords=None, 
                       place_types=None, 
                       radius_meters=None,
                       min_places_required=None,
                       gmaps_client=None):
    """
    Searches for nearby places using Google Places API with configurable criteria.
    """
    client = gmaps_client if gmaps_client else gmaps
    
    if not client:
        raise ValueError("Google Maps client not initialized. Check GOOGLE_API_KEY.")
    
    start_time = time.time()
    
    if search_keywords is None:
        search_keywords = DEFAULT_PLACE_SEARCH_KEYWORDS
    if place_types is None:
        place_types = DEFAULT_PLACE_SEARCH_TYPES
    if radius_meters is None:
        radius_meters = DEFAULT_PLACE_SEARCH_RADIUS_METERS
    if min_places_required is None:
        min_places_required = DEFAULT_MIN_PLACES_REQUIRED
    
    logger.debug(f"Places API search - Location: ({property_lat}, {property_lng}), Radius: {radius_meters}m, Keywords: {len(search_keywords)}, Types: {len(place_types)}")
    
    try:
        matches = []
        all_place_ids = set()
        api_calls_made = 0
        
        for keyword in search_keywords:
            try:
                api_calls_made += 1
                results = make_api_call_with_retry(
                    client.places,
                    query=keyword,
                    location=(property_lat, property_lng),
                    radius=radius_meters,
                    api_type='places'
                )
                
                if results and results.get('results'):
                    places_found = len(results['results'])
                    logger.debug(f"Places API (text search '{keyword}') - Found {places_found} places")
                    for place in results['results']:
                        place_id = place.get('place_id')
                        if place_id and place_id not in all_place_ids:
                            all_place_ids.add(place_id)
                            place_name = place.get('name', 'Unknown')
                            matches.append(place_name)
                elif results is None:
                    logger.warning(f"Places API (text search '{keyword}') failed after retries")
                            
            except Exception as e:
                logger.warning(f"Places API (text search '{keyword}') failed: {str(e)}")
                continue
        
        for place_type in place_types:
            try:
                api_calls_made += 1
                results = make_api_call_with_retry(
                    client.places_nearby,
                    location=(property_lat, property_lng),
                    radius=radius_meters,
                    type=place_type,
                    api_type='places'
                )
                
                if results and results.get('results'):
                    places_found = len(results['results'])
                    logger.debug(f"Places API (type '{place_type}') - Found {places_found} places")
                    for place in results['results']:
                        place_id = place.get('place_id')
                        if place_id and place_id not in all_place_ids:
                            all_place_ids.add(place_id)
                            place_name = place.get('name', 'Unknown')
                            matches.append(place_name)
                elif results is None:
                    logger.warning(f"Places API (type '{place_type}') failed after retries")
                            
            except Exception as e:
                logger.warning(f"Places API (type '{place_type}') failed: {str(e)}")
                continue
        
        total_found = len(matches)
        has_match = total_found >= min_places_required
        elapsed_time = time.time() - start_time
        
        logger.info(f"Places API search completed - {total_found} unique places found in {elapsed_time:.2f}s ({api_calls_made} API calls) - Match: {has_match}")
        
        if total_found > 0:
            return {
                'has_match': has_match,
                'matches': matches[:10],
                'total_found': total_found,
                'status': 'OK'
            }
        else:
            logger.debug("Places API search - No results found")
            return {
                'has_match': False,
                'matches': [],
                'total_found': 0,
                'status': 'NO_RESULTS'
            }
            
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Places API search failed after {elapsed_time:.2f}s: {str(e)}", exc_info=True)
        return {
            'has_match': False,
            'matches': [],
            'total_found': 0,
            'status': 'ERROR'
        }


def find_nearest_place_in_category(property_lat, property_lng, category_name, category_config, radius_meters=None, gmaps_client=None):
    """
    Finds the nearest place matching a category's keywords.
    """
    global place_search_cache
    
    client = gmaps_client if gmaps_client else gmaps
    
    if not client:
        raise ValueError("Google Maps client not initialized. Check GOOGLE_API_KEY.")
    
    if radius_meters is None:
        radius_meters = DEFAULT_PLACE_SEARCH_RADIUS_METERS
    
    cache_key = (round(property_lat, 4), round(property_lng, 4), category_name, radius_meters)
    if cache_key in place_search_cache:
        logger.debug(f"Places API (category '{category_name}') - Cache hit, skipping API call")
        return place_search_cache[cache_key]
    
    start_time = time.time()
    keywords = category_config.get('keywords', [])
    all_places = []
    all_place_ids = set()
    api_calls_made = 0
    
    logger.debug(f"Places API (category '{category_name}') - Searching with {len(keywords)} keywords")
    
    try:
        for keyword in keywords:
            try:
                api_calls_made += 1
                results = make_api_call_with_retry(
                    client.places,
                    query=keyword,
                    location=(property_lat, property_lng),
                    radius=radius_meters,
                    api_type='places'
                )
                
                if results and results.get('results'):
                    for place in results['results']:
                        place_id = place.get('place_id')
                        if place_id and place_id not in all_place_ids:
                            all_place_ids.add(place_id)
                            
                            location = place.get('geometry', {}).get('location', {})
                            place_lat = location.get('lat')
                            place_lng = location.get('lng')
                            
                            if place_lat and place_lng:
                                distance_km = haversine_distance(
                                    property_lat, property_lng,
                                    place_lat, place_lng
                                )
                                
                                all_places.append({
                                    'name': place.get('name', 'Unknown'),
                                    'lat': place_lat,
                                    'lng': place_lng,
                                    'place_id': place_id,
                                    'distance_km': distance_km
                                })
                elif results is None:
                    logger.warning(f"Places API (category '{category_name}', keyword '{keyword}') failed after retries")
                
            except Exception as e:
                logger.warning(f"Places API (category '{category_name}', keyword '{keyword}') failed: {str(e)}")
                continue
        
        if not all_places:
            elapsed_time = time.time() - start_time
            logger.info(f"Places API (category '{category_name}') - No places found in {elapsed_time:.2f}s ({api_calls_made} API calls)")
            result = None
        else:
            nearest = min(all_places, key=lambda x: x['distance_km'])
            elapsed_time = time.time() - start_time
            logger.info(f"Places API (category '{category_name}') - Found nearest: {nearest['name']} ({nearest['distance_km']:.2f} km) in {elapsed_time:.2f}s ({api_calls_made} API calls)")
            result = {
                'name': nearest['name'],
                'lat': nearest['lat'],
                'lng': nearest['lng'],
                'place_id': nearest['place_id'],
                'distance_km': nearest['distance_km'],
                'status': 'OK'
            }
        
        place_search_cache[cache_key] = result
        return result
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Places API (category '{category_name}') failed after {elapsed_time:.2f}s: {str(e)}", exc_info=True)
        return None


def calculate_travel_time_to_place(property_lat, property_lng, place_lat, place_lng, modes=['walking'], gmaps_client=None):
    """
    Calculates walking and/or transit time from property to a place using Distance Matrix API.
    """
    client = gmaps_client if gmaps_client else gmaps
    
    if not client:
        raise ValueError("Google Maps client not initialized. Check GOOGLE_API_KEY.")
    
    start_time = time.time()
    result = {
        'walking_minutes': None,
        'transit_minutes': None,
        'status': 'ERROR'
    }
    
    successful_modes = 0
    api_calls_made = 0
    
    logger.debug(f"Distance Matrix API (to place) - Modes: {modes}, From: ({property_lat}, {property_lng}), To: ({place_lat}, {place_lng})")
    
    for mode in modes:
        try:
            api_calls_made += 1
            api_result = make_api_call_with_retry(
                client.distance_matrix,
                origins=[(property_lat, property_lng)],
                destinations=[(place_lat, place_lng)],
                mode=mode,
                units='metric',
                api_type='distance_matrix'
            )
            
            if api_result and 'rows' in api_result:
                element = api_result['rows'][0]['elements'][0]
                status = element.get('status', 'UNKNOWN')
                
                if status == 'OK':
                    duration_seconds = element['duration']['value']
                    duration_minutes = duration_seconds / 60.0
                    
                    if mode == 'walking':
                        result['walking_minutes'] = duration_minutes
                        logger.debug(f"Distance Matrix API ({mode}) - {duration_minutes:.1f} minutes")
                    elif mode == 'transit':
                        result['transit_minutes'] = duration_minutes
                        logger.debug(f"Distance Matrix API ({mode}) - {duration_minutes:.1f} minutes")
                    
                    successful_modes += 1
                else:
                    logger.warning(f"Distance Matrix API ({mode}) - Status: {status}")
            elif api_result is None:
                logger.warning(f"Distance Matrix API ({mode}) call failed after retries")
            
        except Exception as e:
            logger.warning(f"Distance Matrix API ({mode}) call failed: {str(e)}")
            continue
    
    elapsed_time = time.time() - start_time
    if successful_modes == len(modes):
        result['status'] = 'OK'
        logger.debug(f"Distance Matrix API (to place) - All modes successful in {elapsed_time:.2f}s ({api_calls_made} API calls)")
    elif successful_modes > 0:
        result['status'] = 'PARTIAL'
        logger.warning(f"Distance Matrix API (to place) - Partial success: {successful_modes}/{len(modes)} modes in {elapsed_time:.2f}s")
    else:
        result['status'] = 'ERROR'
        logger.error(f"Distance Matrix API (to place) - All modes failed in {elapsed_time:.2f}s")
    
    return result


# ============================================
# MAIN WORKFLOW FUNCTION (for use by property_finder.py)
# ============================================

def calculate_distances_and_filter(args, input_csv_path=None):
    """
    Calculate distances to work, filter by travel time, and find nearby places.
    
    This is the main workflow function that:
    1. Loads geocoded properties from CSV
    2. Calculates travel time to work for each property
    3. Filters properties by maximum travel time
    4. Finds nearby places (gyms, martial arts, etc.) for filtered properties
    5. Saves results to CSV files
    
    Args:
        args: Argument object with configuration attributes:
            - max_transit_time_work: Maximum travel time to work in minutes (default: 60)
            - test_mode: Whether to limit properties processed (default: False)
            - test_limit: Number of properties in test mode (default: 20)
            - output_dir: Output directory for CSV files (default: 'output')
            - work_lat: Work location latitude (default: 59.899)
            - work_lng: Work location longitude (default: 10.627)
            - search_radius: Search radius in meters (default: 10000)
            - facility_keywords: Custom facility keywords (e.g., ['EVO', 'SATS'])
            - place_keywords: Custom place keywords (e.g., ['boxing', 'MMA'])
            - place_types: Custom place types (e.g., ['gym'])
            - property_type: 'rental' or 'sales' (default: 'rental')
        input_csv_path: Path to input CSV with coordinates (defaults to type-aware property_listings_with_coordinates.csv)
    
    Returns:
        str: Path to output CSV file with filtered results
    """
    global place_search_cache
    place_search_cache = {}  # Clear cache for new run
    
    # Get configuration from args
    max_travel_time = getattr(args, 'max_transit_time_work', DEFAULT_MAX_TRAVEL_TIME_MINUTES)
    test_mode = getattr(args, 'test_mode', DEFAULT_TEST_MODE)
    test_limit = getattr(args, 'test_limit', DEFAULT_TEST_LIMIT)
    output_dir = getattr(args, 'output_dir', 'output')
    work_lat = getattr(args, 'work_lat', DEFAULT_WORK_LAT)
    work_lng = getattr(args, 'work_lng', DEFAULT_WORK_LNG)
    search_radius = getattr(args, 'search_radius', DEFAULT_PLACE_SEARCH_RADIUS_METERS)
    facility_keywords = getattr(args, 'facility_keywords', None)
    place_keywords = getattr(args, 'place_keywords', None)
    place_types = getattr(args, 'place_types', None)
    file_suffix = getattr(args, 'file_suffix', '')
    property_type = getattr(args, 'property_type', 'rental')  # Get property_type from args
    
    # Ensure output_dir is an absolute path
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(script_dir, output_dir)
    
    # Determine input CSV path (type-aware)
    if input_csv_path is None:
        # Use type-aware filename for coordinates file
        coords_filename = get_type_aware_filename('property_listings_with_coordinates', property_type, file_suffix)
        input_csv_path = os.path.join(output_dir, coords_filename)
        # Fallback to old naming for backward compatibility (rental only)
        if not os.path.exists(input_csv_path) and property_type == 'rental':
            old_input_csv_path = os.path.join(output_dir, f'property_listings_with_coordinates{file_suffix}.csv')
            if os.path.exists(old_input_csv_path):
                input_csv_path = old_input_csv_path
    
    # Initialize Google Maps client
    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY is not set in .env!")
    
    gmaps_client = googlemaps.Client(key=GOOGLE_API_KEY)
    
    # Build PLACE_CATEGORIES from args
    place_categories = DEFAULT_PLACE_CATEGORIES.copy()
    
    # Update with custom keywords if provided
    if facility_keywords:
        # Split into EVO and SATS if possible
        evo_keywords = [k for k in facility_keywords if 'evo' in k.lower()]
        sats_keywords = [k for k in facility_keywords if 'sats' in k.lower()]
        other_facility = [k for k in facility_keywords if 'evo' not in k.lower() and 'sats' not in k.lower()]
        
        if evo_keywords:
            place_categories['EVO']['keywords'] = evo_keywords
        if sats_keywords:
            place_categories['SATS']['keywords'] = sats_keywords
        if other_facility:
            # Add other facilities to both categories
            place_categories['EVO']['keywords'].extend(other_facility)
            place_categories['SATS']['keywords'].extend(other_facility)
    
    if place_keywords:
        place_categories['martial_arts']['keywords'] = place_keywords
    
    # Log rate limiting setup
    logger.info("Rate limiting initialized:")
    logger.info(f"  Max requests per {TIME_WINDOW_SECONDS}s window: {MAX_REQUESTS_PER_WINDOW}")
    logger.info(f"  API types tracked: distance_matrix, places")
    
    print("="*70)
    print("DISTANCE CALCULATOR SETUP")
    print("="*70)
    print(f"Work location: ({work_lat}, {work_lng})")
    print(f"Maximum travel time: {max_travel_time} minutes (by public transport)")
    print(f"Search radius: {search_radius / 1000:.1f} km")
    print(f"Test mode: {test_mode} (limit: {test_limit})")
    print()
    
    # ================================================
    # LOAD GEOCODED PROPERTIES
    # ================================================
    
    print(f"📂 Loading CSV: {input_csv_path}")
    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"CSV file not found: {input_csv_path}. Please run Stringtocordinates.py first.")
    
    df = pd.read_csv(input_csv_path)
    
    # Filter to only properties that were successfully geocoded
    df_valid = df[df['geocode_status'] == 'Success'].copy()
    
    print(f"Loaded {len(df)} total properties")
    print(f"Found {len(df_valid)} properties with valid coordinates")
    print()
    
    # ================================================
    # LOAD PROPERTIES THAT WERE PREVIOUSLY TOO FAR AWAY
    # ================================================
    too_far_finnkodes = load_too_far_properties(
        output_dir, file_suffix, property_type,
        work_lat, work_lng, max_travel_time
    )
    if too_far_finnkodes:
        print(f"⏭️  Found {len(too_far_finnkodes)} properties that were previously too far away (will skip distance matrix API calls)")
        print()
    
    # ================================================
    # LOAD EXISTING DISTANCE DATA AND MERGE
    # ================================================
    
    # Use type-aware filename (with backward compatibility)
    distances_filename = get_type_aware_filename('property_listings_with_distances', property_type, file_suffix)
    distances_csv_path = os.path.join(output_dir, distances_filename)
    existing_df = None
    
    # Try type-aware filename first, then old naming for backward compatibility
    if not os.path.exists(distances_csv_path) and property_type == 'rental':
        old_distances_csv_path = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
        if os.path.exists(old_distances_csv_path):
            distances_csv_path = old_distances_csv_path
    
    if os.path.exists(distances_csv_path):
        # Check if file is empty
        file_size = os.path.getsize(distances_csv_path)
        if file_size > 0:
            try:
                existing_df = pd.read_csv(distances_csv_path)
                # Check if dataframe is empty (only has header)
                if len(existing_df) == 0:
                    existing_df = None  # Treat as no existing data
                else:
                    tracker.stats['step5_distance_calculation']['existing_in_distances_csv'] = len(existing_df)
                    print(f"📊 Found {len(existing_df)} existing properties in {os.path.basename(distances_csv_path)}")
                    
                    # ================================================
                    # BACKFILL WORK LOCATION AND MAX_TRANSIT_TIME FOR BACKWARD COMPATIBILITY
                    # ================================================
                    # Properties processed before the fix don't have work_lat/work_lng/max_transit_time columns
                    # Assume they were processed with current values and backfill
                    needs_backfill = False
                    backfilled_work_location_count = 0
                    backfilled_max_transit_time_count = 0
                    
                    # Check if work_lat/work_lng columns exist
                    has_work_location_columns = 'work_lat' in existing_df.columns and 'work_lng' in existing_df.columns
                    
                    # Check if max_transit_time_work_minutes column exists
                    has_max_transit_time_column = 'max_transit_time_work_minutes' in existing_df.columns
                    
                    # Check if we have properties with distance data
                    has_distance_data = 'transit_time_work_minutes' in existing_df.columns
                    
                    if has_distance_data:
                        if not has_work_location_columns:
                            # Columns don't exist - add them
                            existing_df['work_lat'] = None
                            existing_df['work_lng'] = None
                        
                        # Backfill missing work_lat/work_lng for properties with distance data
                        distance_mask = existing_df['transit_time_work_minutes'].notna()
                        missing_work_location = (
                            existing_df['work_lat'].isna() | 
                            existing_df['work_lng'].isna()
                        )
                        backfill_work_location_mask = distance_mask & missing_work_location
                        
                        if backfill_work_location_mask.any():
                            existing_df.loc[backfill_work_location_mask, 'work_lat'] = work_lat
                            existing_df.loc[backfill_work_location_mask, 'work_lng'] = work_lng
                            backfilled_work_location_count = backfill_work_location_mask.sum()
                            needs_backfill = True
                        
                        # Backfill max_transit_time_work_minutes for properties with distance data
                        if not has_max_transit_time_column:
                            # Column doesn't exist - add it
                            existing_df['max_transit_time_work_minutes'] = None
                        
                        missing_max_transit_time = existing_df['max_transit_time_work_minutes'].isna()
                        backfill_max_transit_time_mask = distance_mask & missing_max_transit_time
                        
                        if backfill_max_transit_time_mask.any():
                            existing_df.loc[backfill_max_transit_time_mask, 'max_transit_time_work_minutes'] = max_travel_time
                            backfilled_max_transit_time_count = backfill_max_transit_time_mask.sum()
                            needs_backfill = True
                    
                    # Save updated CSV immediately if backfill was performed
                    if needs_backfill:
                        existing_df.to_csv(distances_csv_path, index=False, encoding='utf-8')
                        if backfilled_work_location_count > 0:
                            print(f"✅ Backfilled work location for {backfilled_work_location_count} properties (assumed current work location)")
                        if backfilled_max_transit_time_count > 0:
                            print(f"✅ Backfilled max_transit_time_work_minutes for {backfilled_max_transit_time_count} properties (assumed current max transit time)")
                        print()
            except pd.errors.EmptyDataError:
                # File exists but has no data (empty or only header)
                existing_df = None
            except Exception as e:
                logger.warning(f"Could not load existing distance data from {distances_csv_path}: {e}")
                existing_df = None
        else:
            existing_df = None  # Empty file, treat as no existing data
            
            # #region agent log
            # Check if backup file exists and has these properties
            import json
            import glob
            backup_files = glob.glob(os.path.join(output_dir, f'*backup*.csv'))
            for backup_file in backup_files:
                if 'sales' in backup_file.lower() and property_type == 'sales':
                    try:
                        backup_df = pd.read_csv(backup_file)
                        target_finnkodes = ['437802416', '442148776', '435383650']
                        for target_fk in target_finnkodes:
                            if 'link' in backup_df.columns:
                                matching = backup_df[backup_df['link'].str.contains(target_fk, na=False)]
                                if len(matching) > 0:
                                    with open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a') as f:
                                        f.write(json.dumps({
                                            'sessionId': 'debug-session',
                                            'runId': 'run1',
                                            'hypothesisId': 'E',
                                            'location': 'distance_calculator.py:954',
                                            'message': f'Property {target_fk} found in backup file but main file empty',
                                            'data': {'finnkode': target_fk, 'backup_file': os.path.basename(backup_file), 'backup_count': len(backup_df)},
                                            'timestamp': int(time.time() * 1000)
                                        }) + '\n')
                    except Exception as e:
                        pass
            # #endregion
    else:
        existing_df = None
        tracker.stats['step5_distance_calculation']['existing_in_distances_csv'] = 0
        print("📊 No existing property_listings_with_distances.csv found - starting fresh")
        print()
    
    # Merge: Combine existing properties with new properties from input (if existing_df exists)
    if existing_df is not None:
        # Deduplicate by finnkode, keeping newer data (from df_valid) when duplicates exist
        print(f"🔄 Merging {len(df_valid)} new properties with {len(existing_df)} existing properties...")
        
        # Ensure both DataFrames have compatible columns
        # Add missing columns to df_valid from existing_df
        for col in existing_df.columns:
            if col not in df_valid.columns:
                df_valid[col] = None
        
        # Add missing columns to existing_df from df_valid
        for col in df_valid.columns:
            if col not in existing_df.columns:
                existing_df[col] = None
        
        # Extract finnkode for deduplication (use finnkode instead of link for reliable matching)
        df_valid['_finnkode'] = df_valid['link'].apply(extract_finnkode)
        existing_df['_finnkode'] = existing_df['link'].apply(extract_finnkode)
        
        # Store original date_read from existing_df before merge (to preserve first processing date)
        original_date_read = {}
        if 'date_read' in existing_df.columns:
            for _, row in existing_df.iterrows():
                finnkode = row.get('_finnkode')
                date_read = row.get('date_read')
                if finnkode and not pd.isna(date_read):
                    original_date_read[finnkode] = date_read
        
        # Create a map of finnkodes to new coordinate data
        new_coords_map = {}
        for _, row in df_valid.iterrows():
            finnkode = row.get('_finnkode')
            if finnkode and (pd.notna(row.get('latitude')) or pd.notna(row.get('longitude'))):
                new_coords_map[finnkode] = {
                    'latitude': row.get('latitude'),
                    'longitude': row.get('longitude'),
                    'geocode_status': row.get('geocode_status')
                }
        
        # Concatenate and deduplicate by finnkode (keep first = existing data with distance calculations)
        # We keep 'first' (existing_df) to preserve distance data
        combined = pd.concat([existing_df, df_valid], ignore_index=True)
        df_valid = combined.drop_duplicates(subset=['_finnkode'], keep='first')
        
        # Update coordinates from new data if they're missing in existing data
        for idx, row in df_valid.iterrows():
            finnkode = row.get('_finnkode')
            if finnkode and finnkode in new_coords_map:
                new_coords = new_coords_map[finnkode]
                if pd.isna(row.get('latitude')) and pd.notna(new_coords['latitude']):
                    df_valid.at[idx, 'latitude'] = new_coords['latitude']
                if pd.isna(row.get('longitude')) and pd.notna(new_coords['longitude']):
                    df_valid.at[idx, 'longitude'] = new_coords['longitude']
                if pd.isna(row.get('geocode_status')) and pd.notna(new_coords.get('geocode_status')):
                    df_valid.at[idx, 'geocode_status'] = new_coords['geocode_status']
        
        # Restore original date_read for properties that already existed (preserve first processing date)
        if 'date_read' in df_valid.columns:
            for idx, row in df_valid.iterrows():
                finnkode = row.get('_finnkode')
                if finnkode and finnkode in original_date_read:
                    df_valid.at[idx, 'date_read'] = original_date_read[finnkode]
        
        # Remove temporary finnkode column
        df_valid = df_valid.drop(columns=['_finnkode'], errors='ignore')
        
        # Re-filter to only successfully geocoded (in case existing_df has some without coords)
        if 'geocode_status' in df_valid.columns:
            df_valid = df_valid[df_valid['geocode_status'] == 'Success'].copy()
        
        print(f"✅ Merged total: {len(df_valid)} properties (after deduplication)")
        print()
    else:
        tracker.stats['step5_distance_calculation']['existing_in_distances_csv'] = 0
        print("📊 No existing property_listings_with_distances.csv found - starting fresh")
        print()
    
    # Load existing distance data as dictionary for quick lookup
    existing_data = load_existing_distance_data(output_dir, file_suffix, property_type)
    if existing_data:
        print(f"📊 Found {len(existing_data)} properties with existing distance data for lookup")
    
    # Apply existing data to the dataframe and check completion status
    completed_count = 0
    incomplete_indices = []
    
    for idx, row in df_valid.iterrows():
        link = row.get('link')
        
        # Extract finnkode from link and use it to match existing data
        finnkode = None
        if link:
            finnkode = extract_finnkode(link)
        
        # Apply existing data if available (match by finnkode instead of link)
        if finnkode and finnkode in existing_data:
            existing_row = existing_data[finnkode]
            for col, val in existing_row.items():
                if col not in df_valid.columns:
                    df_valid.at[idx, col] = val
                elif pd.isna(df_valid.at[idx, col]) and not pd.isna(val):
                    df_valid.at[idx, col] = val
                elif col == 'date_read' and not pd.isna(val):
                    # Preserve original date_read from existing data when property already processed
                    # This ensures date_read reflects when property was first processed, not re-processed
                    df_valid.at[idx, col] = val
        
        # Check completion status
        status = check_property_completion_status(df_valid.loc[idx], place_categories)
        df_valid.at[idx, 'processing_status'] = status
        
        if status == 'completed':
            completed_count += 1
            if finnkode:
                logger.info(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: ✅ SKIPPED (already fully processed)")
        else:
            incomplete_indices.append(idx)
            # Don't log here - we'll log after checking if distance data exists
    
    print(f"✅ Already fully completed: {completed_count} properties (will skip)")
    print(f"📍 Incomplete properties: {len(incomplete_indices)} properties")
    print(f"   (Checking which ones need distance data...)")
    print()
    
    # Track new properties to process
    tracker.stats['step5_distance_calculation']['new_properties_to_process'] = len(incomplete_indices)
    
    # Apply test limit if in test mode - only on incomplete properties
    if test_mode and len(incomplete_indices) > test_limit:
        logger.info(f"TEST MODE: Processing only first {test_limit} of {len(incomplete_indices)} incomplete properties")
        incomplete_indices = incomplete_indices[:test_limit]
        logger.info(f"Limited to {len(incomplete_indices)} properties for testing")
        print(f"TEST MODE: Limited to {len(incomplete_indices)} incomplete properties for testing")
    
    # Initialize API safety
    api_safety = load_api_safety_config()
    distance_matrix_calls = 0
    places_calls = 0
    max_distance_matrix_calls = api_safety['max_distance_matrix_calls_per_run']
    max_places_calls = api_safety['max_places_calls_per_run']
    warning_threshold_dm = int(max_distance_matrix_calls * api_safety['warning_threshold_percent'] / 100)
    warning_threshold_places = int(max_places_calls * api_safety['warning_threshold_percent'] / 100)
    
    # Record start time for total execution timing
    script_start_time = time.time()
    
    # Log API statistics at start
    logger.info("="*70)
    logger.info("STARTING PROPERTY PROCESSING")
    logger.info("="*70)
    stats = get_api_stats()
    logger.info(f"Initial API stats - Total calls: {stats['total_calls']}")
    logger.info(f"Properties to process: {len(df_valid)}")
    logger.info(f"API Safety Limits - Distance Matrix: {max_distance_matrix_calls}, Places: {max_places_calls}")
    
    # ================================================
    # TEST DISTANCE CALCULATION
    # ================================================
    
    print("="*70)
    print("TESTING DISTANCE CALCULATION FUNCTION")
    print("="*70)
    
    if len(df_valid) > 0:
        test_property = df_valid.iloc[0]
        test_lat = test_property['latitude']
        test_lng = test_property['longitude']
        test_address = test_property['address']
        
        print(f"\nTesting with property: {test_address}")
        print(f"Property coordinates: ({test_lat}, {test_lng})")
        print(f"Work coordinates: ({work_lat}, {work_lng})")
        print("\nCalculating distance...")
        
        result = calculate_distance_to_work(
            test_lat, test_lng, work_lat, work_lng,
            mode='transit', gmaps_client=gmaps_client
        )
        
        if result['status'] == 'OK':
            print(f"✅ Success!")
            print(f"   Distance: {result['distance_km']:.2f} km")
            print(f"   Travel time: {result['duration_minutes']:.1f} minutes by public transport")
        else:
            print(f"❌ Failed with status: {result['status']}")
    else:
        print("❌ No properties with valid coordinates found to test with!")
    
    # ================================================
    # CALCULATE DISTANCE FOR INCOMPLETE PROPERTIES
    # ================================================
    
    # Initialize columns if they don't exist
    if 'distance_to_work_km' not in df_valid.columns:
        df_valid['distance_to_work_km'] = None
    if 'transit_time_work_minutes' not in df_valid.columns:
        df_valid['transit_time_work_minutes'] = None
    
    # Count properties that need distance calculation
    # Check if distance data exists independently of completion status
    needs_distance = []
    skipped_existing = 0
    skipped_too_far = 0
    for idx in incomplete_indices:
        # Check if distance data is missing (independent of completion status)
        link = df_valid.at[idx, 'link']
        finnkode = extract_finnkode(link) if link else None
        
        # Check if property was previously too far away (skip distance matrix API call if so)
        if finnkode and finnkode in too_far_finnkodes:
            skipped_too_far += 1
            if finnkode:
                logger.info(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: ✅ SKIPPED (previously too far away, no API calls)")
        elif pd.isna(df_valid.at[idx, 'distance_to_work_km']) or pd.isna(df_valid.at[idx, 'transit_time_work_minutes']):
            # This property needs distance calculation - will make API calls
            needs_distance.append(idx)
            if finnkode:
                logger.info(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: 🔄 WILL PROCESS (making API calls)")
        else:
            # Skip - already has distance data
            skipped_existing += 1
            if finnkode:
                logger.info(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: ✅ SKIPPED (already has distance data, no API calls)")
    
    # Track skipped properties in tracker
    tracker.stats['step5_distance_calculation']['properties_skipped_existing'] = skipped_existing
    tracker.stats['step5_distance_calculation']['properties_skipped_too_far'] = skipped_too_far
    
    if skipped_existing > 0:
        print(f"✅ Skipped {skipped_existing} properties (already have distance data, no API calls)")
    if skipped_too_far > 0:
        print(f"✅ Skipped {skipped_too_far} properties (previously too far away, no API calls)")
    print()
    
    if not needs_distance:
        print("="*70)
        print("SKIPPING DISTANCE CALCULATION (all properties already have distance data)")
        print("="*70)
        print("✅ All properties already have distance data!")
        print()
    else:
        print("="*70)
        print("CALCULATING DISTANCES FOR PROPERTIES MISSING DISTANCE DATA")
        print("="*70)
        print(f"🔄 Processing {len(needs_distance)} properties (will make API calls)...")
        print("(This may take a few minutes due to API rate limits)")
        print()
        
        distance_start_time = time.time()
        distance_total = len(needs_distance)
        successful_count = 0
        failed_count = 0
        
        for i, index in enumerate(needs_distance, 1):
            # Check API safety limits before making distance matrix call
            if distance_matrix_calls >= max_distance_matrix_calls:
                if api_safety['hard_stop_on_limit']:
                    logger.error(f"[{property_type.upper()}] [DISTANCE] API LIMIT REACHED: {distance_matrix_calls}/{max_distance_matrix_calls} distance matrix calls. STOPPING.")
                    print(f"\n⚠️  API LIMIT REACHED: {distance_matrix_calls}/{max_distance_matrix_calls} distance matrix calls")
                    print("   Stopping to prevent API credit exhaustion.")
                    break
                else:
                    logger.warning(f"[{property_type.upper()}] [DISTANCE] API LIMIT REACHED but hard_stop_on_limit is False. Continuing...")
            
            # Check warning threshold
            if distance_matrix_calls >= warning_threshold_dm and distance_matrix_calls < max_distance_matrix_calls:
                logger.warning(f"[{property_type.upper()}] [DISTANCE] Approaching API limit: {distance_matrix_calls}/{max_distance_matrix_calls} calls ({int(distance_matrix_calls*100/max_distance_matrix_calls)}%)")
            
            row = df_valid.loc[index]
            property_address = row['address']
            property_lat = row['latitude']
            property_lng = row['longitude']
            link = row.get('link')
            finnkode = extract_finnkode(link) if link else None
            
            # Calculate remaining time estimate
            if i > 1:
                elapsed = time.time() - distance_start_time
                avg_per_property = elapsed / (i - 1)
                remaining = avg_per_property * (distance_total - i + 1)
                remaining_str = f" (~{remaining/60:.1f} min remaining)" if remaining > 60 else f" (~{remaining:.0f}s remaining)"
            else:
                remaining_str = ""
            
            print(f"[{i}/{distance_total}] Processing: {property_address}{remaining_str}")
            if finnkode:
                logger.info(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: Making distance matrix API call")
            
            if i % 10 == 0:
                stats = get_api_stats()
                logger.info(f"Progress: {i}/{distance_total} properties processed")
                logger.info(f"API stats - Total calls: {stats['total_calls']}, "
                           f"Distance Matrix in window: {stats['distance_matrix_calls_in_window']}, "
                           f"Places in window: {stats['places_calls_in_window']}")
            
            result = calculate_distance_to_work(
                property_lat, property_lng, work_lat, work_lng,
                mode='transit', gmaps_client=gmaps_client
            )
            distance_matrix_calls += 1  # Track API call
            
            # Update the DataFrame in place
            df_valid.at[index, 'distance_to_work_km'] = result['distance_km']
            df_valid.at[index, 'transit_time_work_minutes'] = result['duration_minutes']
            
            if result['status'] == 'OK':
                successful_count += 1
                if finnkode:
                    logger.info(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: SUCCESS - Distance: {result['distance_km']:.2f} km, Time: {result['duration_minutes']:.1f} min")
                print(f"  ✅ Distance: {result['distance_km']:.2f} km, Time: {result['duration_minutes']:.1f} min")
            else:
                failed_count += 1
                if finnkode:
                    logger.warning(f"[{property_type.upper()}] [DISTANCE] Property {finnkode}: FAILED - Status: {result['status']}")
                print(f"  ❌ Status: {result['status']}")
        
        print()
        print("="*70)
        print("DISTANCE CALCULATION SUMMARY")
        print("="*70)
        print(f"✅ Successfully calculated: {successful_count}/{distance_total}")
        print(f"❌ Failed: {failed_count}/{distance_total}")
        logger.info(f"Distance calculation summary - Success: {successful_count}/{distance_total}, Failed: {failed_count}/{distance_total}")
    print()
    
    # ================================================
    # FILTER PROPERTIES BY MAX TRAVEL TIME
    # ================================================
    
    print("="*70)
    print("FILTERING PROPERTIES BY MAX TRAVEL TIME")
    print("="*70)
    print(f"Maximum allowed travel time: {max_travel_time} minutes (by public transport)")
    print()
    
    # Ensure transit_time_work_minutes column exists
    if 'transit_time_work_minutes' not in df_valid.columns:
        df_valid['transit_time_work_minutes'] = None
    
    # Filter properties: only include those with valid transit_time within limit
    # This ensures properties outside the range are NEVER included in places API calls
    df_filtered = df_valid[
        (df_valid['transit_time_work_minutes'].notna()) & 
        (df_valid['transit_time_work_minutes'] <= max_travel_time)
    ].copy()
    
    # Re-apply existing place data to df_filtered to ensure all existing place data is available
    for idx in df_filtered.index:
        link = df_filtered.at[idx, 'link']
        finnkode = extract_finnkode(link) if link else None
        if finnkode and finnkode in existing_data:
            existing_row = existing_data[finnkode]
            for col, val in existing_row.items():
                if col not in df_filtered.columns:
                    df_filtered[col] = None
                    df_filtered.at[idx, col] = val
                elif pd.isna(df_filtered.at[idx, col]) and not pd.isna(val):
                    df_filtered.at[idx, col] = val
    
    # ================================================
    # INCLUDE PROPERTIES THAT NOW QUALIFY DUE TO INCREASED MAX_TRANSIT_TIME
    # ================================================
    # If max_transit_time increased, properties that were previously filtered out
    # but now pass the threshold should get places API calls
    properties_added_due_to_increase = 0
    df_filtered_indices = set(df_filtered.index)
    
    for idx, row in df_valid.iterrows():
        if idx in df_filtered_indices:
            continue  # Already in df_filtered
        
        # Check if property passes current max_transit_time filter
        transit_time = row.get('transit_time_work_minutes')
        if pd.isna(transit_time) or transit_time > max_travel_time:
            continue  # Doesn't pass current filter
        
        # Check work location match
        stored_work_lat = row.get('work_lat')
        stored_work_lng = row.get('work_lng')
        if not work_location_matches(stored_work_lat, stored_work_lng, work_lat, work_lng):
            continue  # Work location changed, skip
        
        # Check if property should be included (either has no places data, or was processed with lower max_transit_time)
        stored_max_transit_time = row.get('max_transit_time_work_minutes')
        
        # Determine if property needs places API calls
        needs_places_api = False
        if pd.isna(stored_max_transit_time):
            # No stored max_transit_time - check if has places data
            has_places_data = False
            for cat_name, cat_config in place_categories.items():
                prefix = cat_config['column_prefix']
                walking_col = f'walking_time_{prefix}_minutes'
                if pd.notna(row.get(walking_col)):
                    has_places_data = True
                    break
            if not has_places_data:
                needs_places_api = True
        else:
            # Has stored max_transit_time - check if it's less than current (was filtered out before)
            if stored_max_transit_time < max_travel_time:
                # Was processed with lower threshold, now qualifies - needs places API
                needs_places_api = True
        
        if needs_places_api:
            # Double-check: ensure property passes current max_transit_time filter
            # This is a safety check to prevent properties outside range from being added
            if pd.isna(transit_time) or transit_time > max_travel_time:
                continue  # Safety check failed - skip this property
            
            # Add property to df_filtered
            new_row = row.copy()
            # Ensure all columns from df_filtered exist in new_row
            for col in df_filtered.columns:
                if col not in new_row.index:
                    new_row[col] = None
            # Ensure transit_time_work_minutes is set correctly
            if 'transit_time_work_minutes' not in new_row.index or pd.isna(new_row.get('transit_time_work_minutes')):
                new_row['transit_time_work_minutes'] = transit_time
            # Add to df_filtered (append as new row)
            df_filtered = pd.concat([df_filtered, new_row.to_frame().T], ignore_index=False)
            properties_added_due_to_increase += 1
    
    if properties_added_due_to_increase > 0:
        print(f"✅ Added {properties_added_due_to_increase} properties that now qualify due to increased max_transit_time (will get places API calls)")
        print()
    
    print(f"Properties before filtering: {len(df_valid)}")
    print(f"Properties after filtering: {len(df_filtered)}")
    print(f"Properties removed: {len(df_valid) - len(df_filtered)}")
    print()
    
    if len(df_filtered) > 0:
        print("✅ Properties within travel time limit:")
        print("-" * 70)
        for idx, row in df_filtered.iterrows():
            print(f"  • {row['address']}")
            print(f"    Distance: {row['distance_to_work_km']:.2f} km, Time: {row['transit_time_work_minutes']:.1f} min")
        print()
    else:
        print("⚠️  No properties found within the travel time limit!")
        print()
    
    # Show excluded properties
    df_excluded = df_valid[
        ~((df_valid['transit_time_work_minutes'].notna()) & (df_valid['transit_time_work_minutes'] <= max_travel_time))
    ].copy()
    
    if len(df_excluded) > 0:
        print("❌ Properties excluded:")
        print("-" * 70)
        for idx, row in df_excluded.iterrows():
            if pd.notna(row['transit_time_work_minutes']):
                print(f"  • {row['address']} - Travel time: {row['transit_time_work_minutes']:.1f} min (exceeds limit)")
            else:
                print(f"  • {row['address']} - Travel time calculation failed")
        print()
    
    # ================================================
    # SAVE INTERMEDIATE RESULTS
    # ================================================
    
    print("="*70)
    print("SAVING INTERMEDIATE RESULTS")
    print("="*70)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Store work location in DataFrame before saving
    # Ensure these columns exist for both rental and sales properties
    if 'work_lat' not in df_valid.columns:
        df_valid['work_lat'] = None
    df_valid['work_lat'] = work_lat  # Update to current work location
    if 'work_lng' not in df_valid.columns:
        df_valid['work_lng'] = None
    df_valid['work_lng'] = work_lng  # Update to current work location
    
    # Store max_transit_time_work_minutes in DataFrame before saving
    # Ensure this column exists for both rental and sales properties (critical for deduplication)
    if 'max_transit_time_work_minutes' not in df_valid.columns:
        df_valid['max_transit_time_work_minutes'] = None
    # Update max_transit_time_work_minutes for all properties (including those that exceed limit)
    # This allows us to track what threshold was used when processing each property
    df_valid['max_transit_time_work_minutes'] = max_travel_time
    
    # Use type-aware filenames
    output_filename_all = get_type_aware_filename('property_listings_with_distances', property_type, file_suffix)
    output_file_all = os.path.join(output_dir, output_filename_all)
    df_valid.to_csv(output_file_all, index=False, encoding='utf-8')
    print(f"💾 Saved all properties with distances to: {output_file_all}")
    
    if len(df_filtered) > 0:
        output_filename_filtered = get_type_aware_filename('property_listings_filtered_by_distance', property_type, file_suffix)
        output_file_filtered = os.path.join(output_dir, output_filename_filtered)
        df_filtered.to_csv(output_file_filtered, index=False, encoding='utf-8')
        print(f"💾 Saved filtered properties to: {output_file_filtered}")
    else:
        print("⚠️  No filtered properties to save (all exceeded distance limit)")
    
    print()
    
    # ================================================
    # FIND NEAREST PLACES BY CATEGORY
    # ================================================
    
    if len(df_filtered) > 0:
        # Initialize place columns if they don't exist
        for cat_name, cat_config in place_categories.items():
            prefix = cat_config['column_prefix']
            if f'nearest_{prefix}' not in df_filtered.columns:
                df_filtered[f'nearest_{prefix}'] = None
            if f'walking_time_{prefix}_minutes' not in df_filtered.columns:
                df_filtered[f'walking_time_{prefix}_minutes'] = None
            if cat_config.get('calculate_transit', False):
                if f'transit_time_{prefix}_minutes' not in df_filtered.columns:
                    df_filtered[f'transit_time_{prefix}_minutes'] = None
        
        # Find properties that need place data for any category
        needs_place_data = []
        all_have_place_data = []
        for idx, row in df_filtered.iterrows():
            needs_any = False
            for cat_name, cat_config in place_categories.items():
                prefix = cat_config['column_prefix']
                walking_col = f'walking_time_{prefix}_minutes'
                
                # If any category is missing walking time, we need to process this property
                if pd.isna(row.get(walking_col)):
                    needs_any = True
                    break
            if needs_any:
                needs_place_data.append(idx)
            else:
                all_have_place_data.append(idx)
        
        # Filter out properties that exceed max_transit_time (safety check - should already be filtered, but ensure no API calls for excluded properties)
        # This is critical for sales properties to avoid unnecessary API calls
        original_count = len(needs_place_data)
        needs_place_data = [
            idx for idx in needs_place_data 
            if pd.notna(df_filtered.at[idx, 'transit_time_work_minutes']) 
            and df_filtered.at[idx, 'transit_time_work_minutes'] <= max_travel_time
        ]
        excluded_count = original_count - len(needs_place_data)
        
        # Additional safety: Remove any properties from df_filtered that exceed max_travel_time
        # This ensures df_filtered only contains properties within the limit
        if excluded_count > 0:
            # Find indices in df_filtered that exceed max_travel_time
            exceed_mask = (
                df_filtered['transit_time_work_minutes'].notna() & 
                (df_filtered['transit_time_work_minutes'] > max_travel_time)
            )
            if exceed_mask.any():
                exceeded_indices = df_filtered[exceed_mask].index.tolist()
                df_filtered = df_filtered[~exceed_mask].copy()
                print(f"⚠️  Removed {len(exceeded_indices)} properties from df_filtered that exceeded max_travel_time (safety check)")
        
        if len(all_have_place_data) > 0:
            print(f"✅ Skipped {len(all_have_place_data)} properties (already have all place data, no API calls)")
        if excluded_count > 0:
            print(f"✅ Excluded {excluded_count} properties (exceed max_transit_time, no API calls)")
        print()
        
        if not needs_place_data:
            print("="*70)
            print("SKIPPING PLACE SEARCH (all properties already have place data)")
            print("="*70)
            print("✅ All filtered properties already have place data!")
            print()
        else:
            print("="*70)
            print("FINDING NEAREST PLACES BY CATEGORY FOR PROPERTIES MISSING DATA")
            print("="*70)
            print(f"🔄 Processing {len(needs_place_data)} properties (will make API calls)...")
            print(f"Search radius: {search_radius / 1000:.1f} km")
            print(f"Categories: {', '.join(place_categories.keys())}")
            print("(This may take several minutes due to API rate limits)")
            print()
            
            processed_count = 0
            total_properties = len(needs_place_data)
            place_start_time = time.time()
            category_found_counts = {cat_name: 0 for cat_name in place_categories.keys()}
            
            for idx in needs_place_data:
                row = df_filtered.loc[idx]
                processed_count += 1
                property_address = row['address']
                property_lat = row['latitude']
                property_lng = row['longitude']
                
                if processed_count > 1:
                    elapsed = time.time() - place_start_time
                    avg_per_property = elapsed / (processed_count - 1)
                    remaining = avg_per_property * (total_properties - processed_count + 1)
                    remaining_str = f" (~{remaining/60:.1f} min remaining)" if remaining > 60 else f" (~{remaining:.0f}s remaining)"
                else:
                    remaining_str = ""
                
                print(f"[{processed_count}/{total_properties}] Processing: {property_address}{remaining_str}")
                
                for cat_name, cat_config in place_categories.items():
                    prefix = cat_config['column_prefix']
                    walking_col = f'walking_time_{prefix}_minutes'
                    
                    # Skip if this category already has data
                    if not pd.isna(df_filtered.at[idx, walking_col]):
                        existing_name = df_filtered.at[idx, f'nearest_{prefix}']
                        print(f"  ⏭️  {cat_name}: Already have data ({existing_name})")
                        category_found_counts[cat_name] += 1
                        continue
                    
                    # Check API safety limits before making places API call
                    if places_calls >= max_places_calls:
                        if api_safety['hard_stop_on_limit']:
                            logger.error(f"[{property_type.upper()}] [PLACES] API LIMIT REACHED: {places_calls}/{max_places_calls} places calls. STOPPING.")
                            print(f"\n⚠️  API LIMIT REACHED: {places_calls}/{max_places_calls} places calls")
                            print("   Stopping to prevent API credit exhaustion.")
                            break
                        else:
                            logger.warning(f"[{property_type.upper()}] [PLACES] API LIMIT REACHED but hard_stop_on_limit is False. Continuing...")
                    
                    # Check warning threshold
                    if places_calls >= warning_threshold_places and places_calls < max_places_calls:
                        logger.warning(f"[{property_type.upper()}] [PLACES] Approaching API limit: {places_calls}/{max_places_calls} calls ({int(places_calls*100/max_places_calls)}%)")
                    
                    link = row.get('link')
                    finnkode = extract_finnkode(link) if link else None
                    if finnkode:
                        logger.info(f"[{property_type.upper()}] [PLACES] Property {finnkode}: Making places API call for category '{cat_name}'")
                    
                    try:
                        nearest = find_nearest_place_in_category(
                            property_lat, property_lng,
                            cat_name, cat_config,
                            radius_meters=search_radius,
                            gmaps_client=gmaps_client
                        )
                        places_calls += 1  # Track API call (approximate - find_nearby_places makes multiple calls)
                        
                        if nearest and nearest.get('status') == 'OK':
                            df_filtered.at[idx, f'nearest_{prefix}'] = nearest['name']
                            
                            # Check if we already have the required travel times
                            modes_needed = []
                            if pd.isna(df_filtered.at[idx, f'walking_time_{prefix}_minutes']):
                                modes_needed.append('walking')
                            if cat_config.get('calculate_transit', False) and pd.isna(df_filtered.at[idx, f'transit_time_{prefix}_minutes']):
                                modes_needed.append('transit')
                            
                            if not modes_needed:
                                # Already have all required travel times, skip API call
                                if finnkode:
                                    logger.info(f"[{property_type.upper()}] [PLACES] Property {finnkode}: SKIPPED transit time calculation (already have data)")
                                category_found_counts[cat_name] += 1
                                walk_str = f"{df_filtered.at[idx, f'walking_time_{prefix}_minutes']:.1f} min walk" if pd.notna(df_filtered.at[idx, f'walking_time_{prefix}_minutes']) else "N/A"
                                transit_str = ""
                                if cat_config.get('calculate_transit', False) and pd.notna(df_filtered.at[idx, f'transit_time_{prefix}_minutes']):
                                    transit_str = f", {df_filtered.at[idx, f'transit_time_{prefix}_minutes']:.1f} min transit"
                                print(f"  ✅ {cat_name}: {nearest['name']} ({nearest['distance_km']:.2f} km) - {walk_str}{transit_str} (using existing data)")
                            else:
                                travel_times = calculate_travel_time_to_place(
                                    property_lat, property_lng,
                                    nearest['lat'], nearest['lng'],
                                    modes=modes_needed,  # Only request modes we need
                                    gmaps_client=gmaps_client
                                )
                                
                                # Update only the modes we calculated
                                if 'walking' in modes_needed:
                                    df_filtered.at[idx, f'walking_time_{prefix}_minutes'] = travel_times.get('walking_minutes')
                                
                                if 'transit' in modes_needed and cat_config.get('calculate_transit', False):
                                    df_filtered.at[idx, f'transit_time_{prefix}_minutes'] = travel_times.get('transit_minutes')
                                elif cat_config.get('calculate_transit', False):
                                    # Preserve existing transit time if we didn't request it
                                    pass
                                
                                category_found_counts[cat_name] += 1
                                
                                walk_str = f"{travel_times.get('walking_minutes') or df_filtered.at[idx, f'walking_time_{prefix}_minutes']:.1f} min walk" if (travel_times.get('walking_minutes') or pd.notna(df_filtered.at[idx, f'walking_time_{prefix}_minutes'])) else "N/A"
                                transit_str = ""
                                if cat_config.get('calculate_transit', False):
                                    transit_val = travel_times.get('transit_minutes') or df_filtered.at[idx, f'transit_time_{prefix}_minutes']
                                    if transit_val:
                                        transit_str = f", {transit_val:.1f} min transit"
                                
                                print(f"  ✅ {cat_name}: {nearest['name']} ({nearest['distance_km']:.2f} km) - {walk_str}{transit_str}")
                        else:
                            print(f"  ⚠️  {cat_name}: No places found within {search_radius / 1000:.1f} km")
                            
                    except Exception as e:
                        print(f"  ❌ {cat_name}: Error - {str(e)}")
                
                print()
            
            print()
            print("="*70)
            print("PLACE SEARCH SUMMARY")
            print("="*70)
            
            for cat_name in place_categories.keys():
                prefix = place_categories[cat_name]['column_prefix']
                found_count = df_filtered[f'walking_time_{prefix}_minutes'].notna().sum()
                total = len(df_filtered)
                print(f"✅ {cat_name}: Found {found_count}/{total} properties with nearby places")
    
    # ================================================
    # UPDATE COMPLETION STATUS
    # ================================================
    
    # Update status for all filtered properties
    for idx, row in df_filtered.iterrows():
        status = check_property_completion_status(row, place_categories)
        df_filtered.at[idx, 'processing_status'] = status
        df_valid.at[idx, 'processing_status'] = status
    
    completed = (df_filtered['processing_status'] == 'completed').sum()
    incomplete = (df_filtered['processing_status'] == 'incomplete').sum()
    print()
    print(f"📊 Status update: {completed} completed, {incomplete} incomplete")
    
    # ================================================
    # SAVE FINAL RESULTS
    # ================================================
    
    print()
    print("="*70)
    print("SAVING FINAL RESULTS")
    print("="*70)
    
    base_columns = ['title', 'address', 'price', 'size', 'link', 'date_read']
    work_columns = ['distance_to_work_km', 'transit_time_work_minutes']
    
    category_columns = []
    for cat_name, cat_config in place_categories.items():
        prefix = cat_config['column_prefix']
        category_columns.append(f'nearest_{prefix}')
        category_columns.append(f'walking_time_{prefix}_minutes')
        if cat_config.get('calculate_transit', False):
            category_columns.append(f'transit_time_{prefix}_minutes')
    
    all_columns = base_columns + work_columns + category_columns
    
    final_columns = [col for col in all_columns if col in df_filtered.columns]
    remaining_columns = [col for col in df_filtered.columns if col not in final_columns]
    final_columns.extend(remaining_columns)
    
    if len(df_filtered) > 0:
        df_filtered = df_filtered[final_columns]
    
    # ================================================
    # PREPARE df_valid WITH ALL COLUMNS AND DATA
    # ================================================
    
    # Add place category columns if they don't exist
    for cat_name, cat_config in place_categories.items():
        prefix = cat_config['column_prefix']
        if f'nearest_{prefix}' not in df_valid.columns:
            df_valid[f'nearest_{prefix}'] = None
            df_valid[f'walking_time_{prefix}_minutes'] = None
            if cat_config.get('calculate_transit', False):
                df_valid[f'transit_time_{prefix}_minutes'] = None
    
    # Update df_valid with data from df_filtered (for place search results)
    for col in df_filtered.columns:
        if col in df_valid.columns:
            df_valid.loc[df_filtered.index, col] = df_filtered[col]
    
    # Prepare df_valid columns in correct order
    final_columns_valid = [col for col in all_columns if col in df_valid.columns]
    remaining_columns_valid = [col for col in df_valid.columns if col not in final_columns_valid]
    final_columns_valid.extend(remaining_columns_valid)
    df_valid = df_valid[final_columns_valid]
    
    # Track final statistics
    completed = (df_valid['processing_status'] == 'completed').sum() if 'processing_status' in df_valid.columns else 0
    incomplete = (df_valid['processing_status'] == 'incomplete').sum() if 'processing_status' in df_valid.columns else 0
    tracker.stats['step5_distance_calculation']['properties_processed'] = len(df_valid)
    tracker.stats['step5_distance_calculation']['properties_completed'] = completed
    tracker.stats['step5_distance_calculation']['properties_incomplete'] = incomplete
    tracker.stats['step5_distance_calculation']['final_count'] = completed  # Only completed properties count
    
    # ================================================
    # CLEAN PRICE DATA BEFORE SAVING
    # ================================================
    # Ensure all prices are clean integers (remove 'kr' suffix, spaces, etc.)
    if 'price' in df_valid.columns:
        df_valid['price'] = df_valid['price'].apply(clean_price)
        print(f"🧹 Cleaned price column (removed 'kr' suffix and non-numeric characters)")
    
    # ================================================
    # SAVE property_listings_complete.csv (ALL properties) - type-aware
    # ================================================
    # This file contains ALL properties (completed + incomplete) for reference
    output_filename_complete = get_type_aware_filename('property_listings_complete', property_type, file_suffix)
    output_file_complete = os.path.join(output_dir, output_filename_complete)
    df_valid.to_csv(output_file_complete, index=False, encoding='utf-8')
    print(f"💾 Saved ALL property listings to: {output_file_complete}")
    print(f"   Total properties: {len(df_valid)} (completed: {completed}, incomplete: {incomplete})")
    
    # ================================================
    # SAVE property_listings_with_distances.csv (PROCESSED properties) - type-aware
    # ================================================
    # This is the source of truth for duplicate checking - contains all processed properties
    # For sales: Save all properties with coordinates and work distance (even if place data incomplete)
    # For rental: Save only fully completed properties (with all place data)
    output_filename_final = get_type_aware_filename('property_listings_with_distances', property_type, file_suffix)
    output_file_final = os.path.join(output_dir, output_filename_final)
    
    if property_type == 'sales':
        # For sales: Save all properties that have been geocoded (successfully processed from emails)
        # This ensures all sales properties from emails are included, even if distance/place data incomplete
        # Priority: geocoded > has work distance > fully completed
        if 'geocode_status' in df_valid.columns:
            df_to_save = df_valid[df_valid['geocode_status'] == 'Success'].copy()
        else:
            # Fallback: if no geocode_status, save all properties with coordinates
            df_to_save = df_valid[
                (df_valid['latitude'].notna()) & 
                (df_valid['longitude'].notna())
            ].copy()
        
        # #region agent log
        target_finnkodes = ['437802416', '442148776', '435383650']
        for target_fk in target_finnkodes:
            if 'link' in df_to_save.columns:
                matching = df_to_save[df_to_save['link'].str.contains(target_fk, na=False)]
                if len(matching) > 0:
                    import json
                    try:
                        with open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a') as f:
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'D',
                                'location': 'distance_calculator.py:1582',
                                'message': f'Property {target_fk} in df_to_save',
                                'data': {'finnkode': target_fk, 'geocode_status': matching.iloc[0].get('geocode_status'), 'has_distance': pd.notna(matching.iloc[0].get('distance_to_work_km'))},
                                'timestamp': int(time.time() * 1000)
                            }) + '\n')
                    except: pass
                else:
                    import json
                    try:
                        with open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a') as f:
                            f.write(json.dumps({
                                'sessionId': 'debug-session',
                                'runId': 'run1',
                                'hypothesisId': 'D',
                                'location': 'distance_calculator.py:1582',
                                'message': f'Property {target_fk} NOT in df_to_save',
                                'data': {'finnkode': target_fk, 'df_to_save_count': len(df_to_save), 'df_valid_count': len(df_valid)},
                                'timestamp': int(time.time() * 1000)
                            }) + '\n')
                    except: pass
        # #endregion
        
        print(f"💾 Saving PROCESSED sales properties (all geocoded properties from emails) to: {output_file_final}")
        print(f"   Processed sales properties: {len(df_to_save)}")
        if len(df_to_save) > 0:
            with_distance = df_to_save['distance_to_work_km'].notna().sum() if 'distance_to_work_km' in df_to_save.columns else 0
            print(f"   Properties with work distance: {with_distance}/{len(df_to_save)}")
    else:
        # For rental: Save only fully completed properties (with all place data)
        df_to_save = df_valid[df_valid['processing_status'] == 'completed'].copy()
        print(f"💾 Saving COMPLETED rental properties (fully processed with all place data) to: {output_file_final}")
        print(f"   Completed rental properties: {len(df_to_save)}")
    
    # Save processed properties to property_listings_with_distances.csv
    df_to_save.to_csv(output_file_final, index=False, encoding='utf-8')
    print(f"✅ Saved {len(df_to_save)} properties to: {output_file_final}")
    
    # Print API usage summary
    print("\n" + "="*70)
    print("API USAGE SUMMARY")
    print("="*70)
    print(f"📊 Distance Matrix API: {distance_matrix_calls}/{max_distance_matrix_calls} calls ({int(distance_matrix_calls*100/max_distance_matrix_calls)}%)")
    print(f"📊 Places API: {places_calls}/{max_places_calls} calls ({int(places_calls*100/max_places_calls)}%)")
    logger.info(f"[{property_type.upper()}] API Usage Summary - Distance Matrix: {distance_matrix_calls}/{max_distance_matrix_calls}, Places: {places_calls}/{max_places_calls}")
    
    print()
    print("="*70)
    print("✅ DISTANCE AND PLACE CALCULATIONS COMPLETE!")
    print("="*70)
    print(f"Summary:")
    print(f"  • Total properties in database: {len(df_valid)}")
    print(f"  • Completed (all data): {completed}")
    print(f"  • Incomplete (missing place data): {incomplete}")
    print(f"  • Properties within {max_travel_time} min travel time: {len(df_filtered)}")
    print(f"  • Categories searched: {', '.join(place_categories.keys())}")
    print(f"  • Search radius: {search_radius / 1000:.1f} km")
    print()
    print("Output files:")
    print(f"  • {output_file_final} (ONLY completed properties - used for duplicate checking)")
    print(f"  • {output_file_complete} (ALL properties - completed + incomplete)")
    print("="*70)
    
    # Log final API statistics
    logger.info("="*70)
    logger.info("FINAL API USAGE STATISTICS")
    logger.info("="*70)
    final_stats = get_api_stats()
    logger.info(f"Total API calls: {final_stats['total_calls']}")
    logger.info(f"Distance Matrix API: {final_stats['distance_matrix_calls_in_window']} calls in current window")
    logger.info(f"Places API: {final_stats['places_calls_in_window']} calls in current window")
    
    total_execution_time = time.time() - script_start_time
    total_minutes = int(total_execution_time // 60)
    total_seconds = int(total_execution_time % 60)
    logger.info(f"Total execution time: {total_minutes} minutes {total_seconds} seconds ({total_execution_time:.1f} seconds)")
    logger.info("="*70)
    
    return output_file_final


# For standalone execution
if __name__ == "__main__":
    # Create a mock args object for standalone execution
    class MockArgs:
        max_transit_time_work = 60
        test_mode = True
        test_limit = 20
        output_dir = 'output'
        work_lat = 59.899
        work_lng = 10.627
        search_radius = 10000
        facility_keywords = None
        place_keywords = None
        place_types = None
    
    args = MockArgs()
    
    # Run the main workflow
    output_path = calculate_distances_and_filter(args)
    print(f"\n✅ Processing complete. Output saved to: {output_path}")
