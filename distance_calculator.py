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

DEFAULT_PLACE_CATEGORIES = {
    'EVO': {
        'keywords': ['EVO', 'Evo Fitness', 'EVO Fitness'],
        'calculate_transit': False,
        'column_prefix': 'EVO'
    },
    'SATS': {
        'keywords': ['SATS', 'SATS Fitness'],
        'calculate_transit': False,
        'column_prefix': 'SATS'
    },
    'martial_arts': {
        'keywords': ['martial arts gym', 'boxing gym', 'jiu jitsu', 'MMA', 'muay thai', 'bjj', 'Wrestling'],
        'calculate_transit': True,
        'column_prefix': 'martial_arts'
    }
}

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


def load_existing_distance_data(output_dir='output', file_suffix=''):
    """
    Load existing distance data from the distances CSV file.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix appended to filename (e.g., '_test')
    
    Returns:
        dict: Dictionary mapping property links to their distance/place data
    """
    # Check both test and production CSVs
    distances_csv = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
    prod_distances_csv = os.path.join('output', 'property_listings_with_distances.csv')
    
    existing_data = {}
    
    for csv_path in [distances_csv, prod_distances_csv]:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    link = row.get('link')
                    if link:
                        # Store all relevant columns for this property
                        existing_data[link] = row.to_dict()
            except Exception as e:
                logger.warning(f"Could not load existing distance data from {csv_path}: {e}")
    
    return existing_data


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
        input_csv_path: Path to input CSV with coordinates (defaults to property_listings_with_coordinates.csv)
    
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
    
    # Ensure output_dir is an absolute path
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(script_dir, output_dir)
    
    # Determine input CSV path
    if input_csv_path is None:
        input_csv_path = os.path.join(output_dir, f'property_listings_with_coordinates{file_suffix}.csv')
    
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
    # LOAD EXISTING DISTANCE DATA AND MERGE
    # ================================================
    
    distances_csv_path = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
    existing_df = None
    
    if os.path.exists(distances_csv_path):
        existing_df = pd.read_csv(distances_csv_path)
        tracker.stats['step5_distance_calculation']['existing_in_distances_csv'] = len(existing_df)
        print(f"📊 Found {len(existing_df)} existing properties in property_listings_with_distances.csv")
        
        # Merge: Combine existing properties with new properties from input
        # Deduplicate by 'link', keeping newer data (from df_valid) when duplicates exist
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
        
        # Concatenate and deduplicate (keep last = newer data from df_valid)
        df_valid = pd.concat([existing_df, df_valid], ignore_index=True)
        df_valid = df_valid.drop_duplicates(subset=['link'], keep='last')
        
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
    existing_data = load_existing_distance_data(output_dir, file_suffix)
    if existing_data:
        print(f"📊 Found {len(existing_data)} properties with existing distance data for lookup")
    
    # Apply existing data to the dataframe and check completion status
    completed_count = 0
    incomplete_indices = []
    
    for idx, row in df_valid.iterrows():
        link = row.get('link')
        
        # Apply existing data if available
        if link and link in existing_data:
            existing_row = existing_data[link]
            for col, val in existing_row.items():
                if col not in df_valid.columns:
                    df_valid.at[idx, col] = val
                elif pd.isna(df_valid.at[idx, col]) and not pd.isna(val):
                    df_valid.at[idx, col] = val
        
        # Check completion status
        status = check_property_completion_status(df_valid.loc[idx], place_categories)
        df_valid.at[idx, 'processing_status'] = status
        
        if status == 'completed':
            completed_count += 1
        else:
            incomplete_indices.append(idx)
    
    print(f"✅ Already completed: {completed_count} properties (will skip)")
    print(f"📍 Need processing: {len(incomplete_indices)} properties")
    print()
    
    # Track new properties to process
    tracker.stats['step5_distance_calculation']['new_properties_to_process'] = len(incomplete_indices)
    
    # Apply test limit if in test mode - only on incomplete properties
    if test_mode and len(incomplete_indices) > test_limit:
        logger.info(f"TEST MODE: Processing only first {test_limit} of {len(incomplete_indices)} incomplete properties")
        incomplete_indices = incomplete_indices[:test_limit]
        logger.info(f"Limited to {len(incomplete_indices)} properties for testing")
        print(f"TEST MODE: Limited to {len(incomplete_indices)} incomplete properties for testing")
    
    # Record start time for total execution timing
    script_start_time = time.time()
    
    # Log API statistics at start
    logger.info("="*70)
    logger.info("STARTING PROPERTY PROCESSING")
    logger.info("="*70)
    stats = get_api_stats()
    logger.info(f"Initial API stats - Total calls: {stats['total_calls']}")
    logger.info(f"Properties to process: {len(df_valid)}")
    
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
    # Only process properties that are incomplete AND missing distance data
    # Explicitly exclude completed properties
    needs_distance = []
    skipped_existing = 0
    for idx in incomplete_indices:
        status = df_valid.at[idx, 'processing_status']
        # Only process if incomplete AND missing distance data
        if status != 'completed':
            if pd.isna(df_valid.at[idx, 'distance_to_work_km']) or pd.isna(df_valid.at[idx, 'transit_time_work_minutes']):
                needs_distance.append(idx)
            else:
                skipped_existing += 1
    
    # Track skipped properties in tracker
    tracker.stats['step5_distance_calculation']['properties_skipped_existing'] = skipped_existing
    
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
        print(f"Processing {len(needs_distance)} properties...")
        print("(This may take a few minutes due to API rate limits)")
        print()
        
        distance_start_time = time.time()
        distance_total = len(needs_distance)
        successful_count = 0
        failed_count = 0
        
        for i, index in enumerate(needs_distance, 1):
            row = df_valid.loc[index]
            property_address = row['address']
            property_lat = row['latitude']
            property_lng = row['longitude']
            
            # Calculate remaining time estimate
            if i > 1:
                elapsed = time.time() - distance_start_time
                avg_per_property = elapsed / (i - 1)
                remaining = avg_per_property * (distance_total - i + 1)
                remaining_str = f" (~{remaining/60:.1f} min remaining)" if remaining > 60 else f" (~{remaining:.0f}s remaining)"
            else:
                remaining_str = ""
            
            print(f"[{i}/{distance_total}] Processing: {property_address}{remaining_str}")
            
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
            
            # Update the DataFrame in place
            df_valid.at[index, 'distance_to_work_km'] = result['distance_km']
            df_valid.at[index, 'transit_time_work_minutes'] = result['duration_minutes']
            
            if result['status'] == 'OK':
                successful_count += 1
                print(f"  ✅ Distance: {result['distance_km']:.2f} km, Time: {result['duration_minutes']:.1f} min")
            else:
                failed_count += 1
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
    
    df_filtered = df_valid[
        (df_valid['transit_time_work_minutes'].notna()) & 
        (df_valid['transit_time_work_minutes'] <= max_travel_time)
    ].copy()
    
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
    
    output_file_all = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
    df_valid.to_csv(output_file_all, index=False, encoding='utf-8')
    print(f"💾 Saved all properties with distances to: {output_file_all}")
    
    if len(df_filtered) > 0:
        output_file_filtered = os.path.join(output_dir, f'property_listings_filtered_by_distance{file_suffix}.csv')
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
        for idx, row in df_filtered.iterrows():
            for cat_name, cat_config in place_categories.items():
                prefix = cat_config['column_prefix']
                walking_col = f'walking_time_{prefix}_minutes'
                
                # If any category is missing walking time, we need to process this property
                if pd.isna(row.get(walking_col)):
                    needs_place_data.append(idx)
                    break
        
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
            print(f"Processing {len(needs_place_data)} properties that need place data...")
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
                    
                    try:
                        nearest = find_nearest_place_in_category(
                            property_lat, property_lng,
                            cat_name, cat_config,
                            radius_meters=search_radius,
                            gmaps_client=gmaps_client
                        )
                        
                        if nearest and nearest.get('status') == 'OK':
                            df_filtered.at[idx, f'nearest_{prefix}'] = nearest['name']
                            
                            modes = ['walking']
                            if cat_config.get('calculate_transit', False):
                                modes.append('transit')
                            
                            travel_times = calculate_travel_time_to_place(
                                property_lat, property_lng,
                                nearest['lat'], nearest['lng'],
                                modes=modes,
                                gmaps_client=gmaps_client
                            )
                            
                            df_filtered.at[idx, f'walking_time_{prefix}_minutes'] = travel_times.get('walking_minutes')
                            
                            if cat_config.get('calculate_transit', False):
                                df_filtered.at[idx, f'transit_time_{prefix}_minutes'] = travel_times.get('transit_minutes')
                            
                            category_found_counts[cat_name] += 1
                            
                            walk_str = f"{travel_times.get('walking_minutes'):.1f} min walk" if travel_times.get('walking_minutes') else "N/A"
                            transit_str = ""
                            if cat_config.get('calculate_transit', False) and travel_times.get('transit_minutes'):
                                transit_str = f", {travel_times.get('transit_minutes'):.1f} min transit"
                            
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
    # SAVE property_listings_complete.csv (ALL properties)
    # ================================================
    # This file contains ALL properties (completed + incomplete) for reference
    output_file_complete = os.path.join(output_dir, f'property_listings_complete{file_suffix}.csv')
    df_valid.to_csv(output_file_complete, index=False, encoding='utf-8')
    print(f"💾 Saved ALL property listings to: {output_file_complete}")
    print(f"   Total properties: {len(df_valid)} (completed: {completed}, incomplete: {incomplete})")
    
    # ================================================
    # SAVE property_listings_with_distances.csv (ONLY completed properties)
    # ================================================
    # This is the source of truth for duplicate checking - only contains fully processed properties
    output_file_final = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
    
    # Filter to only completed properties
    df_completed = df_valid[df_valid['processing_status'] == 'completed'].copy()
    
    # Save ONLY completed properties to property_listings_with_distances.csv
    df_completed.to_csv(output_file_final, index=False, encoding='utf-8')
    print(f"💾 Saved COMPLETED properties to: {output_file_final}")
    print(f"   Completed properties: {len(df_completed)}")
    
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
