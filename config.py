# config.py - ALL USER SETTINGS IN ONE PLACE
# ============================================
# Edit this file to customize Property Finder behavior
# ============================================

CONFIG = {
    # ============================================
    # EMAIL SETTINGS
    # ============================================
    
    # How many days back to fetch emails (e.g., 14 = last 2 weeks, 90 = ~3 months)
    'days_back': 14,
    
    # True = re-read all emails, ignore which ones were already processed
    # False = skip emails that have already been processed
    'reprocess_emails': False,
    
    # Subject keywords to search for in emails
    # Emails matching ANY of these keywords will be fetched
    'subject_keywords': [
        'Nye annonser: Property Finder - Leie',
        'Nye annonser: 1500 m, Hybel med flere',
        'Nye annonser: 30 km, Asker med flere',
        'Nye annonser: sandvika, 700 m',
    ],
    
    # ============================================
    # WORK LOCATION & TRAVEL
    # ============================================
    
    # Work location coordinates (default: Fornebu)
    'work_lat': 59.899,
    'work_lng': 10.627,
    
    # Maximum travel time to work in minutes (properties beyond this are filtered out)
    'max_transit_time_work': 60,
    
    # Search radius for finding nearby places (in meters)
    # 10000 = 10 km
    'search_radius': 10000,
    
    # ============================================
    # PLACE CATEGORIES (Gyms, Fitness Centers, etc.)
    # ============================================
    # Each category will create columns in the output CSV:
    #   - nearest_{name}: Name of nearest place
    #   - walking_time_{name}_minutes: Walking time to nearest place
    #   - transit_time_{name}_minutes: Transit time (only if calculate_transit=True)
    #
    # To add a new category, copy one of the existing ones and modify:
    #   - keywords: Search terms to find this type of place
    #   - calculate_transit: True = also calculate public transport time
    #   - column_prefix: Used for CSV column names (no spaces, use underscore)
    
    'place_categories': {
        'EVO': {
            'keywords': ['EVO', 'Evo Fitness', 'EVO Fitness'],
            'calculate_transit': False,
            'column_prefix': 'EVO',
        },
        'SATS': {
            'keywords': ['SATS', 'SATS Fitness'],
            'calculate_transit': False,
            'column_prefix': 'SATS',
        },
        'martial_arts': {
            'keywords': ['martial arts gym', 'boxing gym', 'jiu jitsu', 'MMA', 'muay thai', 'bjj', 'Wrestling'],
            'calculate_transit': True,
            'column_prefix': 'martial_arts',
        },
    },
    
    # ============================================
    # TEST MODE
    # ============================================
    
    # True = use test_output/ directory, limit processing
    # False = normal operation
    'test_mode': False,
    
    # Number of properties to process in test mode
    'test_limit': 20,
    
    # ============================================
    # DATA FORMATTER (Custom Filtering & Excel Export)
    # ============================================
    # Creates a filtered Excel file with custom formatting
    # This file is attached to the notification email alongside the main CSV
    #
    # Automatically disabled in test mode
    
    'data_formatter': {
        # Enable/disable the data formatter
        'enabled': True,
        
        # ----------------------------------------
        # FILTERS
        # ----------------------------------------
        # All conditions are AND unless wrapped in {'OR': [...]}
        # Supported operators: <=, >=, <, >, ==, !=, contains, startswith, is_empty, is_not_empty
        #
        # Example: Price <= 15500 AND (EVO <= 5 OR SATS <= 5) AND martial_arts <= 10
        'filters': [
            {'column': 'price', 'op': '<=', 'value': 15500},
            {'OR': [
                {'column': 'walking_time_EVO_minutes', 'op': '<=', 'value': 5},
                {'column': 'walking_time_SATS_minutes', 'op': '<=', 'value': 5},
            ]},
            {'column': 'walking_time_martial_arts_minutes', 'op': '<=', 'value': 10},
        ],
        
        # ----------------------------------------
        # SORTING
        # ----------------------------------------
        # List of columns to sort by (first = primary sort)
        # ascending: True = A-Z/low-high, False = Z-A/high-low
        'sort_by': [
            {'column': 'transit_time_work_minutes', 'ascending': False},
            {'column': 'price', 'ascending': False},
        ],
        
        # ----------------------------------------
        # EXCEL FORMATTING
        # ----------------------------------------
        'freeze_header': True,      # Freeze the header row
        'bold_header': True,        # Make header text bold
        'auto_filter': True,        # Add Excel auto-filter dropdowns
        'auto_column_width': True,  # Auto-adjust column widths
        
        # Output filename (saved in output directory)
        'output_filename': 'property_listings_filtered.xlsx',
    },
}


# ============================================
# YAML CONFIGURATION LOADER (Sales Support)
# ============================================
# NEW: Optional YAML configuration support for sales properties
# This is completely separate from the CONFIG dictionary above.
# Existing code continues to use CONFIG unchanged.
# ============================================

from pathlib import Path

# Try to import yaml - graceful fallback if not installed
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


def load_sales_config_from_yaml():
    """
    Load sales configuration from config.yaml.
    
    Returns None if YAML doesn't exist or sales section is missing.
    This function is completely optional and doesn't affect existing CONFIG.
    
    Returns:
        dict or None: Sales configuration dictionary, or None if not available
    """
    # Graceful fallback - return None if yaml module not available
    if not YAML_AVAILABLE:
        return None
    
    config_dir = Path(__file__).parent
    config_yaml_path = config_dir / 'config.yaml'
    
    # Graceful fallback - return None if YAML doesn't exist
    if not config_yaml_path.exists():
        return None
    
    try:
        with open(config_yaml_path, 'r', encoding='utf-8') as f:
            yaml_config = yaml.safe_load(f)
        
        # Extract sales section
        sales_config = yaml_config.get('sales')
        if not sales_config:
            return None
        
        # Extract shared settings (needed for sales processing)
        shared = yaml_config.get('shared', {})
        
        # Transform to a usable format for sales functionality
        # This structure will be used when sales code is implemented (Week 2-3)
        result = {
            # Sales-specific settings
            'enabled': sales_config.get('enabled', False),
            'days_back': sales_config.get('days_back', 14),
            'reprocess_emails': sales_config.get('reprocess_emails', False),
            
            # Email settings
            'email': {
                'subject_prefix': sales_config.get('email', {}).get('subject_prefix', 'New Sales Matches'),
                'subject_keywords': sales_config.get('email', {}).get('subject_keywords', []),
            },
            
            # Filter settings
            'filters': {
                'max_purchase_price': sales_config.get('filters', {}).get('max_purchase_price'),
                'min_size_sqm': sales_config.get('filters', {}).get('min_size_sqm'),
                'allowed_areas': sales_config.get('filters', {}).get('allowed_areas', []),
            },
            
            # Shared settings (from shared section)
            'work_location': {
                'lat': shared.get('work_location', {}).get('lat', 59.899),
                'lng': shared.get('work_location', {}).get('lng', 10.627),
            },
            'max_transit_time_work_minutes': shared.get('max_transit_time_work_minutes', 60),
            'search_radius_meters': shared.get('search_radius_meters', 10000),
            'place_categories': shared.get('place_categories', {}),
        }
        
        return result
        
    except yaml.YAMLError as e:
        # YAML parsing error - return None (don't break existing code)
        return None
    except Exception as e:
        # Any other error - return None (don't break existing code)
        return None


def load_api_safety_config():
    """
    Load API safety configuration from config.yaml.
    
    Returns default values if YAML doesn't exist or api_safety section is missing.
    
    Returns:
        dict: API safety configuration with limits and thresholds
    """
    if not YAML_AVAILABLE:
        # Return default values if YAML not available
        return {
            'max_geocoding_calls_per_run': 100,
            'max_distance_matrix_calls_per_run': 500,
            'max_places_calls_per_run': 200,
            'warning_threshold_percent': 80,
            'hard_stop_on_limit': True,
        }
    
    try:
        config_dir = Path(__file__).parent
        config_yaml_path = config_dir / 'config.yaml'
        
        if not config_yaml_path.exists():
            # Return defaults if YAML doesn't exist
            return {
                'max_geocoding_calls_per_run': 100,
                'max_distance_matrix_calls_per_run': 500,
                'max_places_calls_per_run': 200,
                'warning_threshold_percent': 80,
                'hard_stop_on_limit': True,
            }
        
        with open(config_yaml_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)
            
        api_safety = yaml_data.get('api_safety', {})
        
        return {
            'max_geocoding_calls_per_run': api_safety.get('max_geocoding_calls_per_run', 100),
            'max_distance_matrix_calls_per_run': api_safety.get('max_distance_matrix_calls_per_run', 500),
            'max_places_calls_per_run': api_safety.get('max_places_calls_per_run', 200),
            'warning_threshold_percent': api_safety.get('warning_threshold_percent', 80),
            'hard_stop_on_limit': api_safety.get('hard_stop_on_limit', True),
        }
    except Exception as e:
        # Return defaults on any error
        return {
            'max_geocoding_calls_per_run': 100,
            'max_distance_matrix_calls_per_run': 500,
            'max_places_calls_per_run': 200,
            'warning_threshold_percent': 80,
            'hard_stop_on_limit': True,
        }


# Optional: SALES_CONFIG loaded from YAML
# This will be used when sales functionality is implemented (Week 2-3)
# Returns None if YAML doesn't exist or sales section is missing
SALES_CONFIG = load_sales_config_from_yaml()


def load_property_type_config(property_type: str):
    """
    Load configuration for a specific property type (rental or sales) from YAML.
    
    Merges type-specific settings with shared settings from config.yaml.
    For rental, falls back to existing CONFIG dict if YAML unavailable (backward compatibility).
    
    Args:
        property_type: 'rental' or 'sales'
    
    Returns:
        dict: Unified configuration dict for the property type, or None if not available
    """
    # For rental, try YAML first, but fall back to CONFIG dict for backward compatibility
    if property_type == 'rental':
        # Try to load from YAML
        if not YAML_AVAILABLE:
            # YAML not available - use existing CONFIG dict (backward compatible)
            return {
                'enabled': True,  # Default to enabled for rental
                'days_back': CONFIG['days_back'],
                'reprocess_emails': CONFIG['reprocess_emails'],
                'email': {
                    'subject_prefix': 'New Rental Matches',
                    'subject_keywords': CONFIG['subject_keywords'],
                },
                'filters': {
                    'max_price_per_month': None,
                    'min_size_sqm': None,
                },
                'work_location': {
                    'lat': CONFIG['work_lat'],
                    'lng': CONFIG['work_lng'],
                },
                'max_transit_time_work_minutes': CONFIG['max_transit_time_work'],
                'search_radius_meters': CONFIG['search_radius'],
                'place_categories': CONFIG['place_categories'],
            }
        
        config_dir = Path(__file__).parent
        config_yaml_path = config_dir / 'config.yaml'
        
        # If YAML doesn't exist, fall back to CONFIG dict
        if not config_yaml_path.exists():
            return {
                'enabled': True,
                'days_back': CONFIG['days_back'],
                'reprocess_emails': CONFIG['reprocess_emails'],
                'email': {
                    'subject_prefix': 'New Rental Matches',
                    'subject_keywords': CONFIG['subject_keywords'],
                },
                'filters': {
                    'max_price_per_month': None,
                    'min_size_sqm': None,
                },
                'work_location': {
                    'lat': CONFIG['work_lat'],
                    'lng': CONFIG['work_lng'],
                },
                'max_transit_time_work_minutes': CONFIG['max_transit_time_work'],
                'search_radius_meters': CONFIG['search_radius'],
                'place_categories': CONFIG['place_categories'],
            }
        
        try:
            with open(config_yaml_path, 'r', encoding='utf-8') as f:
                yaml_config = yaml.safe_load(f)
            
            # Extract rental section
            rental_config = yaml_config.get('rental', {})
            shared = yaml_config.get('shared', {})
            
            # Merge rental config with shared settings
            result = {
                'enabled': rental_config.get('enabled', True),  # Default True for backward compat
                'days_back': rental_config.get('days_back', CONFIG['days_back']),
                'reprocess_emails': rental_config.get('reprocess_emails', CONFIG['reprocess_emails']),
                'email': {
                    'subject_prefix': rental_config.get('email', {}).get('subject_prefix', 'New Rental Matches'),
                    'subject_keywords': rental_config.get('email', {}).get('subject_keywords', CONFIG['subject_keywords']),
                },
                'filters': {
                    'max_price_per_month': rental_config.get('filters', {}).get('max_price_per_month'),
                    'min_size_sqm': rental_config.get('filters', {}).get('min_size_sqm'),
                },
                'work_location': {
                    'lat': shared.get('work_location', {}).get('lat', CONFIG['work_lat']),
                    'lng': shared.get('work_location', {}).get('lng', CONFIG['work_lng']),
                },
                'max_transit_time_work_minutes': shared.get('max_transit_time_work_minutes', CONFIG['max_transit_time_work']),
                'search_radius_meters': shared.get('search_radius_meters', CONFIG['search_radius']),
                'place_categories': shared.get('place_categories', CONFIG['place_categories']),
            }
            
            return result
            
        except (yaml.YAMLError, Exception) as e:
            # YAML parsing error - fall back to CONFIG dict
            return {
                'enabled': True,
                'days_back': CONFIG['days_back'],
                'reprocess_emails': CONFIG['reprocess_emails'],
                'email': {
                    'subject_prefix': 'New Rental Matches',
                    'subject_keywords': CONFIG['subject_keywords'],
                },
                'filters': {
                    'max_price_per_month': None,
                    'min_size_sqm': None,
                },
                'work_location': {
                    'lat': CONFIG['work_lat'],
                    'lng': CONFIG['work_lng'],
                },
                'max_transit_time_work_minutes': CONFIG['max_transit_time_work'],
                'search_radius_meters': CONFIG['search_radius'],
                'place_categories': CONFIG['place_categories'],
            }
    
    # For sales, use YAML only (no fallback needed)
    elif property_type == 'sales':
        if not YAML_AVAILABLE:
            return None
        
        config_dir = Path(__file__).parent
        config_yaml_path = config_dir / 'config.yaml'
        
        if not config_yaml_path.exists():
            return None
        
        try:
            with open(config_yaml_path, 'r', encoding='utf-8') as f:
                yaml_config = yaml.safe_load(f)
            
            # Extract sales section
            sales_config = yaml_config.get('sales')
            if not sales_config:
                return None
            
            # Extract shared settings
            shared = yaml_config.get('shared', {})
            
            # Merge sales config with shared settings
            result = {
                'enabled': sales_config.get('enabled', False),
                'days_back': sales_config.get('days_back', 14),
                'reprocess_emails': sales_config.get('reprocess_emails', False),
                'email': {
                    'subject_prefix': sales_config.get('email', {}).get('subject_prefix', 'New Sales Matches'),
                    'subject_keywords': sales_config.get('email', {}).get('subject_keywords', []),
                },
                'filters': {
                    'max_purchase_price': sales_config.get('filters', {}).get('max_purchase_price'),
                    'min_size_sqm': sales_config.get('filters', {}).get('min_size_sqm'),
                    'allowed_areas': sales_config.get('filters', {}).get('allowed_areas', []),
                },
                'work_location': {
                    'lat': shared.get('work_location', {}).get('lat', 59.899),
                    'lng': shared.get('work_location', {}).get('lng', 10.627),
                },
                'max_transit_time_work_minutes': shared.get('max_transit_time_work_minutes', 60),
                'search_radius_meters': shared.get('search_radius_meters', 10000),
                'place_categories': shared.get('place_categories', {}),
            }
            
            return result
            
        except (yaml.YAMLError, Exception):
            return None
    
    # Unknown property type
    return None


# ============================================
# FILENAME UTILITY FUNCTION (Type-Aware)
# ============================================

def get_type_aware_filename(base_name, property_type='rental', file_suffix='', extension='csv'):
    """
    Generate type-aware filename for CSV/Excel files.
    
    First Principles:
    - Filenames are just strings that identify files
    - We construct them by combining: type prefix (optional) + base name + suffix + extension
    - For backward compatibility, rental uses original naming (no prefix)
    - For sales, we add 'sales_' prefix to distinguish files
    
    Args:
        base_name: Base filename (e.g., 'property_listings_with_distances')
        property_type: 'rental' or 'sales' (default: 'rental' for backward compat)
        file_suffix: Optional suffix (e.g., '_test')
        extension: File extension (default: 'csv', can be 'xlsx' for Excel)
    
    Returns:
        str: Complete filename (e.g., 'sales_property_listings_with_distances.csv')
    """
    # For rental, use original naming (backward compatible)
    if property_type == 'rental':
        return f'{base_name}{file_suffix}.{extension}'
    # For sales, add type prefix
    elif property_type == 'sales':
        return f'sales_{base_name}{file_suffix}.{extension}'
    else:
        # Unknown type - default to rental behavior
        return f'{base_name}{file_suffix}.{extension}'

