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
}

