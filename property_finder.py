# property_finder.py
# Master script for the Property Finder automation
# 
# This script orchestrates the entire workflow:
# 1. Fetch emails from Finn.no and parse property listings
# 2. Geocode property addresses to get coordinates
# 3. Calculate distances to work and filter by travel time
# 4. Find nearby places (gyms, martial arts studios, etc.)
# 5. Save results to CSV files
#
# USER CONFIGURATION: Edit config.py to customize settings

import argparse
import os
import sys
import shutil
import copy
from datetime import datetime

# Import user configuration
from config import CONFIG, load_property_type_config

# Import functions from existing modules
from Email_Fetcher import fetch_and_parse_emails_workflow
from Stringtocordinates import geocode_properties
from distance_calculator import calculate_distances_and_filter
from data_formatter import format_and_export
from email_notifier import send_property_results_notification
from tracking_summary import tracker


def archive_property_listings_latest(output_dir='output', file_suffix='', property_type='rental'):
    """
    Archive property_listings_latest.csv to a dated file and clear it.
    
    Creates a file named property_listings_DDMMYY.csv (e.g., property_listings_131224.csv)
    and then clears property_listings_latest.csv (keeping only the header).
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix to append to filename (e.g., '_test')
        property_type: 'rental' or 'sales' (default: 'rental' for backward compat)
    """
    from config import get_type_aware_filename
    
    # Use type-aware filename
    latest_filename = get_type_aware_filename('property_listings_latest', property_type, file_suffix)
    latest_csv = os.path.join(output_dir, latest_filename)
    
    if not os.path.exists(latest_csv):
        print(f"⚠️  {latest_filename} not found - nothing to archive")
        return
    
    try:
        # Read the current file to get header and row count
        import pandas as pd
        df = pd.read_csv(latest_csv)
        row_count = len(df)
        
        if row_count == 0:
            print(f"📋 {latest_filename} is empty - nothing to archive")
            return
        
        # Generate archive filename with date (DDMMYY format, type-aware)
        date_str = datetime.now().strftime('%d%m%y')
        archive_base_name = f'property_listings_{date_str}'
        archive_filename = get_type_aware_filename(archive_base_name, property_type, file_suffix)
        archive_path = os.path.join(output_dir, archive_filename)
        
        # Copy to archive
        shutil.copy2(latest_csv, archive_path)
        print(f"📦 Archived {row_count} properties to: {archive_filename}")
        
        # Clear the original file (keep header only)
        df_empty = pd.DataFrame(columns=df.columns)
        df_empty.to_csv(latest_csv, index=False)
        print(f"🧹 Cleared {latest_filename} (kept header)")
        
    except Exception as e:
        print(f"❌ Error archiving property_listings_latest.csv: {e}")


def run_pipeline(property_type: str, type_config: dict, args: argparse.Namespace):
    """
    Execute the complete property processing pipeline for a specific property type.
    
    This function:
    1. Sets up type-specific output directory
    2. Creates type-specific args object with overrides from type_config
    3. Executes the complete workflow (email fetch → geocode → filter → format → notify)
    4. Returns results summary
    
    Args:
        property_type: 'rental' or 'sales'
        type_config: Configuration dict for this property type (from load_property_type_config)
        args: Command-line arguments (base args, will be cloned and overridden)
    
    Returns:
        dict: Results summary with paths, counts, and success status
    """
    print(f"\n[{property_type.upper()}] Starting pipeline...")
    
    # Setup output directory
    if property_type == 'rental':
        # Rental uses 'output/' directory (backward compatible)
        type_output_dir = 'output'
    elif property_type == 'sales':
        # Sales uses 'output/sales/' directory
        type_output_dir = 'output/sales'
    else:
        print(f"❌ [{property_type.upper()}] Unknown property type")
        return {
            'property_type': property_type,
            'success': False,
            'error': 'Unknown property type'
        }
    
    # Create output directory if it doesn't exist
    os.makedirs(type_output_dir, exist_ok=True)
    
    # Clone args object to avoid modifying the original
    type_args = copy.deepcopy(args)
    
    # Override args with type-specific config values
    type_args.output_dir = type_output_dir
    type_args.subject_keywords = type_config.get('email', {}).get('subject_keywords', [])
    type_args.days_back = type_config.get('days_back', 14)
    type_args.reprocess_emails = type_config.get('reprocess_emails', False)
    type_args.work_lat = type_config.get('work_location', {}).get('lat', 59.899)
    type_args.work_lng = type_config.get('work_location', {}).get('lng', 10.627)
    type_args.max_transit_time_work = type_config.get('max_transit_time_work_minutes', 60)
    type_args.search_radius = type_config.get('search_radius_meters', 10000)
    type_args.property_type = property_type  # Add property_type for future use
    
    # Initialize result tracking
    result = {
        'property_type': property_type,
        'main_csv': None,
        'coords_csv': None,
        'result_csv': None,
        'excel_path': None,
        'success': False,
        'error': None
    }
    
    try:
        # #region agent log
        import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"property_finder.py:146","message":"run_pipeline entry","data":{"property_type":property_type,"type_output_dir":type_output_dir},"timestamp":int(__import__('time').time()*1000)})+'\n')
        # #endregion
        # Track output paths between steps
        main_csv = None
        coords_csv = None
        result_csv = None
        excel_path = None
        
        # ============================================
        # STEP 1: FETCH AND PARSE EMAILS
        # ============================================
        if not type_args.skip_email_fetch:
            print(f"[{property_type.upper()}] Step 1: Fetching and parsing emails...")
            # #region agent log
            import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"property_finder.py:157","message":"Before fetch_and_parse_emails_workflow","data":{"property_type":property_type,"type_args_property_type":getattr(type_args,'property_type','NOT_SET')},"timestamp":int(__import__('time').time()*1000)})+'\n')
            # #endregion
            try:
                main_csv, ambiguous_csv = fetch_and_parse_emails_workflow(type_args)
                
                if main_csv:
                    print(f"[{property_type.upper()}] ✅ Step 1 complete: Properties saved to: {main_csv}")
                    if ambiguous_csv:
                        print(f"[{property_type.upper()}]   Ambiguous addresses saved to: {ambiguous_csv}")
                else:
                    print(f"[{property_type.upper()}] ℹ️  Step 1: No new properties to process")
                    print(f"[{property_type.upper()}]    All extracted properties were already processed (found in distances CSV)")
                    print(f"[{property_type.upper()}]    Skipping remaining steps - no new data to process.")
                    result['error'] = 'No new properties to process - all already processed'
                    return result
            except Exception as e:
                # #region agent log
                import json, traceback; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"property_finder.py:171","message":"Step 1 exception caught","data":{"property_type":property_type,"error":str(e),"traceback":traceback.format_exc()},"timestamp":int(__import__('time').time()*1000)})+'\n')
                # #endregion
                print(f"[{property_type.upper()}] ❌ Error in Step 1: {e}")
                import traceback
                traceback.print_exc()
                result['error'] = f'Step 1 error: {str(e)}'
                return result
        else:
            # Use type-aware filename (with backward compatibility)
            from config import get_type_aware_filename
            latest_filename = get_type_aware_filename('property_listings_latest', property_type, type_args.file_suffix)
            main_csv = os.path.join(type_output_dir, latest_filename)
            # Try old naming for backward compatibility if not found
            if not os.path.exists(main_csv) and property_type == 'rental':
                old_main_csv = os.path.join(type_output_dir, f'property_listings_latest{type_args.file_suffix}.csv')
                if os.path.exists(old_main_csv):
                    main_csv = old_main_csv
            
            print(f"[{property_type.upper()}] Step 1: SKIPPED (using existing CSV)")
            print(f"[{property_type.upper()}] ⏭️  Using existing file: {main_csv}")
            
            if not os.path.exists(main_csv):
                print(f"[{property_type.upper()}] ❌ Error: File not found: {main_csv}")
                result['error'] = f'File not found: {main_csv}'
                return result
        
        result['main_csv'] = main_csv
        
        # ============================================
        # STEP 2: GEOCODE ADDRESSES
        # ============================================
        if not type_args.skip_geocoding:
            print(f"[{property_type.upper()}] Step 2: Geocoding addresses...")
            # #region agent log
            import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"property_finder.py:199","message":"Before Step 2 geocode","data":{"property_type":property_type,"main_csv":main_csv},"timestamp":int(__import__('time').time()*1000)})+'\n')
            # #endregion
            try:
                coords_csv = geocode_properties(type_args, input_csv_path=main_csv)
                print(f"[{property_type.upper()}] ✅ Step 2 complete: Coordinates saved to: {coords_csv}")
                # #region agent log
                import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"property_finder.py:204","message":"Step 2 geocode success","data":{"property_type":property_type,"coords_csv":coords_csv},"timestamp":int(__import__('time').time()*1000)})+'\n')
                # #endregion
            except Exception as e:
                # #region agent log
                import json, traceback; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"property_finder.py:207","message":"Step 2 geocode exception","data":{"property_type":property_type,"error":str(e),"traceback":traceback.format_exc()},"timestamp":int(__import__('time').time()*1000)})+'\n')
                # #endregion
                print(f"[{property_type.upper()}] ❌ Error in Step 2: {e}")
                import traceback
                traceback.print_exc()
                result['error'] = f'Step 2 error: {str(e)}'
                return result
        else:
            # Use type-aware filename (with backward compatibility)
            coords_filename = get_type_aware_filename('property_listings_with_coordinates', property_type, type_args.file_suffix)
            coords_csv = os.path.join(type_output_dir, coords_filename)
            # Try old naming for backward compatibility if not found
            if not os.path.exists(coords_csv) and property_type == 'rental':
                old_coords_csv = os.path.join(type_output_dir, f'property_listings_with_coordinates{type_args.file_suffix}.csv')
                if os.path.exists(old_coords_csv):
                    coords_csv = old_coords_csv
            
            print(f"[{property_type.upper()}] Step 2: SKIPPED (using existing coordinates)")
            print(f"[{property_type.upper()}] ⏭️  Using existing file: {coords_csv}")
            
            if not os.path.exists(coords_csv):
                print(f"[{property_type.upper()}] ❌ Error: File not found: {coords_csv}")
                result['error'] = f'File not found: {coords_csv}'
                return result
        
        result['coords_csv'] = coords_csv
        
        # ============================================
        # STEP 3: CALCULATE DISTANCES AND FILTER
        # ============================================
        print(f"[{property_type.upper()}] Step 3: Calculating distances and filtering...")
        # #region agent log
        import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"property_finder.py:232","message":"Before Step 3 distance calc","data":{"property_type":property_type,"coords_csv":coords_csv},"timestamp":int(__import__('time').time()*1000)})+'\n')
        # #endregion
        try:
            result_csv = calculate_distances_and_filter(type_args, input_csv_path=coords_csv)
            print(f"[{property_type.upper()}] ✅ Step 3 complete: Final results saved to: {result_csv}")
            # #region agent log
            import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"property_finder.py:236","message":"Step 3 distance calc success","data":{"property_type":property_type,"result_csv":result_csv},"timestamp":int(__import__('time').time()*1000)})+'\n')
            # #endregion
        except Exception as e:
            # #region agent log
            import json, traceback; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"property_finder.py:238","message":"Step 3 distance calc exception","data":{"property_type":property_type,"error":str(e),"traceback":traceback.format_exc()},"timestamp":int(__import__('time').time()*1000)})+'\n')
            # #endregion
            print(f"[{property_type.upper()}] ❌ Error in Step 3: {e}")
            import traceback
            traceback.print_exc()
            result['error'] = f'Step 3 error: {str(e)}'
            return result
        
        result['result_csv'] = result_csv
        
        # ============================================
        # STEP 3.5: DATA FORMATTER (Optional)
        # ============================================
        if not type_args.test_mode:
            print(f"[{property_type.upper()}] Step 3.5: Formatting and exporting...")
            try:
                excel_path = format_and_export(type_args, input_csv_path=result_csv)
                result['excel_path'] = excel_path
            except Exception as e:
                print(f"[{property_type.upper()}] ⚠️  Warning: Data formatter error: {e}")
                # Not fatal - continue without Excel file
        
        # ============================================
        # ARCHIVE property_listings_latest.csv
        # ============================================
        if not type_args.test_mode:
            try:
                archive_property_listings_latest(type_output_dir, type_args.file_suffix, property_type)
            except Exception as e:
                print(f"[{property_type.upper()}] ⚠️  Warning: Archive error: {e}")
                # Not fatal - continue
        
        # ============================================
        # STEP 4: SEND EMAIL NOTIFICATION
        # ============================================
        print(f"[{property_type.upper()}] Step 4: Sending email notification...")
        
        # Comprehensive tracking summary
        tracker.print_summary()
        tracker.save_to_file(output_dir=type_output_dir)
        tracker.save_to_history(output_dir=type_output_dir)
        
        # Determine the path to the final CSV file (type-aware)
        from config import get_type_aware_filename
        distances_filename = get_type_aware_filename('property_listings_with_distances', property_type, type_args.file_suffix)
        csv_with_distances = os.path.join(type_output_dir, distances_filename)
        
        # Send email notification (only if not in test mode)
        if type_args.test_mode:
            print(f"[{property_type.upper()}] 🧪 TEST MODE: Skipping email notification")
        else:
            try:
                email_sent = send_property_results_notification(
                    csv_with_distances_path=csv_with_distances,
                    excel_attachment_path=excel_path,
                    recipient_email=None,
                    test_mode=False,
                    property_type=property_type,
                    type_config=type_config
                )
                if not email_sent:
                    print(f"[{property_type.upper()}] ⚠️  Warning: Email notification failed.")
            except Exception as e:
                print(f"[{property_type.upper()}] ⚠️  Warning: Email notification error: {e}")
                # Not fatal - continue
        
        # Mark as successful
        result['success'] = True
        print(f"[{property_type.upper()}] ✅ Pipeline complete!")
        
        return result
        
    except Exception as e:
            # #region agent log
            import json, traceback; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"property_finder.py:306","message":"Unexpected pipeline exception","data":{"property_type":property_type,"error":str(e),"traceback":traceback.format_exc()},"timestamp":int(__import__('time').time()*1000)})+'\n')
            # #endregion
            print(f"[{property_type.upper()}] ❌ Unexpected error in pipeline: {e}")
            import traceback
            traceback.print_exc()
            result['error'] = f'Unexpected error: {str(e)}'
            return result


def main():
    """
    Main entry point for the Property Finder automation.
    
    This function:
    1. Loads configuration from YAML (or falls back to CONFIG dict for rental)
    2. Determines which property types are enabled
    3. Parses command-line arguments
    4. Calls run_pipeline() for each enabled property type sequentially
    5. Prints final summary
    """
    # ============================================
    # LOAD CONFIGURATION
    # ============================================
    print("="*70)
    print("PROPERTY FINDER - AUTOMATED WORKFLOW")
    print("="*70)
    print("\n📋 Loading configuration...")
    
    rental_config = load_property_type_config('rental')
    sales_config = load_property_type_config('sales')
    
    # ============================================
    # DETERMINE ENABLED TYPES
    # ============================================
    enabled_types = []
    # #region agent log
    import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"F","location":"property_finder.py:372","message":"Checking enabled types","data":{"rental_config_exists":bool(rental_config),"rental_enabled":rental_config.get('enabled', True) if rental_config else None,"sales_config_exists":bool(sales_config),"sales_enabled":sales_config.get('enabled', False) if sales_config else None},"timestamp":int(__import__('time').time()*1000)})+'\n')
    # #endregion
    if rental_config and rental_config.get('enabled', True):  # Default True for backward compat
        enabled_types.append('rental')
    if sales_config and sales_config.get('enabled', False):
        enabled_types.append('sales')
    # #region agent log
    import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"F","location":"property_finder.py:378","message":"Enabled types determined","data":{"enabled_types":enabled_types},"timestamp":int(__import__('time').time()*1000)})+'\n')
    # #endregion
    
    if not enabled_types:
        print("⚠️  No property types enabled. Check config.yaml")
        print("   Rental: enabled = true (default)")
        print("   Sales: enabled = true (set in config.yaml)")
        return
    
    print(f"✅ Enabled property types: {', '.join(enabled_types)}")
    
    # ============================================
    # PARSE COMMAND-LINE ARGUMENTS
    # ============================================
    args = parse_arguments()
    
    # ============================================
    # TEST MODE: Adjust output directory and file suffix
    # ============================================
    if args.test_mode:
        # Use separate test_output directory for test runs
        args.output_dir = 'test_output'
        args.file_suffix = '_test'
        print("🧪 TEST MODE ENABLED - Using separate test_output/ directory")
    else:
        args.file_suffix = ''
    
    # ============================================
    # PROCESS EACH ENABLED TYPE SEQUENTIALLY
    # ============================================
    all_results = []
    
    for property_type in enabled_types:
        # #region agent log
        import json; open('/Users/isuruwarakagoda/Projects/.cursor/debug.log', 'a').write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"F","location":"property_finder.py:407","message":"Starting pipeline for property type","data":{"property_type":property_type,"enabled_types":enabled_types},"timestamp":int(__import__('time').time()*1000)})+'\n')
        # #endregion
        print("\n" + "="*70)
        print(f"PROCESSING {property_type.upper()} PROPERTIES")
        print("="*70)
        
        # Get type-specific config
        type_config = rental_config if property_type == 'rental' else sales_config
        
        # Run pipeline for this property type
        result = run_pipeline(property_type, type_config, args)
        all_results.append(result)
    
    # ============================================
    # FINAL SUMMARY
    # ============================================
    print("\n" + "="*70)
    print("PROPERTY FINDER WORKFLOW COMPLETE!")
    print("="*70)
    
    print("\n📊 Summary by property type:")
    for result in all_results:
        property_type = result['property_type']
        if result['success']:
            print(f"\n✅ {property_type.upper()}:")
            if result['main_csv']:
                print(f"   • Properties: {result['main_csv']}")
            if result['coords_csv']:
                print(f"   • With coordinates: {result['coords_csv']}")
            if result['result_csv']:
                print(f"   • Final results: {result['result_csv']}")
            if result['excel_path']:
                print(f"   • Filtered Excel: {result['excel_path']}")
        else:
            error = result.get('error', 'Unknown error')
            if 'No new properties to process' in error or 'No properties found in emails' in error:
                print(f"\nℹ️  {property_type.upper()}: No new properties (all already processed)")
                print(f"   All properties from emails were already found in distances CSV")
            else:
                print(f"\n❌ {property_type.upper()}: Failed")
                print(f"   Error: {error}")
    
    print("\n💡 Open the final results CSV files in Excel or Google Sheets to view filtered properties.")
    print("="*70)
    
    # Exit with error code if any pipeline failed (but not if it's just "no new properties")
    failed_results = [r for r in all_results if not r['success']]
    actual_failures = [r for r in failed_results if 'No new properties to process' not in r.get('error', '') and 'No properties found in emails' not in r.get('error', '')]
    if actual_failures:
        sys.exit(1)


def parse_arguments():
    """
    Parse command-line arguments using argparse.
    
    Default values come from config.py - command-line args override them.
    
    First Principles:
    - argparse converts command-line strings into Python objects
    - Each argument can have a default value, type, and help text
    - Boolean flags are True when present, False when absent
    - Values can be strings, integers, floats, or lists
    """
    parser = argparse.ArgumentParser(
        description='Property Finder: Find properties near gyms and facilities with custom filters',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run with defaults (from config.py)
  python3 property_finder.py
  
  # Skip email fetch, use existing CSV
  python3 property_finder.py --skip-email-fetch
  
  # Skip both email fetch and geocoding
  python3 property_finder.py --skip-email-fetch --skip-geocoding
  
  # Custom max travel time and test mode
  python3 property_finder.py --max-transit-time-work 45 --test-mode --test-limit 10
  
  # Fetch emails from last 3 months and reprocess all
  python3 property_finder.py --days-back 90 --reprocess-emails

Note: Edit config.py to change default settings permanently.
        """
    )
    
    # ============================================
    # EMAIL SETTINGS (defaults from config.py)
    # ============================================
    parser.add_argument(
        '--days-back',
        type=int,
        default=CONFIG['days_back'],
        help=f"How many days back to fetch emails (default: {CONFIG['days_back']} from config.py)"
    )
    
    parser.add_argument(
        '--reprocess-emails',
        action='store_true',
        default=CONFIG['reprocess_emails'],
        help='Temporarily ignore processed email UIDs and re-read all emails from the time window'
    )
    
    # ============================================
    # WORK LOCATION AND TRAVEL TIME
    # ============================================
    parser.add_argument(
        '--max-transit-time-work',
        type=int,
        default=CONFIG['max_transit_time_work'],
        help=f"Maximum travel time to work location in minutes (default: {CONFIG['max_transit_time_work']} from config.py)"
    )
    
    # ============================================
    # TEST MODE
    # ============================================
    parser.add_argument(
        '--test-mode',
        action='store_true',
        default=CONFIG['test_mode'],
        help='Enable test mode to limit the number of properties processed'
    )
    
    parser.add_argument(
        '--test-limit',
        type=int,
        default=CONFIG['test_limit'],
        help=f"Number of properties to process in test mode (default: {CONFIG['test_limit']} from config.py)"
    )
    
    # ============================================
    # SKIP OPTIONS
    # ============================================
    parser.add_argument(
        '--skip-email-fetch',
        action='store_true',
        help='Skip email fetching step and use existing property_listings_latest.csv'
    )
    
    parser.add_argument(
        '--skip-geocoding',
        action='store_true',
        help='Skip geocoding step and use existing property_listings_with_coordinates.csv'
    )
    
    # ============================================
    # OUTPUT DIRECTORY
    # ============================================
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Custom output directory for CSV files (default: output)'
    )
    
    # ============================================
    # FACILITY SEARCH (EVO, SATS, etc.)
    # ============================================
    parser.add_argument(
        '--facility-keywords',
        type=str,
        default=None,
        help='Comma-separated list of facility keywords to search for (e.g., "EVO,SATS,Evo Fitness"). '
             'If not provided, uses config.py settings.'
    )
    
    # ============================================
    # PLACE SEARCH (Martial arts, boxing, etc.)
    # ============================================
    parser.add_argument(
        '--place-keywords',
        type=str,
        default=None,
        help='Comma-separated list of place keywords to search for (e.g., "martial arts,boxing,MMA,muay thai"). '
             'If not provided, uses config.py settings.'
    )
    
    parser.add_argument(
        '--place-types',
        type=str,
        default=None,
        help='Comma-separated list of Google Places API types (e.g., "gym,shopping_mall"). '
             'See: https://developers.google.com/maps/documentation/places/web-service/supported_types'
    )
    
    # ============================================
    # SEARCH RADIUS
    # ============================================
    parser.add_argument(
        '--search-radius',
        type=int,
        default=CONFIG['search_radius'],
        help=f"Search radius for nearby places in meters (default: {CONFIG['search_radius']} from config.py)"
    )
    
    # ============================================
    # WORK LOCATION COORDINATES
    # ============================================
    parser.add_argument(
        '--work-lat',
        type=float,
        default=CONFIG['work_lat'],
        help=f"Work location latitude (default: {CONFIG['work_lat']} from config.py)"
    )
    
    parser.add_argument(
        '--work-lng',
        type=float,
        default=CONFIG['work_lng'],
        help=f"Work location longitude (default: {CONFIG['work_lng']} from config.py)"
    )
    
    # Parse the arguments
    args = parser.parse_args()
    
    # Post-process arguments (convert comma-separated strings to lists)
    if args.facility_keywords:
        args.facility_keywords = [k.strip() for k in args.facility_keywords.split(',')]
    
    if args.place_keywords:
        args.place_keywords = [k.strip() for k in args.place_keywords.split(',')]
    
    if args.place_types:
        args.place_types = [t.strip() for t in args.place_types.split(',')]
    
    # Add subject_keywords from config (not a command-line arg for simplicity)
    args.subject_keywords = CONFIG['subject_keywords']
    
    return args


if __name__ == '__main__':
    main()
