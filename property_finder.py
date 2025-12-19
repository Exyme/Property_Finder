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
from datetime import datetime

# Import user configuration
from config import CONFIG

# Import functions from existing modules
from Email_Fetcher import fetch_and_parse_emails_workflow
from Stringtocordinates import geocode_properties
from distance_calculator import calculate_distances_and_filter
from email_notifier import send_property_results_notification
from tracking_summary import tracker


def archive_property_listings_latest(output_dir='output', file_suffix=''):
    """
    Archive property_listings_latest.csv to a dated file and clear it.
    
    Creates a file named property_listings_DDMMYY.csv (e.g., property_listings_131224.csv)
    and then clears property_listings_latest.csv (keeping only the header).
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix to append to filename (e.g., '_test')
    """
    latest_csv = os.path.join(output_dir, f'property_listings_latest{file_suffix}.csv')
    
    if not os.path.exists(latest_csv):
        print(f"⚠️  property_listings_latest{file_suffix}.csv not found - nothing to archive")
        return
    
    try:
        # Read the current file to get header and row count
        import pandas as pd
        df = pd.read_csv(latest_csv)
        row_count = len(df)
        
        if row_count == 0:
            print("📋 property_listings_latest.csv is empty - nothing to archive")
            return
        
        # Generate archive filename with date (DDMMYY format)
        date_str = datetime.now().strftime('%d%m%y')
        archive_filename = f'property_listings_{date_str}{file_suffix}.csv'
        archive_path = os.path.join(output_dir, archive_filename)
        
        # Copy to archive
        shutil.copy2(latest_csv, archive_path)
        print(f"📦 Archived {row_count} properties to: {archive_filename}")
        
        # Clear the original file (keep header only)
        df_empty = pd.DataFrame(columns=df.columns)
        df_empty.to_csv(latest_csv, index=False)
        print(f"🧹 Cleared property_listings_latest{file_suffix}.csv (kept header)")
        
    except Exception as e:
        print(f"❌ Error archiving property_listings_latest.csv: {e}")


def main():
    """
    Main entry point for the Property Finder automation.
    
    This function:
    1. Parses command-line arguments
    2. Runs each step of the workflow (unless skipped)
    3. Passes output from one step to the next
    4. Handles errors and prints progress
    """
    # Parse command-line arguments
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
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("="*70)
    print("PROPERTY FINDER - AUTOMATED WORKFLOW")
    print("="*70)
    print(f"\nConfiguration (edit config.py to change defaults):")
    print(f"  Days back: {args.days_back}")
    print(f"  Reprocess emails: {args.reprocess_emails}")
    print(f"  Subject keywords: {len(args.subject_keywords)} configured")
    for kw in args.subject_keywords:
        print(f"    • {kw}")
    print(f"  Max travel time to work: {args.max_transit_time_work} minutes")
    print(f"  Test mode: {args.test_mode} (limit: {args.test_limit})")
    print(f"  Output directory: {args.output_dir}")
    print(f"  Search radius: {args.search_radius / 1000:.1f} km")
    print(f"  Work location: ({args.work_lat}, {args.work_lng})")
    if args.facility_keywords:
        print(f"  Facility keywords: {', '.join(args.facility_keywords)}")
    if args.place_keywords:
        print(f"  Place keywords: {', '.join(args.place_keywords)}")
    print()
    
    # Track output paths between steps
    main_csv = None
    coords_csv = None
    result_csv = None
    
    try:
        # ============================================
        # STEP 1: FETCH AND PARSE EMAILS
        # ============================================
        if not args.skip_email_fetch:
            print("="*70)
            print("STEP 1: FETCHING AND PARSING EMAILS")
            print("="*70)
            print()
            
            main_csv, ambiguous_csv = fetch_and_parse_emails_workflow(args)
            
            if main_csv:
                print(f"\n✅ Step 1 complete: Properties saved to: {main_csv}")
                if ambiguous_csv:
                    print(f"   Ambiguous addresses saved to: {ambiguous_csv}")
            else:
                print("\n⚠️  Step 1: No properties found in emails")
                print("    Cannot continue without properties. Exiting.")
                return
        else:
            main_csv = os.path.join(args.output_dir, f'property_listings_latest{args.file_suffix}.csv')
            print("="*70)
            print("STEP 1: SKIPPED (using existing CSV)")
            print("="*70)
            print(f"⏭️  Using existing file: {main_csv}")
            
            if not os.path.exists(main_csv):
                print(f"\n❌ Error: File not found: {main_csv}")
                print("   Run without --skip-email-fetch to fetch new properties.")
                return
        
        print()
        
        # ============================================
        # STEP 2: GEOCODE ADDRESSES
        # ============================================
        if not args.skip_geocoding:
            print("="*70)
            print("STEP 2: GEOCODING ADDRESSES")
            print("="*70)
            print()
            
            coords_csv = geocode_properties(args, input_csv_path=main_csv)
            print(f"\n✅ Step 2 complete: Coordinates saved to: {coords_csv}")
        else:
            coords_csv = os.path.join(args.output_dir, f'property_listings_with_coordinates{args.file_suffix}.csv')
            print("="*70)
            print("STEP 2: SKIPPED (using existing coordinates)")
            print("="*70)
            print(f"⏭️  Using existing file: {coords_csv}")
            
            if not os.path.exists(coords_csv):
                print(f"\n❌ Error: File not found: {coords_csv}")
                print("   Run without --skip-geocoding to geocode addresses.")
                return
        
        print()
        
        # ============================================
        # STEP 3: CALCULATE DISTANCES AND FILTER
        # ============================================
        print("="*70)
        print("STEP 3: CALCULATING DISTANCES AND FILTERING")
        print("="*70)
        print()
        
        result_csv = calculate_distances_and_filter(args, input_csv_path=coords_csv)
        print(f"\n✅ Step 3 complete: Final results saved to: {result_csv}")

        # ============================================
        # ARCHIVE property_listings_latest.csv
        # ============================================
        if not args.test_mode:
            archive_property_listings_latest(args.output_dir, args.file_suffix)

        # ============================================
        # STEP 4: SEND EMAIL NOTIFICATION
        # ============================================
        print()
        print("="*70)
        print("STEP 4: SENDING EMAIL NOTIFICATION")
        print("="*70)
        print()
        
        # ============================================
        # COMPREHENSIVE TRACKING SUMMARY
        # ============================================
        tracker.print_summary()
        tracker.save_to_file(output_dir=args.output_dir)
        tracker.save_to_history(output_dir=args.output_dir)
        
        # ============================================
        # EMAIL NOTIFICATION
        # ============================================
        # Determine the path to the final CSV file
        csv_with_distances = os.path.join(args.output_dir, f'property_listings_with_distances{args.file_suffix}.csv')
        
        # Send email notification (only if not in test mode)
        if args.test_mode:
            print("\n🧪 TEST MODE: Skipping email notification")
        else:
            print("\n📧 Sending email notification...")
            email_sent = send_property_results_notification(
                csv_with_distances_path=csv_with_distances,
                recipient_email=None,  # Will default to your email from .env
                test_mode=False  # Always send email in non-test mode
            )
            if not email_sent:
                print("⚠️  Warning: Email notification failed. Check logs above for details.")
        
        # ============================================
        # FINAL SUMMARY
        # ============================================
        print()
        print("="*70)
        print("✅ PROPERTY FINDER WORKFLOW COMPLETE!")
        print("="*70)
        print(f"\nOutput files:")
        if main_csv:
            print(f"  • Properties: {main_csv}")
        if coords_csv:
            print(f"  • With coordinates: {coords_csv}")
        if result_csv:
            print(f"  • Final results: {result_csv}")
        print()
        print("💡 Open the final results CSV in Excel or Google Sheets to view filtered properties.")
        print("="*70)
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("   Make sure the required input files exist or run earlier steps first.")
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
        print("   Check your .env file and ensure API keys are set.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
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
