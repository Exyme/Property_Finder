# tracking_summary.py
# Comprehensive tracking and reporting for Property Finder workflow

import os
from datetime import datetime

class WorkflowTracker:
    """Tracks statistics at each step of the workflow."""
    
    def __init__(self):
        self.stats = {
            'step1_email_fetch': {
                'emails_read': 0,
                'properties_extracted': 0,
                'ambiguous_addresses': 0,
                'duplicates_found': 0,
                'normal_addresses': 0
            },
            'step2_master_merge': {
                'master_listings_total': 0,
                'master_listings_already_processed': 0,
                'master_listings_unprocessed': 0,
                'duplicates_with_email': 0,
                'master_listings_added': 0,
                'total_after_merge': 0
            },
            'step3_deduplication': {
                'before_count': 0,
                'duplicates_removed': 0,
                'after_count': 0
            },
            'step4_geocoding': {
                'before_count': 0,
                'geocoding_success': 0,
                'geocoding_failed': 0,
                'duplicates_after_geocoding': 0,
                'after_count': 0
            },
            'step5_distance_calculation': {
                'existing_in_distances_csv': 0,
                'new_properties_to_process': 0,
                'properties_processed': 0,
                'properties_completed': 0,
                'properties_incomplete': 0,
                'final_count': 0,
                'api_calls_distance_matrix': 0,
                'api_calls_places': 0,
                'properties_skipped_existing': 0
            }
        }
    
    def print_summary(self):
        """Print a comprehensive summary of all steps."""
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE WORKFLOW TRACKING SUMMARY")
        print("="*80)
        
        # Step 1: Email Fetch
        s1 = self.stats['step1_email_fetch']
        print("\n📧 STEP 1: EMAIL FETCH & PARSE")
        print("-" * 80)
        print(f"   Emails read: {s1['emails_read']}")
        print(f"   Properties extracted: {s1['properties_extracted']}")
        print(f"   ├─ Normal addresses: {s1['normal_addresses']}")
        print(f"   └─ Ambiguous addresses: {s1['ambiguous_addresses']}")
        print(f"   Duplicates found: {s1['duplicates_found']}")
        
        # Step 2: Master Listings Merge
        s2 = self.stats['step2_master_merge']
        print("\n📋 STEP 2: MASTER LISTINGS MERGE")
        print("-" * 80)
        print(f"   Master listings total: {s2['master_listings_total']}")
        print(f"   ├─ Already processed: {s2['master_listings_already_processed']}")
        print(f"   └─ Unprocessed: {s2['master_listings_unprocessed']}")
        print(f"   Duplicates with email: {s2['duplicates_with_email']}")
        print(f"   Master listings added: {s2['master_listings_added']}")
        print(f"   Total after merge: {s2['total_after_merge']}")
        
        # Step 3: Deduplication
        s3 = self.stats['step3_deduplication']
        if s3['before_count'] > 0:
            print("\n🔄 STEP 3: DEDUPLICATION")
            print("-" * 80)
            print(f"   Before: {s3['before_count']} properties")
            print(f"   Duplicates removed: {s3['duplicates_removed']}")
            print(f"   After: {s3['after_count']} properties")
        
        # Step 4: Geocoding
        s4 = self.stats['step4_geocoding']
        print("\n📍 STEP 4: GEOCODING")
        print("-" * 80)
        print(f"   Before: {s4['before_count']} properties")
        print(f"   ├─ Geocoding success: {s4['geocoding_success']}")
        print(f"   └─ Geocoding failed: {s4['geocoding_failed']}")
        if s4['duplicates_after_geocoding'] > 0:
            print(f"   Duplicates after geocoding: {s4['duplicates_after_geocoding']}")
        print(f"   After: {s4['after_count']} properties")
        
        # Step 5: Distance Calculation
        s5 = self.stats['step5_distance_calculation']
        print("\n🚗 STEP 5: DISTANCE CALCULATION")
        print("-" * 80)
        print(f"   Existing in property_listings_with_distances.csv: {s5['existing_in_distances_csv']}")
        print(f"   New properties to process: {s5['new_properties_to_process']}")
        print(f"   Properties skipped (already had data): {s5['properties_skipped_existing']}")
        print(f"   Properties processed: {s5['properties_processed']}")
        print(f"   ├─ Completed: {s5['properties_completed']}")
        print(f"   └─ Incomplete: {s5['properties_incomplete']}")
        print(f"   Final count: {s5['final_count']} properties")
        print(f"   API Calls:")
        print(f"   ├─ Distance Matrix API: {s5['api_calls_distance_matrix']}")
        print(f"   └─ Places API: {s5['api_calls_places']}")
        
        # Overall Summary
        print("\n" + "="*80)
        print("📈 OVERALL SUMMARY")
        print("="*80)
        print(f"   Properties from emails: {s1['normal_addresses']}")
        print(f"   Properties from master: {s2['master_listings_added']}")
        print(f"   Total processed: {s5['final_count']} properties")
        print(f"   Net increase: {s5['final_count'] - s5['existing_in_distances_csv']} properties")
        print("="*80)
    
    def save_to_file(self, output_dir='output'):
        """Save tracking summary to a JSON file."""
        import json
        import numpy as np
        
        def convert_to_native_types(obj):
            """Recursively convert numpy/pandas types to native Python types."""
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_to_native_types(value) for key, value in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_to_native_types(item) for item in obj]
            else:
                return obj
        
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, 'workflow_tracking_summary.json')
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'stats': convert_to_native_types(self.stats)
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n💾 Tracking summary saved to: {filepath}")
    
    def save_to_history(self, output_dir='output'):
        """
        Save a timestamped copy of the tracking summary to the run history folder.
        
        Creates output/run_history/workflow_tracking_YYYYMMDD_HHMMSS.json
        """
        import json
        import numpy as np
        
        def convert_to_native_types(obj):
            """Recursively convert numpy/pandas types to native Python types."""
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_to_native_types(value) for key, value in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_to_native_types(item) for item in obj]
            else:
                return obj
        
        # Create run_history directory
        history_dir = os.path.join(output_dir, 'run_history')
        os.makedirs(history_dir, exist_ok=True)
        
        # Generate timestamped filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'workflow_tracking_{timestamp}.json'
        filepath = os.path.join(history_dir, filename)
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'stats': convert_to_native_types(self.stats)
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        print(f"📂 Run history saved to: {filepath}")

# Global tracker instance
tracker = WorkflowTracker()

