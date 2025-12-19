import os
import re
import json
import pandas as pd
import time
import shutil
from datetime import datetime, timedelta
from imap_tools import MailBox, AND, OR, A

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')  # Loads your Email.env file

from bs4 import BeautifulSoup  # Add this import
from urllib.parse import unquote  # For URL decoding
from tracking_summary import tracker
from config import CONFIG

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')
SERVER = 'imap.gmail.com'  # This is for Gmail; change if using another provider

# Normalize to replace any non-breaking spaces with regular spaces
PASSWORD = PASSWORD.replace('\xa0', ' ').strip()

# ============================================
# PRICE CLEANING UTILITY
# ============================================

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

# ============================================
# PROCESSED EMAILS TRACKING
# ============================================

def get_processed_emails_path(output_dir='output'):
    """Get the path to the processed emails tracking file."""
    return os.path.join(output_dir, 'processed_email_uids.json')


def load_processed_email_uids(output_dir='output'):
    """
    Load the set of successfully processed email UIDs from the tracking file.
    
    Handles both old format (flat list) and new format (with run history).
    
    Args:
        output_dir: Directory where the tracking file is stored
    
    Returns:
        set: Set of email UIDs (strings) that have been successfully processed
    """
    filepath = get_processed_emails_path(output_dir)
    
    if not os.path.exists(filepath):
        return set()
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # New format: has 'all_processed_uids' key
            if 'all_processed_uids' in data:
                return set(str(uid) for uid in data.get('all_processed_uids', []))
            
            # Old format: has 'processed_uids' key (backward compatibility)
            return set(str(uid) for uid in data.get('processed_uids', []))
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️  Warning: Could not load processed emails file: {e}")
        return set()


def load_processed_emails_data(output_dir='output'):
    """
    Load the full processed emails data structure (for run history).
    
    Args:
        output_dir: Directory where the tracking file is stored
    
    Returns:
        dict: Full data structure with runs and all_processed_uids
    """
    filepath = get_processed_emails_path(output_dir)
    
    if not os.path.exists(filepath):
        return {'runs': [], 'all_processed_uids': [], 'total_count': 0}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Convert old format to new format if needed
            if 'all_processed_uids' not in data:
                old_uids = list(set(str(uid) for uid in data.get('processed_uids', [])))
                return {
                    'runs': [{
                        'timestamp': data.get('last_updated', datetime.now().isoformat()),
                        'uids_processed': old_uids,
                        'count': len(old_uids),
                        'note': 'Migrated from old format'
                    }],
                    'all_processed_uids': old_uids,
                    'total_count': len(old_uids)
                }
            
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️  Warning: Could not load processed emails file: {e}")
        return {'runs': [], 'all_processed_uids': [], 'total_count': 0}


def save_processed_email_uids_batch(uids, output_dir='output'):
    """
    Save a batch of processed email UIDs to the tracking file with run history.
    
    Args:
        uids: List of email UIDs to mark as processed
        output_dir: Directory where the tracking file is stored
    """
    if not uids:
        return
    
    filepath = get_processed_emails_path(output_dir)
    
    # Load existing data (with run history)
    data = load_processed_emails_data(output_dir)
    
    # Convert UIDs to strings
    new_uids = [str(uid) for uid in uids]
    
    # Get existing UIDs as a set for efficient lookup
    existing_uids = set(data.get('all_processed_uids', []))
    
    # Filter to only truly new UIDs
    truly_new_uids = [uid for uid in new_uids if uid not in existing_uids]
    
    if truly_new_uids:
        # Add new run entry
        run_entry = {
            'timestamp': datetime.now().isoformat(),
            'uids_processed': truly_new_uids,
            'count': len(truly_new_uids)
        }
        data['runs'].append(run_entry)
        
        # Update all_processed_uids
        all_uids = list(existing_uids | set(truly_new_uids))
        data['all_processed_uids'] = all_uids
        data['total_count'] = len(all_uids)
    
    # Ensure directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save to file
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        print(f"⚠️  Warning: Could not save processed emails file: {e}")


def save_processed_email_uid(uid, output_dir='output'):
    """
    Add a successfully processed email UID to the tracking file.
    
    Note: For better performance with multiple UIDs, use save_processed_email_uids_batch().
    
    Args:
        uid: The email UID to mark as processed
        output_dir: Directory where the tracking file is stored
    """
    save_processed_email_uids_batch([uid], output_dir)

# ============================================
# ADDRESS AMBIGUITY DETECTION
# ============================================

# List of known Norwegian city/town names that are too vague on their own
KNOWN_VAGUE_LOCATIONS = [
    'oslo', 'bergen', 'trondheim', 'stavanger', 'drammen', 'fredrikstad',
    'kristiansand', 'sandnes', 'tromsø', 'sarpsborg', 'skien', 'bodø',
    'ålesund', 'sandefjord', 'larvik', 'arendal', 'tønsberg', 'moss',
    'haugesund', 'porsgrunn', 'halden', 'lillehammer', 'molde', 'harstad',
    'gjøvik', 'steinkjer', 'askøy', 'kongsberg', 'hønefoss', 'elverum',
    'hamar', 'jessheim', 'ski', 'lillestrøm', 'lørenskog', 'asker', 'bærum',
    # Smaller places that often appear without street info
    'hagan', 'åmot', 'dal', 'langhus', 'stabekk', 'bekkestua', 'fornebu',
    'lysaker', 'sandvika', 'kolbotn', 'oppegård', 'drøbak', 'ås', 'vestby'
]

def is_ambiguous_address(address):
    """
    Detect if an address is too vague for accurate geocoding.
    
    An address is considered ambiguous if:
    - It has no comma (no street/city separation)
    - It's just a city/town name
    - It has no street number
    - It's a single word
    
    Args:
        address (str): The address string to check
        
    Returns:
        bool: True if the address is ambiguous, False if it's specific enough
    """
    if not address or address == 'Unknown':
        return True
    
    # Normalize the address for comparison
    addr_lower = address.lower().strip()
    
    # Check 1: No comma = likely missing street/city separation
    if ',' not in address:
        return True
    
    # Check 2: Is it just a known city/town name?
    if addr_lower in KNOWN_VAGUE_LOCATIONS:
        return True
    
    # Check 3: Check if it's a single word (no spaces after stripping city names)
    words = addr_lower.split()
    if len(words) <= 1:
        return True
    
    # Check 4: Does it contain a street number? (digit pattern)
    # Most specific addresses have numbers like "Streetname 5" or "Streetname 5B"
    has_number = bool(re.search(r'\d', address))
    if not has_number:
        # No number - could be just "Streetname, City" without specific address
        # This is borderline - for now we'll allow it
        pass
    
    # Check 5: Is the part before the comma just a city name?
    parts = address.split(',')
    if len(parts) >= 1:
        first_part = parts[0].strip().lower()
        if first_part in KNOWN_VAGUE_LOCATIONS:
            return True
    
    # If none of the above, it's probably specific enough
    return False


def decode_finn_tracking_url(tracking_url):
    """
    Extract the actual Finn.no URL from a tracking/redirect URL.
    
    Tracking URLs have format:
    https://click.mailsvc.finn.no/CL0/https:%2F%2Fwww.finn.no%2F[FINNKODE]%3F.../...
    
    Args:
        tracking_url (str): The full tracking URL from the email
        
    Returns:
        str: The decoded Finn.no URL, or the original URL if decoding fails
    """
    if not tracking_url:
        return tracking_url
    
    try:
        # The tracking URL contains the actual URL encoded after "CL0/"
        if 'click.mailsvc.finn.no/CL0/' in tracking_url:
            # Extract the part after CL0/
            encoded_part = tracking_url.split('CL0/')[1]
            
            # The encoded URL ends at the next "/" that's not part of the URL encoding
            # Split by "/" and take the first part (the encoded URL)
            encoded_url = encoded_part.split('/')[0]
            
            # URL decode it
            decoded_url = unquote(encoded_url)
            
            # Clean up - remove any tracking parameters if desired
            # For now, keep the full URL as it might have useful params
            return decoded_url
        else:
            # Not a tracking URL, return as-is
            return tracking_url
    except Exception as e:
        # If anything goes wrong, return the original
        return tracking_url


def extract_finnkode(url):
    """
    Extract the finnkode (unique property ID) from a Finn.no URL.
    
    Handles multiple URL formats:
    1. Direct URLs: https://www.finn.no/realestate/lettings/ad.html?finnkode=439665457
    2. Short URLs: https://www.finn.no/439665457?... (from decoded tracking URLs)
    3. Tracking URLs: https://click.mailsvc.finn.no/CL0/https:%2F%2Fwww.finn.no%2F... (decodes first)
    4. Malformed URLs: https://www.finn.nohttps://www.finn.no/... (handles double prefix)
    
    Args:
        url (str): Any Finn.no URL format
        
    Returns:
        str: The finnkode (e.g., "439665457") or None if not found
    """
    if not url or not isinstance(url, str):
        return None
    
    try:
        # First, decode tracking URLs
        decoded_url = decode_finn_tracking_url(url)
        
        # Handle malformed URLs with double https://www.finn.no prefix
        if 'www.finn.nohttps://' in decoded_url:
            # Extract the part after the malformed prefix
            decoded_url = 'https://' + decoded_url.split('www.finn.nohttps://')[1]
        
        # Method 1: Look for ?finnkode=XXXXXXXXX parameter
        finnkode_match = re.search(r'[?&]finnkode=(\d+)', decoded_url)
        if finnkode_match:
            return finnkode_match.group(1)
        
        # Method 2: Look for www.finn.no/XXXXXXXXX pattern (short URL format)
        # This handles decoded tracking URLs like: www.finn.no/438366970?...
        short_url_match = re.search(r'finn\.no/(\d{6,12})(?:\?|$)', decoded_url)
        if short_url_match:
            return short_url_match.group(1)
        
        # Method 3: Check in the original URL in case decoding missed something
        if url != decoded_url:
            # Check original for encoded finnkode
            encoded_match = re.search(r'finn\.no%2F(\d{6,12})', url)
            if encoded_match:
                return encoded_match.group(1)
        
        return None
        
    except Exception as e:
        print(f"⚠️  Warning: Could not extract finnkode from URL: {e}")
        return None


def normalize_finn_url(url):
    """
    Normalize a Finn.no URL to a consistent direct URL format.
    
    Converts tracking URLs and malformed URLs to the standard format:
    https://www.finn.no/realestate/lettings/ad.html?finnkode=XXXXXXXXX
    
    Args:
        url (str): Any Finn.no URL format
        
    Returns:
        str: Normalized direct URL, or original URL if normalization fails
    """
    if not url or not isinstance(url, str):
        return url
    
    finnkode = extract_finnkode(url)
    if finnkode:
        return f"https://www.finn.no/realestate/lettings/ad.html?finnkode={finnkode}"
    
    # If we can't extract finnkode, return the decoded URL at least
    return decode_finn_tracking_url(url)


def merge_with_master_listings(email_df, master_csv_path='master_listings.csv', output_dir='output', file_suffix=''):
    """
    Merge email-fetched properties with master_listings.csv.
    
    This function:
    1. Loads processed finnkodes from property_listings_with_distances.csv (already processed)
    2. Loads master_listings.csv and normalizes column names
    3. Filters out master listings that have already been processed
    4. Extracts finnkode from both datasets for deduplication
    5. Prefers email-fetched data when duplicates exist (same finnkode)
    6. Adds non-duplicate, unprocessed properties from master_listings
    7. Normalizes all URLs to direct Finn.no format
    
    Args:
        email_df (DataFrame): Properties fetched from emails with columns:
                              title, address, price, size, link
        master_csv_path (str): Path to master_listings.csv
        output_dir (str): Directory where property_listings_with_distances.csv is stored
        file_suffix (str): Suffix to append to filename (e.g., '_test')
        
    Returns:
        DataFrame: Merged properties with columns: title, address, price, size, link
    """
    print("\n" + "="*70)
    print("MERGING WITH MASTER LISTINGS")
    print("="*70)
    
    # Load processed finnkodes from property_listings_with_distances.csv
    processed_finnkodes = load_processed_finnkodes_from_distances_csv(output_dir, file_suffix)
    if processed_finnkodes:
        print(f"📊 Found {len(processed_finnkodes)} already processed properties (will skip from master)")
    
    # Check if master_listings.csv exists
    if not os.path.exists(master_csv_path):
        print(f"⚠️  Master listings file not found: {master_csv_path}")
        print("   Continuing with email-fetched properties only")
        return email_df
    
    try:
        # Load master_listings.csv
        master_df = pd.read_csv(master_csv_path)
        print(f"📂 Loaded {len(master_df)} properties from {master_csv_path}")
        
        # Normalize column names: Title→title, Address→address, etc.
        column_mapping = {
            'Title': 'title',
            'Address': 'address',
            'Size': 'size',
            'Price': 'price',
            'URL': 'link'
        }
        master_df = master_df.rename(columns=column_mapping)
        
        # Ensure required columns exist
        required_cols = ['title', 'address', 'price', 'size', 'link']
        for col in required_cols:
            if col not in master_df.columns:
                print(f"⚠️  Warning: Missing column '{col}' in master_listings.csv")
                master_df[col] = 'Unknown'
        
        # Keep only required columns
        master_df = master_df[required_cols].copy()
        
        # Extract finnkode from email-fetched properties
        email_finnkodes = set()
        if len(email_df) > 0 and 'link' in email_df.columns:
            email_df = email_df.copy()
            email_df['_finnkode'] = email_df['link'].apply(extract_finnkode)
            email_finnkodes = set(email_df['_finnkode'].dropna().tolist())
            print(f"📧 Email properties: {len(email_df)} ({len(email_finnkodes)} unique finnkodes)")
        else:
            print(f"📧 Email properties: 0")
        
        # Extract finnkode from master listings
        master_df['_finnkode'] = master_df['link'].apply(extract_finnkode)
        master_finnkodes = set(master_df['_finnkode'].dropna().tolist())
        print(f"📋 Master listings: {len(master_df)} ({len(master_finnkodes)} unique finnkodes)")
        
        # Track master listings statistics
        tracker.stats['step2_master_merge']['master_listings_total'] = len(master_df)
        
        # Filter out already processed master listings
        if processed_finnkodes:
            already_processed = master_finnkodes & processed_finnkodes
            if already_processed:
                print(f"⏭️  Skipping {len(already_processed)} already processed master listings")
            tracker.stats['step2_master_merge']['master_listings_already_processed'] = len(already_processed)
            master_df = master_df[~master_df['_finnkode'].isin(processed_finnkodes)].copy()
            master_finnkodes = set(master_df['_finnkode'].dropna().tolist())
            print(f"📋 Unprocessed master listings: {len(master_df)} ({len(master_finnkodes)} unique finnkodes)")
        
        tracker.stats['step2_master_merge']['master_listings_unprocessed'] = len(master_df)
        
        # Find duplicates (properties in both email and master)
        duplicate_finnkodes = email_finnkodes & master_finnkodes
        print(f"🔄 Duplicates found: {len(duplicate_finnkodes)} (will prefer email-fetched)")
        tracker.stats['step2_master_merge']['duplicates_with_email'] = len(duplicate_finnkodes)
        
        # Filter master_df to keep only non-duplicates
        master_unique = master_df[~master_df['_finnkode'].isin(email_finnkodes)].copy()
        
        # Also keep master entries where we couldn't extract finnkode (can't dedupe)
        master_no_finnkode = master_df[master_df['_finnkode'].isna()].copy()
        if len(master_no_finnkode) > 0:
            print(f"⚠️  {len(master_no_finnkode)} master entries have invalid URLs (keeping them)")
            # These are already included if _finnkode is NaN and not in email_finnkodes
        
        print(f"📥 Adding {len(master_unique)} unique, unprocessed properties from master listings")
        tracker.stats['step2_master_merge']['master_listings_added'] = len(master_unique)
        
        # Clean price in master_unique (in case it has 'kr' format)
        master_unique['price'] = master_unique['price'].apply(clean_price)
        
        # Add date_read for master listings entries (set to current timestamp)
        master_unique['date_read'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Normalize links in master_unique to direct Finn.no URL format
        master_unique['link'] = master_unique['link'].apply(normalize_finn_url)
        
        # Normalize links in email_df (convert tracking URLs to direct URLs)
        if len(email_df) > 0:
            email_df['link'] = email_df['link'].apply(normalize_finn_url)
        
        # Remove internal columns before merging
        if '_finnkode' in email_df.columns:
            email_df = email_df.drop(columns=['_finnkode'])
        master_unique = master_unique.drop(columns=['_finnkode'])
        
        # Merge: email-fetched first (priority), then master unique
        if len(email_df) > 0:
            merged_df = pd.concat([email_df, master_unique], ignore_index=True)
        else:
            merged_df = master_unique.copy()
        
        # Final deduplication by link (in case of any remaining duplicates)
        initial_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['link'], keep='first')
        if len(merged_df) < initial_count:
            print(f"🧹 Removed {initial_count - len(merged_df)} additional duplicates by link")
        
        print(f"✅ Merged total: {len(merged_df)} properties")
        tracker.stats['step2_master_merge']['total_after_merge'] = len(merged_df)
        print("="*70)
        
        return merged_df
        
    except Exception as e:
        print(f"❌ Error merging with master listings: {e}")
        print("   Continuing with email-fetched properties only")
        return email_df


def fetch_finn_emails(days_back=None, subject_keywords=None, 
                       test_mode=False, output_dir='output', reprocess_emails=False):
    """
    Fetch Finn.no property emails from your inbox.
    
    Args:
        days_back: How many days back to search (default: from config.py)
        subject_keywords: List of keywords to search for in subject line (default: from config.py)
                         Emails matching ANY keyword will be fetched
        test_mode: If True, fetch all emails. If False, skip already processed emails.
        output_dir: Directory where the processed emails tracking file is stored
        reprocess_emails: If True, temporarily ignore processed UIDs and re-read all emails
    
    Returns:
        Tuple of (list of email messages, mailbox object) - mailbox should be used in a context manager
    """
    # Use defaults from config.py if not provided
    if days_back is None:
        days_back = CONFIG['days_back']
    if subject_keywords is None:
        subject_keywords = CONFIG['subject_keywords']
    
    # Ensure subject_keywords is a list
    if isinstance(subject_keywords, str):
        subject_keywords = [subject_keywords]
    
    recent_date = datetime.now().date() - timedelta(days=days_back)

    print(f"EMAIL: {EMAIL}")  # Should print your email if set
    print(f"PASSWORD is set: {PASSWORD is not None}")  # True if set, False if None
    print(f"📅 Fetching emails from last {days_back} days")
    print(f"🔍 Searching for {len(subject_keywords)} subject keyword(s)")
    if PASSWORD is None:
        raise ValueError("PASSWORD environment variable is not set!")

    mailbox = MailBox(SERVER).login(EMAIL, PASSWORD, 'INBOX')
    
    # Build flexible criteria:
    # - Emails from either sender (your email for forwarded emails OR agent@finn.no)
    # - AND subject contains the keyword (handles "Fwd:" prefix automatically)
    # - AND date is within the specified range
    # Search for emails matching Finn.no criteria (sender or subject)
    criteria = AND(
        OR(
            A(from_=EMAIL),  # For forwarded emails from yourself
            A(from_='agent@finn.no')
        ),
        A(date_gte=recent_date)
    )
    all_emails = list(mailbox.fetch(criteria, reverse=True)) 
    
    # Filter emails to only those matching ANY of the subject keywords
    def matches_any_keyword(subject, keywords):
        return any(keyword in subject for keyword in keywords)
    
    emails = [msg for msg in all_emails if matches_any_keyword(msg.subject, subject_keywords)]
    print(f"📧 Found {len(emails)} emails matching subject keywords")
    
    # Handle filtering by processed status
    if reprocess_emails:
        print("🔄 REPROCESS MODE: Ignoring processed email UIDs - re-reading all emails from time window")
        print(f"   (Note: New UIDs will still be tracked after processing)")
    elif not test_mode:
        # In non-test mode, filter out already processed emails
        # Load processed email UIDs (from production output directory)
        processed_uids = load_processed_email_uids(output_dir='output')
        
        if processed_uids:
            original_count = len(emails)
            emails = [msg for msg in emails if str(msg.uid) not in processed_uids]
            skipped_count = original_count - len(emails)
            
            if skipped_count > 0:
                print(f"📧 Skipping {skipped_count} already processed emails")
    else:
        print("🧪 TEST MODE: Fetching all emails (not filtering by processed status)")
    
    # Return both emails and mailbox (caller must close mailbox)
    return emails, mailbox

def parse_properties_from_email(msg, debug=False):
    
    """
    Parse property details from a Finn.no email HTML.
    
    Args:
        msg: Email message object from imap_tools
        debug: If True, print debug information about parsing
        
    Returns:
        List of dictionaries with property details
    """

    if not msg.html:
        if debug:
            print("  [DEBUG] No HTML content in email")
        return []  # Skip if no HTML body
    
    soup = BeautifulSoup(msg.html, 'html.parser')
    properties = []

    # Find all property listing divs - try multiple patterns
    # Pattern 1: Old format - class contains "idIAvL"
    listing_divs = soup.find_all('div', class_=lambda c: c and 'idIAvL' in c)
    
    # Pattern 2: New format - class contains "ResponsiveList" (newer emails)
    if len(listing_divs) == 0:
        responsive_divs = soup.find_all('div', class_=lambda c: c and 'ResponsiveList' in str(c))
        if responsive_divs:
            # Each ResponsiveList div contains one property in a table structure
            listing_divs = responsive_divs
            if debug:
                print(f"  [DEBUG] Using ResponsiveList pattern: {len(listing_divs)} divs found")
    
    if debug:
        print(f"  [DEBUG] Found {len(listing_divs)} divs with property listings")
        # Also check for alternative patterns
        all_divs = soup.find_all('div')
        print(f"  [DEBUG] Total divs in email: {len(all_divs)}")
        # Check for common Finn.no class patterns
        alt_patterns = ['listing', 'property', 'ad', 'annonse']
        for pattern in alt_patterns:
            matching = soup.find_all('div', class_=lambda c: c and pattern.lower() in str(c).lower())
            if matching:
                print(f"  [DEBUG] Found {len(matching)} divs with '{pattern}' in class")

    # Loop through each listing div and extract the property details
    for listing_div in listing_divs:
        try:
            # Check if this is the new format (ResponsiveList) or old format (idIAvL)
            is_new_format = 'ResponsiveList' in str(listing_div.get('class', []))
            
            # Extract link - try multiple patterns
            link_elem = None
            title = 'Unknown'
            
            if is_new_format:
                # New format: link is directly in the div, title is link text
                link_elem = listing_div.find('a', href=lambda h: h and 'finn.no' in str(h))
                if link_elem:
                    title = link_elem.get_text(strip=True)
            else:
                # Old format: link is in an <h3><a> tag
                title_link = listing_div.find('h3')
                if title_link:
                    link_elem = title_link.find('a')
                    if link_elem:
                        title = link_elem.get_text(strip=True)
            
            if not link_elem or not link_elem.get('href'):
                continue

            # Extract the actual Finn.no URL from the tracking URL
            tracking_url = link_elem.get('href', '')
            # Decode the tracking URL to get the actual Finn.no URL
            decoded_url = decode_finn_tracking_url(tracking_url)
            
            # Extract price - try multiple patterns
            price = 'Unknown'
            if is_new_format:
                # New format: price is in span with AlertAd__PriceText class
                price_elem = listing_div.find('span', class_=lambda c: c and 'PriceText' in str(c))
                if price_elem:
                    price = price_elem.get_text(strip=True)
            else:
                # Old format: look for span containing "kr"
                price_elem = listing_div.find('span', string=lambda t: t and 'kr' in t)
                if price_elem:
                    price = price_elem.get_text(strip=True)
            
            # Extract location and street address
            location = 'Unknown'
            street_address = 'Unknown'
            
            if is_new_format:
                # New format: extract from text parts
                text_parts = [t.strip() for t in listing_div.stripped_strings if t.strip() and len(t.strip()) > 2]
                # Pattern: title, price, location, street, "Privat"
                for i, text in enumerate(text_parts):
                    if 'kr' in text:
                        # Next non-price, non-Privat text is usually location
                        if i + 1 < len(text_parts) and text_parts[i + 1] != 'Privat' and 'kr' not in text_parts[i + 1]:
                            location = text_parts[i + 1]
                        # After location, next non-Privat is usually street
                        if i + 2 < len(text_parts) and text_parts[i + 2] != 'Privat' and 'kr' not in text_parts[i + 2]:
                            street_address = text_parts[i + 2]
                        break
            else:
                # Old format: extract location from spans
                all_spans = listing_div.find_all('span')
                price_found = False
                for span in all_spans:
                    text = span.get_text(strip=True)
                    if 'kr' in text:
                        price_found = True
                    elif price_found and text and 'kr' not in text:  # First span after price that's not the price
                        location = text
                        break
                
                # Extract street address - it's in a <p> tag
                address_paragraphs = listing_div.find_all('p')
                for p in address_paragraphs:
                    text = p.get_text(strip=True)
                    # Skip empty paragraphs, "Privat" text, and any text that looks like a price
                    if text and text != 'Privat' and 'kr' not in text and len(text) > 3:
                        street_address = text
                        break

            # Clean up non-breaking spaces
            price = price.replace('\xa0', ' ').strip()
            location = location.replace('\xa0', ' ').strip()
            street_address = street_address.replace('\xa0', ' ').strip()

            # Build full address (street + location)
            if street_address != 'Unknown' and location != 'Unknown':
                full_address = f"{street_address}, {location}"
            elif street_address != 'Unknown':
                full_address = street_address
            elif location != 'Unknown':
                full_address = location
            else:
                full_address = 'Unknown'

            # Try to extract size from title if it contains "m2" or "m²"
            size = 'Unknown'
            if 'm2' in title.lower() or 'm²' in title.lower():
                # Try to extract number before m2/m²
                size_match = re.search(r'(\d+)\s*m[²2]', title, re.IGNORECASE)
                if size_match:
                    size = size_match.group(1) + ' m²'

            # Check if the address is ambiguous (needs manual enhancement)
            address_is_ambiguous = is_ambiguous_address(full_address)

            properties.append({
                'title': title,
                'address': full_address,
                'price': clean_price(price),  # Clean price to integer
                'size': size,
                'link': tracking_url,  # Keep original tracking URL for reference
                'finn_url': decoded_url,  # Decoded Finn.no URL for easy access
                'is_ambiguous': address_is_ambiguous,  # Flag for ambiguous addresses
                'date_read': datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Timestamp when listing was read
            })
            
        except Exception as e:
            # Skip this listing if there's an error
            print(f"Error parsing listing: {e}")
            continue

    return properties  # Return the list of properties


# ============================================
# MAIN WORKFLOW FUNCTION (for use by property_finder.py)
# ============================================

def load_existing_property_links(output_dir='output', file_suffix=''):
    """
    Load existing property links from the latest CSV file to filter duplicates.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix to append to filename (e.g., '_test')
    
    Returns:
        set: Set of property links (tracking URLs) already in the CSV
    """
    # Check both the test and production CSVs if needed
    latest_csv = os.path.join(output_dir, f'property_listings_latest{file_suffix}.csv')
    
    # Also check the production CSV if we're in test mode
    prod_csv = os.path.join('output', 'property_listings_latest.csv')
    
    existing_links = set()
    
    for csv_path in [latest_csv, prod_csv]:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                if 'link' in df.columns:
                    existing_links.update(df['link'].dropna().tolist())
            except Exception as e:
                print(f"⚠️  Warning: Could not load existing CSV {csv_path}: {e}")
    
    return existing_links


def load_processed_finnkodes_from_distances_csv(output_dir='output', file_suffix=''):
    """
    Load processed finnkodes from property_listings_with_distances.csv.
    This is the source of truth for properties that have completed the full pipeline.
    
    Args:
        output_dir: Directory where CSV files are stored
        file_suffix: Suffix to append to filename (e.g., '_test')
    
    Returns:
        set: Set of finnkodes (strings) that have been fully processed
    """
    distances_csv = os.path.join(output_dir, f'property_listings_with_distances{file_suffix}.csv')
    
    processed_finnkodes = set()
    
    if os.path.exists(distances_csv):
        try:
            df = pd.read_csv(distances_csv)
            if 'link' in df.columns:
                # Extract finnkode from each link
                for link in df['link'].dropna():
                    finnkode = extract_finnkode(link)
                    if finnkode:
                        processed_finnkodes.add(finnkode)
        except Exception as e:
            print(f"⚠️  Warning: Could not load processed finnkodes from {distances_csv}: {e}")
    
    return processed_finnkodes


def fetch_and_parse_emails_workflow(args):
    """
    Fetch emails from Finn.no and parse property listings.
    
    This is the main workflow function that:
    1. Fetches emails from your inbox
    2. Parses property listings from email HTML
    3. Separates normal and ambiguous addresses
    4. Filters out duplicate properties already in CSV
    5. Exports results to CSV files
    
    Args:
        args: Argument object with output_dir, test_mode, file_suffix, days_back, 
              subject_keywords, reprocess_emails attributes
    
    Returns:
        tuple: (main_csv_path, ambiguous_csv_path) or (None, None) if no properties
    """
    # Get settings from args (which come from config.py or command-line overrides)
    output_dir = getattr(args, 'output_dir', 'output')
    test_mode = getattr(args, 'test_mode', False)
    file_suffix = getattr(args, 'file_suffix', '')
    reprocess_emails = getattr(args, 'reprocess_emails', False)
    days_back = getattr(args, 'days_back', CONFIG['days_back'])
    subject_keywords = getattr(args, 'subject_keywords', CONFIG['subject_keywords'])
    
    # Load existing property links to filter duplicates
    # In test mode, don't filter duplicates - we want to test the full workflow
    if test_mode:
        existing_links = set()
        print("🧪 TEST MODE: Not filtering duplicates - testing full workflow")
    else:
        existing_links = load_existing_property_links(output_dir, file_suffix)
        if existing_links:
            print(f"📋 Found {len(existing_links)} existing properties in CSV files")
    
    # Fetch emails (will filter out processed emails in non-test mode unless reprocess_emails is True)
    emails, mailbox = fetch_finn_emails(
        days_back=days_back, 
        subject_keywords=subject_keywords,
        test_mode=test_mode,
        output_dir=output_dir,
        reprocess_emails=reprocess_emails
    )
    
    all_properties = []
    successfully_processed_uids = []  # Track UIDs to mark as processed
    
    # Get test limit from args (for early stopping in test mode)
    test_limit = getattr(args, 'test_limit', 20)

    # Loop through emails with error handling
    for i, msg in enumerate(emails):
        # In test mode, stop early if we have enough normal (non-ambiguous) properties
        if test_mode:
            normal_count = sum(1 for p in all_properties if not p.get('is_ambiguous', False))
            if normal_count >= test_limit:
                print(f"🧪 TEST MODE: Reached {test_limit} normal properties, stopping email fetch")
                break
        
        email_success = False  # Track if this email was processed successfully
        
        try:
            print(f"📧 Email {i+1}/{len(emails)}: {msg.subject[:50]}...", end=" ")

            # Check for attachments (quietly - only mention if found)
            if msg.attachments:
                print(f"[{len(msg.attachments)} attachment(s)]", end=" ")

            # Check if email has HTML content
            if not msg.html:
                print("⚠️  No HTML, skipping")
                mailbox.flag(msg.uid, '\\Seen', True)
                email_success = True  # No HTML = nothing to process, count as success
                continue

            # Parse properties from email
            # Enable debug logging if we're having issues (can be made configurable)
            debug_parsing = True  # Set to True to enable debug output
            props = parse_properties_from_email(msg, debug=debug_parsing)

            # Check if any properties were found
            if not props:
                print("⚠️  No properties found, skipping")
                # Add debug info if enabled
                if debug_parsing:
                    print(f"  [DEBUG] Email subject: {msg.subject}")
                    print(f"  [DEBUG] Email has HTML: {bool(msg.html)}")
                    if msg.html:
                        print(f"  [DEBUG] HTML length: {len(msg.html)} characters")
                mailbox.flag(msg.uid, '\\Seen', True)
                email_success = True  # No properties = nothing to process, count as success
                continue

            # Filter out properties that already exist in CSV (by link)
            new_props = [p for p in props if p.get('link') not in existing_links]
            duplicate_count = len(props) - len(new_props)
            tracker.stats['step1_email_fetch']['duplicates_found'] += duplicate_count
            
            if duplicate_count > 0:
                print(f"({duplicate_count} duplicates skipped) ", end="")
            
            # Add new properties to the list
            if new_props:
                all_properties.extend(new_props)
                # Add these links to existing_links to avoid duplicates within this run
                for p in new_props:
                    existing_links.add(p.get('link'))
                print(f"✅ {len(new_props)} new properties extracted")
            else:
                print(f"✅ All {len(props)} properties already in CSV")
            
            # Mark email as read
            mailbox.flag(msg.uid, '\\Seen', True)
            email_success = True
            time.sleep(2)  # Wait 2 seconds between emails

        except Exception as e:
            print(f"❌ Error: {e}")
            email_success = False
            time.sleep(2)
            continue
        
        finally:
            # Only track as processed if successful and not in test mode
            if email_success and not test_mode:
                successfully_processed_uids.append(msg.uid)
    
    mailbox.logout()
    
    # Save successfully processed email UIDs (only in non-test mode)
    if not test_mode and successfully_processed_uids:
        save_processed_email_uids_batch(successfully_processed_uids, output_dir='output')
        print(f"📝 Marked {len(successfully_processed_uids)} emails as processed")
    
    print(f"\n📊 Total NEW properties extracted: {len(all_properties)}")
    
    # Track email fetch statistics
    tracker.stats['step1_email_fetch']['emails_read'] = len(emails)
    tracker.stats['step1_email_fetch']['properties_extracted'] = len(all_properties)

    # Initialize return paths
    main_csv_path = None
    ambiguous_csv_path = None

    # Create DataFrame from the list of properties
    if all_properties:
        df = pd.DataFrame(all_properties)
        
        # ============================================
        # SEPARATE AMBIGUOUS AND NORMAL ADDRESSES
        # ============================================
        if 'is_ambiguous' in df.columns:
            df_normal = df[df['is_ambiguous'] == False].copy()
            df_ambiguous = df[df['is_ambiguous'] == True].copy()
        else:
            df_normal = df.copy()
            df_ambiguous = pd.DataFrame()
        
        # Track ambiguous addresses
        tracker.stats['step1_email_fetch']['normal_addresses'] = len(df_normal)
        tracker.stats['step1_email_fetch']['ambiguous_addresses'] = len(df_ambiguous)
        
        # In test mode, limit to test_limit normal properties
        if test_mode and len(df_normal) > test_limit:
            print(f"🧪 TEST MODE: Limiting to {test_limit} normal properties (from {len(df_normal)})")
            df_normal = df_normal.head(test_limit).copy()
        
        # ============================================
        # MERGE WITH MASTER LISTINGS
        # ============================================
        # Prepare df_normal for merge (keep only export columns)
        export_cols = ['title', 'address', 'price', 'size', 'link', 'date_read']
        export_cols_available = [c for c in export_cols if c in df_normal.columns]
        df_normal_for_merge = df_normal[export_cols_available].copy()
        
        # Merge with master_listings.csv (skip in test mode to avoid modifying master data)
        if not test_mode:
            df_normal = merge_with_master_listings(df_normal_for_merge, output_dir=output_dir, file_suffix=file_suffix)
        else:
            print("\n🧪 TEST MODE: Skipping merge with master_listings.csv")
            df_normal = df_normal_for_merge
        
        # ============================================
        # CHECK DUPLICATES AGAINST PROCESSED PROPERTIES
        # ============================================
        if len(df_normal) > 0 and not test_mode:
            processed_finnkodes = load_processed_finnkodes_from_distances_csv(output_dir, file_suffix)
            if processed_finnkodes:
                # Extract finnkode from merged properties
                df_normal = df_normal.copy()
                df_normal['_finnkode'] = df_normal['link'].apply(extract_finnkode)
                
                # Count duplicates
                before_count = len(df_normal)
                duplicates_mask = df_normal['_finnkode'].isin(processed_finnkodes)
                duplicate_count = duplicates_mask.sum()
                
                tracker.stats['step3_deduplication']['before_count'] = before_count
                tracker.stats['step3_deduplication']['duplicates_removed'] = duplicate_count
                
                if duplicate_count > 0:
                    print(f"\n🔄 Removing {duplicate_count} properties already processed (in property_listings_with_distances.csv)")
                    df_normal = df_normal[~duplicates_mask].copy()
                    print(f"   Remaining: {len(df_normal)} properties")
                
                tracker.stats['step3_deduplication']['after_count'] = len(df_normal)
                
                # Remove temporary column
                if '_finnkode' in df_normal.columns:
                    df_normal = df_normal.drop(columns=['_finnkode'])
        
        # Simple summary
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"📊 Total properties extracted: {len(df)}")
        print(f"✅ Normal addresses (ready to use): {len(df_normal)}")
        print(f"⚠️  Ambiguous addresses (needs enhancement): {len(df_ambiguous)}")
        print(f"📋 Columns: {', '.join(df.columns)}")
        
        # Quick data quality check (only on normal addresses)
        if len(df_normal) > 0:
            missing_count = df_normal.isnull().sum().sum()
            duplicates_count = df_normal.duplicated(subset=['link']).sum() if 'link' in df_normal.columns else 0
            
            if missing_count > 0:
                print(f"⚠️  Missing values in normal addresses: {missing_count}")
            if duplicates_count > 0:
                print(f"⚠️  Duplicates in normal addresses: {duplicates_count}")
        
        # ============================================
        # AMBIGUOUS ADDRESS ANALYSIS
        # ============================================
        if len(df_ambiguous) > 0:
            print("\n" + "="*70)
            print("AMBIGUOUS ADDRESS ANALYSIS")
            print("="*70)
            
            print(f"\n📋 Ambiguous addresses found:")
            for idx, row in df_ambiguous.iterrows():
                title_preview = row['title'][:40] + '...' if len(row['title']) > 40 else row['title']
                print(f"  - {row['address']} ({title_preview})")
        
        # Show preview of normal addresses (ready to use)
        if len(df_normal) > 0:
            print("\n📋 Preview (first 3 normal addresses - ready to use):")
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 120)
            pd.set_option('display.max_colwidth', 40)
            
            preview_cols = ['title', 'address', 'price', 'size', 'link']
            preview_cols = [c for c in preview_cols if c in df_normal.columns]
            print(df_normal[preview_cols].head(3).to_string(index=False))
            
            pd.reset_option('display.max_columns')
            pd.reset_option('display.width')
            pd.reset_option('display.max_colwidth')
        else:
            print("\n⚠️  No normal addresses found - all addresses are ambiguous!")
        
        # ============================================
        # EXPORT NORMAL ADDRESSES TO MAIN CSV
        # ============================================
        print("\n" + "="*70)
        print("EXPORTING TO CSV")
        print("="*70)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp and suffix
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = os.path.join(output_dir, f'property_listings_{timestamp}{file_suffix}.csv')
        
        # Also create a "latest" version for easy access (with suffix)
        latest_filename = os.path.join(output_dir, f'property_listings_latest{file_suffix}.csv')
        
        try:
            # Only export normal addresses to main CSV
            if len(df_normal) > 0:
                # Remove internal columns (is_ambiguous, finn_url) before export
                export_cols = ['title', 'address', 'price', 'size', 'link', 'date_read']
                export_cols = [c for c in export_cols if c in df_normal.columns]
                df_export = df_normal[export_cols]
                
                # Export to timestamped file
                df_export.to_csv(csv_filename, index=False, encoding='utf-8')
                
                # Check if file was created and get its size
                if os.path.exists(csv_filename):
                    file_size = os.path.getsize(csv_filename)
                    print(f"✅ Exported {len(df_export)} normal addresses: {csv_filename} ({file_size:,} bytes)")
                else:
                    print(f"⚠️  Warning: File may not have been created")
                
                # Backup previous "latest" file if it exists
                if os.path.exists(latest_filename):
                    backup_filename = os.path.join(output_dir, f'property_listings_latest_backup{file_suffix}.csv')
                    shutil.copy2(latest_filename, backup_filename)
                
                # Create/update the "latest" file (only normal addresses)
                df_export.to_csv(latest_filename, index=False, encoding='utf-8')
                print(f"✅ Latest: {latest_filename} ({len(df_export)} properties)")
                print(f"💡 Open in Excel/Google Sheets to view all data")
                
                # Set main CSV path for return
                main_csv_path = latest_filename
            else:
                print("⚠️  No normal addresses to export - skipping main CSV")
            
            # ============================================
            # EXPORT AMBIGUOUS ADDRESSES TO SEPARATE CSV
            # ============================================
            if len(df_ambiguous) > 0:
                print("\n" + "="*70)
                print("EXPORTING AMBIGUOUS ADDRESSES")
                print("="*70)
                
                # Add empty postcode column for manual entry
                df_ambiguous['postcode'] = ''
                df_ambiguous['enhanced_address'] = ''
                
                # Select and reorder columns for the output
                ambiguous_columns = ['title', 'address', 'postcode', 'enhanced_address', 
                                    'price', 'size', 'finn_url']
                ambiguous_columns = [c for c in ambiguous_columns if c in df_ambiguous.columns]
                ambiguous_export_df = df_ambiguous[ambiguous_columns]
                
                # Generate filenames (with suffix)
                ambiguous_filename = os.path.join(output_dir, f'ambiguous_addresses_{timestamp}{file_suffix}.csv')
                ambiguous_latest = os.path.join(output_dir, f'ambiguous_addresses_latest{file_suffix}.csv')
                
                # Export
                ambiguous_export_df.to_csv(ambiguous_filename, index=False, encoding='utf-8')
                ambiguous_export_df.to_csv(ambiguous_latest, index=False, encoding='utf-8')
                
                print(f"✅ Ambiguous addresses: {ambiguous_filename} ({len(df_ambiguous)} properties)")
                print(f"✅ Latest: {ambiguous_latest}")
                print(f"📝 {len(df_ambiguous)} addresses need manual enhancement")
                print(f"💡 Open finn_url links and use extract_postcode.js to add postcodes")
                
                # Set ambiguous CSV path for return
                ambiguous_csv_path = ambiguous_latest
            else:
                print("\n✅ No ambiguous addresses found - all addresses are specific!")
            
        except PermissionError:
            print(f"❌ Error: Cannot write - file may be open in another program")
        except Exception as e:
            print(f"❌ Error exporting: {e}")
    else:
        print("\n⚠️  No properties found in any emails!")
        
        # Even with no email properties, try to load master_listings (not in test mode)
        if not test_mode:
            print("📂 Attempting to load properties from master_listings.csv...")
            empty_df = pd.DataFrame(columns=['title', 'address', 'price', 'size', 'link'])
            df_merged = merge_with_master_listings(empty_df, output_dir=output_dir, file_suffix=file_suffix)
            
            # Check duplicates against processed properties
            if len(df_merged) > 0:
                processed_finnkodes = load_processed_finnkodes_from_distances_csv(output_dir, file_suffix)
                if processed_finnkodes:
                    df_merged = df_merged.copy()
                    df_merged['_finnkode'] = df_merged['link'].apply(extract_finnkode)
                    before_count = len(df_merged)
                    duplicates_mask = df_merged['_finnkode'].isin(processed_finnkodes)
                    duplicate_count = duplicates_mask.sum()
                    if duplicate_count > 0:
                        print(f"🔄 Removing {duplicate_count} properties already processed")
                        df_merged = df_merged[~duplicates_mask].copy()
                    if '_finnkode' in df_merged.columns:
                        df_merged = df_merged.drop(columns=['_finnkode'])
            
            if len(df_merged) > 0:
                # Create output directory if it doesn't exist
                os.makedirs(output_dir, exist_ok=True)
                
                # Generate filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                csv_filename = os.path.join(output_dir, f'property_listings_{timestamp}{file_suffix}.csv')
                latest_filename = os.path.join(output_dir, f'property_listings_latest{file_suffix}.csv')
                
                # Export
                df_merged.to_csv(csv_filename, index=False, encoding='utf-8')
                df_merged.to_csv(latest_filename, index=False, encoding='utf-8')
                
                print(f"✅ Exported {len(df_merged)} properties from master_listings: {csv_filename}")
                main_csv_path = latest_filename

    return (main_csv_path, ambiguous_csv_path)


# For testing: Call the function
if __name__ == "__main__":
    # Create a mock args object for standalone execution
    class MockArgs:
        output_dir = 'output'
    
    args = MockArgs()
    
    # Call the main workflow function
    main_csv, ambiguous_csv = fetch_and_parse_emails_workflow(args)
    
    if main_csv:
        print(f"\n✅ Main CSV saved to: {main_csv}")
    if ambiguous_csv:
        print(f"✅ Ambiguous CSV saved to: {ambiguous_csv}")
