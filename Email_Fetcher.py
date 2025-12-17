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

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')
SERVER = 'imap.gmail.com'  # This is for Gmail; change if using another provider

# Normalize to replace any non-breaking spaces with regular spaces
PASSWORD = PASSWORD.replace('\xa0', ' ').strip()

# ============================================
# PROCESSED EMAILS TRACKING
# ============================================

def get_processed_emails_path(output_dir='output'):
    """Get the path to the processed emails tracking file."""
    return os.path.join(output_dir, 'processed_email_uids.json')


def load_processed_email_uids(output_dir='output'):
    """
    Load the set of successfully processed email UIDs from the tracking file.
    
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
            # Return as set of strings (UIDs can be strings or ints)
            return set(str(uid) for uid in data.get('processed_uids', []))
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️  Warning: Could not load processed emails file: {e}")
        return set()


def save_processed_email_uid(uid, output_dir='output'):
    """
    Add a successfully processed email UID to the tracking file.
    
    Args:
        uid: The email UID to mark as processed
        output_dir: Directory where the tracking file is stored
    """
    filepath = get_processed_emails_path(output_dir)
    
    # Load existing UIDs
    processed_uids = load_processed_email_uids(output_dir)
    
    # Add the new UID
    processed_uids.add(str(uid))
    
    # Ensure directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save back to file
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'processed_uids': list(processed_uids),
                'last_updated': datetime.now().isoformat()
            }, f, indent=2)
    except IOError as e:
        print(f"⚠️  Warning: Could not save processed emails file: {e}")

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


def fetch_finn_emails(days_back=14, subject_keyword='Nye annonser: Property Finder - Leie', 
                       test_mode=False, output_dir='output'):
    """
    Fetch Finn.no property emails from your inbox.
    
    Args:
        days_back: How many days back to search (default: 14)
        subject_keyword: Keyword to search for in subject line (default: "Nye annonser: Property Finder - Leie")
        test_mode: If True, fetch all emails. If False, skip already processed emails.
        output_dir: Directory where the processed emails tracking file is stored
    
    Returns:
        Tuple of (list of email messages, mailbox object) - mailbox should be used in a context manager
    """
    recent_date = datetime.now().date() - timedelta(days=days_back)

    print(f"EMAIL: {EMAIL}")  # Should print your email if set
    print(f"PASSWORD is set: {PASSWORD is not None}")  # True if set, False if None
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
    
    # Filter emails to only those with the specified subject keyword
    emails = [msg for msg in all_emails if subject_keyword in msg.subject]
    
    # In non-test mode, filter out already processed emails
    if not test_mode:
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

def parse_properties_from_email(msg):
    
    """
    Parse property details from a Finn.no email HTML.
    
    Args:
        msg: Email message object from imap_tools
        
    Returns:
        List of dictionaries with property details
    """

    if not msg.html:
        return []  # Skip if no HTML body
    soup = BeautifulSoup(msg.html, 'html.parser')
    properties = []

    # Find all property listing divs - they have a specific class pattern
    # The class name is dynamic but contains "idIAvL"
    listing_divs = soup.find_all('div', class_=lambda c: c and 'idIAvL' in c)

    # Loop through each listing div and extract the property details
    for listing_div in listing_divs:
        try:
            # Extract link - it's in an <h3><a> tag
            title_link = listing_div.find('h3')
            if not title_link:
                continue

            link_elem = title_link.find('a')
            if not link_elem or not link_elem.get('href'):
                continue

            # Extract the actual Finn.no URL from the tracking URL
            tracking_url = link_elem.get('href', '')
            # Decode the tracking URL to get the actual Finn.no URL
            decoded_url = decode_finn_tracking_url(tracking_url)

            # Extract title/address (this is the property title)
            title = link_elem.get_text(strip=True) if link_elem else 'Unknown'
            
            # Extract price - look for span containing "kr"
            price_elem = listing_div.find('span', string=lambda t: t and 'kr' in t)
            price = price_elem.get_text(strip=True) if price_elem else 'Unknown'
            
            # Extract location - it's in a span after the price
            # We need to find the span that comes after the price span
            all_spans = listing_div.find_all('span')
            location = 'Unknown'
            price_found = False
            for span in all_spans:
                text = span.get_text(strip=True)
                if 'kr' in text:
                    price_found = True
                elif price_found and text and 'kr' not in text:  # First span after price that's not the price
                    location = text
                    break

            # Extract street address - it's in a <p> tag
            # The first <p> after the price/location usually contains the street
            address_paragraphs = listing_div.find_all('p')
            street_address = 'Unknown'
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
                'price': price,
                'size': size,
                'link': tracking_url,  # Keep original tracking URL for reference
                'finn_url': decoded_url,  # Decoded Finn.no URL for easy access
                'is_ambiguous': address_is_ambiguous  # Flag for ambiguous addresses
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


def fetch_and_parse_emails_workflow(args, days_back=14, subject_keyword='Nye annonser: Property Finder - Leie'):
    """
    Fetch emails from Finn.no and parse property listings.
    
    This is the main workflow function that:
    1. Fetches emails from your inbox
    2. Parses property listings from email HTML
    3. Separates normal and ambiguous addresses
    4. Filters out duplicate properties already in CSV
    5. Exports results to CSV files
    
    Args:
        args: Argument object with output_dir, test_mode, file_suffix attributes
        days_back: Number of days to look back for emails (default: 14)
        subject_keyword: Keyword to search in email subject
    
    Returns:
        tuple: (main_csv_path, ambiguous_csv_path) or (None, None) if no properties
    """
    # Get output directory and test mode from args
    output_dir = getattr(args, 'output_dir', 'output')
    test_mode = getattr(args, 'test_mode', False)
    file_suffix = getattr(args, 'file_suffix', '')
    
    # Load existing property links to filter duplicates
    # In test mode, don't filter duplicates - we want to test the full workflow
    if test_mode:
        existing_links = set()
        print("🧪 TEST MODE: Not filtering duplicates - testing full workflow")
    else:
        existing_links = load_existing_property_links(output_dir, file_suffix)
        if existing_links:
            print(f"📋 Found {len(existing_links)} existing properties in CSV files")
    
    # Fetch emails (will filter out processed emails in non-test mode)
    emails, mailbox = fetch_finn_emails(
        days_back=days_back, 
        subject_keyword=subject_keyword,
        test_mode=test_mode,
        output_dir=output_dir
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
            props = parse_properties_from_email(msg)

            # Check if any properties were found
            if not props:
                print("⚠️  No properties found, skipping")
                mailbox.flag(msg.uid, '\\Seen', True)
                email_success = True  # No properties = nothing to process, count as success
                continue

            # Filter out properties that already exist in CSV (by link)
            new_props = [p for p in props if p.get('link') not in existing_links]
            duplicate_count = len(props) - len(new_props)
            
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
        for uid in successfully_processed_uids:
            save_processed_email_uid(uid, output_dir='output')
        print(f"📝 Marked {len(successfully_processed_uids)} emails as processed")
    
    print(f"\n📊 Total NEW properties extracted: {len(all_properties)}")

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
        
        # In test mode, limit to test_limit normal properties
        if test_mode and len(df_normal) > test_limit:
            print(f"🧪 TEST MODE: Limiting to {test_limit} normal properties (from {len(df_normal)})")
            df_normal = df_normal.head(test_limit).copy()
        
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
                export_cols = ['title', 'address', 'price', 'size', 'link']
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
