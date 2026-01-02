#!/usr/bin/env python3
"""
Test script to validate sales email parsing functionality.

This script:
1. Loads the sales email sample from finn_sales_email_sample.html
2. Extracts the HTML body from the multipart MIME structure
3. Decodes quoted-printable encoding
4. Creates a mock email message object
5. Calls parse_properties_from_email() to test parsing
6. Validates the extracted properties
"""

import os
import sys
import quopri  # For quoted-printable decoding
import re

# Add parent directory to path to import Email_Fetcher
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Email_Fetcher import parse_properties_from_email


class MockEmailMessage:
    """Mock email message object compatible with imap_tools format."""
    def __init__(self, html_content, subject="Test Sales Email"):
        self.html = html_content
        self.subject = subject
        self.uid = "test_uid_123"


def extract_html_from_raw_email(filepath):
    """
    Extract HTML content from a raw email file.
    
    The email file contains:
    - Email headers
    - Multipart MIME structure with boundaries
    - HTML part with quoted-printable encoding
    
    Args:
        filepath: Path to the raw email file
    
    Returns:
        str: Decoded HTML content
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the HTML part - it starts after "Content-Type: text/html"
    # Look for the boundary before HTML and the HTML content
    html_start_marker = 'Content-Type: text/html;charset=UTF-8'
    html_start_idx = content.find(html_start_marker)
    
    if html_start_idx == -1:
        raise ValueError("Could not find HTML content in email file")
    
    # Find the start of actual HTML (after headers and blank line)
    html_content_start = content.find('<!DOCTYPE', html_start_idx)
    if html_content_start == -1:
        html_content_start = content.find('<html', html_start_idx)
    
    if html_content_start == -1:
        raise ValueError("Could not find HTML start tag")
    
    # Find the end of HTML (before the next boundary or end of file)
    # Look for the boundary marker that ends this part
    boundary_marker = '------=_Part_96930_135890209.1767063182558--'
    html_content_end = content.find(boundary_marker, html_content_start)
    
    if html_content_end == -1:
        # If no boundary found, use end of file
        html_content_end = len(content)
    
    # Extract the HTML content (with quoted-printable encoding)
    encoded_html = content[html_content_start:html_content_end].strip()
    
    # Decode quoted-printable encoding
    # quopri.decodestring returns bytes, so we decode to string
    try:
        decoded_bytes = quopri.decodestring(encoded_html)
        decoded_html = decoded_bytes.decode('utf-8')
    except Exception as e:
        print(f"Warning: Error decoding quoted-printable: {e}")
        print("Trying to use content as-is...")
        # Try to manually decode common patterns
        decoded_html = encoded_html.replace('=3D', '=').replace('=C3=B8', 'ø')
        decoded_html = decoded_html.replace('=C3=A5', 'å').replace('=C3=A6', 'æ')
        decoded_html = decoded_html.replace('=20', ' ').replace('=\n', '')
    
    return decoded_html


def main():
    """Main test function."""
    print("="*70)
    print("SALES EMAIL PARSING TEST")
    print("="*70)
    print()
    
    # Get the path to the sales email sample
    script_dir = os.path.dirname(os.path.abspath(__file__))
    email_file = os.path.join(script_dir, 'finn_sales_email_sample.html')
    
    if not os.path.exists(email_file):
        print(f"❌ Error: Email sample file not found: {email_file}")
        return 1
    
    print(f"📧 Loading email from: {email_file}")
    
    try:
        # Extract and decode HTML
        html_content = extract_html_from_raw_email(email_file)
        print(f"✅ Extracted HTML content ({len(html_content)} characters)")
        
        # Create mock email message
        mock_msg = MockEmailMessage(html_content, subject="Nye annonser: Property Finder - Eie")
        print(f"✅ Created mock email message object")
        print()
        
        # Parse properties
        print("="*70)
        print("PARSING PROPERTIES")
        print("="*70)
        print()
        
        properties = parse_properties_from_email(mock_msg, debug=True)
        
        print()
        print("="*70)
        print("PARSING RESULTS")
        print("="*70)
        print()
        
        if not properties:
            print("⚠️  No properties extracted!")
            return 1
        
        print(f"✅ Extracted {len(properties)} properties")
        print()
        
        # Expected results
        expected_properties = [
            {
                'title_contains': 'Stabekk',
                'address_contains': 'Gamle Drammensvei',
                'location_contains': 'Stabekk'
            },
            {
                'title_contains': 'Kolbotn',
                'address_contains': 'Kantorveien',
                'location_contains': 'Kolbotn'
            }
        ]
        
        # Validate each property
        for i, prop in enumerate(properties, 1):
            print(f"Property {i}:")
            print(f"  Title: {prop.get('title', 'N/A')[:80]}...")
            print(f"  Address: {prop.get('address', 'N/A')}")
            print(f"  Price: {prop.get('price', 'N/A')} (expected: None/Unknown - price not in email)")
            print(f"  Size: {prop.get('size', 'N/A')}")
            print(f"  Link: {prop.get('link', 'N/A')[:80]}...")
            print(f"  Finn URL: {prop.get('finn_url', 'N/A')[:80]}...")
            print(f"  Is Ambiguous: {prop.get('is_ambiguous', 'N/A')}")
            print()
            
            # Validate against expected results
            if i <= len(expected_properties):
                expected = expected_properties[i-1]
                title_ok = expected['title_contains'].lower() in prop.get('title', '').lower()
                address_ok = expected['address_contains'].lower() in prop.get('address', '').lower()
                
                if title_ok and address_ok:
                    print(f"  ✅ Matches expected property {i}")
                else:
                    print(f"  ⚠️  Partial match for property {i}")
                    if not title_ok:
                        print(f"     Expected title to contain: {expected['title_contains']}")
                    if not address_ok:
                        print(f"     Expected address to contain: {expected['address_contains']}")
                print()
        
        # Summary
        print("="*70)
        print("VALIDATION SUMMARY")
        print("="*70)
        print()
        
        # Check price handling
        prices = [prop.get('price') for prop in properties]
        missing_prices = [p for p in prices if p is None or p == 'Unknown']
        
        print(f"Price Handling:")
        print(f"  Total properties: {len(properties)}")
        print(f"  Properties with missing price: {len(missing_prices)}/{len(properties)}")
        if len(missing_prices) == len(properties):
            print(f"  ✅ All prices missing (expected for sales emails)")
        else:
            print(f"  ⚠️  Some prices found: {[p for p in prices if p not in [None, 'Unknown']]}")
        print()
        
        # Check that parsing didn't crash
        print(f"✅ Parsing completed successfully")
        print(f"✅ No errors or exceptions")
        print()
        
        # Check URL decoding
        urls_decoded = [prop.get('finn_url', '') for prop in properties if prop.get('finn_url')]
        print(f"URL Decoding:")
        print(f"  Properties with decoded URLs: {len(urls_decoded)}/{len(properties)}")
        for url in urls_decoded[:2]:  # Show first 2
            if 'finn.no' in url:
                print(f"  ✅ URL decoded: {url[:60]}...")
        print()
        
        print("="*70)
        print("TEST COMPLETE")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

