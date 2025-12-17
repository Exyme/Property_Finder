/**
 * Finn.no Property Data Extractor & CSV Generator
 * 
 * This script extracts property information from a Finn.no listing page and
 * generates a CSV file matching property_listings_latest.csv format.
 * 
 * Usage:
 * 1. Open a Finn.no property listing in your browser
 * 2. Open Developer Tools (F12 or Cmd+Option+I on Mac)
 * 3. Go to the Console tab
 * 4. Paste this entire script and press Enter
 * 5. A CSV file will be automatically downloaded
 * 
 * CSV Format: title,address,price,size,link
 * Files are saved to: enhanced_listings/ folder (configure in your browser download settings)
 */

(function extractPropertyData() {
    const pageText = document.body.innerText;
    const currentUrl = window.location.href;
    
    // Extract Finn.no listing ID from URL
    const finnIdMatch = currentUrl.match(/finnkode=(\d+)/) || currentUrl.match(/\/(\d+)$/);
    const finnId = finnIdMatch ? finnIdMatch[1] : Date.now().toString();
    
    // Helper function to escape CSV values
    function escapeCSV(value) {
        if (!value) return '';
        // Convert to string and escape quotes
        const str = String(value).replace(/"/g, '""');
        // Wrap in quotes if contains comma, newline, or quote
        if (str.includes(',') || str.includes('\n') || str.includes('"')) {
            return `"${str}"`;
        }
        return str;
    }
    
    // Helper function to extract text from selectors
    function extractText(selectors, pattern = null) {
        for (const selector of selectors) {
            const elem = document.querySelector(selector);
            if (elem) {
                const text = elem.textContent.trim();
                if (text) {
                    if (pattern) {
                        const match = text.match(pattern);
                        return match ? match[1] : text;
                    }
                    return text;
                }
            }
        }
        return null;
    }
    
    // ============================================
    // EXTRACT PROPERTY INFORMATION
    // ============================================
    
    let propertyData = {
        title: 'Unknown',
        address: 'Unknown',
        postcode: null,
        fullAddress: 'Unknown',
        price: 'Unknown',
        size: 'Unknown',
        link: currentUrl
    };
    
    // 1. Extract Title
    const titleSelectors = [
        'h1',
        '[data-testid="ad-title"]',
        '.ad-title',
        'h1[class*="title"]',
        'h1[class*="heading"]'
    ];
    propertyData.title = extractText(titleSelectors) || 'Unknown';
    
    // 2. Extract Postcode and Address
    // Try "Kart, XXXX City" pattern
    const kartMatch = pageText.match(/Kart,?\s*(\d{4})\s+([A-Za-zÆØÅæøå\s]+)/i);
    if (kartMatch) {
        propertyData.postcode = kartMatch[1];
        propertyData.fullAddress = kartMatch[1] + ' ' + kartMatch[2].trim();
    }
    
    // Try to find street address before postcode
    // Look for patterns like "Streetname, XXXX City" or "Streetname XXXX City"
    const addressPatterns = [
        /([A-ZÆØÅ][A-Za-zÆØÅæøå\s\d]+),?\s*(\d{4})\s+([A-Za-zÆØÅæøå\s]+)/i,
        /([A-ZÆØÅ][A-Za-zÆØÅæøå\s\d]+\s+\d+[A-Z]?),?\s*(\d{4})\s+([A-Za-zÆØÅæøå\s]+)/i
    ];
    
    for (const pattern of addressPatterns) {
        const match = pageText.match(pattern);
        if (match) {
            propertyData.postcode = match[2] || match[1];
            if (match[1] && match[3]) {
                propertyData.fullAddress = `${match[1].trim()}, ${match[2] || match[1]} ${match[3].trim()}`;
            }
            break;
        }
    }
    
    // If no postcode found yet, try generic postcode pattern
    if (!propertyData.postcode) {
        const postcodeMatch = pageText.match(/\b(\d{4})\s+(Oslo|Bergen|Trondheim|Stavanger|Drammen|[A-Za-zÆØÅæøå]+)\b/i);
        if (postcodeMatch) {
            propertyData.postcode = postcodeMatch[1];
            propertyData.fullAddress = postcodeMatch[0];
        }
    }
    
    // Try to get address from meta tags or structured data
    const metaAddress = document.querySelector('meta[property="og:street-address"]');
    const metaLocality = document.querySelector('meta[property="og:locality"]');
    if (metaAddress && metaLocality) {
        const street = metaAddress.getAttribute('content');
        const city = metaLocality.getAttribute('content');
        if (propertyData.postcode) {
            propertyData.fullAddress = `${street}, ${propertyData.postcode} ${city}`;
        } else {
            propertyData.fullAddress = `${street}, ${city}`;
        }
    }
    
    // Final address assignment
    propertyData.address = propertyData.fullAddress !== 'Unknown' ? propertyData.fullAddress : 'Unknown';
    
    // 3. Extract Price
    const pricePattern = /(\d{1,3}(?:\s?\d{3})*)\s*kr/i;
    const priceMatch = pageText.match(pricePattern);
    if (priceMatch) {
        propertyData.price = priceMatch[1].replace(/\s/g, ' ') + ' kr';
    } else {
        // Try common price selectors
        const priceSelectors = [
            '[data-testid="ad-price"]',
            '.ad-price',
            '[class*="price"]',
            '[class*="Price"]'
        ];
        const priceText = extractText(priceSelectors);
        if (priceText && /kr/i.test(priceText)) {
            propertyData.price = priceText.match(/[\d\s]+kr/i)?.[0] || 'Unknown';
        }
    }
    
    // 4. Extract Size (m²)
    const sizePattern = /(\d+)\s*m[²2]/i;
    const sizeMatch = pageText.match(sizePattern);
    if (sizeMatch) {
        propertyData.size = sizeMatch[1] + ' m²';
    } else {
        // Try size selectors
        const sizeSelectors = [
            '[data-testid="ad-size"]',
            '.ad-size',
            '[class*="size"]',
            '[class*="Size"]'
        ];
        const sizeText = extractText(sizeSelectors, /(\d+\s*m[²2])/i);
        if (sizeText) {
            propertyData.size = sizeText;
        }
    }
    
    // ============================================
    // GENERATE CSV
    // ============================================
    
    // CSV Header (matching property_listings_latest.csv format)
    const csvHeader = 'title,address,price,size,link\n';
    
    // CSV Row
    const csvRow = [
        escapeCSV(propertyData.title),
        escapeCSV(propertyData.address),
        escapeCSV(propertyData.price),
        escapeCSV(propertyData.size),
        escapeCSV(propertyData.link)
    ].join(',') + '\n';
    
    const csvContent = csvHeader + csvRow;
    
    // ============================================
    // DOWNLOAD CSV FILE
    // ============================================
    
    // Create blob and download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    // Filename: enhanced_listing_[FINN_ID].csv
    const filename = `enhanced_listing_${finnId}.csv`;
    
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // ============================================
    // DISPLAY RESULTS
    // ============================================
    
    console.log('='.repeat(70));
    console.log('FINN.NO PROPERTY DATA EXTRACTOR');
    console.log('='.repeat(70));
    console.log('');
    console.log('📋 Extracted Data:');
    console.log('  Title:', propertyData.title);
    console.log('  Address:', propertyData.address);
    console.log('  Postcode:', propertyData.postcode || 'Not found');
    console.log('  Price:', propertyData.price);
    console.log('  Size:', propertyData.size);
    console.log('  Link:', propertyData.link);
    console.log('');
    console.log('💾 CSV File:', filename);
    console.log('📁 Save location: Check your browser downloads folder');
    console.log('   (Suggested: Create "enhanced_listings" folder in your Property_Finder directory)');
    console.log('');
    
    if (propertyData.postcode) {
        console.log('✅ Postcode found! CSV file downloaded.');
        // Also copy postcode to clipboard
        navigator.clipboard.writeText(propertyData.postcode).then(() => {
            console.log('📋 Postcode copied to clipboard:', propertyData.postcode);
        });
    } else {
        console.log('⚠️  Warning: No postcode found. Address may be incomplete.');
        console.log('   You may need to add the postcode manually to the CSV.');
    }
    
    console.log('='.repeat(70));
    
    return propertyData;
})();

/**
 * INSTRUCTIONS FOR FOLDER SETUP:
 * 
 * 1. In your browser, set the download folder to:
 *    Property_Finder/enhanced_listings/
 * 
 *    OR
 * 
 * 2. After downloading, manually move CSV files to:
 *    Property_Finder/enhanced_listings/
 * 
 * 3. Use CSVmerger.py (with modifications) to merge all enhanced_listing_*.csv files
 */
