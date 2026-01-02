# Test Verification Guide

## Implementation Complete ✅

All requirements have been implemented:

### 1. ✅ Enhanced Pipeline Logging
- Property-level logging with finnkode tracking at each stage
- Log format: `[PROPERTY_TYPE] [STAGE] Property {finnkode}: {action}`
- Logs show which properties are SKIPPED (no API calls) vs NEW (API calls made)

### 2. ✅ API Safety Measures
- API call counters for geocoding, distance matrix, and places
- Configurable limits in `config.yaml`:
  - Max geocoding: 100 calls
  - Max distance matrix: 500 calls
  - Max places: 200 calls
  - Warning at 80% of limit
  - Hard stop at 100% of limit
- API usage summary displayed at end of run

### 3. ✅ Sales Output Cleared
- Backup created: `sales_property_listings_with_distances_backup_20251230.csv`
- Sales output cleared for fresh testing

### 4. ✅ URL Normalization (Backward Compatible)
- Rental URLs: Preserved if already in correct format
- Sales URLs: Use short format `https://www.finn.no/{finnkode}`

### 5. ✅ Separate Outputs
- Sales: `output/sales/sales_property_listings_with_distances.csv`
- Rental: `output/rental/property_listings_with_distances.csv`

## How to Verify Requirements

### Requirement 1: No API Calls on Previously Processed Properties

**Check the logs:**
```bash
grep "SKIPPED" output/distance_calculator.log | head -20
```

**Expected output:**
```
[RENTAL] [GEOCODING] Property 328767712: SKIPPED (already geocoded, no API call)
[RENTAL] [DISTANCE] Property 328767712: SKIPPED (already processed, no API call)
```

**Verification:**
- ✅ Properties with "SKIPPED" should NOT have "Making API call" messages
- ✅ Only new properties should have "Making API call" messages

### Requirement 2: Date Read Reflects Correct Processing Date

**Check the output files:**
```bash
python3 verify_outputs_simple.py
```

**Or manually:**
```python
import pandas as pd
df = pd.read_csv('output/sales/sales_property_listings_with_distances.csv')
print(df[['title', 'date_read']].head())
```

**Verification:**
- ✅ `date_read` column should exist
- ✅ Date should reflect when property was first processed (not re-processed date)
- ✅ Properties processed on same day should have same/similar timestamps

### Requirement 3: New Properties Appended (Not Replaced)

**Check file before and after run:**
```bash
# Before run
wc -l output/sales/sales_property_listings_with_distances.csv

# Run pipeline
python3 property_finder.py

# After run
wc -l output/sales/sales_property_listings_with_distances.csv
```

**Verification:**
- ✅ Line count should increase (new properties added)
- ✅ Existing properties should still be in file
- ✅ No duplicate properties (same finnkode)

### Requirement 4: Separate Final Outputs

**Check both files exist:**
```bash
ls -lh output/sales/sales_property_listings_with_distances.csv
ls -lh output/rental/property_listings_with_distances.csv
```

**Check for cross-contamination:**
```python
import pandas as pd

# Check sales file
df_sales = pd.read_csv('output/sales/sales_property_listings_with_distances.csv')
sales_links = df_sales['link'].tolist()
rental_in_sales = sum(1 for link in sales_links if 'realestate/lettings' in str(link))
print(f"Rental properties in sales output: {rental_in_sales}")  # Should be 0

# Check rental file
df_rental = pd.read_csv('output/rental/property_listings_with_distances.csv')
rental_links = df_rental['link'].tolist()
sales_in_rental = sum(1 for link in rental_links if 'realestate/lettings' not in str(link) and 'finn.no/' in str(link))
print(f"Sales properties in rental output: {sales_in_rental}")  # Should be 0
```

**Verification:**
- ✅ Both files should exist
- ✅ Sales file should NOT contain rental URLs (`realestate/lettings`)
- ✅ Rental file should contain rental URLs (`realestate/lettings`)
- ✅ No cross-contamination between files

## Manual Verification Steps

1. **Run the pipeline:**
   ```bash
   python3 property_finder.py
   ```

2. **Check logs for API skipping:**
   ```bash
   grep "SKIPPED" output/distance_calculator.log | wc -l
   # Should show many skipped properties
   ```

3. **Check API usage summary:**
   - Look for "API USAGE SUMMARY" at end of run
   - Should show counts for each API type

4. **Verify output files:**
   ```bash
   python3 verify_outputs_simple.py
   ```

5. **Check URL formats:**
   - Sales: Should use short format `https://www.finn.no/{finnkode}`
   - Rental: Should use `https://www.finn.no/realestate/lettings/ad.html?finnkode={finnkode}`

6. **Verify date_read:**
   - Open CSV files in Excel/Google Sheets
   - Check `date_read` column has timestamps
   - Verify dates are preserved for existing properties

## Expected Results

After running the pipeline, you should see:

1. **Logs showing:**
   - Many properties SKIPPED (already processed)
   - Few properties making NEW API calls
   - API usage summary at end

2. **Output files:**
   - `output/sales/sales_property_listings_with_distances.csv` with sales properties
   - `output/rental/property_listings_with_distances.csv` with rental properties
   - Both files have `date_read` column
   - No cross-contamination

3. **API Safety:**
   - Warnings if approaching limits
   - Hard stop if limits exceeded
   - Usage summary displayed

## Troubleshooting

If output files are empty:
- Pipeline may still be running
- Check logs for errors
- Verify emails are being fetched
- Check API key is valid

If properties are being re-processed:
- Check logs for "SKIPPED" messages
- Verify finnkode extraction is working
- Check existing data files are being loaded correctly

