# Property Finder

Automated property finder that fetches rental and sales listings from Finn.no email alerts, geocodes addresses, calculates travel distances, and filters properties based on commute time. Supports both rental and sales properties with separate pipelines and master listings integration.

---

## ⚠️ IMPORTANT: Security Notice

**This repository does NOT contain any API keys, passwords, or credentials.**

To run this project, you must:
1. Create your **own** `.env` file with **your own** credentials
2. Get your **own** [Google Maps API key](https://console.cloud.google.com/apis/credentials)
3. Use your **own** Gmail account with an [App Password](https://support.google.com/accounts/answer/185833)

The `.env` file is excluded from git via `.gitignore`. See `env.template` for the required format.

**If you're setting up your own Google Maps API key, I strongly recommend:**
- Setting up [billing alerts](https://cloud.google.com/billing/docs/how-to/budgets) in Google Cloud Console
- Restricting your API key to specific APIs and IP addresses
- Setting daily/monthly quotas on your APIs

---

## Features

- **Email Parsing**: Automatically fetches and parses property listing emails from Finn.no (rental and sales)
- **Master Listings Integration**: Merges email-fetched properties with master listings CSV files (`master_listings.csv` for rentals, `master_listings_sales.csv` for sales)
- **Geocoding**: Converts property addresses to coordinates using Google Maps API
- **Distance Calculation**: Calculates travel time/distance to your workplace using Distance Matrix API
- **Smart Filtering**: Filters properties based on maximum commute time (prevents API calls for properties outside range)
- **Config-Aware Deduplication**: Tracks processed properties with config parameters (work location, max transit time) to avoid re-processing unless config changes
- **Email Notifications**: Sends results via email with CSV attachments
- **Duplicate Detection**: Tracks processed emails to avoid duplicates
- **Address Ambiguity Handling**: Detects and logs ambiguous addresses for manual review
- **Sales Properties Support**: Full pipeline support for sales properties with separate output files and configuration

## Workflow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│  Email Fetcher  │───▶│  Geocoding       │───▶│ Distance Calculator │
│  (Finn.no)      │    │  (Google Maps)   │    │ (Distance Matrix)   │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                                          │
                                                          ▼
                                               ┌─────────────────────┐
                                               │  Email Notifier     │
                                               │  (Results Summary)  │
                                               └─────────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the template and fill in your own credentials:

```bash
cp env.template .env
```

Then edit `.env` with your values:

```env
# Email credentials (Gmail with App Password)
EMAIL=your_email@gmail.com
PASSWORD=your_gmail_app_password

# Google Maps API key
GOOGLE_API_KEY=your_google_maps_api_key
```

**Important Notes:**
- For Gmail, you need to use an [App Password](https://support.google.com/accounts/answer/185833), not your regular password
- Enable the following Google Cloud APIs: Geocoding API, Distance Matrix API, Places API
- **Set up billing alerts and API quotas** to avoid unexpected charges

### 3. Configure Property Types (Optional)

The project supports both rental and sales properties. Configuration is done via `config.yaml`:

```yaml
rental:
  enabled: true
  email:
    subject_keywords:
      - 'Nye annonser: Property Finder - Leie'
  days_back: 90

sales:
  enabled: true
  email:
    subject_keywords:
      - 'Nye annonser: Property Finder - Eie'
  days_back: 90
```

If `config.yaml` doesn't exist, the project defaults to rental-only mode using `config.py`.

### 4. Set Up Master Listings (Optional)

You can maintain master listings CSV files that will be merged with email-fetched properties:
- **Rentals**: `master_listings.csv` (comma-delimited)
- **Sales**: `master_listings_sales.csv` (semicolon-delimited)

Properties in master listings are automatically deduplicated against already-processed properties.

### 5. Set Up Finn.no Email Alerts

1. Go to [Finn.no](https://www.finn.no) and create a property search with your criteria
2. Enable email alerts for new listings
3. The script will automatically parse these emails

## Usage

### Basic Usage

```bash
python property_finder.py
```

### Test Mode (uses separate output directory)

```bash
python property_finder.py --test-mode
```

### Skip Specific Steps

```bash
# Skip email fetching (use existing CSV)
python property_finder.py --skip-email

# Skip geocoding
python property_finder.py --skip-geocode

# Skip distance calculation
python property_finder.py --skip-distance
```

### View All Options

```bash
python property_finder.py --help
```

## Project Structure

```
Property_Finder/
├── property_finder.py      # Main orchestration script
├── Email_Fetcher.py        # Email parsing module (rental & sales)
├── Stringtocordinates.py   # Geocoding module
├── distance_calculator.py  # Distance calculation module
├── email_notifier.py       # Email notification module
├── CSVmerger.py            # CSV utility functions
├── config.py               # Configuration (rental defaults)
├── config.yaml             # YAML configuration (rental & sales)
├── extract_postcode.js     # Postcode extraction utility
├── run_property_finder.sh  # Shell script for automation
├── requirements.txt        # Python dependencies
├── env.template            # Environment variables template
├── .env                    # Your credentials (NOT in repo)
├── master_listings.csv     # Master rental listings (optional)
├── master_listings_sales.csv  # Master sales listings (optional)
├── output/                 # Generated output files (NOT in repo)
│   ├── property_listings_*.csv  # Rental outputs
│   ├── sales/                   # Sales outputs
│   │   └── sales_property_listings_*.csv
│   ├── ambiguous_addresses_*.csv
│   └── processed_email_uids.json
└── Test_files/             # Sample test files
    └── finn_email_sample.html
```

## Output Files

### Rental Properties
| File | Description |
|------|-------------|
| `property_listings_latest.csv` | All parsed rental listings |
| `property_listings_with_coordinates.csv` | Rentals with geocoded coordinates |
| `property_listings_with_distances.csv` | Rentals with calculated distances |
| `property_listings_filtered_by_distance.csv` | Rentals filtered by commute time |
| `ambiguous_addresses_latest.csv` | Addresses requiring manual review |

### Sales Properties
| File | Description |
|------|-------------|
| `sales_property_listings_latest.csv` | All parsed sales listings |
| `sales_property_listings_with_coordinates.csv` | Sales with geocoded coordinates |
| `sales_property_listings_with_distances.csv` | Sales with calculated distances |
| `sales_property_listings_filtered_by_distance.csv` | Sales filtered by commute time |
| `sales_ambiguous_addresses_latest.csv` | Sales addresses requiring manual review |

**Note**: Sales properties use the `sales_` prefix to distinguish them from rental properties. All output files are stored in the `output/` directory (or `output/sales/` subdirectory for sales-specific files).

## Automation

Use the included shell script with a LaunchAgent (macOS) or cron job to run automatically:

```bash
# Make the script executable
chmod +x run_property_finder.sh

# Run manually
./run_property_finder.sh
```

## License

MIT License
