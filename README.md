# Property Finder

Automated property finder that fetches rental listings from Finn.no email alerts, geocodes addresses, calculates travel distances, and filters properties based on commute time.

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

- **Email Parsing**: Automatically fetches and parses property listing emails from Finn.no
- **Geocoding**: Converts property addresses to coordinates using Google Maps API
- **Distance Calculation**: Calculates travel time/distance to your workplace using Distance Matrix API
- **Smart Filtering**: Filters properties based on maximum commute time
- **Email Notifications**: Sends results via email with CSV attachments
- **Duplicate Detection**: Tracks processed emails to avoid duplicates
- **Address Ambiguity Handling**: Detects and logs ambiguous addresses for manual review

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

### 3. Set Up Finn.no Email Alerts

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
├── Email_Fetcher.py        # Email parsing module
├── Stringtocordinates.py   # Geocoding module
├── distance_calculator.py  # Distance calculation module
├── email_notifier.py       # Email notification module
├── CSVmerger.py            # CSV utility functions
├── extract_postcode.js     # Postcode extraction utility
├── run_property_finder.sh  # Shell script for automation
├── requirements.txt        # Python dependencies
├── env.template            # Environment variables template
├── .env                    # Your credentials (NOT in repo)
├── output/                 # Generated output files (NOT in repo)
│   ├── property_listings_*.csv
│   ├── ambiguous_addresses_*.csv
│   └── processed_email_uids.json
└── Test_files/             # Sample test files
    └── finn_email_sample.html
```

## Output Files

| File | Description |
|------|-------------|
| `property_listings_latest.csv` | All parsed property listings |
| `property_listings_with_coordinates.csv` | Listings with geocoded coordinates |
| `property_listings_with_distances.csv` | Listings with calculated distances |
| `property_listings_filtered_by_distance.csv` | Filtered by commute time |
| `ambiguous_addresses_latest.csv` | Addresses requiring manual review |

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
