import os
import googlemaps

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')  # Loads your .env file with GOOGLE_API_KEY

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY is not set in .env!")

gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

# Test print
print(gmaps)