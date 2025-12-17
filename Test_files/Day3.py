# Search criteria variables

areas     = ['Oslo', 'Lillestrøm'] # Areas to search in
price_max = 5000000 # Maximum price in NOK
size_min  = 21 # Minimum size in square meters
max_distance_km = 10 # Maximum distance from work location
gym_keywords = ['Evo', 'Evo Fitness', 'Trening', 'Fitness', 'SATS'] # Keywords for nearby gyms

# Reference work location in Fornebu (Telenor Building)
WORK_LAT = 59.8985 
WORK_LNG = 10.6232
WORK_ADDRESS = 'Fornebu, Bærum, Norway' # Reference address

if __name__ == "__main__":
    print("Starting property finder...")
    print(f"Searching for properties in {areas} with a maximum price of {price_max} NOK")
    print(f"Searching for properties with a minimum size of {size_min} square meters")
    print(f"Searching for properties with a maximum distance of {max_distance_km} kilometers from {WORK_ADDRESS}")
    print(f"Searching for properties with the keywords {gym_keywords}")
    print(f"Work location: {WORK_ADDRESS} ({WORK_LAT}, {WORK_LNG})")
    print("Done")