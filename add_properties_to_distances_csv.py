#!/usr/bin/env python3
"""
Script to add properties from coordinates CSV to distances CSV with placeholder values.
This marks them as processed so they won't be distance matrixed again.
"""

import pandas as pd
import os
import re

# Work location from config.yaml
WORK_LAT = 59.899
WORK_LNG = 10.627

# Placeholder distance values (very high so they'll be filtered out)
PLACEHOLDER_DISTANCE_KM = 999.0
PLACEHOLDER_TRANSIT_TIME_MIN = 999.0

def extract_finnkode(url):
    """Extract finnkode from URL"""
    if not url or pd.isna(url):
        return None
    url = str(url)
    match = re.search(r'finnkode[=:](\d+)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r'/(\d{6,})', url)
    if match:
        return match.group(1)
    return None

# All 408 finnkodes that need to be marked
target_finnkodes = [
    '328767712', '360747759', '440541373', '366405174', '334813637', '212091683',
    '417581735', '440578633', '366849352', '347584378', '226099946', '440541016',
    '440539915', '440530920', '316215235', '440518798', '337300627', '437662585',
    '437614461', '415067542', '316280619', '437512508', '382261710', '437502061',
    '437460678', '437462548', '432348212', '437425475', '437423767', '437324424',
    '300130557', '208869892', '323531982', '437264472', '267228012', '318198775',
    '371594341', '437184171', '337126602', '439102398', '308411985', '266546383',
    '439251161', '433745035', '300203544', '161992583', '438860904', '325036091',
    '439109075', '439098191', '410748418', '258657567', '439050138', '439048252',
    '439011770', '431322056', '117480789', '438996996', '68506006', '423678831',
    '437964122', '417591256', '433241782', '411851206', '438919053', '337323485',
    '287419694', '230250690', '231647991', '152548665', '438850891', '438847834',
    '319996143', '352722369', '438820703', '412329746', '438796663', '438788186',
    '438786061', '438768434', '374462656', '381539037', '437258082', '437918911',
    '437902104', '437890605', '203198840', '437874912', '437866030', '375345381',
    '437859776', '437839316', '437834167', '369234137', '257529749', '437803828',
    '277126124', '437762531', '437718802', '437757446', '410227864', '55673968',
    '427943616', '318610043', '368877278', '283516128', '438682759', '436378158',
    '419811133', '314498521', '438654643', '427960328', '263891025', '438314391',
    '275487719', '417234323', '244454169', '437705301', '220274220', '438430544',
    '429426266', '440502473', '113633620', '440461594', '202453679', '440459869',
    '438160939', '440434696', '338241835', '105006652', '230069965', '79860078',
    '146091920', '429831028', '440377487', '387494295', '440364431', '246670595',
    '440341300', '440321030', '440285324', '440144042', '440291759', '88586924',
    '366805907', '435247726', '316076119', '151308150', '435045104', '427227923',
    '434879482', '434863241', '404296902', '434746759', '434732015', '434714751',
    '434547030', '320149552', '196043175', '434162687', '433487187', '356045171',
    '433983114', '433935482', '140488434', '117833273', '440257190', '440218559',
    '440235804', '440197326', '440222914', '440077610', '440205876', '439885986',
    '329917894', '363315925', '386469488', '440145771', '440138836', '258733745',
    '440126140', '349570720', '440125210', '203333412', '166231072', '409009991',
    '74623454', '182129604', '438281925', '438268915', '438265271', '361568073',
    '111030808', '363488907', '265561091', '196752457', '280870671', '199333316',
    '166733202', '375820808', '438061041', '438032615', '438014850', '438016410',
    '379726666', '432619047', '437988430', '437965484', '329809386', '313765999',
    '437104205', '437101450', '361520925', '112738840', '437058486', '322180191',
    '112155269', '333534352', '437014783', '437000004', '239955843', '436965093',
    '436961933', '139853474', '417042236', '435778893', '436698113', '431784347',
    '373755469', '282165701', '433550719', '433122595', '422811642', '60644840',
    '439773274', '439014921', '257796359', '439756393', '295663905', '330374511',
    '229897049', '439703797', '439705843', '439176531', '84345902', '237165838',
    '429281639', '218163574', '273617254', '439265967', '439332028', '439608489',
    '382076557', '227271803', '336099373', '179325935', '439528901', '431590965',
    '269670404', '439487104', '439469884', '273361770', '90172688', '399666398',
    '110846935', '439020435', '379322731', '439396513', '297672463', '439378004',
    '428072804', '438089798', '438362272', '152354687', '434929687', '277681371',
    '439289584', '225514231', '439274963', '439271231', '439264560', '64759382',
    '202124323', '76802127', '433116000', '422234420', '432340200', '432351779',
    '432044683', '432100145', '431959892', '431569754', '431332501', '289633441',
    '430566679', '427207146', '426910626', '426350518', '425175634', '356428344',
    '419118577', '414483631', '393033771', '436743947', '436735725', '214963806',
    '74308980', '436691563', '431658701', '436626478', '426538214', '436644397',
    '168899909', '148699367', '155686911', '436609485', '436556035', '436377439',
    '295225067', '394530687', '324391930', '273583292', '276244074', '438716243',
    '436141960', '373716602', '177400028', '412601361', '439922194', '439905266',
    '438808141', '164260258', '88165728', '399473132', '439881304', '439879560',
    '439869430', '275920437', '439864230', '282667310', '393105146', '439790751',
    '404804712', '436026873', '435987833', '435997706', '382139127', '171445134',
    '435898137', '435861020', '356170681', '435726929', '435704487', '301550275',
    '435576613', '400001480', '244018152', '435500778', '410720317', '435422153',
    '435384234', '436471442', '436000701', '402858851', '338962054', '436445373',
    '435787293', '184727022', '430840329', '380474914', '436209882', '386837319',
    '436193640', '113857647', '436092730', '436081702', '204929686', '436067426',
    '381666320', '413457141', '153203386', '293794225', '182080417', '210948497',
    '440077260', '440076360', '429732243', '424513627', '440034732', '439240670',
    '440030814', '440030526', '440029335', '436635914', '440013542', '435111880',
    '429807273', '242911899', '439988597', '348950578', '307224823', '395532765',
    '385492088', '335243519', '345347247', '344024936', '269169978', '179995811'
]

target_set = set(target_finnkodes)

# Load coordinates CSV to get property data
coords_csv = 'output/property_listings_with_coordinates.csv'
distances_csv = 'output/property_listings_with_distances.csv'

if not os.path.exists(coords_csv):
    print(f"❌ Coordinates CSV not found: {coords_csv}")
    exit(1)

print(f"📖 Reading {coords_csv}...")
df_coords = pd.read_csv(coords_csv)
df_coords['_finnkode'] = df_coords['link'].apply(extract_finnkode)

# Filter to target properties
df_to_add = df_coords[df_coords['_finnkode'].isin(target_set)].copy()

if len(df_to_add) == 0:
    print(f"❌ No target properties found in coordinates CSV")
    exit(1)

print(f"📊 Found {len(df_to_add)} target properties in coordinates CSV")

# Load existing distances CSV if it exists
if os.path.exists(distances_csv):
    print(f"📖 Reading {distances_csv}...")
    df_distances = pd.read_csv(distances_csv)
    df_distances['_finnkode'] = df_distances['link'].apply(extract_finnkode)
    
    # Remove target properties that already exist
    existing_finnkodes = set(df_distances['_finnkode'].dropna().unique())
    df_to_add = df_to_add[~df_to_add['_finnkode'].isin(existing_finnkodes)].copy()
    print(f"📊 {len(df_to_add)} properties need to be added (others already exist)")
else:
    df_distances = pd.DataFrame()

# Prepare properties to add with distance data
for idx in df_to_add.index:
    df_to_add.at[idx, 'distance_to_work_km'] = PLACEHOLDER_DISTANCE_KM
    df_to_add.at[idx, 'transit_time_work_minutes'] = PLACEHOLDER_TRANSIT_TIME_MIN
    df_to_add.at[idx, 'work_lat'] = WORK_LAT
    df_to_add.at[idx, 'work_lng'] = WORK_LNG

# Remove temporary column
df_to_add = df_to_add.drop(columns=['_finnkode'], errors='ignore')

# Merge with existing distances CSV
if len(df_distances) > 0:
    # Ensure columns match
    for col in df_distances.columns:
        if col not in df_to_add.columns:
            df_to_add[col] = None
    for col in df_to_add.columns:
        if col not in df_distances.columns:
            df_distances[col] = None
    
    # Concatenate
    df_result = pd.concat([df_distances.drop(columns=['_finnkode'], errors='ignore'), df_to_add], ignore_index=True)
    print(f"📊 Merged with existing {len(df_distances)} properties")
else:
    df_result = df_to_add
    print(f"📊 Creating new distances CSV")

# Save
df_result.to_csv(distances_csv, index=False, encoding='utf-8')
print(f"✅ Added {len(df_to_add)} properties to {distances_csv}")
print(f"💾 Total properties in distances CSV: {len(df_result)}")
print(f"\n⚠️  Note: Properties marked with placeholder distance values (999 km, 999 min)")
print(f"   They will be skipped in future runs but will exceed the distance filter.")

