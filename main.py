import ee
from ndvi_utils import get_daily_NDVI_illinois_counties,get_satellite_availability_per_county, convert_to_df
from config import ASSET_ID_NDVI, ASSET_ID_SAT
import pandas as pd

# connecting Google Earth Engine
ee.Authenticate()
ee.Initialize(project='gee-nvdi-county')

# testing daily avg NDVI for 2 counties only.
# daily_ndvi_task = get_daily_NDVI_illinois_counties(custom_date="2025-11-09")
# print(f"Starting export for {ASSET_ID_NDVI}...")
# print(daily_ndvi_task.status())

# new utils method -> converts GEE asset to csv 
# later csv to json may be needed. 
# df = convert_to_df(ASSET_ID_NDVI)
# df.to_csv("test_csv_ndvi.csv")

print("\n")

# testing satellite availability for 2 counties only. 
# daily_sat_task = get_satellite_availability_per_county(date="2025-11-09")
# print(f"Starting export for {ASSET_ID_SAT}...")
# print(daily_sat_task.status())
# df = convert_to_df(ASSET_ID_SAT)
# df.to_csv("test_csv_sat.csv")


