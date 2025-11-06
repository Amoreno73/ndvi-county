import ee
from ndvi_utils import get_daily_NDVI_illinois_counties,get_satellite_availability_per_county 
from config import ASSET_ID_NDVI, ASSET_ID_SAT

# connecting Google Earth Engine
ee.Authenticate()
ee.Initialize(project='gee-nvdi-county')

# testing daily avg NDVI for 2 counties only.
daily_ndvi_task = get_daily_NDVI_illinois_counties()
print(f"Starting export for {ASSET_ID_NDVI}...")
print(daily_ndvi_task.status())

print("\n")

# testing satellite availability for 2 counties only. 
daily_sat_task = get_satellite_availability_per_county()
print(f"Starting export for {ASSET_ID_SAT}...")
print(daily_sat_task.status())
