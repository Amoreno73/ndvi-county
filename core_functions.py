import ee
import config
import time
from datetime import datetime, timedelta
from pytz import timezone
import geemap
import os as os
from utils import *

ee.Authenticate()
ee.Initialize(project='gee-nvdi-county')

# below is the main function that performs the county wise NDVI calculations

def get_mean_NDVI_per_unit(state_fips, asset_id, custom_date="today", analysis_level="county", ndvi_threshold=0.6, fallback_days=7, limit_units=None):
  '''
  **OPTIMIZED** (creates GEE Asset instead of running everything locally) \n

  ``state_fips`` -> State code in string format. Example: '17' corresponds to Illinois.\n
  ``asset_id`` -> Path to export task to in Google Earth Engine. \n
  ``custom_date`` -> Corresponds to end date of ``fallback_days`` date range. Format should follow 'YYYY-MM-DD''.\n
  ``analysis_level`` -> Either `county` for a county level analysis or `census` for a census-tract level analysis. \n
  ``ndvi_threshold`` -> The NDVI threshold to calculate the total area in km^2 for. \n
  ``fallback_days`` -> The period to calculate the mean NDVI county/census tract level analysis over.\n
  ``limit_units`` -> For testing mainly to only calculate a few GEE features at a time. \n

  Returns (and starts) a Google Earth Engine Task for calculating the mean NDVI at the specified analysis level for the specified date range. 
  '''

  # TIGER: US Census Counties 2018 -> filter by Illinois FIPS code of 17.
  # note: i am defining units to be either counties or census tracts. 
  
  units = get_geographic_units(state_fips, analysis_level)

  if limit_units:
    units = units.limit(limit_units)

  geometry = units.geometry()
  start_date, end_date = parse_date(custom_date, fallback_days)
  ndvi_combined = combine_ndvis_sats(start_date, end_date, geometry)
  
  if ndvi_combined is None:
    # redefine start and end to be the fallback 2 week period
    print("--FALLBACK LOGIC TRIGGERED--")
    start_date, end_date = parse_date(custom_date, fallback_days=7)
    ndvi_combined = combine_ndvis_sats(start_date, end_date, geometry)

  daily_images = create_daily_composite(ndvi_combined, geometry)
  
  def create_features(img):
    return get_ndvi_per_unit_helper(img, units, ndvi_threshold)
  
  per_image_per_unit = daily_images.map(create_features).flatten()

  task = ee.batch.Export.table.toAsset(
      collection=per_image_per_unit,
      description=f'NDVI_{analysis_level}_availability_export_statefips{state_fips}',
      assetId=asset_id
  )

  task.start()
  print(f"Export started for Asset ID: {asset_id}. {analysis_level}-level analysis performed.")

  return task

# below is the function that calculates the satellite availability for NDVI calculations
# new flow after modularizing code: parse date, get unit for analysis, combine ndvi, get availability, export to gee 
def get_satellite_availability_per_unit(state_fips, asset_id, custom_date="today", analysis_level="county", fallback_days=7, limit_units=None):
  '''
  **OPTIMIZED** (creates GEE Asset instead of running everything locally) \n

  ``state_fips`` -> State code in string format. Example: '17' corresponds to Illinois.\n
  ``asset_id`` -> Path to export task to in Google Earth Engine. \n
  ``custom_date`` -> Corresponds to end date of ``fallback_days`` date range. Format should follow 'YYYY-MM-DD''.\n
  ``analysis_level`` -> Either `county` for a county level analysis or `census` for a census-tract level analysis. \n
  ``ndvi_threshold`` -> The NDVI threshold to calculate the total area in km^2 for. \n
  ``fallback_days`` -> The period to calculate the mean NDVI county/census tract level analysis over.\n
  ``limit_units`` -> For testing mainly to only calculate a few GEE features at a time. \n

  Returns (and starts) a Google Earth Engine Task for calculating the satellite availability (bool) at the specified analysis level for the specified date range. 
  '''
  units = get_geographic_units(state_fips, analysis_level)
    ## (vvv if not none)
  if (limit_units):
    # for testing purposes, we may limit the GEE query to n-counties or n-census tracts.
    units = units.limit(limit_units)
  
  geometry = units.geometry()
  start_date, end_date = parse_date(custom_date, fallback_days)
  ndvi_combined = combine_ndvis_sats(start_date, end_date, geometry)

#   if ndvi_combined is None:
#     # redefine start and end to be the fallback 1 week period
#     start_date, end_date = parse_date(custom_date, fallback_days=7)
#     ndvi_combined = combine_ndvis_sats(start_date, end_date, geometry)
  
  if ndvi_combined is None:
    # redefine start and end to be the fallback 2 week period
    start_date, end_date = parse_date(custom_date, fallback_days=7)
    ndvi_combined = combine_ndvis_sats(start_date, end_date, geometry)

  availability_features = get_satellite_availability_helper(ndvi_combined, units)

  task = ee.batch.Export.table.toAsset(
    collection=availability_features,
    description=f'Satellite_{analysis_level}_availability_export_statefips{state_fips}',
    assetId=asset_id
  )

  task.start()
  print(f"Export started for Asset ID: {asset_id}. {analysis_level}-level analysis performed.")

  return task

