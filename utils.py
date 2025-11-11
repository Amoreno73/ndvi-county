import ee
import config
import time
from datetime import datetime, timedelta
from pytz import timezone
import geemap
import os as os


# TODO : MAKE SURE TO HANDLE FALLBACK DATE WHENEVER A GIVEN DATE RETURNS NO QUERY DATA

####### handling dates #######

def parse_date(custom_date="today", fallback_days=7, time_zone="America/Chicago"):
  '''
  Used in the main NDVI and satellite availability functions.\n
  A ``custom_date`` of "today" uses the date the function was ran (default CST time).\n
  Returns ``start_date`` and ``end_date`` ee.Date objects.
  '''
  if custom_date == "today":
    cst_timezone = timezone(time_zone)
    current_time_cst = datetime.now(cst_timezone)
    start_date = current_time_cst - timedelta(days=fallback_days)
    end_date = current_time_cst
  else:
    try:
      parsed_date = datetime.strptime(custom_date, "%Y-%m-%d")
      start_date = parsed_date - timedelta(days=fallback_days)
      end_date = parsed_date
    except ValueError:
      raise ValueError("custom_date is not formatted correctly!\nFormat should follow 'YYYY-MM-DD'")

  return ee.Date(start_date), ee.Date(end_date)


#### census tract or county units based on STATEFIPS ####

def get_geographic_units(state_fips="17", analysis_level="county"):
  '''
  Returns geographic units to perform calculations in, which is either county or census.\n
  Geographic units are specified by ``analysis_level``, which is ``county`` (default) or ``census``.\n
  A year may be specified as well, to get the corresponding TIGER: US Census Counties dataset. \n
  Note: state_fips must be a string. Ex. Illinois state fips is "17".\n
  Returns an ee.FeatureCollection object. 
  '''

  if analysis_level.lower() == "census":
    units = ee.FeatureCollection("TIGER/2020/TRACT").filter(ee.Filter.eq('STATEFP', state_fips))
  elif analysis_level.lower() == "county":
    units = ee.FeatureCollection(f"TIGER/2018/Counties").filter(ee.Filter.eq('STATEFP', state_fips))
  else:
    raise ValueError("analysis_level must be 'county' or 'census' (case sensitive)")

  return units

### convert to dataframe (to be used in main.py) ### 

def convert_to_df(asset_id_path, export_csv=False, save_path="gee_df_to_csv.csv"):
  '''
  Convert given Google Earth Asset (path) to Pandas dataframe.\n
  Option to export saved df to csv.\n
  Example save path: save_path='gee_df_to_csv.csv'
  FeatureCollection -> DataFrame
  '''
  try:
    fc = ee.FeatureCollection(asset_id_path)
    _ = fc.size().getInfo() # this is unused but forces python to trigger and catch GEE exception
  except:
    print(f"Feature collection for {asset_id_path} not yet ready.\nCheck status at https://code.earthengine.google.com/ under 'Tasks' ")
    return None
  
  df = geemap.ee_to_df(fc)

  if (export_csv):
    df = convert_to_df(asset_id_path)
    if os.path.exists(save_path):
        print(f"'{save_path}' already exists. Will overwrite.")
    else:
        print(f"{save_path} does not exist, creating {save_path}\n")
        print(f"retreived asset at '{asset_id_path}'\nsaved as CSV to: {save_path}")
    df.to_csv(save_path)

  return df

####### adding custom bands to images #######

# Helper functions: NDVI per-pixel calculation, adding bands, combining NDVI data

def addNDVI_lndst8(image):
  # https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LC08_C02_T1_L2
  scaled = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
  ndvi = scaled.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI")
  return image.addBands(ndvi).copyProperties(image, image.propertyNames())

def addNDVI_lndst9(image):
  # https://developers.google.com/earth-engine/datasets/catalog/LANDSAT_LC09_C02_T1_L2
  scaled = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
  ndvi = scaled.normalizedDifference(["SR_B5","SR_B4"]).rename("NDVI")
  return image.addBands(ndvi).copyProperties(image, image.propertyNames())

def addNDVI_stl2(image):
  # https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S2_SR_HARMONIZED
  # Note: I removed cloud masking here since it was producing little to no NDVI data as a result.
  ndvi = image.select(['B8', 'B4']).unmask(0).normalizedDifference(["B8","B4"]).rename("NDVI")
  return image.addBands(ndvi).copyProperties(image, image.propertyNames())

# Adding a unique tag to the Image object (raster data structures).
# This is used for satellite aggregation and satellite info function.
def tag(image, name):
  return image.set('satellite', name)

# This is adding a date to each Image object.
# Simiarly, this is used for date aggregation.
def add_date_band(image):
  date = ee.Date(image.get('system:time_start'))
  return image.set('date_string', date.format('YYYY-MM-dd'))

####### end of adding custom bands #######

# This function gets all NDVI data at once for the average NDVI per geometry calculation.
def combine_ndvis_sats(start_date, end_date, geometry):
  '''
  Gets satellite data from the following sources for the specified geometry:
   * USGS Landsat 8 Level 2, Collection 2, Tier 1
   * USGS Landsat 9 Level 2, Collection 2, Tier 1
   * Harmonized Sentinel-2 MSI: MultiSpectral Instrument, Level-2A (SR)\n
  Note: this handles the fallback to a safe 7-day period if no data was found for the initial date range.\n
  Returns an ee.ImageCollection of all merged NDVI data.
  '''

  lndst8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2").filterDate(start_date, end_date). \
  filterBounds(geometry).map(addNDVI_lndst8).map(lambda img: tag(img, 'LANDSAT_8')).map(add_date_band)

  lndst9 = ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").filterDate(start_date, end_date). \
  filterBounds(geometry).map(addNDVI_lndst9).map(lambda img: tag(img, 'LANDSAT_9')).map(add_date_band)

  stl2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate(start_date, end_date). \
  filterBounds(geometry).map(addNDVI_stl2).map(lambda img: tag(img, 'SENTINEL_2')).map(add_date_band)

  combined_ndvi = lndst8.merge(lndst9).merge(stl2).map(lambda img: img.select('NDVI').copyProperties(img, img.propertyNames()))
  
  # this is causing API limit to be reached. 
  # try:
  #   combined_ndvi.first().getInfo()
  # except Exception as e:
  #   print(e)
  #   print("No satellite data found for this period, falling back to 7-day period")
  #   return None
  
  return combined_ndvi


#### daily composites (ndvi composites using combine_ndvis_sats) ####

def create_daily_composite(ndvi_combined, geometry):
    '''
    Calculate daily mean NDVI composites from the combined three-satellite data.\n
    ``ndvi_combined`` is an ee.ImageCollection of combined ndvi satellite data. \n
    ``geometry`` is the geometry that will be clipped to.\n
    Returns an ee.ImageCollection object of daily composite images. 
    '''
    def daily_composite(date_obj):
      date_str = ee.String(date_obj)
      filtered = ndvi_combined.filter(ee.Filter.eq("date_string", date_str))
      # calculate mean over all NDVI pixels from satellites (if any available)
      # does this for the current date.
      composite = filtered.mean().rename(['NDVI']).clip(geometry).set('date_string', date_str)
      return composite
    # bug fix
    dates = ndvi_combined.aggregate_array("date_string").distinct()
    return ee.ImageCollection.fromImages(dates.map(daily_composite))

##### getting ndvi for each unit, which is either a census tract or county #####

def get_ndvi_per_unit_helper(img, units, ndvi_threshold=0.6):
  '''
  ``img`` is a single ee.Image \n
  ``units`` is a county or census tract ee.FeatureCollection. 
  ``ndvi_threshold`` is used for calculating area (in km^2) of NDVI above the threshold (default 0.6). \n   
  Returns an ee.FeatureCollection object with NDVI data per unit.
  '''
  date_str = img.get("date_string")

  def geometry_mean(geom):
    # use reduce region for the current unit geometry
    # this is what actually calculates the mean NDVI per unit (using geom)

    mean_result = img.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geom.geometry(),
        scale=30,
        maxPixels=1e9
    )

    # new change -> for each county, return total area (ex. km^2 of NDVI > 0.6)
    bool_ndvi_threshold = img.select('NDVI').gt(ndvi_threshold)

    ndvi_threshold_area = bool_ndvi_threshold.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=geom.geometry(),
        scale=30,
        maxPixels=1e9
    )
    high_ndvi_pixels = ee.Number(ndvi_threshold_area.get('NDVI'))
    high_ndvi_area_km2 = high_ndvi_pixels.multiply(900).divide(1e6)

    return ee.Feature(geom.geometry().centroid(), {

        'unit_fips': geom.get('GEOID'),
        'unit_name': geom.get('NAMELSAD'), 
        'date_string': date_str,
        'NDVI': mean_result.get('NDVI'),
        'high_ndvi_area_km2': high_ndvi_area_km2
    })
  
  return units.map(geometry_mean)


##### get satellite availability for each unit (census tract/county) #####
def get_satellite_availability_helper(ndvi_combined, units):
  '''
  Check if a specified unit has an orbiting satellite for a given date (from img). \n
  ``ndvi_combined`` has "date_string" band, which is used here. \n
  Returns an ee.FeatureCollection object.
  '''
  dates = ndvi_combined.aggregate_array("date_string").distinct()

  def satellite_status_daily(date_str):
    daily_images = ndvi_combined.filter(ee.Filter.eq("date_string",date_str))
    def check_for_unit(unit):
      unit_geom = unit.geometry()

      # only get the images for the current county geometry
      unit_images = daily_images.filterBounds(unit_geom)

      # get count of each satellite's appearance
      # boolean yes/no -> compare size to 0 (true if size > 0)
      # in the final asset: will be represented as 0 (false) or 1 (true)
      landsat8_bool = unit_images.filter(ee.Filter.eq('satellite', 'LANDSAT_8')).size().gt(0)
      landsat9_bool = unit_images.filter(ee.Filter.eq('satellite', 'LANDSAT_9')).size().gt(0)
      sentinel2_bool = unit_images.filter(ee.Filter.eq('satellite', 'SENTINEL_2')).size().gt(0)
      
      # return ee.Feature for given county gemometry on given date. 
      return ee.Feature(unit_geom.centroid(), {
        # GEOID and NAMELSAD should be flexible for both county or tract
        # https://developers.google.com/earth-engine/datasets/catalog/TIGER_2020_TRACT
        # https://developers.google.com/earth-engine/datasets/catalog/TIGER_2018_Counties 
        'unit_fips': unit.get('GEOID'),
        'unit_name': unit.get('NAMELSAD'),
        'date_string': date_str,
        'LANDSAT_8': landsat8_bool,
        'LANDSAT_9': landsat9_bool,
        'SENTINEL_2': sentinel2_bool
      })
    
    return units.map(check_for_unit)
  
  availability_list = dates.map(satellite_status_daily)
  availability_features = ee.FeatureCollection(availability_list).flatten()

  return availability_features
