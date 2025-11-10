import ee
import config
import time
from datetime import datetime, timedelta
from pytz import timezone
import geemap

ee.Authenticate()
ee.Initialize(project='gee-nvdi-county')

### convert to dataframe ### 

def convert_to_df(asset_id_path):
  '''
  Convert given Google Earth Asset (path) to Pandas dataframe.\n
  FeatureCollection -> DataFrame
  '''

  fc = ee.FeatureCollection(asset_id_path)
  df = geemap.ee_to_df(fc)

  return df

### convert to csv ###


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

# This function gets all NDVI data at once for the average NDVI per county calculation.
def combine_ndvis_sats(start_date, end_date, county_geom):
  '''
  Gets satellite data from the following sources for the specified county geometry:
  # USGS Landsat 8 Level 2, Collection 2, Tier 1
  # USGS Landsat 9 Level 2, Collection 2, Tier 1
  # Harmonized Sentinel-2 MSI: MultiSpectral Instrument, Level-2A (SR)

  '''
  lndst8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2").filterDate(start_date, end_date). \
  filterBounds(county_geom).map(addNDVI_lndst8).map(lambda img: tag(img, 'LANDSAT_8')).map(add_date_band)

  lndst9 = ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").filterDate(start_date, end_date). \
  filterBounds(county_geom).map(addNDVI_lndst9).map(lambda img: tag(img, 'LANDSAT_9')).map(add_date_band)

  stl2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate(start_date, end_date). \
  filterBounds(county_geom).map(addNDVI_stl2).map(lambda img: tag(img, 'SENTINEL_2')).map(add_date_band)

  combined_ndvi = lndst8.merge(lndst9).merge(stl2). \
  map(lambda img: img.select('NDVI').copyProperties(img))
  return combined_ndvi


# below is the main function that performs the county wise NDVI calculations

def get_daily_NDVI_illinois_counties(asset_id=config.ASSET_ID_NDVI, custom_date="today"):
  '''
  ***OPTIMIZED (creates GEE Asset instead of running everything locally)***
  Get the past ten day average NDVI data per county (Illinois only) for a specified date.
  custom_date should be formatted as "YYYY-MM-DD", or enter "today" for most recent ten day data.
  Example: custom_date = "2025-10-30"
  '''

  # TIGER: US Census Counties 2018 -> filter by Illinois FIPS code of 17.
  counties = ee.FeatureCollection("TIGER/2018/Counties").filter(ee.Filter.eq('STATEFP', '17'))

  # test with n counties
  counties = counties.limit(2)

  county_geom = counties.geometry()

  # default date is "today" -> whenever the function was ran (in CST time).
  # get past 24 hr data from 'date'
  if custom_date == "today":
    cst_timezone = timezone('America/Chicago')
    current_time_cst = datetime.now(cst_timezone)
    start_date = current_time_cst - timedelta(days=1)
    end_date = current_time_cst
  else:
    try:
      parsed_date = datetime.strptime(custom_date, "%Y-%m-%d")
      start_date = parsed_date - timedelta(days=7)
      end_date = parsed_date
    except:
      print("custom_date is not formatted correctly!\nFormat should follow 'YYYY-MM-DD'")

  # use the helper function to combine all ndvi data from satellites used for this analysis
  ndvi_combined = combine_ndvis_sats(start_date, end_date, county_geom)

  # this calculates the DAILY NDVI composite from the satellites
  def daily_composite(date_str):
    filtered = ndvi_combined.filter(ee.Filter.eq("date_string", date_str))
    # calculate mean over all NDVI pixels from satellites (if any available)
    # does this for the current date.
    composite = filtered.mean().rename(['NDVI']).clip(county_geom).set('date_string', date_str)
    return composite

  # in the resulting asset:
  # no dates will be shown for those days that did not have any satellite orbit over Illinois
  dates = ndvi_combined.aggregate_array("date_string").distinct()
  daily_images = ee.ImageCollection.fromImages(dates.map(daily_composite))

  def create_county_features(img):
    date_str = img.get('date_string')

    def county_mean(county):
      # use reduce region for the current county geometry
      # this is what actually calculates the mean NDVI per county (using geometry)

      mean_result = img.reduceRegion(
          reducer=ee.Reducer.mean(),
          geometry=county.geometry(),
          scale=30,
          maxPixels=1e9
      )

      # new change -> for each county, return total area (square mi. of NDVI > 0.6)
      bool_ndvi_threshold = img.select('NDVI').gt(0.6)

      ndvi_threshold = bool_ndvi_threshold.reduceRegion(
          reducer=ee.Reducer.sum(),
          geometry=county.geometry(),
          scale=30,
          maxPixels=1e9
      )
      high_ndvi_pixels = ee.Number(ndvi_threshold.get('NDVI'))
      high_ndvi_area_km2 = high_ndvi_pixels.multiply(900).divide(1e6)

      # use county centroid as geometry
      # then finally create ee.Feature for each county on each date
      return ee.Feature(county.geometry().centroid(), {
          'GEOID': county.get('GEOID'),
          'NAME': county.get('NAME'),
          'county_fips': county.get('GEOID'),
          'county_name': county.get('NAME'),
          'date_string': date_str,
          'NDVI': mean_result.get('NDVI'),
          'high_ndvi_area_km2': high_ndvi_area_km2
      })
    return counties.map(county_mean)

  per_image_per_county = daily_images.map(create_county_features).flatten()

  task = ee.batch.Export.table.toAsset(
      collection=per_image_per_county,
      description='NDVI_county_daily_export',
      assetId=asset_id
  )

  task.start()
  print(f"Export started for Asset ID: {asset_id}")

  return task

# below is the function that calculates the satellite availability for NDVI calculations

def get_satellite_availability_per_county(asset_id=config.ASSET_ID_SAT, custom_date="today"):
  '''
  Count daily satellite image availability for each Illinois county over the past 10 days.
  Returns counts for LANDSAT_8, LANDSAT_9, and SENTINEL_2.
  Output: GEE Asset with satellite counts per county per day
  '''
  counties = ee.FeatureCollection("TIGER/2018/Counties").filter(ee.Filter.eq('STATEFP', '17'))

  # test with n counties
  counties = counties.limit(2)

  # same date parsing process as get_daily_NDVI_illinois_counties
  if custom_date == "today":
    cst_timezone = timezone('America/Chicago')
    current_time_cst = datetime.now(cst_timezone)
    start_date = current_time_cst - timedelta(days=1)
    end_date = current_time_cst
  else:
    try:
      parsed_date = datetime.strptime(custom_date, "%Y-%m-%d")
      start_date = parsed_date - timedelta(days=7)
      end_date = parsed_date
    except:
      print("custom_date is not formatted correctly!\nFormat should follow 'YYYY-MM-DD'")

  county_geom = counties.geometry()
  ndvi_combined = combine_ndvis_sats(start_date, end_date, county_geom)

  dates = ndvi_combined.aggregate_array('date_string').distinct()

  def count_satellites_by_date(date_str):
    # Filter images for given date 
    daily_images = ndvi_combined.filter(ee.Filter.eq('date_string', date_str))

    def check_for_county(county):
      county_geom = county.geometry()

      # only get the images for the current county geometry
      county_images = daily_images.filterBounds(county_geom)

      # get count of each satellite's appearance
      # boolean yes/no -> compare size to 0 (true if size > 0)
      # in the final asset: will be represented as 0 (false) or 1 (true)
      landsat8_bool = county_images.filter(ee.Filter.eq('satellite', 'LANDSAT_8')).size().gt(0)
      landsat9_bool = county_images.filter(ee.Filter.eq('satellite', 'LANDSAT_9')).size().gt(0)
      sentinel2_bool = county_images.filter(ee.Filter.eq('satellite', 'SENTINEL_2')).size().gt(0)
      
      # return ee.Feature for given county gemometry on given date. 
      return ee.Feature(county.geometry().centroid(), {
        'county_fips': county.get('GEOID'),
        'county_name': county.get('NAME'),
        'date_string': date_str,
        'LANDSAT_8': landsat8_bool,
        'LANDSAT_9': landsat9_bool,
        'SENTINEL_2': sentinel2_bool
      })

    return counties.map(check_for_county)

  # Fix: Use fromImages to convert list to ImageCollection, then flatten properly
  availability_list = dates.map(count_satellites_by_date)
  availability_features = ee.FeatureCollection(availability_list).flatten()

  task = ee.batch.Export.table.toAsset(
    collection=availability_features,
    description='Satellite_availability_export',
    assetId=asset_id
  )

  task.start()
  print(f"Export started for Asset ID: {asset_id}")

  return task
