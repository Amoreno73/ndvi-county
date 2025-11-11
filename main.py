from config import *
import os as os
from utils import convert_to_df
from core_functions import get_mean_NDVI_per_unit, get_satellite_availability_per_unit

# Note:
# once a task finishes on GEE, go back and comment the task generation here
# then rerun the convert_to_df function since it is finished now.
# when finished with the csv, comment out the corresponding convert_to_df function

########################
# CSV GENERATION STATS #
#                      #
# NDVI County Done     #
# NDVI Census Running  #
# SAT County  Running  #
# SAT Census  Running  #
# -------------------- #
# 1/4 CSV Files Done   #
########################

############ GOOGLE EARTH TASKS - ASSET_ID_NDVI_COUNTY ############
# print(f"Starting export for {ASSET_ID_NDVI_COUNTY}...")

# ndvi_county = get_mean_NDVI_per_unit(
#     state_fips='17',
#     asset_id=ASSET_ID_NDVI_COUNTY,
#     custom_date="2025-11-10",
#     analysis_level='county', 
#     ndvi_threshold=0.6,
#     fallback_days=7
# )

# print("Finished GEE export:\n")
# print(ndvi_county.status())

# print("\n")
# ndvi_county_path = r".\output\illinois_ndvi_county_level.csv"
# convert_to_df(ASSET_ID_NDVI_COUNTY, export_csv=True, save_path=ndvi_county_path)
# print("\n")
####################################################################


########### GOOGLE EARTH TASKS - ASSET_ID_NDVI_TRACT ############
# print(f"Starting export for {ASSET_ID_NDVI_TRACT}...")

# ndvi_census = get_mean_NDVI_per_unit(
#     state_fips='17',
#     asset_id=ASSET_ID_NDVI_TRACT,
#     custom_date="2025-11-10",
#     analysis_level='census', 
#     ndvi_threshold=0.6,
#     fallback_days=7
# )

# print("Finished GEE export:\n")
# print(ndvi_census.status())

print("\n")
ndvi_census_path = r".\output\illinois_ndvi_census_level.csv"
convert_to_df(ASSET_ID_NDVI_TRACT, export_csv=True, save_path=ndvi_census_path)
print("\n")
###################################################################


############ GOOGLE EARTH TASKS - ASSET_ID_SAT_COUNTY ############
# print(f"Starting export for {ASSET_ID_SAT_COUNTY}...")

# sat_county = get_satellite_availability_per_unit(
#     state_fips='17',
#     asset_id=ASSET_ID_SAT_COUNTY,
#     custom_date="2025-11-10",
#     analysis_level='county', 
#     fallback_days=7
# )

# print("Finished GEE export:\n")
# print(sat_county.status())

print("\n")
sat_county_path = r".\output\illinois_satellite_county_level.csv"
convert_to_df(ASSET_ID_SAT_COUNTY, export_csv=True, save_path=sat_county_path)
print("\n")
####################################################################


############ GOOGLE EARTH TASKS - ASSET_ID_SAT_TRACT ############
# print(f"Starting export for {ASSET_ID_SAT_TRACT}...")

# sat_census = get_satellite_availability_per_unit(
#     state_fips='17',
#     asset_id=ASSET_ID_SAT_TRACT,
#     custom_date="2025-11-10",
#     analysis_level='census', 
#     fallback_days=7
# )

# print("Finished GEE export:\n")
# print(sat_census.status())

print("\n")
sat_census_path = r".\output\illinois_satellite_census_level.csv"
convert_to_df(ASSET_ID_SAT_TRACT, export_csv=True, save_path=sat_census_path)
print("\n")
####################################################################

