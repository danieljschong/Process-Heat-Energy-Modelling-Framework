#this code calculates GXp between two points

import arcpy
import numpy as np
import os
import pandas as pd
import openpyxl
from arcpy.sa import *
from openpyxl import load_workbook
import time

ref_extent_path = "gxp_distance_within_a_buffer\\buffer.shp"
arcpy.env.extent = arcpy.Describe(ref_extent_path).extent
print(arcpy.Describe(ref_extent_path).extent)

# file path
excel_file_path = r"gxp_distance_within_a_buffer\\buffer barrier2.xlsx"
starting_point_xy = r"Projects\\MyProject5\\point1"
feature_class_path = r"Projects\\MyProject5"
barrier_path = "MyProject5\\erased_barrier_1.shp"
distance_accumulation_path =f"MyProject5//Distance_Site.tif"
end_point_xy = r"Projects\\MyProject5\\point2"
distance_accumulation_raster_format = "Projects//MyProject5//Distance_Site.tif"
extract_values = r"Projects\MyProject5\\extractvaluespts.shp"

df = pd.read_excel(excel_file_path)

arcpy.env.overwriteOutput = True
arcpy.env.cellSize = 500

df['Distance[m]'] = 0
global_count=0

total=(len(df))
for index,row in df.iterrows():


    # XY Table to Point
    #Source points
    x_field1 = row["NZTM_easting1"]
    y_field1 = row["NZTM_northing1"]

    #Destination points
    x_field2 = row["NZTM_easting2"]
    y_field2 = row["NZTM_northing2"]

    # Path to the output feature class (point)
    point_start = starting_point_xy

    # Create a new point feature class
    arcpy.management.CreateFeatureclass(feature_class_path, "point1", "POINT")

    # Insert the point into the feature class
    with arcpy.da.InsertCursor(point_start, ["SHAPE@XY"]) as cursor:
        cursor.insertRow([(x_field1, y_field1)])


    #Distance accumulation
    inSources = point_start
    inBarrier = barrier_path
    arcpy.CheckOutExtension("Spatial")
    outDistAcc = DistanceAccumulation(inSources, inBarrier)
    DA_raster_path = distance_accumulation_path
    outDistAcc.save(DA_raster_path)


    # Path to the output feature class (point)
    point_end = end_point_xy

    # Create a new point feature class
    arcpy.management.CreateFeatureclass(feature_class_path, "point2", "POINT")

    # Insert the point into the feature class
    with arcpy.da.InsertCursor(point_end, ["SHAPE@XY"]) as cursor:
        cursor.insertRow([(x_field2, y_field2)])


    #Extract Values to Points
    inPointFeatures = point_end  # point number 2
    inRaster = distance_accumulation_raster_format  # saved file of dist acc
    outPointFeatures = extract_values
    arcpy.CheckOutExtension("Spatial")
    ExtractValuesToPoints(inPointFeatures, inRaster, outPointFeatures, "INTERPOLATE", "VALUE_ONLY")

    #Read from attribute table
    with arcpy.da.SearchCursor(extract_values, "RASTERVALU") as cursor:
        for col in cursor:
            print(col[0],"\n")
            df.at[index, 'Distance[m]'] = col[0]
    with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        # Ensure other sheets are not affected
        writer.book = openpyxl.load_workbook(excel_file_path)
        writer.sheets = {ws.title: ws for ws in writer.book.worksheets}

        # Write changes to the specific sheet
        df.to_excel(writer,  index=False)

    percent_complete = (index + 1) / total
    bar_length = 50
    filled_length = int(bar_length * percent_complete)
    bar = filled_length * '#' + (bar_length - filled_length) * '-'
    os.system('cls')
    print(f"\rProgress: [{bar}] {percent_complete:.1%}", end='')

print()
