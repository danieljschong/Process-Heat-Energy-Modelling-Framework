#this code is used to calculate distance between biommass to plant

# Import necessary libraries
import pandas as pd
import numpy as np
from openpyxl import Workbook
import time
from arcpy.sa import *
import arcpy
import openpyxl
import os

ref_extent_path = r"polygon_barrier_erase_750.shp" #need to change this
arcpy.env.extent = arcpy.Describe(ref_extent_path).extent
print(arcpy.Describe(ref_extent_path).extent)

# Function to read spreadsheet and return dataframe
def read_spreadsheet(file_path,sheet):
    return pd.read_excel(file_path, sheet_name=sheet)

def progress_bar (index,df):
    total =len(df)

    percent_complete = (index + 1) / total
    bar_length = 50
    filled_length = int(bar_length * percent_complete)
    bar = filled_length * '#' + (bar_length - filled_length) * '-'
    os.system('cls')
    print(f"\rProgress: [{bar}] {percent_complete:.1%}\n", end='')
    index += 1
    return index

# Main function to process the spreadsheets and calculate distances
def process_spreadsheets(source_path,sheet1, sheet2):
    # Read source and destination data
    source_df = read_spreadsheet(source_path,sheet1)
    destination_df = read_spreadsheet(source_path,sheet2)
    wb = Workbook()
    ws = wb.active
    ws.title = "Distances"
    ws.append(["Source ID", "Source Latitude", "Source Longitude","Destination ID", "Destination Latitude", "Destination Longitude", "Distance"])
    new_file_path = r"factory_to_biomass\Book2.xlsx"
    df = pd.read_excel(new_file_path)
    index = 0
    total = len(source_df)
    print(f"importing")
    for _, source_row in source_df.iterrows():
        x_field1 = source_row["NZTM_X"]
        y_field1 = source_row["NZTM_Y"]
        point_start = r"factory_to_biomass\factoryxy.shp"
        arcpy.management.CreateFeatureclass(r"factory_to_biomass","factoryxy.shp", "POINT")
        arcpy.management.DefineProjection(point_start, arcpy.SpatialReference(2193))
        print(f"feature class created")
        with arcpy.da.InsertCursor(point_start, ["SHAPE@XY"]) as cursor:
             cursor.insertRow([(x_field1, y_field1)])
        inSources = point_start
        inBarrier = r"factory_to_biomass\polygon_barrier_erase_750.shp"
        arcpy.CheckOutExtension("Spatial")
        print("check extension")
        outDistAcc = DistanceAccumulation(inSources, inBarrier)
        print("Sad")
        DA_raster_path = r"factory_to_biomass\Distance_Site.tif"
        outDistAcc.save(DA_raster_path)
        print(f"DistanceAccumulation is successful for {_+1}")
        print(index)
        index = progress_bar(index, source_df)

        for _, dest_row in destination_df.iterrows():
            distance = None
            # Check if difference in latitude and longitude is more than 5 degrees
            if abs(source_row['NZTM_Y'] - dest_row['NZTM_y']) > 200000 or abs(
                    source_row['NZTM_X'] - dest_row['NZTM_x']) > 200000 or source_row['North_South'] != dest_row['North_South']:
                pass

            # Calculate Distances
            else:
                x_field2 = dest_row["NZTM_x"]
                y_field2 = dest_row["NZTM_y"]

                point_end = r"factory_to_biomass\forestxy.shp"
                arcpy.management.CreateFeatureclass(r"factory_to_biomass","forestxy", "POINT")
                arcpy.management.DefineProjection(point_end, arcpy.SpatialReference(2193))
                with arcpy.da.InsertCursor(point_end, ["SHAPE@XY"]) as cursor:
                    cursor.insertRow([(x_field2, y_field2)])

                inPointFeatures = point_end  # point number 2
                inRaster = DA_raster_path # saved file of dist acc
                outPointFeatures = r"factory_to_biomass\extractvaluespts.shp"
                arcpy.CheckOutExtension("Spatial")
                ExtractValuesToPoints(inPointFeatures, inRaster, outPointFeatures, "INTERPOLATE", "VALUE_ONLY")

                values_exists = 0
                with arcpy.da.SearchCursor(outPointFeatures,"RASTERVALU") as cursor:
                    for col in cursor:
                        # print("cursor",cursor)

                        # print(col[0], "\n")
                        distance = col[0]
                        values_exists = 1

                if values_exists == 0:
                    print("no values")

                else:


                    ws.append([
                        source_row['ObjectID'], source_row['NZTM_Y'], source_row['NZTM_X'],
                        dest_row['OBJECTID *'], dest_row['NZTM_y'], dest_row['NZTM_x'],
                        distance
                    ])

                    if os.path.exists(new_file_path):
                        with pd.ExcelWriter(new_file_path,engine='openpyxl',mode='a',if_sheet_exists='replace') as writer:
                            df.to_excel(writer, sheet_name='Distance', index=False)  # <-- set your sheet name
                    else:
                        # File doesn't exist yet: create it
                        with pd.ExcelWriter(new_file_path, engine='openpyxl') as writer:
                            df.to_excel(writer, sheet_name='Distance', index=False)


    # Save the new workbook
    wb.save(new_file_path)
    return new_file_path


distance=0
biomass_source_dest= r"Island factory test.xlsx" #test data

arcpy.env.workspace = r"factory_to_biomass\MyProject15\MyProject15.gdb"
arcpy.env.overwriteOutput = True
arcpy.env.cellSize = 400
process_spreadsheets(biomass_source_dest,"Factory", "Biomass")
