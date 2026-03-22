#this code is used to calculate distance between biommass to generation plants

# Import necessary libraries
import pandas as pd
import numpy as np
from openpyxl import Workbook
import time
from arcpy.sa import *
import arcpy
import openpyxl
import os
import time
import json

# Record the starting time
start_time = time.time()

#modelling

ref_extent_path = r"biomass_to_gen\polygon_barrier_erase_750.shp"
arcpy.env.extent = arcpy.Describe(ref_extent_path).extent
print(arcpy.Describe(ref_extent_path).extent)

# Function to read spreadsheet and return dataframe
def read_spreadsheet(file_path,sheet):
    return pd.read_excel(file_path, sheet_name=sheet)

# Function to calculate Euclidean distance between two points
def euclidean_distance(lat1, lon1, lat2, lon2):
    return np.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2)

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
    new_file_path = r"Gen_biomass_distances.csv"

    json_file_path = r"distances1.json"
    # df = pd.read_excel(new_file_path)
    #df = pd.read_csv(new_file_path)
    index = 0
    data_list = []
    total = len(source_df)
    print(f"importing")
    for _, source_row in source_df.iterrows():

        if _ == 0 or x_field1 != source_row["NZTM_x"]:
            x_field1 = source_row["NZTM_x"]
            y_field1 = source_row["NZTM_y"]

            print(x_field1,y_field1)
            point_start = r"biomass_to_gen\generationxy.shp"
            arcpy.management.CreateFeatureclass(r"biomass_to_gen","generationxy.shp", "POINT")
            arcpy.management.DefineProjection(point_start, arcpy.SpatialReference(2193))
            print(f"feature class created")
            with arcpy.da.InsertCursor(point_start, ["SHAPE@XY"]) as cursor:
                 cursor.insertRow([(x_field1, y_field1)])
            inSources = point_start
            inBarrier = r"biomass_to_gen\polygon_barrier_erase_750.shp"
            arcpy.CheckOutExtension("Spatial")
            print("check extension")
            outDistAcc = DistanceAccumulation(inSources, inBarrier) #,source_maximum_accumulation=4100000
            elapsed_time = time.time() - start_time
            print(f"Time elapsed: {elapsed_time:.2f} seconds\n")
            print("Sad")
            DA_raster_path = r"biomass_to_gen\MyProject15\\Distance_Site.tif"
            outDistAcc.save(DA_raster_path)
        print(f"DistanceAccumulation is successful for {_+1}")
        print(index)
        index = progress_bar(index, source_df)

        for _, dest_row in destination_df.iterrows():
            distance = None
            # Check if difference in latitude and longitude is more than 5 degrees
            if abs(source_row['NZTM_y'] - dest_row['NZTM_y']) > 200000 or abs(
                    source_row['NZTM_x'] - dest_row['NZTM_x']) > 200000 or source_row['North_South'] != dest_row['North_South']:
                pass

            # Calculate Distances
            else:
                x_field2 = dest_row["NZTM_x"]
                y_field2 = dest_row["NZTM_y"]

                point_end = r"biomass_to_gen\forestxy.shp"
                arcpy.management.CreateFeatureclass(r"biomass_to_gen","forestxy", "POINT")
                arcpy.management.DefineProjection(point_end, arcpy.SpatialReference(2193))
                with arcpy.da.InsertCursor(point_end, ["SHAPE@XY"]) as cursor:
                    cursor.insertRow([(x_field2, y_field2)])

                inPointFeatures = point_end  # point number 2
                inRaster = DA_raster_path # saved file of dist acc
                outPointFeatures = r"biomass_to_gen\extractvaluespts.shp" #wip
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
                        source_row['ObjectID'], source_row['NZTM_x'], source_row['NZTM_y'],
                        dest_row['OBJECTID *'], dest_row['NZTM_x'], dest_row['NZTM_y'],
                        distance
                    ])

                    data_list.append({
                        "ObjectID": source_row['ObjectID'],
                        "NZTM_x": source_row['NZTM_x'],
                        "NZTM_y": source_row['NZTM_y'],
                        "ORIG_FID": dest_row['OBJECTID *'],
                        "NZTM_x_2": dest_row['NZTM_x'],
                        "NZTM_y_2": dest_row['NZTM_y'],#warning, cannot have same name for column
                        "Distance": distance
                    })

    
    # Save
    try:
        # print("Excel working")
        # wb.save(new_file_path)
        df = pd.DataFrame(data_list)
        print("csv working")
        df.to_csv(new_file_path, mode='w', index=False)
        json_data = json.dumps(data_list, indent=4)
        with open(json_file_path, "w") as json_file:
                json_file.write(json_data)
    except Exception as e:
        print("Json working")

        json_data = json.dumps(data_list, indent=4)
        with open(json_file_path, "w") as json_file:
                json_file.write(json_data)

    total_time = time.time() - start_time
    print(f"Total time elapsed: {total_time:.2f} seconds")
    return new_file_path


distance=0
# biomass_source_dest= r"Island_factory_gen_biomass_test.xlsx" #testing file
biomass_source_dest= r"biomass_to_gen\Island_factory_gen_biomass.xlsx" #real file

arcpy.env.workspace = r"biomass_to_gen\MyProject15\MyProject15.gdb"
arcpy.env.overwriteOutput = True
arcpy.env.cellSize = 400
process_spreadsheets(biomass_source_dest,"Gen", "Bio")
