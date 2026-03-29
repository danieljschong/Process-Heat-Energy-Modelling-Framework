#try to model a monte carlo option
#latest oct 2025 file

import time
# import all libraries
from Pgraph.Pgraph import Pgraph  # This is our Pgraph library
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import os
import sys
import numpy as np
import tracemalloc
import pickle
from datetime import datetime
from types import SimpleNamespace
import shutil
from collections import defaultdict
import ast


def memory_alloc(logic="true"):
    if logic == "true":
        # Take a snapshot after the function
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        print("[ Top 10 memory-consuming lines ]")
        for stat in top_stats[:10]:
            print(stat)

        # Stop tracing memory allocations
        tracemalloc.stop()

os.system("cls" if os.name == "nt" else "clear")

# ============================================================
# Project root
# ============================================================
# Assumes this script is in the repo root.
# If this script is inside a subfolder, change to:
# PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Optional sibling repo
NATIONAL_ENERGY_ROOT = PROJECT_ROOT.parent / "National_energy_modelling"
if NATIONAL_ENERGY_ROOT.exists() and str(NATIONAL_ENERGY_ROOT) not in sys.path:
    sys.path.append(str(NATIONAL_ENERGY_ROOT))

# ============================================================
# Imports from local project
# ============================================================
from test_folder.normal_dist_1000datas import sample_normal_from_df
from test_folder.cf_8760_to_normal_dist import hourly_to_4hourly

from national_energy_modelling_function_file import *
from pgraph_output_class_value import *
from pgraph_csv_organiser_rev import *
from pgraph_excel_organiser import *
from pgraph_output_organiser import *
import email_myself

# ============================================================
# Common folders
# ============================================================
TEST_FOLDER = PROJECT_ROOT / "test_folder"
OUTPUT_FOLDER = PROJECT_ROOT / "folder_output_main"
MONTE_CARLO_OUTPUT = PROJECT_ROOT / "MonteCarloOutput"

# Optional external/local data folders
DOWNLOADS_18SEPT = Path.home() / "Downloads" / "18sept"
ROYAL_SOC_FOLDER = Path.home() / "Downloads" / "Royal_Soc" / "Grid stuff" / "RETA ERGO reports"
HYDRO_FOLDER = Path.home() / "Downloads" / "20dec24 work" / "infrastructure"

# ============================================================
# Timing setup
# ============================================================
time_setup = 0
time_step = time.time()
time_csv = 0
time_solving = 0

current = datetime.now()
start_time = time.time()
solved_time = time.time()

current_time = current.strftime("%H:%M:%S")
print("Current Time is:", current_time)

directory_path = OUTPUT_FOLDER

# ============================================================
# Main input workbook
# ============================================================
# Pick one of these as needed
# main_path = TEST_FOLDER / "test5_26march_few_plants.xlsx"
# main_path = TEST_FOLDER / "test5_26march_one_plant.xlsx"
# main_path = TEST_FOLDER / "test5_26march.xlsx"
main_path = DOWNLOADS_18SEPT / "Compiled Process Heat Data.xlsx"
# main_path = DOWNLOADS_18SEPT / "Compiled Process Heat Data_one_plant.xlsx"
# main_path = DOWNLOADS_18SEPT / "Compiled Process Heat Data few plants.xlsx"
# main_path = DOWNLOADS_18SEPT / "Compiled Process Heat Data_one_plant_example_5.xlsx"

# ============================================================
# Other input paths
# ============================================================
electricity_price_monthly_path = TEST_FOLDER / "combined_monthly_hours.csv"
electricity_price_hourly_path = TEST_FOLDER / "ExcelDF3.pkl"

industry_curve_path = TEST_FOLDER / "Industries_curve.xlsx"
capacity_factor_monthly_gen_dict_path = TEST_FOLDER / "capacity_factor.pkl"
capacity_factor_hourly_gen_dict_path = TEST_FOLDER / "capacity_factor_hourly.pkl"
capacity_factor_hourly_gen_csv_path = TEST_FOLDER / "capacity_factor_hourly_len_24.pkl"
capacity_factor_monthly_gen_csv_path = TEST_FOLDER / "capacity_factor_hourly_monthly.pkl"

wholesale_electricity_ABY_price_path = TEST_FOLDER / "Relative_Price_Stats_4h.csv"
wholesale_electricity_relative_price_path = TEST_FOLDER / "Simulated_Prices_Q1.csv"
wholesale_electricity_ABY_dem_path = TEST_FOLDER / "Relative_Demand_Stats.csv"
wholesale_electricity_relative_dem_path = TEST_FOLDER / "Simulated_Demand_Q1.csv"

industry_curve_path_4hourly = TEST_FOLDER / "Industrial_curve_4hourly.xlsx"

compiled_data_path = ROYAL_SOC_FOLDER / "compiled.xlsx"
hydrostorage_path = HYDRO_FOLDER / "20231231_EnergyStorageCapacity(DerivedForScheme) 1980 onwards.xlsx"

# ============================================================
# Read transmission and hydro data
# ============================================================
Pd_transmission = pd.read_excel(
    compiled_data_path,
    sheet_name="ERGO transmission cost",
    header=0,
    index_col=None,
    usecols="A:G",
    nrows=15,
)
Pd_transmission.columns = Pd_transmission.columns.str.strip()

Pd_hydro = pd.read_excel(
    hydrostorage_path,
    sheet_name="Exported",
    header=0,
    index_col=None,
    usecols="A:O",
    nrows=25,
)

transmission_cost_lookup = {
    (int(row["Transmission Cost"]), row["Single/Double"].strip()): row["TAC"]
    for _, row in Pd_transmission.iterrows()
}

# ============================================================
# Read main workbook sheets
# ============================================================
Pd_Generation = pd.read_excel(
    main_path,
    sheet_name="Generation_updated_v1",
    header=0,
    index_col=None,
    usecols="A:AD",
    nrows=422,
)

Pd_Factory = pd.read_excel(
    main_path,
    sheet_name="Factory_updated (5)",
    header=0,
    index_col=None,
    usecols="A:BD",
    nrows=429,
)

Pd_GXP_edited = pd.read_excel(
    main_path,
    sheet_name="GXP_edited_v4",
    header=0,
    index_col=None,
    usecols="A:BA",
    nrows=217,
)

Pd_Technologies = pd.read_excel(
    main_path,
    sheet_name="Technologies",
    header=0,
    index_col=None,
    usecols="A:I",
    nrows=12,
)

Pd_GxpGxp_Connections = pd.read_excel(
    main_path,
    sheet_name="gxp_gxp_connection_v1",
    header=0,
    index_col=None,
    usecols="A:H",
    nrows=653,
)

Pd_biomass_resource = pd.read_excel(
    main_path,
    sheet_name="biomass_resource",
    header=0,
    index_col=None,
    usecols="A:O",
    nrows=214,
)

Pd_biomass_factory_connections = pd.read_excel(
    main_path,
    sheet_name="biomass_factory_distance",
    header=0,
    index_col=None,
    usecols="A:I",
    nrows=27564,
)

Pd_biomass_generation_connections = pd.read_excel(
    main_path,
    sheet_name="biomass_to_generation_sites",
    header=0,
    index_col=None,
    usecols="A:I",
    nrows=26197,
)

# ============================================================
# Read Monte Carlo CSV inputs
# ============================================================
Pd_simulated_ABY = pd.read_csv(wholesale_electricity_relative_price_path)
Pd_relative_price = pd.read_csv(wholesale_electricity_ABY_price_path)

Pd_simulated_ABY_dem = pd.read_csv(wholesale_electricity_relative_dem_path)
Pd_relative_dem = pd.read_csv(wholesale_electricity_ABY_dem_path)



# Solver selection: "INSIDEOUT" "SSGLP" "SSG" "MSG"
solve_type = "INSIDEOUT"
num_sol = 1
tracemalloc.start()
num_simulations = 1

rng = np.random.default_rng(num_sol)

island = "Northr"
distance_constraint = 200000
lines_charges = 36.9641344  # NZD/kVA
proportional_cost_op = {'fix_cost':0.00000 ,'proportional_cost':0.00000}
fpvv_factor = [0.67, 0.74, 0.77, 0.8, 0.8, 0.85, 0.92, 0.91, 0.84, 0.75, 0.75, 0.68]
assumed_cost_coefficient = 2

time_period = 6
time_multiplier = []
for l in range(1, time_period+1):
    time_multiplier.append((f"{l:02}"))
time_resolution = "hourly" #hourly, monthly


#import cf datasets
if time_resolution == 'hourly':
    capacity_factor_gen= hourly_to_4hourly(capacity_factor_hourly_gen_dict_path)
    # print(capacity_factor_gen)
#     with open(capacity_factor_hourly_gen_dict_path, 'rb') as f:
#         capacity_factor_gen = pickle.load(f)
    # print(capacity_factor_gen)
elif time_resolution == 'monthly':
    with open(capacity_factor_monthly_gen_csv_path, 'rb') as f:
        capacity_factor_gen = pickle.load(f)
CF_coordinates = {capacity_factor_gen['index'][i]: capacity_factor_gen['coordinates'][i] for i in range(len(capacity_factor_gen['index']))}
# print(CF_coordinates)


if time_resolution == 'hourly':
    CF_value = sample_normal_from_df(capacity_factor_gen,rng)

elif time_resolution == 'monthly':
    CF_value = {capacity_factor_gen['index'][i]: capacity_factor_gen['cf'][i][:24] for i in range(len(capacity_factor_gen['index']))}
# print(CF_value)

# print(capacity_factor_gen)
# Extract important columns
Pd_keys = ['ObjectID', 'Types of power station', 'Type', 'Capacity (MW)','Generation (GWh/yr)', 'North_South', 'NEAR_DIST', 'POC code',
           'Names', 'Status']
Pd_Gen = python_dict_dot_notation(Pd_keys, Pd_Generation)

Pd_keys = ['ObjectID', 'Company name', 'Energy Estimated', 'Industry', 'POC', 'NEAR_DIST_1', '60°C2', '90°C3',
           '140°C4', '180°C5', '>180°C6', 'North_South', 'Operating_Hours', "Switch", "Capacity", "IUR2025_V1", "IUR2025__1"]
Pd_Fac = python_dict_dot_notation(Pd_keys, Pd_Factory)

Pd_keys = ['Index', 'POC', 'Electricity Demand 2024 (GWh/yr)', "North_South","N Demand (MVA)","N Generation (MVA)",
           "Demand Transformers Req",	"Dem Large transformer",	"Dem Medium transformer",	"Dem Small transformer",	"Dem Tiny transformer",
            "Supply Transformers Req",	"Sup Large transformer",	"Sup Medium transformer",	"Sup Small transformer",	"Sup Tiny transformer",
           "GXP Price","Tot_Jan","Tot_Feb","Tot_Mar","Tot_Apr","Tot_May","Tot_Jun","Tot_Jul",
           "Tot_Aug","Tot_Sep","Tot_Oct","Tot_Nov","Tot_Dec"]
Pd_GXP = python_dict_dot_notation(Pd_keys, Pd_GXP_edited)

# print(Pd_GXP)

Pd_keys = ['Point 1', 'Point 2', 'Distance', 'North_South', 'Value (MVA)',	'kV',	'line type 2']
Pd_GXP_GXP = python_dict_dot_notation(Pd_keys, Pd_GxpGxp_Connections)

Pd_keys = ['ORIG ID', 'Inforest harvest',	'K logs',	'Sawmill chip',	'Straw and Stover',	'Pellets', 'Row Labels', 'North_South']
Pd_bio = python_dict_dot_notation(Pd_keys, Pd_biomass_resource)

Pd_keys = ['Source ID', 'Biomass ID', 'Distance', 'North_South']
Pd_bio_fac = python_dict_dot_notation(Pd_keys, Pd_biomass_factory_connections)

Pd_keys = ['Source ID', 'Biomass ID', 'Distance', 'North_South']
Pd_bio_gen = python_dict_dot_notation(Pd_keys, Pd_biomass_generation_connections)



# add hydrogen into the model and potential electricity generation
# vlookup from different tables for reference
Pd_Technologies_Price_MWh = Pd_Technologies.set_index('Type')['Average'].to_dict()
Pd_Technologies_Rank = Pd_Technologies.set_index('Type')['Rank'].to_dict()
Pd_Technologies_Capacity_Factor = Pd_Technologies.set_index('Type')['Capacity_factor'].to_dict()
Pd_Technologies_Ou_Cost = Pd_Technologies.set_index('Type')['Ou_Cost'].to_dict()
Pd_Technologies_Op_Main_Cost = Pd_Technologies.set_index('Type')['Op_Main_Cost'].to_dict()

# Problem specification
G = nx.DiGraph()
analyzer = MonteCarloAnalysis('OO')
# print(type(analyzer).__module__)    # Shows just the module name


gop_reference =[]
goo_reference = []

mat_units_GWh = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'GWh'}
mat_units_GJ = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'GJ'}
mat_units_MWh = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'MWh'}
mat_units_ton = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'tons'}



df2 = pd.read_excel(main_path)
# print(df)
if time_resolution == 'hourly':
    df3 = pd.read_pickle(electricity_price_hourly_path)
    df_industrial_curve = pd.read_excel(industry_curve_path_4hourly,sheet_name="Sheet1", header=0, index_col=None, usecols="A:G", nrows=9)
elif time_resolution == 'monthly':
    df3 = pd.read_csv(electricity_price_monthly_path)
    df_industrial_curve = pd.read_excel(industry_curve_path,sheet_name="Sheet1", header=0, index_col=None, usecols="A:M", nrows=9)


# print(df3)
#relative price file
relative_price_headers = Pd_relative_price.columns
relative_price = pd.DataFrame(Pd_relative_price)
relative_price = relative_price.to_dict(orient='list')
#relative dem file
relative_dem_headers = Pd_relative_dem.columns
relative_dem = pd.DataFrame(Pd_relative_dem)
relative_dem = relative_dem.to_dict(orient='list')


#speculated price and demand path
if time_resolution == 'hourly':
    rows_dict_price = {}
    row_dict_demand = {}
    for index, row in Pd_simulated_ABY.iterrows():
        rows_dict_price[index] = row.to_list()
    for index, row in Pd_simulated_ABY_dem.iterrows():
        row_dict_demand[index] = row.to_list()    
# print(rows_dict_price)
# print(row_dict_demand)

# gxp settings
for num in range(0,num_simulations):
    
    gxp = {'poc': [], 'dem': [], 'index': [], 'price': [] ,'dem_n':[], 'gen_n':[]}
    column_names = ["Tot_Jan","Tot_Feb","Tot_Mar","Tot_Apr","Tot_May","Tot_Jun","Tot_Jul","Tot_Aug","Tot_Sep","Tot_Oct","Tot_Nov","Tot_Dec"]

    for k in range(len(Pd_GXP_edited)):  # map out the gxp points
        if str(Pd_GXP.North_South[k]) != island:
            gxp['poc'].append(Pd_GXP.POC[k])
            gxp['index'].append(int(2000 + Pd_GXP.Index[k]))
            gxp['dem_n'].append(Pd_GXP.N_Demand_MVA[k])
            gxp['gen_n'].append(Pd_GXP.N_Generation_MVA[k])           
            
            if time_resolution == 'monthly':
            # Extract monthly data for the current row (k)
                monthly_data = [getattr(Pd_GXP, col)[k] for col in column_names]
                gxp['dem'].append(monthly_data)

    for i in range(len(gxp['poc'])):
        if time_resolution == 'hourly':
            match_found = False  # Track if we found a match
            dem_4_hourly = 4
            for j in range(len(relative_dem_headers)):
                if f"R_{gxp['poc'][i]}" == relative_dem_headers[j]:
                    #print(f"R_{gxp['poc'][i]} == {relative_dem_headers[j]}")
                    multiplied_result = [a * b * dem_4_hourly for a, b in zip(relative_dem[relative_dem_headers[j]], row_dict_demand[num])]
                    gxp['dem'].append(multiplied_result[:time_period])
                    # print(multiplied_result[:time_period])
                    match_found = True
                    break  # Exit loop early if match found
            if not match_found:
                gxp['dem'].append(np.zeros(time_period).tolist())  # Corrected zeros issue
    gxp['price'] = []
    for i in range(len(gxp['poc'])):
        if time_resolution == 'hourly':
            match_found = False  # Track if we found a match
            adjustment_other_charges = 2
            for j in range(len(relative_price_headers)):
                if f"R_{gxp['poc'][i]}" == relative_price_headers[j]:
                    multiplied_result = [a * b * adjustment_other_charges for a, b in zip(relative_price[relative_price_headers[j]], rows_dict_price[num])]
                    gxp['price'].append(multiplied_result[:time_period]) 
                    match_found = True
                    break
            if not match_found:
                multiplied_result = [a * b * adjustment_other_charges for a, b in zip(relative_price[relative_price_headers[1]], rows_dict_price[num])]
                # print(multiplied_result)
                gxp['price'].append(multiplied_result[:time_period])
            
        elif time_resolution == 'monthly':
            for k in range(len(df3)):
                if gxp['poc'][i] == str(df3['POC'][k]):
                    base_prices = (df3.iloc[k].tolist()[1:])
                    multiplied_result = [round(a * b, 3) for a, b in zip(fpvv_factor, base_prices)] #wip
                    gxp['price'].append(multiplied_result)
                    
            if gxp['poc'][i] not in df3['POC'].values:

                base_prices = (df3.iloc[0].tolist()[1:])
                multiplied_result = [round(a * b, 3)for a, b in zip(fpvv_factor, base_prices)] #wip
                gxp['price'].append(multiplied_result)
    # print("gdd",gxp['price'])
    for i in range(len(gxp['poc'])):
        for l in range(len(time_multiplier)):
            G.add_node("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]), names="M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]),type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
            G.add_node("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]), names="O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),
                       capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
            
            # print(gxp['dem'][i][l])
            G.add_node("M" + str(gxp['index'][i]) + str(time_multiplier[l]),
                        names="M" + str(gxp['index'][i]) + str(time_multiplier[l]), type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_GWh)  #warning
            # print((gxp['index'][i] + 300),str(time_multiplier[l]),";", gxp['price'][i][l])
            G.add_node("O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 300)+ str(time_multiplier[l]), capacity_lower_bound=0,
                    capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)  #warning

            G.add_node("M" + str(gxp['index'][i] + 600) + str(time_multiplier[l]),names="M" + str(gxp['index'][i]  + 600) + str(time_multiplier[l]), type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_GWh)  #warning
            
            G.add_edge("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]),"O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),weight=1)
            G.add_edge("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),"M" + str(gxp['index'][i]) + str(time_multiplier[l]),weight=0.99)
       
            
            G.add_edge("M" + str(gxp['index'][i]) + str(time_multiplier[l]),"O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]),weight=1)
            G.add_edge("O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]),"M" + str(gxp['index'][i] + 600) + str(time_multiplier[l]),weight=0.99)


            #demand at each gxp
            if gxp['dem'][i][l]!=0:
                if time_resolution == "monthly":
                    G.add_node("O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 2000)+ str(time_multiplier[l]), capacity_lower_bound=0,
                            capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0) #warning (gxp['price'][i][l])*1e3/fpvv_factor[l]*3
                    
                elif time_resolution == "hourly":
                    G.add_node("O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 2000)+ str(time_multiplier[l]), capacity_lower_bound=0,
                            capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0 ) #warning (gxp['price'][i][l])*1e3/fpvv_factor[l]*3                   
                G.add_node("M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]), names="M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]), type='product',
                            flow_rate_lower_bound=gxp['dem'][i][l], flow_rate_upper_bound=1.01*gxp['dem'][i][l], price=0, units=mat_units_GWh)
                        
                G.add_edge("M" + str(gxp['index'][i] + 600) + str(time_multiplier[l]),"O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]),weight=1)
                G.add_edge("O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]),"M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]),weight=0.99)
                
            else:
                pass
        
    def gen_nodes():
        gen['capacity'].append(Pd_Gen.Capacity_MW[i])
        gen['gwh'].append(Pd_Gen.Generation_GWh_yr[i])
        #gen['price'].append(Pd_Technologies_Price_MWh[gen['type'][-1]])
        gen['rank'].append(Pd_Technologies_Rank[gen['type'][-1]])
        gen['distance'].append(round(Pd_Gen.NEAR_DIST[i]/1000,3)) #to km
        gen['name'].append(Pd_Gen.Names[i])
        gen['poc'].append(Pd_Gen.POC_code[i])
        gen['reference'].append(Pd_Gen.ObjectID[i])
        gen['index'].append(int(10000 + Pd_Gen.ObjectID[i]))  # odd
        gen['status'].append(Pd_Gen.Status[i])
        
        if str(gen['status'][-1]) != 'Commissioning':
            gen['price'].append(round(Pd_Technologies_Op_Main_Cost[gen['type'][-1]],3))
        elif str(gen['status'][-1]) == 'Commissioning' and str(gen['type'][-1]) in {"Bioenergy","Thermal","Cogeneration" }:
            gen['price'].append(round(Pd_Technologies_Op_Main_Cost[gen['type'][-1]]*0.4,3)) #penalty for thermal
        else:
            gen['price'].append(round(Pd_Technologies_Ou_Cost[gen['type'][-1]],3))
            

        if str(Pd_Gen.Type[i]) !='nan':
            gen['local'].append(0)
        else:
            gen['local'].append(0)

        for k in range(len(gxp['poc'])):
            if gen['poc'][-1] == gxp['poc'][k]:
                gen['poc_index'].append(gxp['index'][k])
        temp_cf = CF_value[gen['reference'][-1]]
        if temp_cf == []:
            temp_cf = list(np.ones((len(time_multiplier),), dtype=int) * Pd_Technologies_Capacity_Factor[gen['type'][-1]]) #warning if houurs
        else:
            pass
        gen['cf'].append(temp_cf)
        # print(gen['cf'])
        if sum(gen['cf'][-1][0:time_period])>=0.001:
            
#this is for the commissioning generation without any fuel usage. Removes the 14000 level nodes as capacity is unncessary since it has already been built.
            if str(gen['status'][-1]) == 'Commissioning' and str(gen['type'][-1]) not in {"Bioenergy","Thermal","Cogeneration" }:
                G.add_node("M" + str(6000 + gen['index'][-1]), names="M" + str(6000 + gen['index'][-1]), type="raw_material", flow_rate_lower_bound=0,
                    flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)
                G.add_node("M" + str(2500 + gen['index'][-1]), names="M" + str(2500 + gen['index'][-1]), type='product',
                    flow_rate_lower_bound=gen['capacity'][-1]*time_period, flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)
                
                G.add_node("M" + str(3000 + gen['index'][-1]),names="M" + str(3000 + gen['index'][-1]), type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_MWh)
                
                if time_resolution == 'hourly': 
                    # newly updated nov 11 2025
                    G.add_node("O" + str(4000 + gen['index'][-1]), names="O" + str(4000 + gen['index'][-1]), capacity_lower_bound=0,
                            capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=gen['price'][-1]/365/24*time_period)  # gen capacity
                    #prevent double counting of electricitry price                    
                    
                    G.add_node("M" + str(9000 + gen['index'][-1]), names="M" + str(9000 + gen['index'][-1]),type='product',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=100*gen['capacity'][-1], price=gen['price'][-1]/365/24,units=mat_units_MWh)
                    
                elif time_resolution == 'monthly':
                    # newly updated nov 11 2025
                    G.add_node("O" + str(4000 + gen['index'][-1]), names="O" + str(4000 + gen['index'][-1]), capacity_lower_bound=0,
                        capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=gen['price'][-1]/12*time_period)  # gen capacity #warning wip
                    #prevent double counting of electricitry price
                    G.add_node("M" + str(9000 + gen['index'][-1]), names="M" + str(9000 + gen['index'][-1]),type='product',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=100*gen['capacity'][-1], price=gen['price'][-1]/12,units=mat_units_MWh)     
                    
                #ratio 6nov2025
                if time_resolution =="monthly":
                    G.add_node("M" + str(3500 + gen['index'][-1]), names="M" + str(3500 + gen['index'][-1]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_MWh)   
                elif time_resolution =="hourly":
                    G.add_node("M" + str(3500 + gen['index'][-1]), names="M" + str(3500 + gen['index'][-1]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_MWh)                   
                
                G.add_node("M" + str(15500), names="M" + str(15500),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,units=mat_units_MWh)     
                G.add_node("O" + str(4500 + gen['index'][-1]), names="O" + str(4500 + gen['index'][-1]), capacity_lower_bound=0,capacity_upper_bound=1e6, fix_cost=0, proportional_cost=0)
                G.add_edge("O" + str(4000 + gen['index'][-1]),"M" + str(3500 + gen['index'][-1]),weight=0.6)
                G.add_edge("M" + str(3500 + gen['index'][-1]),"O" + str(4500 + gen['index'][-1]),weight=1)
                G.add_edge("O" + str(4500 + gen['index'][-1]),"M" + str(15500),weight=1)



                for l in range(len(time_multiplier)):

                    if gen['cf'][-1][l]>=0.001:

                        G.add_node("M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]),
                                names="M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_MWh)

                        G.add_node("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),names="O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),capacity_lower_bound=0,
                            capacity_upper_bound=1e8, **proportional_cost_op)
                        G.add_node("M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]), names="M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,units=mat_units_MWh) 
                        G.add_edge("M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),"O" + str(4500 + gen['index'][-1]),weight=1)

                        if time_resolution == 'hourly':
                            G.add_node("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), names="O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), capacity_lower_bound=0,
                            capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=0)
                            G.add_edge("O" + str(2000 + gen['index'][-1])+ str(time_multiplier[l]),
                                    "M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), weight=gen['cf'][-1][l] * 1)
                            G.add_edge("M" + str(6000 + gen['index'][-1]), "O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1)
                            G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(2500 + gen['index'][-1]), weight=1)
                                                        
                            G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),weight=1/(gen['cf'][-1][l])) #cf wip6/11/2025
                            
                        elif time_resolution == 'monthly':
                            G.add_node("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), names="O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), capacity_lower_bound=0,
                                capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=0)
                            G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]),
                                    "M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), weight=gen['cf'][-1][l]*730)    
                                        
                            G.add_edge("M" + str(6000 + gen['index'][-1]), "O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1)
                            G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(2500 + gen['index'][-1]), weight=1)
            
                            G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),weight=1/(gen['cf'][-1][l]*730)) #cf wip6/11/2025
          
                        G.add_edge("M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), "O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1)
                        
                        #prevent double counting edge
                        if time_resolution == 'hourly':
                            G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][-1]) ,weight=1/(gen['cf'][-1][l])/4)
                        elif time_resolution == 'monthly':
                            G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][-1]) ,weight=1/gen['cf'][-1][l]/730)
                    
                    #curtail
                        G.add_node("O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),
                                    names="O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e7, **proportional_cost_op) 
                        G.add_node("M" + str(11000 + gen['index'][-1]) + str(time_multiplier[l]),
                                    names="M" + str(11000 + gen['index'][-1]) + str(time_multiplier[l]), type='product',
                                    flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_GWh)        
                        G.add_edge("M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]),"O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),weight=1)
                        G.add_edge("O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(11000 + gen['index'][-1]) + str(time_multiplier[l]),weight=1)
                
                if sum(gen['cf'][-1][0:time_period])>=0.001:
                    G.add_edge("M" + str(3000 + gen['index'][-1]),  "O" + str(4000 + gen['index'][-1]), weight=1e-7)    
                    for l in range(len(time_multiplier)):
                        G.add_node("O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),names="O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                        G.add_node("M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),names="M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_GWh)
                        G.add_edge("O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),"M" + str(3000 + gen['index'][-1]),weight=1)
                        if time_resolution == 'hourly':
                            G.add_edge( "O" + str(4000 + gen['index'][-1]),"M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),weight=4)
                        elif time_resolution == 'monthly':
                            G.add_edge( "O" + str(4000 + gen['index'][-1]),"M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),weight=1)
                        G.add_edge("M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),"O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),weight=1)


                

            else:
                G.add_node("M" + str(6000 + gen['index'][-1]), names="M" + str(6000 + gen['index'][-1]),
                        type="raw_material", flow_rate_lower_bound=0,
                        flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)

                G.add_node("O" + str(gen['index'][-1]), names="O" + str(gen['index'][-1]), capacity_lower_bound=0,
                        capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)

                G.add_node("M" + str(1000 + gen['index'][-1]), names="M" + str(1000 + gen['index'][-1]),
                        type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,
                        units=mat_units_MWh)         

                G.add_edge("M" + str(6000 + gen['index'][-1]), "O" + str(gen['index'][-1]), weight=1)
                G.add_edge("O" + str(gen['index'][-1]), "M" + str(1000 + gen['index'][-1]), weight=1)
                G.add_node("M" + str(3000 + gen['index'][-1]),
                        names="M" + str(3000 + gen['index'][-1]), type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_MWh)
                
                if time_resolution == 'hourly':
                    if gen['type'][-1] in {'Solar Utility', 'Solar Commercial', 'Solar Residential'}:
                        G.add_node("O" + str(4000 + gen['index'][-1]), names="O" + str(4000 + gen['index'][-1]), capacity_lower_bound=0,
                            capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=gen['price'][-1]/365/24*time_period)  # gen capacity
                        #prevent double counting of electricitry price
                        G.add_node("M" + str(9000 + gen['index'][-1]), names="M" + str(9000 + gen['index'][-1]),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=10000*gen['capacity'][-1], price=gen['price'][-1]/365/24,
                            units=mat_units_MWh)
                    else:               
                        G.add_node("O" + str(4000 + gen['index'][-1]), names="O" + str(4000 + gen['index'][-1]), capacity_lower_bound=0,
                            capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=gen['price'][-1]/365/24*time_period)  # gen capacity
                        #prevent double counting of electricitry price
                        G.add_node("M" + str(9000 + gen['index'][-1]), names="M" + str(9000 + gen['index'][-1]),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=100*gen['capacity'][-1], price=gen['price'][-1]/365/24,
                            units=mat_units_MWh)
                            
                elif time_resolution == 'monthly':
                    G.add_node("O" + str(4000 + gen['index'][-1]), names="O" + str(4000 + gen['index'][-1]), capacity_lower_bound=0,
                        capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=gen['price'][-1]/12*time_period)  # gen capacity #warning wip
                    #prevent double counting of electricitry price
                    G.add_node("M" + str(9000 + gen['index'][-1]), names="M" + str(9000 + gen['index'][-1]),type='product',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=100*gen['capacity'][-1], price=gen['price'][-1]/12,
                        units=mat_units_MWh)           

                #ratio 6nov2025
                if time_resolution =="monthly":
                    G.add_node("M" + str(3500 + gen['index'][-1]), names="M" + str(3500 + gen['index'][-1]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,
                                units=mat_units_MWh)   
                elif time_resolution =="hourly":
                    G.add_node("M" + str(3500 + gen['index'][-1]), names="M" + str(3500 + gen['index'][-1]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e9, price=0,
                                units=mat_units_MWh)                       
                G.add_node("M" + str(15500), names="M" + str(15500),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,
                            units=mat_units_MWh)     
                G.add_node("O" + str(4500 + gen['index'][-1]), names="O" + str(4500 + gen['index'][-1]), capacity_lower_bound=0,
                            capacity_upper_bound=1e6, fix_cost=0, proportional_cost=0)
                G.add_edge("O" + str(4000 + gen['index'][-1]),"M" + str(3500 + gen['index'][-1]),weight=0.6)
                G.add_edge("M" + str(3500 + gen['index'][-1]),"O" + str(4500 + gen['index'][-1]),weight=1)
                G.add_edge("O" + str(4500 + gen['index'][-1]),"M" + str(15500),weight=1)
                
                
            
                for l in range(len(time_multiplier)):
                    if gen['cf'][-1][l]>=0.001:
                        # print(gen['index'][-1],gen['cf'][-1])
                        # G.add_node("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]),
                        #        names="O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), **proportional_cost_op)            

                        # G.add_node("M" + str(5000 + gen['index'][-1]) + str(time_multiplier[l]),
                        #         names="M" + str(5000 + gen['index'][-1]) + str(time_multiplier[l]), type='intermediate',
                        #         flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_MWh)

                        # adding h2 here
                        G.add_node("M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]),
                                names="M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_MWh)

                        G.add_node("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),
                                names="O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), **proportional_cost_op)
                        
                        G.add_node("M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]), names="M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,
                            units=mat_units_MWh) 
                        G.add_edge("M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),"O" + str(4500 + gen['index'][-1]),weight=1)
                                                
                        #if not commissioning and no fuel is needed.......
                        if str(gen['type'][-1]) not in {"Bioenergy","Thermal","Cogeneration" }:
                            
                            if time_resolution == 'hourly':
                                G.add_edge("M" + str(1000 + gen['index'][-1]),"O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1) #fix the time 4h

                                G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]),
                                        "M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), weight=gen['cf'][-1][l] * 1)
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),weight=1/(gen['cf'][-1][l])) #cf wip6/11/2025
                                
                            elif time_resolution == 'monthly':
                                G.add_edge("M" + str(1000 + gen['index'][-1]),"O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), weight=730)
                                
                                G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]),
                                        "M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), weight=gen['cf'][-1][l]*730)                                 
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),weight=1/(gen['cf'][-1][l]*730)) #cf wip6/11/2025
                                                        
                        elif str(gen['type'][-1]) in {"Bioenergy","Thermal","Cogeneration" }:
                            
                            
                            if time_resolution == 'hourly':
                                G.add_edge("M" + str(1000 + gen['index'][-1]),"O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), weight=4/gen['cf'][-1][l]) #time fix 4h
                                G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]),
                                        "M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1)#error need x4
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),weight=1/(gen['cf'][-1][l]*4)) #cf wip6/11/2025

                            elif time_resolution == 'monthly':
                                G.add_edge("M" + str(1000 + gen['index'][-1]),"O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]), weight=730/gen['cf'][-1][l]) #monthly fix
                                G.add_edge("O" + str(2000 + gen['index'][-1]) + str(time_multiplier[l]),
                                        "M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1*730)     #error remove cf
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][-1])+str(time_multiplier[l]),weight=1/(gen['cf'][-1][l]*730)) #cf wip6/11/2025
                         
                        else:
                            print("error")
                        
                        G.add_edge("M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]), "O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), weight=1)
                        
                        #prevent double counting edge
                        if str(gen['type'][-1]) not in {"Bioenergy","Thermal","Cogeneration" }:
                            if time_resolution == 'hourly':
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][-1]) ,weight=1/gen['cf'][-1][l]/4)
                            elif time_resolution == 'monthly':
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][-1]) ,weight=1/gen['cf'][-1][l]/730)
                        elif str(gen['type'][-1]) in {"Bioenergy","Thermal","Cogeneration" }:
                            if time_resolution == 'hourly':
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][-1]) ,weight=1)
                            elif time_resolution == 'monthly':
                                G.add_edge("O" + str(8000 + gen['index'][-1]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][-1]) ,weight=1/730)
                    
                    #curtail
                        G.add_node("O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),
                                    names="O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]), **proportional_cost_op) 
                        
                        G.add_node("M" + str(11000 + gen['index'][-1]) + str(time_multiplier[l]),
                                    names="M" + str(11000 + gen['index'][-1]) + str(time_multiplier[l]), type='product',
                                    flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_GWh)        
                        G.add_edge("M" + str(7000 + gen['index'][-1]) + str(time_multiplier[l]),"O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),weight=1)
                        G.add_edge("O" + str(10000 + gen['index'][-1]) + str(time_multiplier[l]),"M" + str(11000 + gen['index'][-1]) + str(time_multiplier[l]),weight=1)
                                            
                if sum(gen['cf'][-1][0:time_period])>=0.001:
                    G.add_edge("M" + str(3000 + gen['index'][-1]),  "O" + str(4000 + gen['index'][-1]), weight=1e-7)    
                    for l in range(len(time_multiplier)):
                        G.add_node("O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),names="O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                        G.add_node("M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),names="M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_GWh)
                        G.add_edge("O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),"M" + str(3000 + gen['index'][-1]),weight=1)
                        if time_resolution == 'hourly':
                            G.add_edge( "O" + str(4000 + gen['index'][-1]),"M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),weight=4)
                        elif time_resolution == 'monthly':
                            G.add_edge( "O" + str(4000 + gen['index'][-1]),"M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),weight=1)
                            
                        G.add_edge("M" + str(5000 + gen['index'][-1])+str(time_multiplier[l]),"O" + str(2000 + gen['index'][-1])+str(time_multiplier[l]),weight=1)

            # adding hydrogen connections
            #print(gen['cf'][-1][l]
        
        return gen['cf'][-1]


    # electricity generation sites
    gen = {'name': [], 'type': [], 'rank': [], 'capacity': [], 'gwh': [], 'price': [], 'distance': [], 'poc': [], 'index': [], 'poc_index':[],
        'reference': [], 'local':[], 'cf':[], 'status':[]}
    for i in range(len(Pd_Generation)):  # map out the generation sites, and the respective operating units
        if str(Pd_Gen.North_South[i]) != island:
            if ((Pd_Gen.Generation_GWh_yr[i] >= 10) and str(Pd_Gen.Generation_GWh_yr[i]) != 'nan'):  # filter
                if str(Pd_Gen.Types_of_power_station[i]) == "Solar":
                    gen['type'].append(Pd_Gen.Type[i])

                    temp_cf=gen_nodes()

                elif str(Pd_Gen.Types_of_power_station[i]) == "Unknown" or str(
                        Pd_Gen.Types_of_power_station[i]) == "Battery" or str(
                        Pd_Gen.Types_of_power_station[i]) == "Marine":
                    pass

                else:
                    gen['type'].append(Pd_Gen.Types_of_power_station[i])
                    temp_cf=gen_nodes()

    
    # generation to poc
    for ii in range(len(gen['name'])):  # matching gen to poc
        for jj in range(len(gxp['poc'])):
            for l in range(len(time_multiplier)):
                if str(gen['poc'][ii]) == str(gxp['poc'][jj]):
                    if G.has_node("O" + str(8000 + gen['index'][ii]) + str(time_multiplier[l])) is True:
                        G.add_edge("O" + str(8000 + gen['index'][ii]) + str(time_multiplier[l]),
                                "M" + str(gxp['index'][jj])+str(101) + str(time_multiplier[l]), weight=1/1000) #convert MWh to GWh
                else:
                    pass


    #GXPs connections
    gxp_codes = {key: [] for key in gxp['poc']}
    gxp_codes2 = {key: [] for key in gxp['poc']}
    gxp_gxp_index1a = []
    ME = []
    gxp_connections_dict = {'Op_ID': [], 'From': [], 'To': [], "Distance":[], "Capacity":[],"flow":[],"grid_lines_needed":[],"kV":[],"Line":[],"Cost":[]}
    gxp_gxp_count = 0
    
    for kk in range(len(Pd_GXP_GXP.Point_1)):
        for ll in range(len(gxp['poc'])):
            for mm in range(len(gxp['poc'])):
                if Pd_GXP_GXP.North_South[kk] != island:
                    # print(Pd_GXP_GXP.Point_1[kk],"=", gxp['poc'][ll], Pd_GXP_GXP.Point_2[kk],'=', gxp['poc'][mm])
                    if ((str(Pd_GXP_GXP.Point_1[kk])) == str(gxp['poc'][ll])) and (
                            (str(Pd_GXP_GXP.Point_2[kk])) == str(gxp['poc'][mm])):
                        gxp_gxp_index1a.append((6000 + kk))

                        gxp_connections_dict['Op_ID'].append('O' + str(gxp_gxp_index1a[-1]))
                        gxp_connections_dict['From'].append("M" + str(gxp['index'][ll]))
                        gxp_connections_dict['To'].append("M" + str(gxp['index'][mm]))
                        
                        gxp_connections_dict['Distance'].append(Pd_GXP_GXP.Distance[kk])
                        
                        if time_resolution == 'hourly':
                            gxp_connections_dict['Capacity'].append(Pd_GXP_GXP.Value_MVA[kk]/1000*4)

                        elif time_resolution == 'monthly':
                            gxp_connections_dict['Capacity'].append(Pd_GXP_GXP.Value_MVA[kk]/1000*730)
                                
                        gxp_connections_dict['kV'].append(Pd_GXP_GXP.kV[kk])
                        gxp_connections_dict['Line'].append(Pd_GXP_GXP.line_type_2[kk])
                        
                        # print(gxp_connections_dict)
                        gxp_codes[(Pd_GXP_GXP.Point_1[kk])].append('O' + str(gxp_gxp_index1a[-1]))
                        gxp_codes2[(Pd_GXP_GXP.Point_2[kk])].append('O' + str(gxp_gxp_index1a[-1]))
                        # print("pass")
    
    for i in range(len(gxp_connections_dict['Op_ID'])):
        tac = transmission_cost_lookup.get((gxp_connections_dict['kV'][i], gxp_connections_dict['Line'][i]), None)
        if tac is not None:
            total_cost = tac * gxp_connections_dict['Distance'][i]
            gxp_connections_dict['Cost'].append(round(total_cost,2))
        else:
            gxp_connections_dict['Cost'].append(None)
            print(f"No match for kV={gxp_connections_dict['kV'][i]}, Line={gxp_connections_dict['Line'][i]}")
 
 
    # print(gxp_connections_dict)
    # print(gxp_gxp_index1a)
    for i in range(len(gxp_connections_dict['Op_ID'])):
        
        for l in range(len(time_multiplier)):
            G.add_node(gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]),names=gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=gxp_connections_dict['Capacity'][i]*4, fix_cost=0, proportional_cost=0.00000001)
            # G.add_node(gxp_connections_dict['From'][i]+str(time_multiplier[l]),names=gxp_connections_dict['From'][i]+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0)
            # G.add_node(gxp_connections_dict['To'][i]+str(time_multiplier[l]),names=gxp_connections_dict['To'][i]+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0)

            G.add_edge(gxp_connections_dict['From'][i]+str(time_multiplier[l]), gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]),weight=1.035)
            G.add_edge(gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]), gxp_connections_dict['To'][i]+str(time_multiplier[l]),weight=1)
            
            if int(gxp_connections_dict['Op_ID'][i][1:]) % 2 == 1 and int(gxp_connections_dict['Op_ID'][i][1:]) >= 6001:
                ME.append([(gxp_connections_dict['Op_ID'][i-1]) + str(time_multiplier[l]),(gxp_connections_dict['Op_ID'][i]) + str(time_multiplier[l])])

    dem_biomass_accessible_index = set()
    for i in range(len(Pd_Factory)):
        if Pd_Fac.North_South[i] != island:
            if Pd_Fac.Energy_Estimated[i] >= 5 and Pd_Fac.Industry[i] in ["Wood Processing","Pulp"]:#12
                dem_biomass_accessible_index.add(int(3000 + Pd_Fac.ObjectID[i]))
            elif Pd_Fac.Energy_Estimated[i] >= 5 and Pd_Fac.IUR2025_V1[i] not in [11]:#12
                dem_biomass_accessible_index.add(int(3000 + Pd_Fac.ObjectID[i]))
    # print(len(dem_biomass_accessible_index))
       
    dem_coal_boilers = {"index":[],"coal_capacity":[],"dem_hours":[],"dem_ID":[]}
    for i in range(len(Pd_Factory)):
        if Pd_Fac.North_South[i] != island:
            if (Pd_Fac.Energy_Estimated[i] >= 5 and Pd_Fac.Switch[i] == "Yes" and int(3000 + Pd_Fac.ObjectID[i]) in dem_biomass_accessible_index):#12
                    dem_coal_boilers["index"].append(int((3000 + Pd_Fac.ObjectID[i])))
                    dem_coal_boilers["dem_ID"].append(int((Pd_Fac.ObjectID[i])))
                    dem_coal_boilers["dem_hours"].append(int((Pd_Fac.Operating_Hours[i])))
                    dem_coal_boilers["coal_capacity"].append(int((Pd_Fac.Capacity[i])))                
  

    # print(gxp_codes)
    # demand sites
    dem = {"index": [], "dem_gwh": [], "dem_name": [], "dem_distance": [], "dem_poc": [], "dem_reference": [],
        "dem_60": [], "dem_90": [], "dem_140": [], "dem_180": [], "dem_180hi": [], "dem_hours": [], "dem_ID": [],
        "dem_ind": [],'grid_flow':[],'max_flow':[],'grid_cost':[], 'fpvv':[]}

    for i in range(len(Pd_Factory)):  # factory build up
        if Pd_Fac.North_South[i] != island:

            if (Pd_Fac.Energy_Estimated[i] >= 5 ):
                dem["index"].append(int((3000 + Pd_Fac.ObjectID[i])))
                dem["dem_ID"].append(int((Pd_Fac.ObjectID[i])))
                dem["dem_gwh"].append(Pd_Fac.Energy_Estimated[i])
                dem["dem_name"].append(Pd_Fac.Company_name[i])
                dem["dem_distance"].append(Pd_Fac.NEAR_DIST_1[i]/1000) #m to km
                dem["dem_poc"].append(Pd_Fac.POC[i])
                dem["dem_reference"].append(Pd_Fac.ObjectID[i])
                dem["dem_ind"].append(Pd_Fac.Industry[i])

                dem["dem_60"].append(Pd_Fac._60_C2[i])
                dem["dem_90"].append(Pd_Fac._90_C3[i])
                dem["dem_140"].append(Pd_Fac._140_C4[i])
                dem["dem_180"].append(Pd_Fac._180_C5[i])
                dem["dem_180hi"].append(Pd_Fac._180_C6[i])
              
                    
                dem["dem_hours"].append(Pd_Fac.Operating_Hours[i])
                for j in range(len(gxp['poc'])):
                    if dem['dem_poc'][-1] == gxp['poc'][j]:
                        gxp_price_array = np.array(gxp['price'][j])  # shape: (n, 12)
                        if time_resolution == "monthly":
                            dem['fpvv'].append([p * assumed_cost_coefficient for p in gxp_price_array])
                        elif time_resolution == "hourly":
                            
                            dem['fpvv'].append([p for p in gxp_price_array])
                        
                        # print(dem['fpvv'])

                    
                        # print(dem['fpvv'][-1])
                        break
                        # dem['fpvv'].append(gxp['price'][j]*assumed_cost_coefficient)
                for l in range(len(time_multiplier)):
                    G.add_node("M" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]),
                            names="M" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]),
                            type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,
                            units=mat_units_GWh)  # fix according to location
                    
                    if dem["dem_ind"][-1] != "Pulp" and dem['dem_ind'][-1] != "Wood Processing":

                        G.add_node("O" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]),
                                names="O" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]), fix_cost=0, proportional_cost=dem['fpvv'][-1][l]*1000)  # lines charge 41.46NZD/kVA 1E6/(dem_hours[-1])*lines_charges
                    else:
                        G.add_node("O" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]),
                                names="O" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]), fix_cost=0, proportional_cost=dem['fpvv'][-1][l]*1000*1e7)  # lines charge 41.46NZD/kVA 1E6/(dem_hours[-1])*lines_charges                          
                        
                    G.add_edge("O" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]),
                            "M" + str(dem["index"][-1]) + str(103) + str(time_multiplier[l]), weight=1)
            else:
                pass
                    # G.add_node("O" + str(4000 + gen['index'][-1]), names="O" + str(4000 + gen['index'][-1]), capacity_lower_bound=0,
                    #     capacity_upper_bound=gen['capacity'][-1], fix_cost=0, proportional_cost=gen['price'][-1]/12*time_period)  # gen capacity #warning wip

    aggregated_dem = zip(dem["dem_60"], dem["dem_90"], dem["dem_140"], dem["dem_180"], dem["dem_180hi"])
    aggregated_dem_lists = [list(group) for group in aggregated_dem]
    aggregated_dem_dict = {(key): lst for key, lst in zip(dem["index"], aggregated_dem_lists)}
    
    # demand to poc
    # poc to factory
    behind_gxp_not_needed = {str(gxp['index'][jj]) for jj in range(len(gxp['index']))}
    # print(behind_gxp_not_needed)
    for kk in range(len(dem["dem_name"])):
        for jj in range(len(gxp['poc'])):
            if str(dem["dem_poc"][kk]) == str(gxp['poc'][jj]):
                behind_gxp_not_needed.discard(str(gxp['index'][jj]))
                for l in range(len(time_multiplier)):
                    G.add_edge("M" + str(gxp['index'][jj] +600) + str(time_multiplier[l]),
                            "O" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), weight=1.035)
                    
    for jj in range(len(gxp['poc'])):        
        if gxp['dem'][jj] !=0:
            behind_gxp_not_needed.discard(str(gxp['index'][jj]))
    # print('as',behind_gxp_not_needed)
    for i in behind_gxp_not_needed:
        for l in range(len(time_multiplier)):
            G.remove_node("O" + str(int(i)+300)+str(time_multiplier[l]))
            G.remove_node("M" + str(int(i)+600)+str(time_multiplier[l]))
            
    # print(gxp['poc'])     
    
    for i in range(len(gxp['poc'])):
        for l in range(len(time_multiplier)):
            if G.in_degree("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))==0:
                # print(("M" + str(gxp['index'][i]) + str(time_multiplier[l])))
                # print("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))
                G.remove_node("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))
                G.remove_node("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]))
                
                if G.in_degree("M" + str(gxp['index'][i]) + str(time_multiplier[l]))==0:
                    # print(("M" + str(gxp['index'][i]) + str(time_multiplier[l])))
                    G.remove_node("M" + str(gxp['index'][i]) + str(time_multiplier[l]))
                    G.remove_node("O" + str(gxp['index'][i]+300) + str(time_multiplier[l]))
                    G.remove_node("M" + str(gxp['index'][i]+600) + str(time_multiplier[l]))
                
       
    # factory conditions
    industrial_data, industry = pandas_to_dict(df_industrial_curve)
    # print('industrial_data',industrial_data)
    cop = [4.572445696, 2.685323156, 1.733107565, 0.99, 0.99] #heating electric tech
    proportional_cost_heat = [ 274631.3545 ,274631.3545 ,274631.3545, 147848.4974 ,147848.4974 ]

    # print('Generating industrial demand profiles...')
    # print(industrial_data)
    # print(industry)

    dem_resolution_UF = {}
    if time_resolution == "monthly":
        hours_per_month = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 743]  # hours in each month of the year
    elif time_resolution == "hourly":
        pass
    # print(len(dem["dem_name"]))
    # print((industry))
    for i in range(len(dem["dem_name"])):
        dem_resolution_UF[dem["index"][i]] = []
        for j in range(len(industry)):
            # print(str(dem["dem_ind"][i]),"--",str(industry[j]))
            if str(dem["dem_ind"][i]) == str(industry[j]):  # match demand file with industry file
                if time_resolution == "monthly":
                    dem_resolution_UF[dem["index"][i]] = np.divide(industrial_data[industry[j]], 730).tolist()
                elif time_resolution == "hourly":
                    dem_resolution_UF[dem["index"][i]] = industrial_data[industry[j]]

    
    # print('dem_resolution_UF',dem_resolution_UF)
    
    dem_time_period_profile = {}
    for j, k in aggregated_dem_dict.items():
        if time_resolution == 'hourly':
            for l in range(len(time_multiplier)):
                dem_profile = dem_resolution_UF[j][l] 
                # print('dem_profile',dem_profile)
                dem_time_period_profile[j, time_multiplier[l]] = (np.array(aggregated_dem_dict[j]) * dem_profile/8760 *4 ).tolist() #4hourly
        elif time_resolution == 'monthly':
            for l in range(len(time_multiplier)):
                dem_profile = dem_resolution_UF[j][l]
                dem_time_period_profile[j, time_multiplier[l]] = (np.array(aggregated_dem_dict[j]) * dem_profile/12).tolist()

    # print('dem_profile',dem_profile)
    # print('dem_time_period_profile',dem_time_period_profile)
    # print('aggregated_dem_dict',aggregated_dem_dict)

    dem_by_index_tech_monthly = {}
    for i in dem["index"]:
        for j in range(0, 5):
            if (i, j) not in dem_by_index_tech_monthly:  # j is temp profile
                dem_by_index_tech_monthly[(i, j)] = []
            for l in time_multiplier:
                dem_by_index_tech_monthly[i, j].append(dem_time_period_profile[i, l][j] )

    # print('jj',dem_by_index_tech_monthly)


    def heat_technology(i, a):
        for j in range(a, 5):  # o104 m114
            if j != 3:
                if time_resolution == 'hourly':
                    G.add_node("O" + str(dem["index"][i]) + str(134 + j),
                        names="O" + str(dem["index"][i]) + str(134 + j),
                        fix_cost=1, proportional_cost=proportional_cost_heat[j]/8760*time_period)  # o134
                elif time_resolution == 'monthly':
                    G.add_node("O" + str(dem["index"][i]) + str(134 + j),
                        names="O" + str(dem["index"][i]) + str(134 + j),
                        fix_cost=1, proportional_cost=proportional_cost_heat[j]/12*time_period)  # o134
                    
                for l in range(len(time_multiplier)):  # o119+time m124+time o134 m1191+time m1192+time
                    G.add_node("M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                names="M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                type='intermediate',flow_rate_lower_bound=0,flow_rate_upper_bound=1e6, price=0,units=mat_units_GWh)  # M104

                    #print("O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]),':',dem_profile_price[dem["index"][i], time_multiplier[l]])
                    G.add_node("O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]), names="O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]),
                            **proportional_cost_op)  # o119 electric
                    
                    #hot water storage?
                    # G.add_node("O" + str(dem["index"][i]) + str(114 + j) + str(time_multiplier[l]), 
                    #         names="O" + str(dem["index"][i]) + str(114 + j) + str(time_multiplier[l]),
                    #             **proportional_cost_op) 
                    # G.add_edge("M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),"O" + str(dem["index"][i]) + str(114 + j) + str(time_multiplier[l]), weight=1)
                    # if time_multiplier[l] != time_multiplier[-1]:
                    #     G.add_edge("O" + str(dem["index"][i]) + str(114 + j) + str(time_multiplier[l]),"M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l+1]), weight=1)
                    # else :
                    #     G.add_edge("O" + str(dem["index"][i]) + str(114 + j) + str(time_multiplier[l]),"M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[0]), weight=1)
                        
                    if dem_by_index_tech_monthly[dem["index"][i], j][l] > 0:
                        G.add_node("O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                            names="O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                            **proportional_cost_op) #o109       
                        
                        G.add_node("M" + str(dem["index"][i]) + str(124 + j) + str(time_multiplier[l]),
                                names="M" + str(dem["index"][i]) + str(124 + j) + str(time_multiplier[l]),
                                type='product',
                                flow_rate_lower_bound=dem_by_index_tech_monthly[dem["index"][i], j][l],
                                flow_rate_upper_bound=1.01*dem_by_index_tech_monthly[dem["index"][i], j][l], price=0,
                                units=mat_units_GWh)  # M124
                        demand_product_nodes.add("M" + str(dem["index"][i]) + str(124 + j))
                        
                    elif dem_by_index_tech_monthly[dem["index"][i], j][l] == 0:
                        pass
                    
                    G.add_edge("M" + str(dem["index"][i]) + str(103) + str(time_multiplier[l]),
                            "O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]),
                            weight=1)  # m114 o119 time
                    G.add_edge("O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]),
                            "M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                            weight=cop[j])  # o119 time , m104


                    if dem_by_index_tech_monthly[dem["index"][i], j][l] > 0:
                        G.add_edge("M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                            "O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                            weight=1)  # m104 , o109
                        G.add_edge("O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                                "M" + str(dem["index"][i]) + str(124 + j) + str(time_multiplier[l]),
                                weight=1)  # o109 , m124
                        
                    elif dem_by_index_tech_monthly[dem["index"][i], j][l] == 0:
                        pass
                                
                    G.add_node("M" + str(dem["index"][i]) + str(119 + j) + str(1) + str(time_multiplier[l]),
                            names="M" + str(dem["index"][i]) + str(119 + j) + str(1) + str(time_multiplier[l]),
                            type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,
                            units=mat_units_GWh)  # m119_1_01
                    G.add_node("M" + str(dem["index"][i]) + str(119 + j) + str(2) + str(time_multiplier[l]),
                            names="M" + str(dem["index"][i]) + str(119 + j) + str(2) + str(time_multiplier[l]),
                            type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,
                            units=mat_units_GWh)  # m119_2_01

                    G.add_edge("O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]),
                            "M" + str(dem["index"][i]) + str(119 + j) + str(1) + str(time_multiplier[l]), weight=1)
                    G.add_edge("M" + str(dem["index"][i]) + str(119 + j) + str(1) + str(time_multiplier[l]),
                            "O" + str(dem["index"][i]) + str(134 + j), weight=1E-7)
                    if time_resolution == "monthly":
                        G.add_edge("O" + str(dem["index"][i]) + str(134 + j),
                                "M" + str(dem["index"][i]) + str(119 + j) + str(2) + str(time_multiplier[l]), weight=0.8*730/1000)
                    elif time_resolution == "hourly":
                        G.add_edge("O" + str(dem["index"][i]) + str(134 + j),
                                "M" + str(dem["index"][i]) + str(119 + j) + str(2) + str(time_multiplier[l]), weight=0.8*4/1000)
                        
                    G.add_edge("M" + str(dem["index"][i]) + str(119 + j) + str(2) + str(time_multiplier[l]),
                            "O" + str(dem["index"][i]) + str(119 + j) + str(time_multiplier[l]),
                            weight=1)
            else:  
                for l in range(len(time_multiplier)):
                    if dem_by_index_tech_monthly[dem["index"][i], j][l] > 0:
                        G.add_node("M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                    names="M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                    type='intermediate',
                                    flow_rate_lower_bound=0,
                                    flow_rate_upper_bound=1e6, price=0,
                                    units=mat_units_GWh)  # M104
                        G.add_node("O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                                names="O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                                **proportional_cost_op) #o109                  
                    
                        
                        G.add_node("M" + str(dem["index"][i]) + str(124 + j) + str(time_multiplier[l]),
                                    names="M" + str(dem["index"][i]) + str(124 + j) + str(time_multiplier[l]),
                                    type='product',
                                    flow_rate_lower_bound=dem_by_index_tech_monthly[dem["index"][i], j][l],
                                    flow_rate_upper_bound=1.01*dem_by_index_tech_monthly[dem["index"][i], j][l], price=0,
                                    units=mat_units_GWh)  # M124
                        demand_product_nodes.add("M" + str(dem["index"][i]) + str(124 + j))

                        G.add_edge("M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                "O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                                weight=1)  # m104 , o109                
                        G.add_edge("O" + str(dem["index"][i]) + str(109 + j) + str(time_multiplier[l]),
                                "M" + str(dem["index"][i]) + str(124 + j) + str(time_multiplier[l]),
                                weight=1)  # o109 , m124   
                        
                    elif dem_by_index_tech_monthly[dem["index"][i], j][l] == 0:
                        G.add_node("M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                    names="M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]),
                                    type='intermediate',
                                    flow_rate_lower_bound=0,
                                    flow_rate_upper_bound=1e6, price=0,
                                    units=mat_units_GWh)  # M104
            # print(dem_by_index_tech_monthly)

    def heat_transfer(i, a):
        
        for j in range(a, 4):  # 129+time,
            if j == 3:
                
                for l in range(len(time_multiplier)):  
                    G.add_node("O" + str(dem["index"][i]) + str(129 + j) + str(time_multiplier[l]),
                            names="O" + str(dem["index"][i]) + str(129 + j) + str(time_multiplier[l]),
                            **proportional_cost_op)  # o129
                    G.add_edge("M" + str(dem["index"][i]) + str(105 + j) + str(time_multiplier[l]),
                            "O" + str(dem["index"][i]) + str(129 + j) + str(time_multiplier[l]), weight=1)  # m125 o129
                    G.add_edge("O" + str(dem["index"][i]) + str(129 + j) + str(time_multiplier[l]),
                            "M" + str(dem["index"][i]) + str(104 + j) + str(time_multiplier[l]), weight=0.95)  # o129 m124
                    # print('heat transfer',j,l)
                    

    # tech display
    demand_product_nodes = set()   # ← create the empty set

    for i in range(len(dem["index"])):
        for a in range(5):
            if aggregated_dem_lists[i][a] != 0:
                heat_technology(i, a)
                heat_transfer(i, a)
                break
    

    biomass_properties = {"resource_type": ['Inforest harvest', 'K_logs', 'Sawmill chip', 'Straw_and_Stover', 'Pellets'],
        "raw_material_cost_north_nzd_per_ton": [70.7, 113, 108,  75, 484],
        "raw_material_cost_south_nzd_per_ton": [76, 123, 48,  75, 484],
        "energy_content_gj_per_ton": [11.0, 6.9, 6.9,  13.4, 17.5],
        "resource_id": [0,1,2,3,4],
        "boiler":[1,0,0,1,0],
        "L0":[0.725/0.8,0.875/0.95,0.65/0.7,0.625/0.75,1], #0.8,0.95,0.7,0.75 adjusted to one because it is already L1
        "L1":[1,1,1,1,1], #0.8,0.95,0.7,0.75 adjusted to one because it is already L1
        "L2":[0.65/0.8,0.8/0.95,0.6/0.7,0.5/0.75,1], #0.65,0.8,0.6,0.5
        "biomass_distance_cost_constant":[0.0000981818181818182,0.000156521739130435,0.000156521739130435,0.0000805970149253731,0.0000617142857142857]}#NZD/MWh


    biomass_distance_cost_constant = 1.575e-3 #1.575 #NZD/MWh m #5.714e-5 #GJ to MWh into account
    
    biomass_heat_proportional_cost = [125864.2392 ,  308017.703]
    biomass_heat_efficiency = [0.8, 0.75]


    # biomass works #starts from 1 not 0
    # to convert biomass resource allocated to plants id20
    unique_list = {'name': [], 'biomass_fac_index': [],'island':[], '0': [], '1': [],'2': [], '3': [], '4': []}
    unique_list1 = {'name': [], 'biomass_gen_index': [],'island':[], '0': [], '1': [], '2': [], '3': [], '4': []}


    for index, item in enumerate(Pd_bio_fac.Biomass_ID):
        if (Pd_bio_fac.North_South[index] != island) and (Pd_bio_fac.Distance[index] <= distance_constraint) and (
                Pd_bio_fac.Source_ID[index] in dem["dem_ID"]):
            if item not in unique_list['biomass_fac_index']:
                unique_list['biomass_fac_index'].append(item)

    for index, item in enumerate(Pd_bio_gen.Biomass_ID):
        if (Pd_bio_gen.North_South[index] != island) and (Pd_bio_gen.Distance[index] <= distance_constraint) and (
                Pd_bio_gen.Source_ID[index] in gen["reference"]):
            if item not in unique_list1['biomass_gen_index']:
                unique_list1['biomass_gen_index'].append(item)

    unique_list['biomass_fac_index'].sort()
    unique_list1['biomass_gen_index'].sort()
    
    level = "L0"
    level_factors = biomass_properties[level]
    # print(unique_list)
    # print(unique_list1)

    for j in range(len(unique_list['biomass_fac_index'])):
        for i in range(len(Pd_bio.Row_Labels)):
            if Pd_bio.North_South[i] != island:
                if (unique_list['biomass_fac_index'][j] == Pd_bio.ORIG_ID[i]):
                    if time_resolution == 'hourly': #4hourly
                        unique_list['island'].append(Pd_bio.North_South[i])
                        unique_list['0'].append(Pd_bio.Inforest_harvest[i]/8760*4*time_period *level_factors[0])
                        unique_list['1'].append(Pd_bio.K_logs[i]/8760*4*time_period*level_factors[1])
                        unique_list['2'].append(Pd_bio.Sawmill_chip[i]/8760*4*time_period*level_factors[2])
                        unique_list['3'].append(Pd_bio.Straw_and_Stover[i]/8760*4*time_period*level_factors[3])                        
                        unique_list['4'].append(Pd_bio.Pellets[i]/8760*4*time_period*level_factors[4])
                    
                        unique_list['name'].append(Pd_bio.Row_Labels[i])
                        # print("error not worked yet")
                    elif time_resolution == 'monthly':
                        unique_list['island'].append(Pd_bio.North_South[i])
                        unique_list['0'].append(Pd_bio.Inforest_harvest[i]/12*time_period*level_factors[0])
                        unique_list['1'].append(Pd_bio.K_logs[i]/12*time_period*level_factors[1])
                        unique_list['2'].append(Pd_bio.Sawmill_chip[i]/12*time_period*level_factors[2])
                        unique_list['3'].append(Pd_bio.Straw_and_Stover[i]/12*time_period*level_factors[3])                        
                        unique_list['4'].append(Pd_bio.Pellets[i]/12*time_period*level_factors[4])
                        
                        unique_list['name'].append(Pd_bio.Row_Labels[i])                        
                else:
                    pass
    # print(unique_list1)

    for j in range(len(unique_list1['biomass_gen_index'])):
        for i in range(len(Pd_bio.Row_Labels)):
            if Pd_bio.North_South[i] != island:
                if (unique_list1['biomass_gen_index'][j] == Pd_bio.ORIG_ID[i]):
                    if time_resolution == 'hourly': #4hourly
                        unique_list1['island'].append(Pd_bio.North_South[i])
                        unique_list1['0'].append(Pd_bio.Inforest_harvest[i]/8760*4*time_period*level_factors[0])
                        unique_list1['1'].append(Pd_bio.K_logs[i]/8760*4*time_period*level_factors[1])
                        unique_list1['2'].append(Pd_bio.Sawmill_chip[i]/8760*4*time_period*level_factors[2])
                        unique_list1['3'].append(Pd_bio.Straw_and_Stover[i]/8760*4*time_period*level_factors[3])
                        unique_list1['4'].append(Pd_bio.Pellets[i]/8760*4*time_period*level_factors[4])
                        
                        unique_list1['name'].append(Pd_bio.Row_Labels[i])
                        # print("error not worked yet")
                    elif time_resolution == 'monthly':
                        unique_list1['island'].append(Pd_bio.North_South[i])
                        unique_list1['0'].append(Pd_bio.Inforest_harvest[i]/12*time_period*level_factors[0])
                        unique_list1['1'].append(Pd_bio.K_logs[i]/12*time_period*level_factors[1])
                        unique_list1['2'].append(Pd_bio.Sawmill_chip[i]/12*time_period*level_factors[2])
                        unique_list1['3'].append(Pd_bio.Straw_and_Stover[i]/12*time_period*level_factors[3])
                        unique_list1['4'].append(Pd_bio.Pellets[i]/12*time_period*level_factors[4])

                        unique_list1['name'].append(Pd_bio.Row_Labels[i])
                else:
                    pass
    # print(unique_list1)
    # print(len(unique_list1["biomass_gen_index"]),len(unique_list["biomass_fac_index"]))

    unique_list2 = {'name': [], 'biomass_index': [],'island':[], '0': [], '1': [], '2': [], '3': [], '4': []}

    for j in range(len(unique_list['biomass_fac_index'])):
        unique_list2['name'].append(unique_list['name'][j])
        unique_list2['biomass_index'].append(unique_list['biomass_fac_index'][j])
        if time_resolution == 'hourly':
            unique_list2['island'].append(unique_list['island'][j])
            unique_list2['0'].append(unique_list['0'][j])
            unique_list2['1'].append(unique_list['1'][j])
            unique_list2['2'].append(unique_list['2'][j])
            unique_list2['3'].append(unique_list['3'][j])
            unique_list2['4'].append(unique_list['4'][j])

        elif time_resolution == 'monthly':
            unique_list2['island'].append(unique_list['island'][j])            
            unique_list2['0'].append(unique_list['0'][j])
            unique_list2['1'].append(unique_list['1'][j])
            unique_list2['2'].append(unique_list['2'][j])
            unique_list2['3'].append(unique_list['3'][j])
            unique_list2['4'].append(unique_list['4'][j])

    for j in range(len(unique_list1['biomass_gen_index'])):
        if unique_list1['biomass_gen_index'][j] not in unique_list2['biomass_index']:
            unique_list2['name'].append(unique_list1['name'][j])
            unique_list2['biomass_index'].append(unique_list1['biomass_gen_index'][j])
            if time_resolution == 'hourly':
                unique_list2['island'].append(unique_list['island'][j])
                unique_list2['0'].append(unique_list1['0'][j])
                unique_list2['1'].append(unique_list1['1'][j])
                unique_list2['2'].append(unique_list1['2'][j])
                unique_list2['3'].append(unique_list1['3'][j])
                unique_list2['4'].append(unique_list1['4'][j])
                
            elif time_resolution == 'monthly':
                unique_list2['island'].append(unique_list['island'][j])       
                unique_list2['0'].append(unique_list1['0'][j])
                unique_list2['1'].append(unique_list1['1'][j])
                unique_list2['2'].append(unique_list1['2'][j])
                unique_list2['3'].append(unique_list1['3'][j])
                unique_list2['4'].append(unique_list1['4'][j])

    unique_list2['biomass_index'].sort()
    # p=pd.DataFrame(unique_list2)
    # d= p.to_csv('biomass.csv')


    methanol_plant_op_cost = 700 #NZD/y/ton # 666.67 #NZD/y/ton #
    methanol_selling_price = 600 #USD400/MT
    
    def process_biomass_data(a=unique_list2, b=G, units=mat_units_GJ, d=mat_units_GJ, resource_id='0'):
        # resource_id is a string like '0','1','2','3','4'
        col = str(resource_id)
        unique_biomass_data = {'node': [], 'gen_id_biomass': [], 'fac_id_biomass': [],
            'gen_id_fuel': [], 'biomass_distance': [], 'biomass_id': [],'island':[]}
        if col not in a:
            return unique_biomass_data
        for i in range(len(a[col])):
            if a[col][i] > 0:
                unique_biomass_data['node'].append(str(80000  + a['biomass_index'][i])+ str(col))
        # print(unique_biomass_data)
        # print(col)
        return unique_biomass_data


    def process_biomass_green_industry(unique_list2, units=mat_units_GJ, resource_id=None, gen_existing=None, biomass_props=biomass_properties):
        unique_biomass_data = process_biomass_data(unique_list2,resource_id=resource_id)
        generation_exists = gen_existing if gen_existing is not None else set() #prevent over defining the generation side (destination)
        biomass_exists = set()  #prevent over define biomass resource
        text1_exists = set()
        for i in range(len(Pd_bio_gen.Biomass_ID)):
            #print(unique_biomass_data)
            if (10000 + Pd_bio_gen.Source_ID[i] in gen["index"]) and (Pd_bio_gen.North_South[i] != island) and (
                    Pd_bio_gen.Distance[i] <= distance_constraint) and (
                    (str(80000  + Pd_bio_gen.Biomass_ID[i])+ str(resource_id)) in unique_biomass_data['node']):
                for j in range(len(gen['index'])):
                    if (10000 + Pd_bio_gen.Source_ID[i]) == gen['index'][j] and gen['status'][j] != "Commissioning":
                        unique_biomass_data['biomass_distance'].append(Pd_bio_gen.Distance[i])
                        unique_biomass_data['biomass_id'].append(str(80000+ Pd_bio_gen.Biomass_ID[i]) + str(resource_id) )
                        unique_biomass_data['island'].append(Pd_bio_gen.North_South[i])
                        if gen['local'][j] == 1:
                            unique_biomass_data['gen_id_biomass'].append(600 + gen['poc_index'][j])
                        elif gen['local'][j] == 0:
                            unique_biomass_data['gen_id_biomass'].append(10000 + Pd_bio_gen.Source_ID[i])
                        # print(unique_biomass_data)


        for i in range(len(unique_biomass_data['gen_id_biomass'])):
            # print(unique_list2)
            for j in range(len(unique_list2[str(resource_id)])): #to make sure no repeated biomass
                if unique_list2[str(resource_id)][j] > 0 and str(unique_biomass_data['biomass_id'][i]) == (str(80000 + unique_list2['biomass_index'][j] )+ str(resource_id) ):  #show biomass node if it exists
                    # print("gg",str((unique_biomass_data['biomass_id'][i])))
                    biomass_id_str = str(unique_biomass_data['biomass_id'][i])
                    biomass_location = unique_biomass_data['island'][i]
                    text = "M" + biomass_id_str
                    if biomass_id_str not in biomass_exists:
                        if biomass_location == 'North':
                            # print("North")
                            G.add_node(text, names=text, type='raw_material',
                                    flow_rate_lower_bound=0,
                                    flow_rate_upper_bound=unique_list2[str(resource_id)][j]*biomass_properties["energy_content_gj_per_ton"][resource_id],#ton ->GJ,  # convert to GJ
                                    price=biomass_properties["raw_material_cost_north_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id],
                                    units=mat_units_GJ)
                            biomass_exists.add(biomass_id_str)  # store ID string consistently                            
                            # print("gen_bio",biomass_properties["raw_material_cost_north_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id])
                        elif biomass_location == "South":
                            # print("South")
                            G.add_node(text, names=text, type='raw_material',
                                    flow_rate_lower_bound=0,
                                    flow_rate_upper_bound=unique_list2[str(resource_id)][j]*biomass_properties["energy_content_gj_per_ton"][resource_id],#ton ->GJ,  # convert to GJ
                                    price=biomass_properties["raw_material_cost_south_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id],
                                    units=mat_units_GJ)
                            biomass_exists.add(biomass_id_str)  # store ID string consistently
                            # print("gen_bio",biomass_properties["raw_material_cost_south_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id])
                    else:
                        pass
                else:
                    pass

            #connect biomass with gen
            #print(unique_biomass_data['gen_id_biomass'][i])
            G.add_node("O" + str(unique_biomass_data['biomass_id'][i]) + str(unique_biomass_data['gen_id_biomass'][i]), names= "O" + str(unique_biomass_data['biomass_id'][i]) + str(unique_biomass_data['gen_id_biomass'][i]), fix_cost=0, proportional_cost=unique_biomass_data['biomass_distance'][
                                                i] * biomass_properties['biomass_distance_cost_constant'][resource_id])
            G.add_edge("M" + str(unique_biomass_data['biomass_id'][i]),"O" + str(unique_biomass_data['biomass_id'][i]) + str(unique_biomass_data['gen_id_biomass'][i]),weight=3.6)
            # print("M" + str(unique_biomass_data['biomass_id'][i]),">>","O" + str(unique_biomass_data['biomass_id'][i]) + str(unique_biomass_data['gen_id_biomass'][i]))
            item = "M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(170 + resource_id)
            if item not in generation_exists:
                generation_exists.add(item)
                G.add_node(item, names=item, type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,
                            units=mat_units_MWh)

                if time_resolution == 'monthly':
                    G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i])+str(280),
                            names="O" + str(unique_biomass_data['gen_id_biomass'][i])+str(280),capacity_lower_bound=0,capacity_upper_bound=2000, fix_cost =0,proportional_cost = methanol_plant_op_cost*time_period)  # methanol plant
                elif time_resolution == 'hourly':
                    G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i])+str(280),
                            names="O" + str(unique_biomass_data['gen_id_biomass'][i])+str(280), capacity_lower_bound=0,capacity_upper_bound=2000/730*4, fix_cost =0,proportional_cost = methanol_plant_op_cost*time_period)  # methanol plant
                    
            G.add_edge("O" + str(unique_biomass_data['biomass_id'][i]) + str(unique_biomass_data['gen_id_biomass'][i]),
                        item, weight=1)

            for l in range(len(time_multiplier)):
                G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(180+resource_id) + str(
                                    time_multiplier[l]),names="O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(180+resource_id) + str(
                                    time_multiplier[l]), **proportional_cost_op) #time slicing
                G.add_node("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(190+resource_id) + str(time_multiplier[l]),
                                names="M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(190+resource_id) + str(time_multiplier[l]),
                                type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,units=mat_units_MWh)
                G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(200 + resource_id) + str(
                        time_multiplier[l]),names="O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(200 + resource_id) + str(
                        time_multiplier[l]), **proportional_cost_op)

                text1 = "M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(210) + str(time_multiplier[l]) #improvements

                if text1 not in text1_exists: #joining carbon compounds
                    text1_exists.add(text1)
                    G.add_node(text1,names=text1,type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,units=mat_units_MWh)
                else:
                    pass

                G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(170+resource_id),"O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(180+resource_id) + str(
                                    time_multiplier[l]),weight=1)
                G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(180+resource_id) + str(
                                time_multiplier[l]),"M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(190+resource_id) + str(
                                    time_multiplier[l]), weight=1)
                G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(190 + resource_id) + str(
                        time_multiplier[l]),
                            "O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(200 + resource_id) + str(
                        time_multiplier[l]),
                            weight=1)
                G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(200 + resource_id) + str(
                        time_multiplier[l]),text1,
                            weight=1)
                G.add_edge(text1,"O" + str(unique_biomass_data['gen_id_biomass'][i]) +str(250) +str(time_multiplier[l]), weight=3.96) #biomass basis for 1 tonne methanol

                G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(230) + str(time_multiplier[l]),
                        names="O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(230) + str(time_multiplier[l]),
                        **proportional_cost_op)
                #print(str(unique_biomass_data['gen_id_biomass'][i]) + str(230) + str(time_multiplier[l]))
                G.add_node("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(240) + str(time_multiplier[l]),
                        names="M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(240) + str(time_multiplier[l]),
                        type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_MWh)
                #print("M" + str(9500 + unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l]))
                G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(250) + str(time_multiplier[l]),
                        names="O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(250) + str(time_multiplier[l]),
                        **proportional_cost_op)
                G.add_node("M" + str(unique_biomass_data['gen_id_biomass'][i])+ str(260) + str(time_multiplier[l]),
                        names="M" + str(unique_biomass_data['gen_id_biomass'][i])+ str(260) + str(time_multiplier[l]),type='product',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e5, price=methanol_selling_price, units=mat_units_ton)
                #wip quesition to ask botond
                G.add_node("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(270)+ str(time_multiplier[l]),
                        names="M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(270)+ str(time_multiplier[l]),type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_MWh)
                G.add_node("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(290)+ str(time_multiplier[l]),
                        names="M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(290)+ str(time_multiplier[l]),type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_MWh)
                # connections but make sure you know to add which station if biomass connects to utility or local generations
                # print(text1)
                if text1[1] == "2":

                    G.add_node("O" + str(unique_biomass_data['gen_id_biomass'][i]-300) + str(time_multiplier[l]),
                            names="O" + str(unique_biomass_data['gen_id_biomass'][i]-300) + str(time_multiplier[l]), capacity_lower_bound=0,
                            capacity_upper_bound=1e6, **proportional_cost_op)
                    G.add_node("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l]),
                            names="M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l]), type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_MWh)
                    G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i]-600) + str(time_multiplier[l]),
                            "O" + str(unique_biomass_data['gen_id_biomass'][i]-300) + str(time_multiplier[l]), weight=1)
                    G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i]-300) + str(time_multiplier[l]),
                            "M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l]), weight=1)

                    G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l]),
                            "O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(230) + str(time_multiplier[l]),
                            weight=1)
                elif text1[1] == "1":
                    if G.has_node("M" + str(7000 + unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l])) is True:
                        G.add_edge("M" + str(7000 + unique_biomass_data['gen_id_biomass'][i]) + str(time_multiplier[l]),
                            "O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(230) + str(time_multiplier[l]),
                            weight=1)
                
                G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(230)+ str(time_multiplier[l]),
                        "M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(240)+ str(time_multiplier[l]), weight=1)

                G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(240)+ str(time_multiplier[l]),
                        "O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(250)+ str(time_multiplier[l]), weight=4.36) #electricity input per ton methanol basis
                G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(250)+ str(time_multiplier[l]),
                        "M" + str(unique_biomass_data['gen_id_biomass'][i])  + str(260)+ str(time_multiplier[l]), weight=1)
                G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i]) + str(250)+ str(time_multiplier[l]),
                        "M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(270)+ str(time_multiplier[l]), weight=1)
                G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i]) + str(270)+ str(time_multiplier[l]),
                        "O" + str(unique_biomass_data['gen_id_biomass'][i]) +str(280), weight=1e-7)
                G.add_edge("O" + str(unique_biomass_data['gen_id_biomass'][i])+str(280),
                        "M" + str(unique_biomass_data['gen_id_biomass'][i]) +str(290)+ str(time_multiplier[l]), weight=1)
                G.add_edge("M" + str(unique_biomass_data['gen_id_biomass'][i])+str(290) + str(time_multiplier[l]),
                        "O" + str(unique_biomass_data['gen_id_biomass'][i])+str(250) + str(time_multiplier[l]), weight=1)

        # print('bgi biomass_exists',biomass_exists)
        # print('generation_exists',generation_exists)
        # print(unique_biomass_data['gen_id_biomass'])
        return unique_biomass_data,biomass_exists,generation_exists


    def process_biomass_gen_fuel(a=unique_list2, b=G, units=mat_units_GJ, d=mat_units_GJ, resource_id=None,bio_existing=None, gen_existing=None):
        unique_biomass_data = process_biomass_data(unique_list2,resource_id=resource_id)
        generation_exists = gen_existing if gen_existing is not None else set() #prevent over defining the generation side (destination)
        biomass_exists = bio_existing if bio_existing is not None else set()  #prevent over define biomass resource #need to wip update
        text1_exists = set()
        
        for i in range(len(Pd_bio_gen.Biomass_ID)):
            if (10000 + Pd_bio_gen.Source_ID[i] in gen["index"]) and (Pd_bio_gen.North_South[i] != island) and (
                    Pd_bio_gen.Distance[i] <= distance_constraint) and (
                    (str(80000 + Pd_bio_gen.Biomass_ID[i])+ str(resource_id) ) in unique_biomass_data['node']):
                
                for j in range(len(gen['index'])):
                    if (10000 + Pd_bio_gen.Source_ID[i]) == gen['index'][j]:
                        if gen['type'][j] in {"Bioenergy","Thermal","Cogeneration" }: 
                            unique_biomass_data['gen_id_fuel'].append(10000 + Pd_bio_gen.Source_ID[i])
                            unique_biomass_data['biomass_distance'].append(Pd_bio_gen.Distance[i])
                            unique_biomass_data['biomass_id'].append(str(80000 + Pd_bio_gen.Biomass_ID[i])+ str(resource_id) )    
                            unique_biomass_data['island'].append(Pd_bio_gen.North_South[i])      
                                  
        for i in range(len(unique_biomass_data['gen_id_fuel'])):
            for j in range(len(unique_list2[str(resource_id)])): #to make sure no repeated biomass
                if unique_list2[str(resource_id)][j] > 0 and str(unique_biomass_data['biomass_id'][i]) == str(str(80000 + unique_list2['biomass_index'][j]) + str(resource_id)):  #show biomass node if it exists
                    # print( str(unique_biomass_data['biomass_id'][i]))
                    biomass_id_str = str(unique_biomass_data['biomass_id'][i])
                    biomass_location = unique_biomass_data['island'][i]
                    text = "M" + biomass_id_str

                    if biomass_id_str not in biomass_exists:
                        if biomass_location == 'North':
                            biomass_resource_price = biomass_properties["raw_material_cost_north_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id]
                            G.add_node(text,
                                names=text,type='raw_material',flow_rate_lower_bound=0,flow_rate_upper_bound=unique_list2[str(resource_id)][j]*biomass_properties["energy_content_gj_per_ton"][resource_id],#ton ->GJ,
                                price=biomass_resource_price,units=mat_units_GJ)
                            biomass_exists.add(biomass_id_str)
                            # print("gen_bio2",biomass_properties["raw_material_cost_north_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id])

                        elif biomass_location == 'South':
                            biomass_resource_price = biomass_properties["raw_material_cost_south_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id]
                            G.add_node(text,
                                names=text,type='raw_material',flow_rate_lower_bound=0,flow_rate_upper_bound=unique_list2[str(resource_id)][j]*biomass_properties["energy_content_gj_per_ton"][resource_id],#ton ->GJ,
                                price=biomass_resource_price,units=mat_units_GJ) #nzd/GJ
                            biomass_exists.add(biomass_id_str)
                            # print("gen_bio2",biomass_properties["raw_material_cost_south_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id])
                            
                        # print(text)
                        # print(biomass_exists)

                    else:
                        pass
                else:
                    pass

            #connect biomass with gen_fuel
            G.add_node("O" + str(unique_biomass_data['biomass_id'][i]) +str(300)+ str(unique_biomass_data['gen_id_fuel'][i]), names= "O"+ str(unique_biomass_data['biomass_id'][i])  +str(300)
                       + str(unique_biomass_data['gen_id_fuel'][i]), fix_cost=0, proportional_cost=unique_biomass_data['biomass_distance'][i] * biomass_properties['biomass_distance_cost_constant'][resource_id])
            G.add_edge("M" + str(unique_biomass_data['biomass_id'][i]),"O" + str(unique_biomass_data['biomass_id'][i]) +str(300)+ str(unique_biomass_data['gen_id_fuel'][i]),weight=3.6)
            G.add_edge("O" + str(unique_biomass_data['biomass_id'][i]) +str(300)+ str(unique_biomass_data['gen_id_fuel'][i]),"M"+ str(unique_biomass_data['gen_id_fuel'][i]+1000),weight=1)

            #new fix 5/2/2026 add product node for bio as thermal use 0.99 for prod to be prevent rounding errors
            biomass_resource_price_2 = G.nodes["M" + str(unique_biomass_data['biomass_id'][i])]["price"]
            G.add_node("M" + str(unique_biomass_data['biomass_id'][i]) +str(400)+ str(unique_biomass_data['gen_id_fuel'][i]),names="M" + str(unique_biomass_data['biomass_id'][i]) +str(400)+ str(unique_biomass_data['gen_id_fuel'][i]),type='product', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0.999*(unique_biomass_data['biomass_distance'][i] * biomass_properties['biomass_distance_cost_constant'][resource_id]+biomass_resource_price_2*3.6), units=mat_units_MWh)
            # print("O" + str(unique_biomass_data['biomass_id'][i]))
            G.add_edge("O" + str(unique_biomass_data['biomass_id'][i]) +str(300)+ str(unique_biomass_data['gen_id_fuel'][i]),"M" + str(unique_biomass_data['biomass_id'][i]) +str(400)+ str(unique_biomass_data['gen_id_fuel'][i]),weight=1)
            
            if G.has_node("M"+ str(unique_biomass_data['gen_id_fuel'][i]+6000)) is True:

                G.remove_node("M"+ str(unique_biomass_data['gen_id_fuel'][i]+6000))
                G.remove_node("O"+ str(unique_biomass_data['gen_id_fuel'][i]))
            
            # print('bgf biomass_exists',biomass_exists)
            # print('generation_exists',generation_exists)            
            # print(unique_biomass_data['gen_id_fuel'])
            return unique_biomass_data,biomass_exists,generation_exists



    def process_biomass_factory(unique_list2,biomass_existing=None, b=G, units=mat_units_GJ, resource_id=None,biomass_props=biomass_properties):
        unique_biomass_data = process_biomass_data(unique_list2,resource_id=resource_id)
        # print(unique_biomass_data)
        aa = set()
        cc = set()

        if biomass_existing is None:
            biomass_existing = set()
        for i in range(len(Pd_bio_fac.Biomass_ID)):
            biomass_id = str(80000 + Pd_bio_fac.Biomass_ID[i])+ str(resource_id) 
            biomass_id_str = str(biomass_id)
            biomass_location = Pd_bio_fac.North_South[i]

            if (3000 + Pd_bio_fac.Source_ID[i] in dem_biomass_accessible_index) and (Pd_bio_fac.North_South[i] != island) and (
                    Pd_bio_fac.Distance[i] <= distance_constraint) and (biomass_id in unique_biomass_data['node']):
                # print(3000 + Pd_bio_fac.Source_ID[i],"___",dem_biomass_accessible_index)
                unique_biomass_data['fac_id_biomass'].append(3000 + Pd_bio_fac.Source_ID[i])
                unique_biomass_data['biomass_distance'].append(Pd_bio_fac.Distance[i])
                unique_biomass_data['biomass_id'].append(biomass_id_str)
                unique_biomass_data['island'].append(Pd_bio_fac.North_South[i])

                for j in range(len(unique_list2[str(resource_id)])):
                    expected_id = str(80000 + unique_list2['biomass_index'][j])+ str(resource_id) 
                    
                    if biomass_id == expected_id and unique_list2[str(resource_id)][j] > 0:
                        # print(biomass_id)
                        text = "M" + biomass_id_str
                         
                        if biomass_id_str not in biomass_existing:
                            if biomass_location == "North":
                                G.add_node(text,names=text,type='raw_material',
                                flow_rate_lower_bound=0,flow_rate_upper_bound=unique_list2[str(resource_id)][j]*biomass_properties["energy_content_gj_per_ton"][resource_id],#ton ->GJ,
                                price=biomass_properties["raw_material_cost_north_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id],units=mat_units_GJ)
                                biomass_existing.add(biomass_id_str)
                                # print("gen_FAC",biomass_properties["raw_material_cost_north_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id])
                                
                            if biomass_location == "South":
                                G.add_node(text,names=text,type='raw_material',
                                flow_rate_lower_bound=0,flow_rate_upper_bound=unique_list2[str(resource_id)][j]*biomass_properties["energy_content_gj_per_ton"][resource_id],#ton ->GJ,
                                price=biomass_properties["raw_material_cost_south_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id],units=mat_units_GJ)
                                biomass_existing.add(biomass_id_str)                                
                                # print("gen_FAC",biomass_properties["raw_material_cost_south_nzd_per_ton"][resource_id]/biomass_properties["energy_content_gj_per_ton"][resource_id])
                                
                            # print("a", unique_list2[str(call_count)][j])
                            # print("b", text)
                            # print("c", biomass_id_str)

                        else:
                            pass
                    else:
                        pass

                G.add_node("O" + str(unique_biomass_data['biomass_id'][-1]) + str(
                    unique_biomass_data['fac_id_biomass'][-1]),
                        names="O" + str(unique_biomass_data['biomass_id'][-1]) + str(
                            unique_biomass_data['fac_id_biomass'][-1]),
                        fix_cost=0,
                        proportional_cost=unique_biomass_data['biomass_distance'][
                                                -1] * biomass_properties['biomass_distance_cost_constant'][resource_id] *1000 )  # o600003000 for loop plants 1000 here is because MWh to GWh


                item2 = "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(150 + biomass_properties["boiler"][resource_id])
                if item2 not in cc:
                    # print(item2)
                    cc.add(item2)
                    G.add_node("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(150 + biomass_properties["boiler"][resource_id]),
                            names="M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(150 + biomass_properties["boiler"][resource_id]),
                            type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,
                            units=mat_units_GWh)  # connect o60000 3000     m3000 150
                else:
                    pass

                G.add_edge("M" + str(unique_biomass_data['biomass_id'][-1]),
                        "O" + str(unique_biomass_data['biomass_id'][-1]) + str(unique_biomass_data['fac_id_biomass'][-1]),
                        weight=3.6*1000)  # m80000 -- o60000
                G.add_edge("O" + str(unique_biomass_data['biomass_id'][-1]) + str(unique_biomass_data['fac_id_biomass'][-1]),
                        "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(150 + biomass_properties["boiler"][resource_id]), weight=1)
                item = "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(150 + biomass_properties["boiler"][resource_id])
                if item not in aa:
                    aa.add(item)
                    if time_resolution == 'hourly':                        
                        G.add_node("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]),
                                names="O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]),
                                fix_cost=1,proportional_cost=biomass_heat_proportional_cost[biomass_properties['boiler'][resource_id]]/8760*time_period)  # capital cost
                    elif time_resolution == 'monthly':
                        G.add_node("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]),
                                names="O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]),
                                fix_cost=1,proportional_cost= biomass_heat_proportional_cost[biomass_properties['boiler'][resource_id]]/12*time_period)  # capital cost                    
                    
                for l in range(len(time_multiplier)):
                        G.add_node("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                            time_multiplier[l]),
                                names="O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                    time_multiplier[l]), **proportional_cost_op)  #
                        G.add_node("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(1) + str(
                                time_multiplier[l]),
                            names="M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                1) + str(time_multiplier[l]), type='intermediate', flow_rate_lower_bound=0,
                            flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
                        G.add_node("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(2) + str(
                                time_multiplier[l]),
                            names="M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                2) + str(time_multiplier[l]), type='intermediate', flow_rate_lower_bound=0,
                            flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
                        G.add_edge("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(150 + biomass_properties["boiler"][resource_id]),
                                "O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                    time_multiplier[l]), weight=1)
                        G.add_edge("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                            time_multiplier[l]),
                                "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                    1) + str(time_multiplier[l]), weight=1)
                        G.add_edge("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(1) + str(
                                time_multiplier[l]),
                            "O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]), weight=1e-7)
                        
                        if time_resolution == 'monthly':
                            G.add_edge("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]),
                                    "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                        2) + str(time_multiplier[l]), weight=0.8*730/1000)
                        elif time_resolution == 'hourly':
                            G.add_edge("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(142 + biomass_properties["boiler"][resource_id]),
                                    "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                        2) + str(time_multiplier[l]), weight=0.8/1000*4)                        
                        G.add_edge(
                            "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(2) + str(
                                time_multiplier[l]),
                            "O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                                time_multiplier[l]), weight=1)
                        
                        G.add_node("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(144) + str(time_multiplier[l]), names="M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(144) + str(time_multiplier[l]),type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,
                            units=mat_units_GWh)
                        
                        G.add_edge("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(140 + biomass_properties["boiler"][resource_id]) + str(
                            time_multiplier[l]), "M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(144) + str(
                            time_multiplier[l]), weight=biomass_heat_efficiency[biomass_properties['boiler'][resource_id]])
                cascade_heat_eff_loss = [1,0.99,0.98,0.97,0.96] #cascade heat efficiency loss    

                # print(demand_product_nodes)
                for m in range(0,5):
                    node_name = f"M{unique_biomass_data['fac_id_biomass'][-1]}{124 + m}"
                    if node_name in demand_product_nodes:
                        # print("pass")
                        for l in range(len(time_multiplier)):
                            G.add_node("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(145 + m) + str(time_multiplier[l]),names="O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(145 + m) + str(time_multiplier[l]),**proportional_cost_op)        
                            G.add_edge("M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(144) + str(time_multiplier[l]),"O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(145 + m) + str(time_multiplier[l]),weight=1)
                            G.add_edge("O" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(145 + m) + str(time_multiplier[l]),"M" + str(unique_biomass_data['fac_id_biomass'][-1]) + str(124 + m) + str(time_multiplier[l]),weight=cascade_heat_eff_loss[m])
                            
        # print('bfac biomass_exists',biomass_existing)
                                
        return unique_biomass_data,biomass_existing

    

    def coal_boilers_retrofitting(dem_coal_boilers):
        coal_efficiency = 0.75
        coal_boiler_retrofit_cost_nzd_per_MW =  54926.2709  #Capacity op cost (Adjusted) $/MW yearly
        for i in range(len(dem_coal_boilers["index"])):
            if G.has_node("M"+str(dem_coal_boilers["index"][i])+str(150)) == True:
                if time_resolution == 'monthly':
                    G.add_node("O"+str(dem_coal_boilers["index"][i])+str(162), names="O"+str(dem_coal_boilers["index"][i])+str(162),capacity_lower_bound=0,
                            capacity_upper_bound=dem_coal_boilers["coal_capacity"][i]*dem_coal_boilers["dem_hours"][i]/8760*8760*time_period/12/1000,  fix_cost=1, proportional_cost=coal_boiler_retrofit_cost_nzd_per_MW/12*time_period)
                elif time_resolution == 'hourly': #wip
                    G.add_node("O"+str(dem_coal_boilers["index"][i])+str(162), names="O"+str(dem_coal_boilers["index"][i])+str(162),capacity_lower_bound=0,
                            capacity_upper_bound=dem_coal_boilers["coal_capacity"][i],  fix_cost=1, proportional_cost=coal_boiler_retrofit_cost_nzd_per_MW/8760*time_period)
                
                for l in range(len(time_multiplier)):
                    G.add_node("O"+str(dem_coal_boilers["index"][i])+str(160)+str(time_multiplier[l]), names="O"+str(dem_coal_boilers["index"][i])+str(160)+str(time_multiplier[l]), **proportional_cost_op)
                    G.add_node("M"+str(dem_coal_boilers["index"][i])+str(160)+str(1)+str(time_multiplier[l]), names="M"+str(dem_coal_boilers["index"][i])+str(160)+str(1)+str(time_multiplier[l]), type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
                    G.add_node("M"+str(dem_coal_boilers["index"][i])+str(160)+str(2)+str(time_multiplier[l]), names="M"+str(dem_coal_boilers["index"][i])+str(160)+str(2)+str(time_multiplier[l]), type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
                    
                    G.add_edge("M"+str(dem_coal_boilers["index"][i])+str(150), "O"+str(dem_coal_boilers["index"][i])+str(160)+str(time_multiplier[l]), weight=1)
                    G.add_edge("O"+str(dem_coal_boilers["index"][i])+str(160)+str(time_multiplier[l]), "M"+str(dem_coal_boilers["index"][i])+str(160)+str(1)+str(time_multiplier[l]),weight=1)
                    G.add_edge("M"+str(dem_coal_boilers["index"][i])+str(160)+str(1)+str(time_multiplier[l]), "O"+str(dem_coal_boilers["index"][i])+str(162), weight=1e-7)
                    if time_resolution == 'monthly':
                        G.add_edge("O"+str(dem_coal_boilers["index"][i])+str(162), "M"+str(dem_coal_boilers["index"][i])+str(160)+str(2)+str(time_multiplier[l]), weight=0.8*730/1000)
                    elif time_resolution == 'hourly': #4hourly
                        G.add_edge("O"+str(dem_coal_boilers["index"][i])+str(162), "M"+str(dem_coal_boilers["index"][i])+str(160)+str(2)+str(time_multiplier[l]), weight=0.8/1000*4)
                    G.add_edge("M"+str(dem_coal_boilers["index"][i])+str(160)+str(2)+str(time_multiplier[l]), "O"+str(dem_coal_boilers["index"][i])+str(160)+str(time_multiplier[l]), weight=1)
                    
                    G.add_edge("O"+str(dem_coal_boilers["index"][i])+str(160)+str(time_multiplier[l]), "M"+str(dem_coal_boilers["index"][i])+str(144)+str(time_multiplier[l]), weight=coal_efficiency)
            elif G.has_node("M"+str(dem_coal_boilers["index"][i])+str(150)) == False:
                pass
            
    def process_all_biomass(unique_list, biomass_props):
        results = {}
        bio_exi, gen_exi = None, None
        print(f"Done process_dem_gen_gxp at time:{time.time() - start_time:.2f} seconds")

        for i in range(len(biomass_props["resource_id"])):
            resource = biomass_props["resource_type"][i]
            
            green_out, bio_exi, gen_exi = process_biomass_green_industry(unique_list, resource_id=biomass_props["resource_id"][i], gen_existing=gen_exi,biomass_props=biomass_props)
            print(f"Done process_biomass_green_industry {i} at time:{time.time() - start_time:.2f} seconds")
            gen_out = process_biomass_gen_fuel(unique_list, resource_id=biomass_props["resource_id"][i], bio_existing=bio_exi, gen_existing=gen_exi)
            print(f"Done process_biomass_gen_fuel{i} at time:{time.time() - start_time:.2f} seconds")
            factory_out, r_out = process_biomass_factory(unique_list, bio_exi, resource_id=biomass_props["resource_id"][i], biomass_props=biomass_props)
            print(f"Done process_biomass_factory{i} at time:{time.time() - start_time:.2f} seconds")
            results[resource] = {"green_industry": green_out,"gen_fuel": gen_out,"factory": factory_out,"r": r_out,
                "bio_exi": bio_exi, "gen_exi": gen_exi,}

        return results

    if time_resolution == "monthly":
        Pd_keys = ['Index', 'ObjectID', "Available storage GWh", "North_South"]
        Pd_hydro_st = python_dict_dot_notation(Pd_keys, Pd_hydro)

        hydro = {"index":[],"gen_id":[],"storage":[], "island":[],}
        for i in range(len(Pd_hydro_st.Index)):
            if Pd_hydro_st.North_South[i] != island and Pd_hydro_st.ObjectID[i] in gen["index"]:
                hydro["index"].append(Pd_hydro_st.Index[i])
                hydro["gen_id"].append(Pd_hydro_st.ObjectID[i])
                hydro["storage"].append(Pd_hydro_st["Available storage GWh"][i])
        for i in range(len(hydro["index"])):
            G.add_node("O"+str(16000+hydro["gen_id"][i])+str(3), names = "O"+str(16000+hydro["gen_id"][i])+str(3), capacity_lower_bound=0,
                        capacity_upper_bound=hydro["storage"][i], fix_cost=0, proportional_cost=0)   
            for l in range(len(time_multiplier)):
                next_l = (l + 1) % len(time_multiplier)  # wraps last to first

                G.add_node("O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]), names = "O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]), capacity_lower_bound=0,
                        capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                G.add_node("M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]), names= "M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]),flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh,type='intermediate')
                G.add_node("M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]), names= "M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]),flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh,type='intermediate')

                G.add_edge("M"+str(17000+hydro["gen_id"][i])+str(time_multiplier[l]),"O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]),weight=1)
                G.add_edge("O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]),"M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]),weight=1)
                G.add_edge("M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]),"O"+str(16000+hydro["gen_id"][i])+str(3),weight=1e-7)
                G.add_edge("O"+str(16000+hydro["gen_id"][i])+str(3),"M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]),weight=1)
                G.add_edge("M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]),"O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]),weight=1)

                # cyclic link — last period loops to first
                G.add_edge("O" + str(16000 + hydro["gen_id"][i]) + str(1) + str(time_multiplier[l]),"M" + str(17000 + hydro["gen_id"][i]) + str(time_multiplier[next_l]),weight=0.99)


    #heating water 
    if time_resolution == "hourly":
        #hot water storage from electric heating
        for i in range(len(dem["index"])):          
            G.add_node("O"+str(dem["index"][i])+str(179), names="O"+str(dem["index"][i])+str(179)
                        ,capacity_lower_bound=0, capacity_upper_bound=1e6, proportional_cost=29059.5646,fix_cost=0, units=mat_units_GWh)  
            G.add_node("O"+str(dem["index"][i])+str(180), names="O"+str(dem["index"][i])+str(180)
                        ,capacity_lower_bound=0, capacity_upper_bound=1e6, proportional_cost=29059.5646,fix_cost=0, units=mat_units_GWh)          
            for l in range(len(time_multiplier)):
                for a in range(4): #electric to heat low temp
                    if G.has_node("M"+str(dem["index"][i])+str(104+a)+str(time_multiplier[l])):
                        G.add_node("O"+str(dem["index"][i])+str(165+a)+str(time_multiplier[l]), names="O"+str(dem["index"][i])+str(165+a)+str(time_multiplier[l])
                        ,capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh )
                        G.add_edge("M"+str(dem["index"][i])+str(104+a)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(165+a)+str(time_multiplier[l]),weight=1)
                        G.add_edge("O"+str(dem["index"][i])+str(165+a)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(175)+str(time_multiplier[l]),weight=0.999)    
                
            
                for c in range(3):#electric to heat low-mid temp
                    if G.has_node("M"+str(dem["index"][i])+str(105+c)+str(time_multiplier[l])):
                        G.add_node("O"+str(dem["index"][i])+str(170+c)+str(time_multiplier[l]), names="O"+str(dem["index"][i])+str(170+c)+str(time_multiplier[l])
                            ,capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh)
                        G.add_edge("M"+str(dem["index"][i])+str(105+c)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(170+c)+str(time_multiplier[l]),weight=1)
                        G.add_edge("O"+str(dem["index"][i])+str(170+c)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(176)+str(time_multiplier[l]),weight=0.999)    
                        
                for b in range(2):
                    G.add_node("M"+str(dem["index"][i])+str(175+b)+str(time_multiplier[l]), names="M"+str(dem["index"][i])+str(175+b)+str(time_multiplier[l]),
                                        type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_GWh)
                    G.add_node("O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[l]), names="O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[l]),
                                        capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh)
                    G.add_node("M"+str(dem["index"][i])+str(179+b)+str(1)+str(time_multiplier[l]), names="M"+str(dem["index"][i])+str(179+b)+str(1)+str(time_multiplier[l]),
                                        type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_GWh)            
                    G.add_node("M"+str(dem["index"][i])+str(179+b)+str(2)+str(time_multiplier[l]), names="M"+str(dem["index"][i])+str(179+b)+str(2)+str(time_multiplier[l]),
                                        type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_GWh)     

                    G.add_edge("M"+str(dem["index"][i])+str(175+b)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[l]),weight=1)
                    G.add_edge("O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(179+b)+str(1)+str(time_multiplier[l]),weight=1)
                    G.add_edge("M"+str(dem["index"][i])+str(179+b)+str(1)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(179+b),weight=1e-7)
                    G.add_edge("O"+str(dem["index"][i])+str(179+b),"M"+str(dem["index"][i])+str(179+b)+str(2)+str(time_multiplier[l]),weight=1)
                    G.add_edge("M"+str(dem["index"][i])+str(179+b)+str(2)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[l]),weight=1)
                    

                    G.add_node("M"+str(dem["index"][i])+str(181+b)+str(time_multiplier[l]), names="M"+str(dem["index"][i])+str(181+b)+str(time_multiplier[l]),
                                        type='intermediate',flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0, units=mat_units_GWh)
                    G.add_node("O"+str(dem["index"][i])+str(183+b)+str(time_multiplier[l]),names="O"+str(dem["index"][i])+str(183+b)+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh)   

                    G.add_edge("O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(181+b)+str(time_multiplier[l]),weight=1)
                    G.add_edge("M"+str(dem["index"][i])+str(181+b)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(183+b)+str(time_multiplier[l]),weight=1)
                    G.add_edge("O"+str(dem["index"][i])+str(183+b)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(124+b)+str(time_multiplier[l]),weight=1)
                
                G.add_node("O"+str(dem["index"][i])+str(185)+str(time_multiplier[l]),names="O"+str(dem["index"][i])+str(185)+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh)   
                G.add_edge("M"+str(dem["index"][i])+str(182)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(185)+str(time_multiplier[l]),weight=1)
                G.add_edge("O"+str(dem["index"][i])+str(185)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(124)+str(time_multiplier[l]),weight=1)
                
            for l in range(len(time_multiplier)):
                next_l = l + 1
                if next_l == len(time_multiplier):
                    next_l = 0
                for b in range(2):
                    G.add_edge("M"+str(dem["index"][i])+str(181+b)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(177+b)+str(time_multiplier[next_l]),weight=0.993)
            
                
    #working for biomass
        for i in range(len(dem["index"])):          
            for l in range(len(time_multiplier)):
                if G.has_node("M"+str(dem["index"][i])+str(144)+str(time_multiplier[l])):
                    for a in range(5):
                        G.add_node("O"+str(dem["index"][i])+str(190+a)+str(time_multiplier[l]),names="O"+str(dem["index"][i])+str(190+a)+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh)   
                        G.add_edge("M"+str(dem["index"][i])+str(144)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(190+a)+str(time_multiplier[l]),weight=1)
                        G.add_edge("O"+str(dem["index"][i])+str(190+a)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(175)+str(time_multiplier[l]),weight=0.999)
                
                        G.add_node("O"+str(dem["index"][i])+str(195+a)+str(time_multiplier[l]),names="O"+str(dem["index"][i])+str(195+a)+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e6, **proportional_cost_op, units=mat_units_GWh)   
                        G.add_edge("M"+str(dem["index"][i])+str(144)+str(time_multiplier[l]),"O"+str(dem["index"][i])+str(195+a)+str(time_multiplier[l]),weight=1)
                        G.add_edge("O"+str(dem["index"][i])+str(195+a)+str(time_multiplier[l]),"M"+str(dem["index"][i])+str(176)+str(time_multiplier[l]),weight=0.999)




    results = process_all_biomass(unique_list2, biomass_properties)
    coal_boilers_retrofitting(dem_coal_boilers)   
    

    setupped_time = time.time() - time_step
    elapsed_time = time.time() - start_time
    time_step = time.time()
    time_setup +=setupped_time

    
    print(f"Time elapsed done for setting up simulation {num+1}: {elapsed_time:.2f} seconds\n")
    ## setup solver
    # print(ME)
    
    framework = "Pyomo"  # choose between 'Pyomo' and 'Pgraph'
    P = Pgraph(problem_network=G, mutual_exclusion=ME, solver=solve_type, max_sol=num_sol)

    if framework == "Pgraph":
        #Monte Carlo things

        P.run()
        gop_reference.append(P.goplist)
        goo_reference.append(P.goolist)
        print((P.goolist))

        main_excel_output(P, solve_type,statement='no',file_path="output_results/output.xlsx")

    elif framework == "Pyomo":
        sys.path.append(r"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-convertor")
        from pgraph_convertor_v5 import run_file
        P.create_solver_input()
        file_path = r"C:\Users\dc278\.conda\envs\pyomo_pgraph_converter\Lib\Pgraph/solver/input.in"
        model = run_file(file_path, output_file_path = r"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\pyomo_results_hourly.xlsx")
        
    solved_time = time.time() - time_step
    elapsed_time = time.time() - start_time
    time_step = time.time()
    time_solving += solved_time


    csved_time = time.time() - time_step
    elapsed_time = time.time() - start_time
    time_step = time.time()
    time_csv += csved_time
    
    
    print(f"Time elapsed done for simulation {num+1}: {elapsed_time:.2f} seconds")

#Monte Carlo things
# Analyze datasets
if num_simulations != 1:
    print(goo_reference)
    for i, goplist in enumerate(goo_reference):
        analyzer.analyze(goplist)

    # Show the final plot after processing all datasets
    analyzer.plot_results()

    # Access intermediate results if needed

    print("\nAll Simulation Data:", analyzer.all_simulation_data)
    print("Normalized Data:", analyzer.normalised_data)
    print("Mean Values:", analyzer.mean_values)



    elapsed_time = time.time() - start_time
    print(f"Time elapsed done: {elapsed_time:.2f} seconds\n")
    print(f'All {num+1} Simulation Completed!')
    
    print('time_setup:',time_setup)
    print('time_solving:',time_solving)
    print('time_csv:',time_csv)

# print(goo_reference)
if elapsed_time <= 120:
    string = P.to_studio(path='./', file_name="studio_file52d.pgsx", verbose=False)



with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/gen.pkl', 'wb') as f:
    pickle.dump(gen, f)

with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/dem.pkl', 'wb') as f:
    pickle.dump(dem, f)

with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/gxp_connections_dict.pkl', 'wb') as f:
    pickle.dump(gxp_connections_dict, f)

with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/gxp.pkl', 'wb') as f:
    pickle.dump(gxp, f)
    
with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/island.pkl', 'wb') as f:
    pickle.dump(island, f)
    
with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/time_multiplier.pkl', 'wb') as f:
    pickle.dump(time_multiplier, f)    

with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/time_resolution.pkl', 'wb') as f:
    pickle.dump(time_resolution, f)   
    
with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/time_period.pkl', 'wb') as f:
    pickle.dump(time_period, f)   
        
with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/main_path.pkl', 'wb') as f:
    pickle.dump(main_path, f)      

with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/framework.pkl', 'wb') as f:
    pickle.dump(framework, f)

if time_resolution == "monthly":
    with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/hydro.pkl', 'wb') as f:
        pickle.dump(hydro, f)

with open(r'C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput/unique_list2.pkl', 'wb') as f:
    pickle.dump(unique_list2, f)





def save_input_and_output_file():
    # Source path
    input_source_path = r"C:\Users\dc278\.conda\envs\pyomo_pgraph_converter\Lib\Pgraph/solver/input.in"
    output_source_path = r"C:\Users\dc278\.conda\envs\pyomo_pgraph_converter\Lib\Pgraph/solver/test_out.out"
    # Destination file path
    input_destination_path = r"input_1st_stage.in"
    output_destination_path = r"output_1st_stage.out"
    # Copying the file
    shutil.copy(input_source_path, input_destination_path)
    shutil.copy(output_source_path, output_destination_path)

    print(f"File copied")
save_input_and_output_file()
# memory_alloc()