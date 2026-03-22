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

os.system("cls" if os.name == "nt" else "clear")

PROJECT_ROOT = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

NATIONAL_ENERGY_ROOT = PROJECT_ROOT.parent / "National_energy_modelling"
if NATIONAL_ENERGY_ROOT.exists() and str(NATIONAL_ENERGY_ROOT) not in sys.path:
    sys.path.append(str(NATIONAL_ENERGY_ROOT))

from test_folder.normal_dist_1000datas import sample_normal_from_df
from test_folder.cf_8760_to_normal_dist import hourly_to_4hourly

from national_energy_modelling_function_file import *
from pgraph_output_class_value import *
from pgraph_csv_organiser_rev import *
from pgraph_excel_organiser import *
from pgraph_output_organiser import *
import email_myself

MONTE_CARLO_OUTPUT = PROJECT_ROOT / "MonteCarloOutput"
TEST_FOLDER = PROJECT_ROOT / "test_folder"

def load_pickle(path: Path):
    with open(path, "rb") as f:
        return pickle.load(f)

pickle_files = {
    "gen": MONTE_CARLO_OUTPUT / "gen.pkl",
    "dem": MONTE_CARLO_OUTPUT / "dem.pkl",
    "gxp_connections_dict": MONTE_CARLO_OUTPUT / "gxp_connections_dict.pkl",
    "gxp": MONTE_CARLO_OUTPUT / "gxp.pkl",
    "island": MONTE_CARLO_OUTPUT / "island.pkl",
    "time_multiplier": MONTE_CARLO_OUTPUT / "time_multiplier.pkl",
    "time_resolution": MONTE_CARLO_OUTPUT / "time_resolution.pkl",
    "time_period": MONTE_CARLO_OUTPUT / "time_period.pkl",
    "main_path": MONTE_CARLO_OUTPUT / "main_path.pkl",
    "framework": MONTE_CARLO_OUTPUT / "framework.pkl",
    "unique_list2": MONTE_CARLO_OUTPUT / "unique_list2.pkl",
}

loaded = {name: load_pickle(path) for name, path in pickle_files.items()}

gen = loaded["gen"]
dem = loaded["dem"]
gxp_connections_dict = loaded["gxp_connections_dict"]
gxp = loaded["gxp"]
island = loaded["island"]
time_multiplier = loaded["time_multiplier"]
time_resolution = loaded["time_resolution"]
time_period = loaded["time_period"]
main_path = loaded["main_path"]
framework = loaded["framework"]
unique_list2 = loaded["unique_list2"]

if time_resolution == "monthly":
    hydro = load_pickle(MONTE_CARLO_OUTPUT / "hydro.pkl")

file_paths = {
    "capacity_factor_monthly_gen_dict_path": TEST_FOLDER / "capacity_factor.pkl",
    "capacity_factor_hourly_gen_dict_path": TEST_FOLDER / "capacity_factor_hourly.pkl",
    "capacity_factor_hourly_gen_csv_path": TEST_FOLDER / "capacity_factor_hourly_len_24.pkl",
    "capacity_factor_monthly_gen_csv_path": TEST_FOLDER / "capacity_factor_hourly_monthly.pkl",
    "wholesale_electricity_ABY_price_path": TEST_FOLDER / "Relative_Price_Stats_4h.csv",
    "wholesale_electricity_relative_price_path": TEST_FOLDER / "Simulated_Prices_Q1.csv",
    "wholesale_electricity_ABY_dem_path": TEST_FOLDER / "Relative_Demand_Stats.csv",
    "wholesale_electricity_relative_dem_path": TEST_FOLDER / "Simulated_Demand_Q1.csv",
}

capacity_factor_monthly_gen_dict_path = file_paths["capacity_factor_monthly_gen_dict_path"]
capacity_factor_hourly_gen_dict_path = file_paths["capacity_factor_hourly_gen_dict_path"]
capacity_factor_hourly_gen_csv_path = file_paths["capacity_factor_hourly_gen_csv_path"]
capacity_factor_monthly_gen_csv_path = file_paths["capacity_factor_monthly_gen_csv_path"]
wholesale_electricity_ABY_price_path = file_paths["wholesale_electricity_ABY_price_path"]
wholesale_electricity_relative_price_path = file_paths["wholesale_electricity_relative_price_path"]
wholesale_electricity_ABY_dem_path = file_paths["wholesale_electricity_ABY_dem_path"]
wholesale_electricity_relative_dem_path = file_paths["wholesale_electricity_relative_dem_path"]

##### Read data from Excel file
Pd_Generation = pd.read_excel(main_path, sheet_name="Generation_updated_v1", header=0, index_col=None, usecols="A:AD", nrows=422)
Pd_Factory = pd.read_excel(main_path, sheet_name="Factory_updated (5)", header=0, index_col=None, usecols="A:BD", nrows=429)
Pd_GXP_edited = pd.read_excel(main_path, sheet_name="GXP_edited_v4", header=0, index_col=None, usecols="A:BA", nrows=217)
Pd_Technologies = pd.read_excel(main_path, sheet_name="Technologies", header=0, index_col=None, usecols="A:I", nrows=12)
Pd_GxpGxp_Connections = pd.read_excel(main_path, sheet_name="gxp_gxp_connection_v1", header=0, index_col=None, usecols="A:H",nrows=653)
Pd_biomass_resource = pd.read_excel(main_path, sheet_name="biomass_resource", header=0, index_col=None, usecols="A:K",nrows=214)
Pd_biomass_factory_connections = pd.read_excel(main_path, sheet_name="biomass_factory_distance", header=0, index_col=None,usecols="A:I", nrows=27564)
Pd_biomass_generation_connections = pd.read_excel(main_path, sheet_name="biomass_to_generation_sites", header=0, index_col=None,usecols="A:I", nrows=26197)

Pd_simulated_ABY = pd.read_csv(wholesale_electricity_relative_price_path)
Pd_relative_price = pd.read_csv(wholesale_electricity_ABY_price_path)

Pd_simulated_ABY_dem = pd.read_csv(wholesale_electricity_relative_dem_path)
Pd_relative_dem = pd.read_csv(wholesale_electricity_ABY_dem_path)


mat_units_GWh = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'GWh'}
mat_units_GJ = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'GJ'}
mat_units_MWh = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'MWh'}
mat_units_ton = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'tons'}

gop_reference =[]
goo_reference = []

# Solver selection: "INSIDEOUT" "SSGLP" "SSG" "MSG"
solve_type = "INSIDEOUT"
num_sol = 1
num_simulations = 1

current = datetime.now()
start_time = time.time()

PGRAPH_TWO_STAGE_ROOT = PROJECT_ROOT.parent / "P-graph-two-stage-optimisation"
if PGRAPH_TWO_STAGE_ROOT.exists() and str(PGRAPH_TWO_STAGE_ROOT) not in sys.path:
    sys.path.append(str(PGRAPH_TWO_STAGE_ROOT))

from pipeline_flow_2 import extract_data_for_Monte_Carlo

excel_main_path = MONTE_CARLO_OUTPUT / "pyomo_results_hourly.xlsx"

materials, operating_units, ME, connections = extract_data_for_Monte_Carlo(excel_main_path)


#you could change the number 0 to something else, len of current is what num should be
for num in range(0,num_simulations):
    rng = np.random.default_rng(num)
    print("rng",rng)
    #import cf datasets
    if time_resolution == 'hourly':
        capacity_factor_gen= hourly_to_4hourly(capacity_factor_hourly_gen_dict_path)
    CF_coordinates = {capacity_factor_gen['index'][i]: capacity_factor_gen['coordinates'][i] for i in range(len(capacity_factor_gen['index']))}

    if time_resolution == 'hourly':
        CF_value = sample_normal_from_df(capacity_factor_gen,rng)
    # print(CF_value)

    #for the CF value
    gen['cf'] = []
    Pd_Technologies_Capacity_Factor = Pd_Technologies.set_index('Type')['Capacity_factor'].to_dict()

    for i in range(len(gen["index"])):

        temp_cf = CF_value[gen['reference'][i]]
        if temp_cf == []:
            temp_cf = list(np.ones((len(time_multiplier),), dtype=int) * Pd_Technologies_Capacity_Factor[gen['type'][i]]) #warning if houurs
        else:
            pass
        gen['cf'].append(temp_cf)
    print("gen['cf']",gen['cf'])


    #for the electricity demand at gxps and price at gxps
    relative_price_headers = Pd_relative_price.columns
    relative_price = pd.DataFrame(Pd_relative_price)
    relative_price = relative_price.to_dict(orient='list')
    relative_dem_headers = Pd_relative_dem.columns
    relative_dem = pd.DataFrame(Pd_relative_dem)
    relative_dem = relative_dem.to_dict(orient='list')

    if time_resolution == 'hourly':
        rows_dict_price = {}
        row_dict_demand = {}
        for index, row in Pd_simulated_ABY.iterrows():
            rows_dict_price[index] = row.to_list()
        for index, row in Pd_simulated_ABY_dem.iterrows():
            row_dict_demand[index] = row.to_list()    
            
    # print('k',gxp['dem'])
    gxp['dem'] = []
    for i in range(len(gxp['poc'])):
        if time_resolution == 'hourly':
            match_found = False  # Track if we found a match
            dem_4_hourly = 4
            for j in range(len(relative_dem_headers)):
                if f"R_{gxp['poc'][i]}" == relative_dem_headers[j]:
                    # print(f"R_{gxp['poc'][i]} == {relative_dem_headers[j]}")
                    multiplied_result = [a * b * dem_4_hourly for a, b in zip(relative_dem[relative_dem_headers[j]], row_dict_demand[num])]
                    gxp['dem'].append(multiplied_result[:time_period])                         
                    match_found = True
                    break  # Exit loop early if match found
            if not match_found:
                gxp['dem'].append(np.zeros(time_period).tolist())  # Corrected zeros issue
    # print('m',gxp['dem'])
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
    print("gxp['price']",gxp['price'])         
    print("gxp['dem']",gxp['dem'])
                

    #demand price at gxps
    dem['fpvv']=[]
    for i in range(len(dem['index'])):
        for j in range(len(gxp['poc'])):
            if dem['dem_poc'][i] == gxp['poc'][j]:
                gxp_price_array = np.array(gxp['price'][j])  # shape: (n, 12)
                if time_resolution == "hourly":          
                    dem['fpvv'].append([p for p in gxp_price_array])    
                break
            
    #Cost of biomass resource
    
    # print(unique_list2)
    raw_material_cost_north_nzd_per_ton= [70.7, 113, 50,  75, 484]
    raw_material_cost_south_nzd_per_ton=[76, 123, 48,  75, 484]
    uncertainty = 0.2  # 5 GCV percent
    # Create bounds
    north_low  = [c * (1 - 0.2) for c in raw_material_cost_north_nzd_per_ton]
    north_high = [c * (1 + 0.2) for c in raw_material_cost_north_nzd_per_ton]

    south_low  = [c * (1 - 0.2) for c in raw_material_cost_south_nzd_per_ton]
    south_high = [c * (1 + 0.2) for c in raw_material_cost_south_nzd_per_ton]

    # One Monte Carlo draw
    north_random = rng.uniform(north_low, north_high).tolist()
    south_random = rng.uniform(south_low, south_high).tolist()    
    print("North:", north_random)
    print("South:", south_random)
    #Recoverability Biomass

    L1 = [0.8,0.95,0.7,0.75,1]
    L2 = [0.65,0.8,0.6,0.5,1]
    Level_factor=[1/0.725,1/0.875,1/0.65,1/0.625,1] #fron L0
    energy_content_gj_per_ton=[11.0, 6.9, 6.9,  13.4, 17.5]
    random_draw = rng.uniform(low=L2, high=L1) #np.random.uniform(low=L2, high=L1)
    random_list = random_draw.tolist()
    print("random_list",random_list)

