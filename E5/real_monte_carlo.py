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
from pathlib import Path

os.system("cls" if os.name == "nt" else "clear")

# ============================================================
# Project root
# ============================================================
# Assumes this script is in the repo root.
# If not, change .parent to .parents[1], etc.
PROJECT_ROOT = Path(__file__).resolve().parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Optional sibling repos
NATIONAL_ENERGY_ROOT = PROJECT_ROOT.parent / "National_energy_modelling"
if NATIONAL_ENERGY_ROOT.exists() and str(NATIONAL_ENERGY_ROOT) not in sys.path:
    sys.path.append(str(NATIONAL_ENERGY_ROOT))

PGRAPH_TWO_STAGE_ROOT = PROJECT_ROOT.parent / "P-graph-two-stage-optimisation"
if PGRAPH_TWO_STAGE_ROOT.exists() and str(PGRAPH_TWO_STAGE_ROOT) not in sys.path:
    sys.path.append(str(PGRAPH_TWO_STAGE_ROOT))

# ============================================================
# Local imports
# ============================================================
from test_folder.normal_dist_1000datas import sample_normal_from_df
from test_folder.cf_8760_to_normal_dist import hourly_to_4hourly

from national_energy_modelling_function_file import *
from pgraph_output_class_value import *
from pgraph_csv_organiser_rev import *
from pgraph_excel_organiser import *
from pgraph_output_organiser import *
from pipeline_flow_2 import extract_data_for_Monte_Carlo
import email_myself

# ============================================================
# Common folders
# ============================================================
MONTE_CARLO_OUTPUT = PROJECT_ROOT / "MonteCarloOutput"
TEST_FOLDER = PROJECT_ROOT / "test_folder"

# ============================================================
# Helper
# ============================================================
def load_pickle(path):
    with open(path, "rb") as f:
        return pickle.load(f)

# ============================================================
# Load MonteCarloOutput pickle files
# ============================================================
gen = load_pickle(MONTE_CARLO_OUTPUT / "gen.pkl")
dem = load_pickle(MONTE_CARLO_OUTPUT / "dem.pkl")
gxp_connections_dict = load_pickle(MONTE_CARLO_OUTPUT / "gxp_connections_dict.pkl")
gxp = load_pickle(MONTE_CARLO_OUTPUT / "gxp.pkl")
island = load_pickle(MONTE_CARLO_OUTPUT / "island.pkl")
time_multiplier = load_pickle(MONTE_CARLO_OUTPUT / "time_multiplier.pkl")
time_resolution = load_pickle(MONTE_CARLO_OUTPUT / "time_resolution.pkl")
time_period = load_pickle(MONTE_CARLO_OUTPUT / "time_period.pkl")
main_path = load_pickle(MONTE_CARLO_OUTPUT / "main_path.pkl")
framework = load_pickle(MONTE_CARLO_OUTPUT / "framework.pkl")
unique_list2 = load_pickle(MONTE_CARLO_OUTPUT / "unique_list2.pkl")

if time_resolution == "monthly":
    hydro = load_pickle(MONTE_CARLO_OUTPUT / "hydro.pkl")

# ============================================================
# Input file paths
# ============================================================
capacity_factor_monthly_gen_dict_path = TEST_FOLDER / "capacity_factor.pkl"
capacity_factor_hourly_gen_dict_path = TEST_FOLDER / "capacity_factor_hourly.pkl"
capacity_factor_hourly_gen_csv_path = TEST_FOLDER / "capacity_factor_hourly_len_24.pkl"
capacity_factor_monthly_gen_csv_path = TEST_FOLDER / "capacity_factor_hourly_monthly.pkl"

wholesale_electricity_ABY_price_path = TEST_FOLDER / "Relative_Price_Stats_4h.csv"
wholesale_electricity_relative_price_path = TEST_FOLDER / "Simulated_Prices_Q1.csv"
wholesale_electricity_ABY_dem_path = TEST_FOLDER / "Relative_Demand_Stats.csv"
wholesale_electricity_relative_dem_path = TEST_FOLDER / "Simulated_Demand_Q1.csv"

# ============================================================
# Read data from main workbook
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
    usecols="A:K",
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

# ============================================================
# Units and solver setup
# ============================================================
mat_units_GWh = {"time_unit": "month", "money_unit": "NZD", "mass_unit": "GWh"}
mat_units_GJ = {"time_unit": "month", "money_unit": "NZD", "mass_unit": "GJ"}
mat_units_MWh = {"time_unit": "month", "money_unit": "NZD", "mass_unit": "MWh"}
mat_units_ton = {"time_unit": "month", "money_unit": "NZD", "mass_unit": "tons"}

gop_reference = []
goo_reference = []

solve_type = "INSIDEOUT"
num_sol = 1
num_simulations = 1000
G = nx.DiGraph()
analyzer = MonteCarloAnalysis("OO")

current = datetime.now()
start_time = time.time()

# ============================================================
# Two-stage optimisation input
# ============================================================
excel_main_path = MONTE_CARLO_OUTPUT / "pyomo_results_hourly.xlsx"

materials, operating_units, ME, connections = extract_data_for_Monte_Carlo(
    excel_main_path
)

for mat_name, mat_props in materials.items():
    G.add_node(mat_name,names=mat_name,type=mat_props["type"],flow_rate_lower_bound=mat_props["lower_bound"],
        flow_rate_upper_bound=mat_props["upper_bound"],price=mat_props["price"],units=mat_units_GWh)
for op_name, op_props in operating_units.items():
    G.add_node(op_name,names=op_name,capacity_lower_bound=op_props["capacity_lower_bound"],capacity_upper_bound=op_props["capacity_upper_bound"],
               fix_cost=op_props["fix_cost"],proportional_cost=op_props["proportional_cost"])
for connection in connections:
    # print(connection)
    G.add_edge(connection[0],connection[1],weight=connection[2])

#you could change the number 0 to something else, len of current is what num should be

for num in range(146,num_simulations):
    rng = np.random.default_rng(num)
    # print(rng)
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
        if sum(gen['cf'][i][0:time_period])>=0.001:
            if str(gen['status'][i]) == 'Commissioning' and str(gen['type'][i]) not in {"Bioenergy","Thermal","Cogeneration" }:
                for l in range(len(time_multiplier)):
                    if gen['cf'][i][l]>=0.001:
                        if time_resolution == 'hourly':
                            G.add_edge("O" + str(2000 + gen['index'][i])+ str(time_multiplier[l]),
                                    "M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=gen['cf'][i][l] * 1)
                            G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l])) #cf wip6/11/2025
                        if time_resolution == 'hourly':
                            G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/(gen['cf'][i][l])/4)    
                if sum(gen['cf'][i][0:time_period])>=0.001:
                    pass
                
            else:
                for l in range(len(time_multiplier)):
                    if gen['cf'][i][l]>=0.001:
                        if str(gen['type'][i]) not in {"Bioenergy","Thermal","Cogeneration" }:
                            if time_resolution == 'hourly':
                                G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=gen['cf'][i][l] * 1)
                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l])) #cf wip6/11/2025
                        elif str(gen['type'][i]) in {"Bioenergy","Thermal","Cogeneration" }:
                            if time_resolution == 'hourly':
                                G.add_edge("M" + str(1000 + gen['index'][i]),"O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=4/gen['cf'][i][l]) #time fix 4h
                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l]*4)) #cf wip6/11/2025

                        if str(gen['type'][i]) not in {"Bioenergy","Thermal","Cogeneration" }:
                            if time_resolution == 'hourly':
                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/gen['cf'][i][l]/4)
            if sum(gen['cf'][i][0:time_period])>=0.001:
                pass

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
                
    for i in range(len(gxp['poc'])):
        for l in range(len(time_multiplier)):      
            if gxp['dem'][i][l]!=0:
                if time_resolution == "hourly":
                    G.add_node("O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 2000)+ str(time_multiplier[l]), capacity_lower_bound=0,
                                capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0) #warning (gxp['price'][i][l])*1e3/fpvv_factor[l]*3                   
                G.add_node("M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]), names="M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]), type='product',
                                flow_rate_lower_bound=gxp['dem'][i][l], flow_rate_upper_bound=1.1*gxp['dem'][i][l], price=0, units=mat_units_GWh)            
                # print(G.nodes["M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l])]["flow_rate_lower_bound"])
            else:
                pass                      

    #demand price at gxps
    dem['fpvv']=[]
    for i in range(len(dem['index'])):
        for j in range(len(gxp['poc'])):
            if dem['dem_poc'][i] == gxp['poc'][j]:
                gxp_price_array = np.array(gxp['price'][j])  # shape: (n, 12)
                if time_resolution == "hourly":          
                    dem['fpvv'].append([p for p in gxp_price_array])    
                break
            
        for l in range(len(time_multiplier)):        
            if dem["dem_ind"][i] != "Pulp" and dem['dem_ind'][i] != "Wood Processing":
                G.add_node("O" + str(dem["index"][i]) + str(103) + str(time_multiplier[l]),
                                    names="O" + str(dem["index"][i]) + str(103) + str(time_multiplier[l]), fix_cost=0, proportional_cost=dem['fpvv'][i][l]*1000)  # lines charge 41.46NZD/kVA 1E6/(dem_hours[i])*lines_charges
            else:
                G.add_node("O" + str(dem["index"][i]) + str(103) + str(time_multiplier[l]),
                                    names="O" + str(dem["index"][i]) + str(103) + str(time_multiplier[l]), fix_cost=0, proportional_cost=dem['fpvv'][i][l]*1000*1e7)  # lines charge 41.46NZD/kVA 1E6/(dem_hours[-1])*lines_charges                        

            # print(G.nodes["O" + str(dem["index"][i]) + str(103) + str(time_multiplier[l])]["proportional_cost"] )
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
    # print("North:", north_random)
    # print("South:", south_random)
    #Recoverability Biomass

    L1 = [0.8,0.95,0.7,0.75,1]
    L2 = [0.65,0.8,0.6,0.5,1]
    Level_factor=[1/0.725,1/0.875,1/0.65,1/0.625,1] #fron L0
    energy_content_gj_per_ton=[11.0, 6.9, 6.9,  13.4, 17.5]
    random_draw = rng.uniform(low=L2, high=L1) #np.random.uniform(low=L2, high=L1)
    random_list = random_draw.tolist()
    # print(random_list)

    #biomass monte carlo
    for i in range(len(unique_list2["biomass_index"])):
        for j in range(len(L1)):
            # print(unique_list2[str(j)][i])
            if G.has_node("M"+str(80000+unique_list2["biomass_index"][i]) + str(j)):
                # G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["flow_rate_upper_bound"]= G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["flow_rate_upper_bound"]/Level_factor[j]*random_list[j]
                G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["flow_rate_upper_bound"]= unique_list2[str(j)][i]/Level_factor[j]*random_list[j]*energy_content_gj_per_ton[j]
                # print(G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["flow_rate_upper_bound"])
                #unique_list
                # print(random_list[j],"M"+str(80000+unique_list2["biomass_index"][i]) + str(j),unique_list2[str(j)][i]*Level_factor[j]*random_list[j]*energy_content_gj_per_ton[j])
                # print(G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["flow_rate_upper_bound"])
                if unique_list2["island"][i]== "North":
                    G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["price"] = north_random[j]/energy_content_gj_per_ton[j]
                    
                elif unique_list2["island"][i]== "South":
                    G.nodes["M"+str(80000+unique_list2["biomass_index"][i]) + str(j)]["price"] = south_random[j]/energy_content_gj_per_ton[j]
            else:
                pass
            
    framework = "Pyomo"       
    P = Pgraph(problem_network=G, mutual_exclusion=ME, solver=solve_type, max_sol=num_sol*1000)    
    if framework == "Pyomo":
        sys.path.append(r"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-convertor")
        from pgraph_convertor_v5 import run_file
        P.create_solver_input()
        file_path = r"C:\Users\dc278\.conda\envs\pyomo_pgraph_converter\Lib\Pgraph/solver/input.in"
        model = run_file(file_path, output_file_path = rf"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-monte-carlo\MonteCarloOutput\pyomo_results_second_try{num}.xlsx")
        # print(model)
        if isinstance(model, (int, float)):
            goo_reference.append(f"{model:.6e}")
        else:
            goo_reference.append(str(model))
                
        # goo_reference.append(str(model))
        print(goo_reference)
    solved_time = time.time()

                
if num_simulations != 1:
    print(goo_reference)
    for i, goplist in enumerate(goo_reference):
        analyzer.analyze([goplist])

    # Show the final plot after processing all datasets
    analyzer.plot_results()

    # Access intermediate results if needed
    print("\nAll Simulation Data:", analyzer.all_simulation_data)
    print("Normalized Data:", analyzer.normalised_data)
    print("Mean Values:", analyzer.mean_values)

    elapsed_time = time.time() - start_time
    print(f"Time elapsed done: {elapsed_time:.2f} seconds\n")
    print(f'All {num+1} Simulation Completed!')
    

                    