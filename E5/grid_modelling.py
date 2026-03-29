#try to model a monte carlo option
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
import re
from transformer_3 import  transformer_nodes_v3
import shutil
from national_energy_modelling_function_file import *
from pgraph_output_class_value import *
from pgraph_csv_organiser_rev import *
import email_myself
from pgraph_excel_organiser import *
from pgraph_output_organiser import *
from class_cost_lookup import CostLookup
from class_distribution import *
from class_parse_text_files import FeasibleStructureExtractor
from class_gxp_connection_2 import GXPConnectionProcessor

def feasible_structures_2(
    base_sales_electricity_cost=pd.DataFrame(),
    output_file_path=None,
    input_file_path=None,
):
    start_time = time.time()
    remove = 1

    # ============================================================
    # Optional sibling repo import path
    # ============================================================
    NATIONAL_ENERGY_ROOT = PROJECT_ROOT.parent / "National_energy_modelling"
    if NATIONAL_ENERGY_ROOT.exists() and str(NATIONAL_ENERGY_ROOT) not in sys.path:
        sys.path.append(str(NATIONAL_ENERGY_ROOT))

    # ============================================================
    # Common folders
    # ============================================================
    OUTPUT_RESULTS = PROJECT_ROOT / "output_results"
    DOWNLOADS_ROYAL_SOC = (
        Path.home() / "Downloads" / "Royal_Soc" / "Grid stuff" / "RETA ERGO reports"
    )

    # ============================================================
    # Helper
    # ============================================================
    def load_pickle(path):
        with open(path, "rb") as f:
            return pickle.load(f)

    # ============================================================
    # Load first-stage outputs
    # ============================================================
    gen = load_pickle(OUTPUT_RESULTS / "gen.pkl")
    dem = load_pickle(OUTPUT_RESULTS / "dem.pkl")
    gxp_connections_dict = load_pickle(OUTPUT_RESULTS / "gxp_connections_dict.pkl")
    gxp = load_pickle(OUTPUT_RESULTS / "gxp.pkl")
    island = load_pickle(OUTPUT_RESULTS / "island.pkl")
    time_multiplier = load_pickle(OUTPUT_RESULTS / "time_multiplier.pkl")
    time_resolution = load_pickle(OUTPUT_RESULTS / "time_resolution.pkl")
    time_period = load_pickle(OUTPUT_RESULTS / "time_period.pkl")
    main_path = load_pickle(OUTPUT_RESULTS / "main_path.pkl")
    framework = load_pickle(OUTPUT_RESULTS / "framework.pkl")
    hydro = load_pickle(OUTPUT_RESULTS / "hydro.pkl")

    # ============================================================
    # File selection
    # ============================================================
    choice = 4

    if choice == 1:
        file_path_out = PROJECT_ROOT / "output.out"
        file_path_in = PROJECT_ROOT / "input.in"

    elif choice == 3:
        file_path_out = PROJECT_ROOT / "output 2.out"
        file_path_in = PROJECT_ROOT / "input 2.in"

    elif choice == 2:
        # Environment-specific solver path
        # Keep only if you truly need this local conda path
        file_path_out = Path(
            "C:/Users/dc278/.conda/envs/base_dell/base_dell/envs/tsyet_updated/Lib/Pgraph/solver/test_out.out"
        )
        file_path_in = Path(
            "C:/Users/dc278/.conda/envs/base_dell/base_dell/envs/tsyet_updated/Lib/Pgraph/solver/input.in"
        )

    elif choice == 4:
        file_path_out = PROJECT_ROOT / "output_1st_stage.out"
        file_path_in = PROJECT_ROOT / "input_1st_stage.in"

    # ============================================================
    # Costing path
    # ============================================================
    compiled_data_path = DOWNLOADS_ROYAL_SOC / "compiled.xlsx"

    Pd_transmission = pd.read_excel(
        compiled_data_path,
        sheet_name="ERGO transmission cost",
        header=0,
        index_col=None,
        usecols="A:G",
        nrows=16,
    )

    Pd_distribution = pd.read_excel(
        compiled_data_path,
        sheet_name="ERGO distribution cost",
        header=0,
        index_col=None,
        usecols="A:J",
        nrows=7,
    )

    Pd_transformer = pd.read_excel(
        compiled_data_path,
        sheet_name="ERGO transformer cost",
        header=0,
        index_col=None,
        usecols="A:H",
        nrows=5,
    )

    cost_lookup = CostLookup(
        pd_distribution=Pd_distribution,
        pd_transmission=Pd_transmission,
        pd_transformer=Pd_transformer,
    )

    # ============================================================
    # Read feasible structure
    # ============================================================
    if framework == "Pgraph":
        extractor = FeasibleStructureExtractor(file_path_out)
        extractor.extract_output_values("feasible_structure.xlsx")

        materials_out_df = pd.read_excel(
            "feasible_structure.xlsx",
            sheet_name="Materials",
            index_col=0,
        )
        op_out_df = pd.read_excel(
            "feasible_structure.xlsx",
            sheet_name="Operating Units",
            index_col=0,
        )

    elif framework == "Pyomo":
        if input_file_path is None:
            input_file_path = PROJECT_ROOT / "pyomo_results.xlsx"

        materials_out_df = pd.read_excel(
            input_file_path,
            sheet_name="Materials",
            index_col=None,
        )
        op_out_df = pd.read_excel(
            input_file_path,
            sheet_name="Operating Units",
            index_col=None,
        )

    gen['flow'] = []
    gen['cost'] = []
    gen['max_flow'] = []
    dem['flow'] = []
    dem['cost'] = [] 
    dem['demand_node'] = []
    dem['max_flow'] = []
    gxp_connections_dict["flow"]= []
    gxp_connections_dict["grid_lines_needed"]= []
    gxp_connections_dict["max_flow"]= []
    gxp_connections_dict["cost"]= []

    bio_gen = {'ID':[],'Capacity':[],'Delivery_Cost':[],'Bio_ID':[],'Gen_ID':[],'Type':[],'Biomass_Cost':[]}
    for _, row in op_out_df.iterrows():
        unit_id = str(row["ID"])

        # Check length condition
        if len(unit_id) == 15:
            bio_gen["ID"].append(unit_id)

            # Capacity Multiplier column
            bio_gen["Capacity"].append(row["Capacity Multiplier"])

            # Proportional cost column (replace with correct name if different)
            bio_gen["Delivery_Cost"].append(row["Proportional Cost"])

            # Extract parts from ID string
            bio_gen["Bio_ID"].append(unit_id[1:6])      # characters 1–5
            bio_gen["Gen_ID"].append(unit_id[10:15])    # characters 11–14
            bio_gen["Type"].append(unit_id[6])          # character at position 7

    materials_filtered = materials_out_df.copy()
    materials_filtered["ID"] = materials_filtered["ID"].astype(str)
    materials_filtered = materials_filtered[materials_filtered["Type"] == "raw_material"]
    materials_filtered = materials_filtered[(materials_filtered["ID"].str.startswith("M8")) &(materials_filtered["ID"].str.len() == 7)].copy()
    
    materials_filtered["Type_Code"] = materials_filtered["ID"].str[-1].astype(str)

    # print(materials_filtered)   
    type_to_cost = (materials_filtered.drop_duplicates("Type_Code").set_index("Type_Code")["Price"].to_dict()) #price per Gj not Total cost
    bio_gen["Biomass_Cost"] = [type_to_cost.get(str(t), None) for t in bio_gen["Type"]]
    # print(bio_gen)        
            
    # for bio_gen:
    #     scan through materials_out_df (matching with starts with M8, length =7, type=last digit must match with bio_gen["Type"] )
    #     bio_gen['Biomass_Cost'] . append(materials_out_df[])



    # Distribution done
    #################### still need to work on distributions
    # Create an instance of the DistributionFlowCalculator
    distribution_calculator = DistributionFlowCalculator(op_out_df, cost_lookup, time_multiplier,time_resolution)

    # print(dem)
    distribution_calculator.process_demand(dem)
    # print("stupid",distribution_calculator.process_demand(dem))
    distribution_calculator.process_generation(gen)

    # print(dem)

    # Transmission Grid 

    transmission = GXPConnectionProcessor(
        gxp_connections_dict=gxp_connections_dict,
        output_file=op_out_df,  # your loaded dataframe
        transmission_cost_lookup=cost_lookup.transmission,  # or whatever lookup dict
        time_multiplier=time_multiplier,time_resolution=time_resolution,time_period=time_period
    )

    transmission.process_gxp()


    # Transformer things
    ## Demand extraction
    #filter 3000103 nodes factory

    filter_output_ph_dem = {'ID':[],'Capacity Multiplier':[]}
    for i in range(len(op_out_df['ID'])):
        if op_out_df['Cost'][i]==0 and len(op_out_df['ID'][i])==10 and op_out_df['ID'][i][5:8]==str(103):
            filter_output_ph_dem['ID'].append(op_out_df['ID'][i])
            filter_output_ph_dem['Capacity Multiplier'].append(op_out_df['Capacity Multiplier'][i])
    # print(filter_output_ph_dem)

    ################################### Modelling Works ###################################
    G = nx.DiGraph()

    mat_units_GWh = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'GWh'}
    mat_units_GJ = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'GJ'}
    mat_units_MWh = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'MWh'}
    mat_units_ton = {'time_unit': 'month', 'money_unit': 'NZD', 'mass_unit': 'tons'}
    proportional_cost_op = {'fix_cost':0.00000 ,'proportional_cost':0.00000}

    ME1 =[]


    # gxp, +dem transformer
    # print(gxp)
    #########  GXP nodes ##################
    for i in range(len(gxp['poc'])):
        for l in range(len(time_multiplier)):
            G.add_node("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]), names="M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]),type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
            # if time_resolution == 'monthly':    
            #     G.add_node("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]), names="O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),
            #                 capacity_lower_bound=0, capacity_upper_bound=gxp['gen_n'][i]/1000*730, fix_cost=0, proportional_cost=0)
            # elif time_resolution == 'hourly':    
            #     G.add_node("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]), names="O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),
            #                 capacity_lower_bound=0, capacity_upper_bound=gxp['gen_n'][i]/1000, fix_cost=0, proportional_cost=0)
            # print(gxp['dem'][i][l])
            G.add_node("M" + str(gxp['index'][i]) + str(time_multiplier[l]),
                            names="M" + str(gxp['index'][i]) + str(time_multiplier[l]), type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_GWh)  #warning
            # if time_resolution == 'monthly':    
            #     G.add_node("O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 300)+ str(time_multiplier[l]), capacity_lower_bound=0,
            #             capacity_upper_bound=gxp['dem_n'][i]/1000*730, fix_cost=0, proportional_cost=0)  #warning
            # elif time_resolution == 'hourly':    
            #     G.add_node("O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 300)+ str(time_multiplier[l]), capacity_lower_bound=0,
            #             capacity_upper_bound=gxp['dem_n'][i]/1000, fix_cost=0, proportional_cost=0)  #warning
                
            G.add_node("M" + str(gxp['index'][i] + 600) + str(time_multiplier[l]),names="M" + str(gxp['index'][i]  + 600) + str(time_multiplier[l]), type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_GWh)  #warning
                
            # G.add_edge("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]),"O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),weight=1)
            # G.add_edge("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]),"M" + str(gxp['index'][i]) + str(time_multiplier[l]),weight=1)
        
                
            # G.add_edge("M" + str(gxp['index'][i]) + str(time_multiplier[l]),"O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]),weight=1)
            # G.add_edge("O" + str(gxp['index'][i] + 300) + str(time_multiplier[l]),"M" + str(gxp['index'][i] + 600) + str(time_multiplier[l]),weight=0.99)

            #demand at each gxp
            if gxp['dem'][i][l]!=0:
                G.add_node("O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]), names="O" + str(gxp['index'][i] + 2000)+ str(time_multiplier[l]), capacity_lower_bound=0,
                            capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                G.add_node("M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]), names="M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]), type='product',
                                flow_rate_lower_bound=gxp['dem'][i][l], flow_rate_upper_bound=1.01*gxp['dem'][i][l], price=100, units=mat_units_GWh)#modify  gxp['dem'][i][l]
                            
                G.add_edge("M" + str(gxp['index'][i] + 600) + str(time_multiplier[l]),"O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]),weight=1)
                G.add_edge("O" + str(gxp['index'][i] + 2000) + str(time_multiplier[l]),"M" + str(gxp['index'][i] + 2300) + str(time_multiplier[l]),weight=0.99)
                    

            else:
                pass
        
    Pd_GXP_edited = pd.read_excel(main_path, sheet_name="GXP_edited_v4", header=0, index_col=None, usecols="A:AX", nrows=217)
    Pd_keys = ['Index', 'POC','North_South', 'Demand Transformers Req',	'Dem Large transformer',	'Dem Medium transformer',	'Dem Small transformer',	'Dem Tiny transformer',
            'Supply Transformers Req',	'Sup Large transformer',	'Sup Medium transformer',	'Sup Small transformer',	'Sup Tiny transformer','N Demand (MVA)','N Generation (MVA)' ]
    Pd_GXP = python_dict_dot_notation(Pd_keys, Pd_GXP_edited)

    # print("Hi",Pd_GXP)
    
    gxp_1 = {'index':[], 'POC':[], 'Num_of_Dem_transformer':[], 'Num_of_Sup_transformer':[],'Dem_transformer':[],'Sup_transformer':[],'dem_n':[],'gen_n':[] }
    dem_cols = ['Dem Large transformer',	'Dem Medium transformer',	'Dem Small transformer',	'Dem Tiny transformer']
    gen_cols = ['Sup Large transformer',	'Sup Medium transformer',	'Sup Small transformer',	'Sup Tiny transformer']

    for idx, row in Pd_GXP_edited.iterrows():
        if str(Pd_GXP.North_South[idx]) != island:

            gxp_1['index'].append(row['Index'])  # Still get row-by-row values
            gxp_1['POC'].append(row['POC'])
            gxp_1['Num_of_Dem_transformer'].append(row['Demand Transformers Req'])
            gxp_1['Num_of_Sup_transformer'].append(row['Supply Transformers Req'])

            gxp_1['Dem_transformer'].append([row[col] for col in dem_cols])
            gxp_1['Sup_transformer'].append([row[col] for col in gen_cols])
            
            gxp_1['gen_n'].append(row['N Generation (MVA)'])
            gxp_1['dem_n'].append(row['N Demand (MVA)'])
            
    # print(gxp_1)

    # print(cost_lookup.transformer)

    # gxp-gxp connection part but with transmission constraints
    # print(gxp_connections_dict['Distance'])

    # for i in range(len(gxp_connections_dict['Op_ID'])):
            
    #     for l in range(len(time_multiplier)):
    #         G.add_node(gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]),names=gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=gxp_connections_dict['Capacity'][i]*1, fix_cost=0, proportional_cost=0)
    #         # print(gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]))
    #         # G.add_node(gxp_connections_dict['From'][i]+str(time_multiplier[l]),names=gxp_connections_dict['From'][i]+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0)
    #         # G.add_node(gxp_connections_dict['To'][i]+str(time_multiplier[l]),names=gxp_connections_dict['To'][i]+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0)

    #         G.add_edge(gxp_connections_dict['From'][i]+str(time_multiplier[l]), gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]),weight=1)
    #         G.add_edge(gxp_connections_dict['Op_ID'][i]+str(time_multiplier[l]), gxp_connections_dict['To'][i]+str(time_multiplier[l]),weight=0.98)
                
    #         if int(gxp_connections_dict['Op_ID'][i][1:]) % 2 == 1 and int(gxp_connections_dict['Op_ID'][i][1:]) >= 6001:
    #             ME1.append([(gxp_connections_dict['Op_ID'][i-1]) + str(time_multiplier[l]),(gxp_connections_dict['Op_ID'][i]) + str(time_multiplier[l])])


    ME2=transmission.nodes_update(G)
    # print(ME2)

    # generation + gen distribution
    # print(gen)

    for i in range(len(gen['index'])):
        if sum(gen['cf'][i][0:time_period])>=0.001:
            if str(gen['status'][i]) == 'Commissioning' and str(gen['type'][i]) not in {"Bioenergy","Thermal","Cogeneration" }:
                
                    G.add_node("M" + str(6000 + gen['index'][i]), names="M" + str(6000 + gen['index'][i]), type="raw_material", flow_rate_lower_bound=0,
                        flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)
                    G.add_node("M" + str(2500 + gen['index'][i]), names="M" + str(2500 + gen['index'][i]), type='product',
                        flow_rate_lower_bound=gen['capacity'][i]*0.95, flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)
                    
                    G.add_node("M" + str(3000 + gen['index'][i]),
                            names="M" + str(3000 + gen['index'][i]), type='intermediate',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh) #readded for capacity
                                    
                                    
                    if time_resolution == 'hourly': 
                        G.add_node("M" + str(9000 + gen['index'][i]), names="M" + str(9000 + gen['index'][i]),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=10000*gen['capacity'][i], price=0,
                            units=mat_units_MWh) #gen['price'][i]/365/24
                        # newly updated nov 11 2025
                        G.add_node("O" + str(4000 + gen['index'][i]), names="O" + str(4000 + gen['index'][i]), capacity_lower_bound=0,
                                capacity_upper_bound=gen['capacity'][i]*remove, fix_cost=0, proportional_cost=0)  # gen capacity gen['price'][i]/365/24*time_period
                        #prevent double counting of electricitry price
                        
                        
                    elif time_resolution == 'monthly':

                        G.add_node("M" + str(9000 + gen['index'][i]), names="M" + str(9000 + gen['index'][i]),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=10000*gen['capacity'][i], price=0,
                            units=mat_units_MWh) #gen['price'][i]/12
                        G.add_node("O" + str(4000 + gen['index'][i]), names="O" + str(4000 + gen['index'][i]), capacity_lower_bound=0,
                            capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=0)  # gen capacity gen['price'][i]/12*time_period
                        
                    #ratio 6nov2025
                    G.add_node("M" + str(3500 + gen['index'][i]), names="M" + str(3500 + gen['index'][i]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_MWh)   
                    
                    G.add_node("M" + str(15500), names="M" + str(15500),type='product',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,units=mat_units_MWh)    
                    
                    G.add_node("O" + str(4500 + gen['index'][i]), names="O" + str(4500 + gen['index'][i]), capacity_lower_bound=0,
                                capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                    G.add_edge("O" + str(4000 + gen['index'][i]),"M" + str(3500 + gen['index'][i]),weight=0.6)
                    G.add_edge("M" + str(3500 + gen['index'][i]),"O" + str(4500 + gen['index'][i]),weight=1)
                    G.add_edge("O" + str(4500 + gen['index'][i]),"M" + str(15500),weight=1)

                    op_node_as_capacity = "O" + str(4000 + gen['index'][i])
                    op_node_as_out_flow = "O" + str(2000 + gen['index'][i])
                    mat_in_node = "M" + str(3000 + gen['index'][i])
                    mat_out_node = "M" + str(5000 + gen['index'][i])
                        
        
                    for l in range(len(time_multiplier)):
                        if gen['cf'][i][l]>=0.001:
                            G.add_node("M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]),
                                    names="M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), type='intermediate',
                                    flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_MWh)

                            G.add_node("M" + str(6500 + gen['index'][i])+str(time_multiplier[l]), names="M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,units=mat_units_MWh) 
                            G.add_edge("M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),"O" + str(4500 + gen['index'][i]),weight=1)

                            if time_resolution == 'hourly':
                                G.add_node("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), names="O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), capacity_lower_bound=0,
                                capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=0)
                                G.add_edge("O" + str(2000 + gen['index'][i])+ str(time_multiplier[l]),"M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=gen['cf'][i][l])
                                G.add_edge("M" + str(6000 + gen['index'][i]), "O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=1)
                                G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(2500 + gen['index'][i]), weight=1)                                                                         
                                
                                G.add_node("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),names="O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0 , proportional_cost=gen['cost'][i]*.03/8760)
                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l])) #cf wip6/11/2025
                                
                            elif time_resolution == 'monthly':
                                G.add_node("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), names="O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), capacity_lower_bound=0,
                                    capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=0)
                                G.add_edge("O" + str(2000 + gen['index'][i])+ str(time_multiplier[l]),"M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=gen['cf'][i][l]*730)  
                                G.add_edge("M" + str(6000 + gen['index'][i]), "O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=1)
                                
                                G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(2500 + gen['index'][i]), weight=1)                                         
                                G.add_node("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),names="O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0 , proportional_cost=gen['cost'][i]*.03/730)

                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l]*730)) #cf wip6/11/2025
                
                
                            G.add_edge("M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]),"O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), weight=1)
                            
                            #prevent double counting edge
                            if time_resolution == 'hourly':
                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/gen['cf'][i][l])
                            elif time_resolution == 'monthly':
                                G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/gen['cf'][i][l]/730)
                        
                        #curtail
                            G.add_node("O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),
                                        names="O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),capacity_lower_bound=0,capacity_upper_bound=1e8, **proportional_cost_op) 
                            
                            G.add_node("M" + str(11000 + gen['index'][i]) + str(time_multiplier[l]),
                                        names="M" + str(11000 + gen['index'][i]) + str(time_multiplier[l]), type='product',
                                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)        
                            G.add_edge("M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]),"O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),weight=1)
                            G.add_edge("O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(11000 + gen['index'][i]) + str(time_multiplier[l]),weight=1)

                    if sum(gen['cf'][i][0:time_period])>=0.001:
                        text_file_bug_error_2(op_node_as_out_flow,op_node_as_capacity,mat_in_node,mat_out_node,time_period,time_multiplier,G,mat_units_GWh)


            else: #things that are not commissioning or bioenergy/thermal/cogeneration
                    if (gen['type'][i]) in {"Bioenergy", "Cogeneration", "Thermal"}:
                        G.add_node("M" + str(1000 + gen['index'][i]), names="M" + str(1000 + gen['index'][i]),
                                type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_MWh)      
                                                
                        G.add_node("M" + str(3000 + gen['index'][i]),names="M" + str(3000 + gen['index'][i]), type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)
                        
                    else:
                        G.add_node("M" + str(6000 + gen['index'][i]), names="M" + str(6000 + gen['index'][i]),
                                type="raw_material", flow_rate_lower_bound=0,flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)

                        G.add_node("O" + str(gen['index'][i]), names="O" + str(gen['index'][i]), capacity_lower_bound=0,
                                capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)

                        G.add_node("M" + str(1000 + gen['index'][i]), names="M" + str(1000 + gen['index'][i]),
                                type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_MWh)         

                        G.add_edge("M" + str(6000 + gen['index'][i]), "O" + str(gen['index'][i]), weight=1)
                        G.add_edge("O" + str(gen['index'][i]), "M" + str(1000 + gen['index'][i]), weight=1)
                        G.add_node("M" + str(3000 + gen['index'][i]),names="M" + str(3000 + gen['index'][i]), type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_MWh)
                    
                    if time_resolution == 'hourly':
                        if gen['type'][i] in {'Solar Utility', 'Solar Commercial', 'Solar Residential'}:
                            G.add_node("O" + str(4000 + gen['index'][i]), names="O" + str(4000 + gen['index'][i]), capacity_lower_bound=gen['capacity'][i],
                                capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=gen['price'][i]/365/24*time_period)  # gen capacity
                            #prevent double counting of electricitry price
                            G.add_node("M" + str(9000 + gen['index'][i]), names="M" + str(9000 + gen['index'][i]),type='product',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=10000*gen['capacity'][i], price=0,
                                units=mat_units_MWh) #gen['price'][i]/365/24
                        else:               
                            G.add_node("O" + str(4000 + gen['index'][i]), names="O" + str(4000 + gen['index'][i]), capacity_lower_bound=0,
                                capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=gen['price'][i]/365/24*time_period)  # gen capacity
                            #prevent double counting of electricitry price
                            G.add_node("M" + str(9000 + gen['index'][i]), names="M" + str(9000 + gen['index'][i]),type='product',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=10000*gen['capacity'][i], price=0,
                                units=mat_units_MWh) #gen['price'][i]/365/24
                        
                    elif time_resolution == 'monthly':
                        G.add_node("O" + str(4000 + gen['index'][i]), names="O" + str(4000 + gen['index'][i]), capacity_lower_bound=0,
                            capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=gen['price'][i]/12*time_period)  # gen capacity #warning wip
                        #prevent double counting of electricitry price
                        G.add_node("M" + str(9000 + gen['index'][i]), names="M" + str(9000 + gen['index'][i]),type='product',
                            flow_rate_lower_bound=0, flow_rate_upper_bound=10000*gen['capacity'][i], price=0,
                            units=mat_units_MWh)  # gen['price'][i]/12            

                    #ratio 6nov2025
                    G.add_node("M" + str(3500 + gen['index'][i]), names="M" + str(3500 + gen['index'][i]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=mat_units_MWh)   
                    G.add_node("M" + str(15500), names="M" + str(15500),type='product',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,
                                units=mat_units_MWh)     
                    G.add_node("O" + str(4500 + gen['index'][i]), names="O" + str(4500 + gen['index'][i]), capacity_lower_bound=0,
                                capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                    G.add_edge("O" + str(4000 + gen['index'][i]),"M" + str(3500 + gen['index'][i]),weight=0.6)
                    G.add_edge("M" + str(3500 + gen['index'][i]),"O" + str(4500 + gen['index'][i]),weight=1)
                    G.add_edge("O" + str(4500 + gen['index'][i]),"M" + str(15500),weight=1)
                    
                    
                    op_node_as_capacity = "O" + str(4000 + gen['index'][i])
                    op_node_as_out_flow = "O" + str(2000 + gen['index'][i])
                    mat_in_node = "M" + str(3000 + gen['index'][i])
                    mat_out_node = "M" + str(5000 + gen['index'][i])
                    
                
                    for l in range(len(time_multiplier)):

                        if gen['cf'][i][l]>=0.001:

                            # adding h2 here
                            G.add_node("M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]),
                                    names="M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), type='intermediate',
                                    flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0, units=mat_units_MWh)

                            G.add_node("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),
                                    names="O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), **proportional_cost_op)
                            
                            G.add_node("M" + str(6500 + gen['index'][i])+str(time_multiplier[l]), names="M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),type='intermediate',
                                flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,units=mat_units_MWh) 
                            G.add_edge("M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),"O" + str(4500 + gen['index'][i]),weight=1)
                            
                            
                            #if not commissioning and no fuel is needed.......
                            if str(gen['type'][i]) not in {"Bioenergy","Thermal","Cogeneration" }:
                                
                                if time_resolution == 'hourly':
                                    G.add_edge("M" + str(1000 + gen['index'][i]),"O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=1) #fix the time 
                                    G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]),
                                            "M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=gen['cf'][i][l])
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l])) #cf wip6/11/2025
                                    
                                elif time_resolution == 'monthly':
                                    G.add_edge("M" + str(1000 + gen['index'][i]),"O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=730)
                                    G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]),
                                            "M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=gen['cf'][i][l]*730)                                 
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l]*730)) #cf wip6/11/2025
                                                           
                            elif str(gen['type'][i]) in {"Bioenergy","Thermal","Cogeneration" }:
                                
                                if time_resolution == 'hourly':
                                    G.add_edge("M" + str(1000 + gen['index'][i]),"O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=1/gen['cf'][i][l]) #fixed the hourly
                                    G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]),
                                            "M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=1)
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l])) #cf wip6/11/2025
                                    
                                elif time_resolution == 'monthly':
                                    G.add_edge("M" + str(1000 + gen['index'][i]),"O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]), weight=730/gen['cf'][i][l]) #fixed the monthly
                                    G.add_edge("O" + str(2000 + gen['index'][i]) + str(time_multiplier[l]),
                                            "M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]), weight=730)   
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(6500 + gen['index'][i])+str(time_multiplier[l]),weight=1/(gen['cf'][i][l]*730)) #cf wip6/11/2025
                                                            
                            else:
                                print("error")
                            
                            G.add_edge("M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]),
                                        "O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), weight=1)
                            
                            #prevent double counting edge
                            if str(gen['type'][i]) not in {"Bioenergy","Thermal","Cogeneration" }:
                            
                                if time_resolution == 'hourly':
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/gen['cf'][i][l])
                                elif time_resolution == 'monthly':
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/gen['cf'][i][l]/730)
                            elif str(gen['type'][i]) in {"Bioenergy","Thermal","Cogeneration" }:
                                if time_resolution == 'hourly':
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1)
                                elif time_resolution == 'monthly':
                                    G.add_edge("O" + str(8000 + gen['index'][i]) + str(time_multiplier[l]), "M" + str(9000 + gen['index'][i]) ,weight=1/730)
                                    G.add_node("O" + str(4000 + gen['index'][i]), names="O" + str(4000 + gen['index'][i]), capacity_lower_bound=gen['capacity'][i],
                            capacity_upper_bound=gen['capacity'][i], fix_cost=0, proportional_cost=gen['price'][i]/12*time_period)  # gen capacity #warning wip
                        
                        #curtail
                            G.add_node("O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),names="O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),capacity_lower_bound=0,capacity_upper_bound=1e7, **proportional_cost_op) 
                            
                            G.add_node("M" + str(11000 + gen['index'][i]) + str(time_multiplier[l]),names="M" + str(11000 + gen['index'][i]) + str(time_multiplier[l]), type='product',
                                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)        
                            G.add_edge("M" + str(7000 + gen['index'][i]) + str(time_multiplier[l]),"O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),weight=1)
                            G.add_edge("O" + str(10000 + gen['index'][i]) + str(time_multiplier[l]),"M" + str(11000 + gen['index'][i]) + str(time_multiplier[l]),weight=1)
        
                    if sum(gen['cf'][i][0:time_period])>=0.001:
                        text_file_bug_error_2(op_node_as_out_flow,op_node_as_capacity,mat_in_node,mat_out_node,time_period,time_multiplier,G,mat_units_GWh)
    if time_resolution == 'monthly':
    
        for i in range(len(hydro["index"])):
            G.add_node("O"+str(16000+hydro["gen_id"][i])+str(3), names = "O"+str(16000+hydro["gen_id"][i])+str(3), capacity_lower_bound=0,
                        capacity_upper_bound=hydro["storage"][i], fix_cost=0, proportional_cost=0)   
            for l in range(len(time_multiplier)):
                next_l = (l + 1) % len(time_multiplier)  # wraps last to first

                G.add_node("O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]), names = "O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]), capacity_lower_bound=0,
                        capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
                G.add_node("M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]), names= "M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]),flow_rate_lower_bound=0, flow_rate_upper_bound=1e10, price=0, units=mat_units_GWh,type='intermediate')
                G.add_node("M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]), names= "M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]),flow_rate_lower_bound=0, flow_rate_upper_bound=1e10, price=0, units=mat_units_GWh,type='intermediate')

                G.add_edge("M"+str(17000+hydro["gen_id"][i])+str(time_multiplier[l]),"O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]),weight=1)
                G.add_edge("O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]),"M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]),weight=1)
                G.add_edge("M"+str(16000+hydro["gen_id"][i])+str(2)+str(time_multiplier[l]),"O"+str(16000+hydro["gen_id"][i])+str(3),weight=1e-7)
                G.add_edge("O"+str(16000+hydro["gen_id"][i])+str(3),"M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]),weight=1)
                G.add_edge("M"+str(16000+hydro["gen_id"][i])+str(4)+str(time_multiplier[l]),"O"+str(16000+hydro["gen_id"][i])+str(1)+str(time_multiplier[l]),weight=1)

                # cyclic link — last period loops to first
                G.add_edge("O" + str(16000 + hydro["gen_id"][i]) + str(1) + str(time_multiplier[l]),"M" + str(17000 + hydro["gen_id"][i]) + str(time_multiplier[next_l]),weight=0.99)

    # bio_gen
    # print(bio_gen)
    for i in range(len(bio_gen['ID'])): #what if the bio node repeats itself
        if G.has_node("M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i])):
            G.nodes["M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i])]["flow_rate_upper_bound"] += bio_gen["Capacity"][i] * 3.6
            # print("M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i]),"value",G.nodes["M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i])]["flow_rate_upper_bound"])
        else:
            G.add_node("M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i]), names ="M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i]), flow_rate_lower_bound=0, flow_rate_upper_bound=bio_gen["Capacity"][i]*3.6*1.1, price=0, units=mat_units_GJ,type='raw_material') #3.6 is conversion for that
        G.add_node(bio_gen["ID"][i],names=bio_gen["ID"][i],capacity_lower_bound=0,capacity_upper_bound=1e8, fix_cost=0, proportional_cost=bio_gen["Delivery_Cost"][i])	
        G.add_node("M"+str(1000+int(bio_gen["Gen_ID"][i])),names="M"+str(1000+int(bio_gen["Gen_ID"][i])),flow_rate_lower_bound=0, flow_rate_upper_bound=1e7, price=0, units=mat_units_MWh,type='intermediate')
        G.add_edge("M"+str(bio_gen["Bio_ID"][i])+ str(bio_gen["Type"][i]),bio_gen["ID"][i],weight=3.6)
        G.add_edge(bio_gen["ID"][i],"M"+str(1000+int(bio_gen["Gen_ID"][i])),weight=1)

        

    #methanol stuff
    filter_output_green_methanol = {'ID':[],'Capacity Multiplier':[],"Unique_ID":[],"time_step":[],"price":[]} #
    for i in range(len(op_out_df['ID'])):
        if len(op_out_df['ID'][i])==11 and op_out_df['ID'][i][:2]=="O1" and op_out_df['ID'][i][-5:-2]=="230":
            filter_output_green_methanol['ID'].append(op_out_df['ID'][i])
            filter_output_green_methanol['Capacity Multiplier'].append(op_out_df['Capacity Multiplier'][i])
            filter_output_green_methanol["Unique_ID"].append(op_out_df['ID'][i][:6])
            filter_output_green_methanol["time_step"].append(op_out_df['ID'][i][-2:])
    # print(gen['index'])
    for i in range(len(filter_output_green_methanol['ID'])):
        matched = False
        # print(filter_output_green_methanol['ID'][i][1:6])
        for j in range(len(gen['index'])):
            # print(int(filter_output_green_methanol['ID'][i][1:6]),";",int(gen['index'][j]))
            if int(filter_output_green_methanol['ID'][i][1:6]) == int(gen['index'][j]):
                if time_resolution == 'monthly':
                    filter_output_green_methanol['price'].append(gen['price'][j]) 
                matched = True
                break
                    # print(filter_output_green_methanol['ID'][i][1:6],'==', gen['index'][j])
    
    # print(filter_output_green_methanol['ID'],len(filter_output_green_methanol['ID']))        
    # print(filter_output_green_methanol['price'],len(filter_output_green_methanol['price']))    
    # print(gen['price'],len(gen['price']))

    #     # use if exists,has_node... do something...
    for i  in range(len(filter_output_green_methanol['ID'])):
        

        if filter_output_green_methanol["Unique_ID"][i]:
            for l in range(len(time_multiplier)):
                if filter_output_green_methanol["time_step"][i] == str(time_multiplier[l]):
                    G.add_node("O"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(230)+str(time_multiplier[l]),names="O"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(230)+str(time_multiplier[l]),**proportional_cost_op)
                    if time_resolution == 'monthly':
                        G.add_node("M"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(260)+str(time_multiplier[l]),names="M"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(260)+str(time_multiplier[l]), type='product',
                                flow_rate_lower_bound=filter_output_green_methanol['Capacity Multiplier'][i], flow_rate_upper_bound=filter_output_green_methanol['Capacity Multiplier'][i], price=filter_output_green_methanol['price'][i]/12/730, units=mat_units_GWh) #warning, flag the ratio
                        
                    G.add_edge("M"+str(7000+int(filter_output_green_methanol['ID'][i][1:6]))+ str(time_multiplier[l]),"O"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(230)+str(time_multiplier[l]),weight=1)
                    G.add_edge("O"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(230)+str(time_multiplier[l]),"M"+str(int(filter_output_green_methanol['ID'][i][1:6]))+str(260)+str(time_multiplier[l]),weight=1)


    for i in range(len(gen['index'])):
        if sum(gen['cf'][i][0:time_period]) >= 0.001:
            if str(gen['status'][i]) != 'Commissioning':
                for number, ((lower_bound, upper_bound), fixed_cost) in enumerate(cost_lookup.distribution.items()):
                    if time_resolution == 'monthly':
                        G.add_node("O" + str(8000 + gen['index'][i]) + str(number)+str("01"),names="O" + str(8000 + gen['index'][i]) + str(number) +str("01"),
                        capacity_lower_bound=lower_bound, capacity_upper_bound=upper_bound,fix_cost=fixed_cost*gen['distance'][i]*time_period/12,  # $/km, 
                        proportional_cost=0)                    
                    elif time_resolution == 'hourly':
                        # print(upper_bound)
                        G.add_node("O" + str(8000 + gen['index'][i]) + str(number)+str("01"),names="O" + str(8000 + gen['index'][i]) + str(number) +str("01"),
                        capacity_lower_bound=lower_bound, capacity_upper_bound=upper_bound,fix_cost=fixed_cost*gen['distance'][i]*time_period/8760,  # $/km, 
                        proportional_cost=0)      
                    
                    # print( "O" + str(8000 + gen['index'][i]) + str(number)+str("01"))
                    # print(fixed_cost*gen['distance'][i])
                # G.add_node("O" + str(8000+gen['index'][i])+str(101), names="O" + str(8000+gen['index'][i])+str(101), capacity_lower_bound=0,capacity_upper_bound=1e8, fix_cost=gen['cost'][i], proportional_cost=0)
        
                for l in range(len(time_multiplier)):
                    G.add_node('M' + str(8000+gen['index'][i])+str(103)+str(time_multiplier[l]), names='M' + str(8000+gen['index'][i])+str(103)+str(time_multiplier[l]), type='intermediate',
                        flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0, units=mat_units_GWh)
                    G.add_edge('M' + str(8000+gen['index'][i])+str(103)+str(time_multiplier[l]), "O" + str(8000+gen['index'][i]) + str(time_multiplier[l]), weight=1)

                    for n in range(len(cost_lookup.distribution)):
                        if time_resolution == 'monthly':
                            G.add_edge("O" + str(8000+gen['index'][i])+str(n)+str("01"), 'M' + str(8000+gen['index'][i])+str(103)+str(time_multiplier[l]), weight =730)
                        elif time_resolution == 'hourly':
                            G.add_edge("O" + str(8000+gen['index'][i])+str(n)+str("01"), 'M' + str(8000+gen['index'][i])+str(103)+str(time_multiplier[l]), weight =1)




    # connect gen with gxps        
    for ii in range(len(gen['name'])):  # matching gen to poc
            for jj in range(len(gxp['poc'])):
                for l in range(len(time_multiplier)):
                    if str(gen['poc'][ii]) == str(gxp['poc'][jj]):
                        if G.has_node("O" + str(8000 + gen['index'][ii]) + str(time_multiplier[l])) is True:
                            G.add_edge("O" + str(8000 + gen['index'][ii]) + str(time_multiplier[l]),
                                    "M" + str(gxp['index'][jj])+str(101) + str(time_multiplier[l]), weight=1/1000) #convert MWh to GWh
                    else:
                        pass

    # dem, link with gxp.
    # print(dem)
    behind_gxp_not_needed = {str(gxp['index'][jj]) for jj in range(len(gxp['index']))  }
    
    for kk in range(len(dem["dem_name"])):
        for jj in range(len(gxp['poc'])):
            if str(dem["dem_poc"][kk]) == str(gxp['poc'][jj]):
                behind_gxp_not_needed.discard(str(gxp['index'][jj]))
                for l in range(len(time_multiplier)):
                    # print("M" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]),":::",dem['max_flow'][kk])
                    G.add_node("O" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), names="O" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), capacity_lower_bound=0,capacity_upper_bound=1e8, fix_cost=dem['cost'][kk]/time_period, proportional_cost=dem['fpvv'][-1][l]*1000) #fix_cost=dem['cost'][kk]/time_period no assumed_cost_coefficient because it is just the network charges
                    G.add_node("M" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), names="M" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), type='product', 
                                flow_rate_lower_bound=dem['demand_node'][kk][l], flow_rate_upper_bound=1.01*dem['demand_node'][kk][l], price=0, units=mat_units_GWh)#modify dem['max_flow'][kk]
                    
                    G.add_edge("M" + str(gxp['index'][jj] +600) + str(time_multiplier[l]),
                                "O" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), weight=1.035)
                    G.add_edge("O" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]), "M" + str(dem["index"][kk]) + str(103) + str(time_multiplier[l]),weight =1)
                    

    transformer_nodes_v3(G,gxp_1,"DEMAND",cost_lookup.transformer,time_multiplier,time_resolution)
    transformer_nodes_v3(G,gxp_1,"GENERATION",cost_lookup.transformer,time_multiplier,time_resolution)
        
                    
    for jj in range(len(gxp['poc'])):        
        if gxp['dem'][jj] !=0:
            behind_gxp_not_needed.discard(str(gxp['index'][jj]))
        # print('as',behind_gxp_not_needed)
    for i in behind_gxp_not_needed:
        for l in range(len(time_multiplier)):
            G.remove_node("O" + str(int(i)+300)+str(time_multiplier[l]))
            G.remove_node("M" + str(int(i)+600)+str(time_multiplier[l]))


    # biomass is not included all demand nodes are removed
    # node = "M200810101"  # some node name
    for i in range(len(gxp['poc'])):
        
        for l in range(len(time_multiplier)):
            
            if G.in_degree("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))==0:
                # print("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))
                G.remove_node("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))
                # G.remove_node("O" + str(gxp['index'][i]) +str(102)+ str(time_multiplier[l]))
            
            if G.in_degree("M" + str(gxp['index'][i]+600) + str(time_multiplier[l]))==0:
                # print("M" + str(gxp['index'][i]+600) + str(time_multiplier[l]))
                G.remove_node("M" + str(gxp['index'][i]+600) + str(time_multiplier[l]))
                
    if base_sales_electricity_cost.empty:
            pass
    else:
        # with open('C:\\Users\\dc278\\OneDrive - The University of Waikato\\Documents\\GitHub\\P-graph-monte-carlo\\output_results\\time_multiplier.pkl', 'rb') as f:
        #     time_multiplier = pickle.load(f)   

        # with open('C:\\Users\\dc278\\OneDrive - The University of Waikato\\Documents\\GitHub\\P-graph-monte-carlo\\output_results\\dem.pkl', 'rb') as f:
        #     dem = pickle.load(f)
        for idx in range(len(dem['index'])):  # e.g., index = 500
            for l in range(len(time_multiplier)):  # e.g., time_multiplier = 12
                name = "O" + str(dem["index"][idx]) + str(103) + str(time_multiplier[l])
                
                if G.has_node(name) and name in base_sales_electricity_cost.index:
                    G.nodes[name]["proportional_cost"] = base_sales_electricity_cost.loc[name, "proportional_cost"]
                    # print(G.nodes[name],G.nodes[name]["proportional_cost"])
                    # print(G.nodes["O300910301"],G.nodes["O300910301"]["proportional_cost"])
        pass


    ME1 = ME1 + ME2

    P = Pgraph(problem_network=G, mutual_exclusion=ME1, solver="INSIDEOUT", max_sol=1)
    
    framework2 = "Pyomo"
    elapsed_time = time.time() - start_time
    print(elapsed_time,"seconds")

    if framework2 == "Pgraph":
        P.run()
        print((P.goolist))    
        main_excel_output(P, "INSIDEOUT",statement='no',file_path="output_stage2.xlsx")

    elif framework2 == "Pyomo":
        sys.path.append(r"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-convertor")
        from pgraph_convertor_v5 import run_file    
        P.create_solver_input()
        input_file_path = r"C:\Users\dc278\.conda\envs\pyomo_pgraph_converter\Lib\Pgraph/solver/input.in"
        model = run_file(input_file_path,output_file_path)


    elapsed_time = time.time() - start_time
    print(elapsed_time,"seconds")

    if elapsed_time <= 30:
        string = P.to_studio(path='./', file_name="studio_file_structure2.pgsx", verbose=False)
        print("Done exporting to P-graph Studio")
    #option to run pgraph solver

    # string = P.to_studio(path='./', file_name="studio_file.pgsx", verbose=False)  # export to p-graph studio
if __name__ == "__main__":

    
    
    base_sales_electricity_cost = pd.DataFrame()
    feasible_structures_2(base_sales_electricity_cost)