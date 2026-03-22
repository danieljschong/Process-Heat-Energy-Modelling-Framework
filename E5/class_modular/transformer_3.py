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


from national_energy_modelling_function_file import *
from pgraph_output_class_value import *
from pgraph_csv_organiser_rev import *
import email_myself
from pgraph_excel_organiser import *
from pgraph_output_organiser import *



G = nx.DiGraph()

time_period = 1
time_multiplier = []
for l in range(1, time_period+1):
    time_multiplier.append((f"{l:02}"))
island="North"    
    
    
gxp_edited_path = r"reference_file.xlsx"


Pd_GXP_edited = pd.read_excel(gxp_edited_path, sheet_name="GXP_edited_v4", header=0, index_col=None, usecols="A:AX", nrows=217)

Pd_keys = ['Index', 'POC','North_South', 'Demand Transformers Req',	'Dem Large transformer',	'Dem Medium transformer',	'Dem Small transformer',	'Dem Tiny transformer',
           'Supply Transformers Req',	'Sup Large transformer',	'Sup Medium transformer',	'Sup Small transformer',	'Sup Tiny transformer','N Demand (MVA)','N Generation (MVA)' ]
Pd_GXP = python_dict_dot_notation(Pd_keys, Pd_GXP_edited)

# print(Pd_GXP)

compiled_data_path = "C:\\Users\\dc278\\Downloads\\Royal_Soc\\Grid stuff\\RETA ERGO reports\\compiled.xlsx"
Pd_transformer = pd.read_excel(compiled_data_path, sheet_name="ERGO transformer cost", header=0, index_col=None, usecols="A:H", nrows=4)
Pd_keys = ['Lower Bound (MVA)',	'Upper Bound (MVA)', 'TAC']
Pd_transformer_cost = python_dict_dot_notation(Pd_keys, Pd_transformer)

transformer_cost_lookup = {
    (row['Lower Bound (MVA)'], row['Upper Bound (MVA)']): row['TAC']
    for _, row in Pd_transformer.iterrows()
}
# print(Pd_transformer_cost)


gxp = {'index':[], 'POC':[], 'Num_of_Dem_transformer':[], 'Num_of_Sup_transformer':[],'Dem_transformer':[],'Sup_transformer':[],'N_Demand_MVA':[],'N_Generation_MVA':[]}
dem_cols = ['Dem Large transformer',	'Dem Medium transformer',	'Dem Small transformer',	'Dem Tiny transformer']
gen_cols = ['Sup Large transformer',	'Sup Medium transformer',	'Sup Small transformer',	'Sup Tiny transformer']

for idx, row in Pd_GXP_edited.iterrows():
    if str(Pd_GXP.North_South[idx]) != island:

        gxp['index'].append(row['Index'])  # Still get row-by-row values
        gxp['POC'].append(row['POC'])
        gxp['Num_of_Dem_transformer'].append(row['Demand Transformers Req'])
        gxp['Num_of_Sup_transformer'].append(row['Supply Transformers Req'])

        gxp['Dem_transformer'].append([row[col] for col in dem_cols])
        gxp['Sup_transformer'].append([row[col] for col in gen_cols])
        
        gxp['N_Demand_MVA'].append(row['N Demand (MVA)'])
        gxp['N_Generation_MVA'].append(row['N Generation (MVA)'])

# print("who",gxp)

# print(transformer_cost_lookup)
def transformer_nodes_v3(G,gxp,side,transformer_cost_lookup,time_multiplier,time_resolution):
    # tuples to list
    lower_bound_list = [key[0] for key in transformer_cost_lookup.keys()]
    upper_bound_list = [key[1] for key in transformer_cost_lookup.keys()]
    tac_list = list(transformer_cost_lookup.values())
    # print("Works")
    time_period=(len(time_multiplier))
    if side=='DEMAND':
        number_of_transformers = gxp['Num_of_Dem_transformer']
        transformer = gxp['Dem_transformer']
        suffix = ""
        stream_addition = 300
        outlet_addition = 600
    elif side =='GENERATION':
        number_of_transformers = gxp['Num_of_Sup_transformer']
        transformer = gxp['Sup_transformer']
        suffix = "101"
        stream_addition = 0
        outlet_addition = 0

    else:
        print("error")

    # print(gxp)    
    up_down_value = 1 #useful for piecewise linear function
    list_product = [13,9,6,4,6,1,6,1]    #drop when deployed
    for i in range(len(gxp['index'])):
        # print("M"+str(2000+gxp['index'][i])+str(101)+("01"))

        stream = str((2000)+stream_addition+gxp['index'][i])
        inlet_stream = str((2000)+gxp['index'][i]) + str(suffix) 
        outlet_stream = str((2000)+outlet_addition+gxp['index'][i])
        #            if G.in_degree("M" + str(gxp['index'][i]) +str(101)+ str(time_multiplier[l]))==0:
        if G.in_degree("M"+str(2000+gxp['index'][i])+str(101)+("01"))==0 and side=="GENERATION":#str(01 is actually time multiplier)
            # print("M"+str(2000+gxp['index'][i])+str(101)+("01"))
            pass
        elif G.out_degree("M"+str(2600+gxp['index'][i])+("01"))==0 and side=="DEMAND":#str(01 is actually time multiplier)
            # print("M"+str(2600+gxp['index'][i])+("01"))
            pass
        else:
            # print(gxp['index'][i])
            if number_of_transformers[i] > 0:            
                for l in range(len(time_multiplier)):    #remember to change product to raw material when used, also drop list_product
                    if G.has_node("M"+inlet_stream+time_multiplier[l])==False:
                        G.add_node("M"+inlet_stream+time_multiplier[l],names="M"+inlet_stream+time_multiplier[l],type='raw_material', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0) 
                        G.add_node("M"+outlet_stream+time_multiplier[l],names="M"+outlet_stream+time_multiplier[l],type='product', flow_rate_lower_bound=list_product[l], flow_rate_upper_bound=1.1*list_product[l], price=0) #remove when necessary
                        print('Error in Transformer')
                        
                    elif G.has_node("M"+inlet_stream+time_multiplier[l])==True:
                        G.add_node("M"+inlet_stream+time_multiplier[l],names="M"+inlet_stream+time_multiplier[l],type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0) 
                        G.add_node("M"+outlet_stream+time_multiplier[l],names="M"+outlet_stream+time_multiplier[l],type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0) #remove when necessary
            
    
                for l in range(len(time_multiplier)):
                    G.add_node(f"M{stream}59{time_multiplier[l]}",names=f"M{stream}59{time_multiplier[l]}",type='intermediate',
                        flow_rate_lower_bound=0,flow_rate_upper_bound=1e6,price=0)
                    G.add_node(f"O{stream}57{time_multiplier[l]}",names=f"O{stream}57{time_multiplier[l]}",capacity_lower_bound=0,
                        capacity_upper_bound=1e8,fix_cost=0,proportional_cost=0)
                    
                    G.add_edge(f"M{stream}59{time_multiplier[l]}", f"O{stream}57{time_multiplier[l]}", weight=1)
                    G.add_edge(f"M{inlet_stream}{time_multiplier[l]}", f"O{stream}57{time_multiplier[l]}", weight=1)
                    G.add_edge(f"O{stream}57{time_multiplier[l]}", f"M{outlet_stream}{time_multiplier[l]}", weight=0.99)

                # ✅ Add transformers
                transformer_count = 1  # unique transformer node counter
                # print(transformer)

                for m in range(4):  # loop over 4 transformer types
                    num_transformers = transformer[i][m]
                    if num_transformers > 0:
                        lower_bound = lower_bound_list[m]
                        upper_bound = upper_bound_list[m]
                        transformer_capacity_fix_cost = tac_list[m]

                        for n in range(num_transformers):
                            str_n = f"{transformer_count:01}"  # pad transformer number
                            upgrades_node_name = f"O{stream}{str_n}53"
                            # print(node_name)
                            # print(f"✅ Adding transformer node: {node_name} with capacity: {lower_bound}-{upper_bound} MVA and fix cost: {transformer_capacity_fix_cost}")
                            # if n<4:
                            #     n=1
                            if time_resolution == "monthly":
                                G.add_node(upgrades_node_name,names=upgrades_node_name,capacity_lower_bound=lower_bound,capacity_upper_bound=upper_bound*(n+1),
                                        fix_cost=transformer_capacity_fix_cost/12*time_period*(n+1),proportional_cost=0)  # you can customize this if needed
                                # print("time_period",time_period)
                            elif time_resolution == "hourly":
                                G.add_node(upgrades_node_name,names=upgrades_node_name,capacity_lower_bound=lower_bound,capacity_upper_bound=upper_bound*(n+1),
                                        fix_cost=transformer_capacity_fix_cost/8760*time_period*(n+1),proportional_cost=0)  # you can customize this if needed  
                                                        
                            for l in range(len(time_multiplier)):
                                if time_resolution == "monthly":
                                    G.add_edge(upgrades_node_name,f"M{stream}59{time_multiplier[l]}",weight=730 / 1000)  # monthly hours divided for GWh
                                elif time_resolution == "hourly":
                                    G.add_edge(upgrades_node_name,f"M{stream}59{time_multiplier[l]}",weight=1 / 1000)  # monthly hours divided for GWh

                            transformer_count += 1  # increment for next transformer

    # print("why",gxp)
    for i in range(len(gxp['index'])): #to build existing transformers
        if side == "GENERATION" and G.in_degree("M"+str(2000+gxp['index'][i])+str(101)+("01"))!=0:
            existing=str((2000+gxp['index'][i]))+str(102)
            G.add_node("O"+existing, names="O"+existing, capacity_lower_bound=0,capacity_upper_bound=gxp['gen_n'][i], fix_cost=0, proportional_cost=0)
            for l in range(len(time_multiplier)):
                if time_resolution == "monthly":
                    G.add_edge("O"+existing,f"M{(2000+gxp['index'][i])}59{time_multiplier[l]}",weight=730 / 1000)
                elif time_resolution == "hourly":
                    G.add_edge("O"+existing,f"M{(2000+gxp['index'][i])}59{time_multiplier[l]}",weight=1 / 1000)
                
        elif side == "DEMAND" and G.out_degree("M"+str(2600+gxp['index'][i])+("01"))!=0:
            existing=str((2300+gxp['index'][i]))
            G.add_node("O"+existing, names="O"+existing, capacity_lower_bound=0,capacity_upper_bound=gxp['dem_n'][i], fix_cost=0, proportional_cost=0)
            for l in range(len(time_multiplier)):
                if time_resolution == "monthly":
                    G.add_edge("O"+existing,f"M{(2300+gxp['index'][i])}59{time_multiplier[l]}",weight=730 / 1000)
                elif time_resolution == "hourly":
                    G.add_edge("O"+existing,f"M{(2300+gxp['index'][i])}59{time_multiplier[l]}",weight=1 / 1000)
                
            
            
            
            
            
            
            
            
            
        