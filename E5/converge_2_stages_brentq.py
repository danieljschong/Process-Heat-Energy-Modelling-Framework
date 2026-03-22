import pandas as pd
import subprocess
import pipeline_flow_2 as pf2
#this file does all the calculation for convergence
import time
import sys
import pickle
from pipeline_flow_2 import sales_electricity_price_from_first_stage_first_iteration,is_o3_103
from scipy.optimize import root_scalar
import numpy as np


sys.path.append(r"C:\Users\dc278\OneDrive - The University of Waikato\Documents\GitHub\P-graph-convertor")
import pgraph_convertor_v5 as pyo
import pgraph_filter_feasible_structures_3 as fs3


# Load the Excel file and read the specific sheet
def process_stage_1(file_path_stage_1_results=None, first_stage_costing_path="first_stage_output_costings.xlsx",fraction=1.0):
    #stage 1 checks the electricity price
    # Load file (auto-detect extension)
    if file_path_stage_1_results.endswith(".csv"):
        df_stage_1 = pd.read_csv(file_path_stage_1_results)
    else:
        df_stage_1 = pd.read_excel(file_path_stage_1_results, sheet_name="Operating Units")
    df_stage_1.columns = df_stage_1.columns.str.strip().str.lower().str.replace(" ", "_")

    def filter_id_stage_1(row):
        id_str = row['id']

        if not isinstance(id_str, str):
            return False
        # Electricity price for factories
        if id_str.startswith("O3") and len(id_str) == 10:
            if id_str[5:8] == "103":
                # print("Electricity price for factories:", row)
                return True
        return False

    # Apply filter
    filtered_df_stage_1 = df_stage_1[df_stage_1.apply(filter_id_stage_1, axis=1)]

    # Export filtered dataframe
    filtered_df_stage_1.to_excel(first_stage_costing_path, index=False)

    # Calculate total cost
    non_industrial_cost_2022 = 0 #refer to thesis writing tables 1.01E+10
    
    non_industrial_cost_proj = fraction * (1.45E+09 +1.01E+10)

    total_cost_1 = filtered_df_stage_1['cost'].sum() + non_industrial_cost_2022 + non_industrial_cost_proj

    print(f"Stage 1 Total Cost: {total_cost_1:.2f}")

    return total_cost_1


###### Stage 2 #############
# --- Define filtering rules for Operating Units ---
def filter_id_stage_2(row):
        id_val = row.get("id", None)
        if pd.isna(id_val):
            return False

        id_str = str(id_val)
        length = len(id_str)

        #proportional cost related
        if id_str.startswith("O14") and length == 6: #gen capacity O14017
            return True        
        # if id_str.startswith("O6") and length == 8: # O6009101 transmisison opex
        #     return True        
        if id_str.startswith("O8") and length == 15: #O80004130010013 biomass supply transport
            return True
        # if id_str.startswith("O18") and length == 8: #O1800704 subtransmission opex
        #     return True

        #fix cost related
        if id_str.startswith("O6") and length == 12 and id_str[5] == "6": #O60086009473 transmisison capex
            return True
        if id_str.startswith("O18") and length == 9 and id_str.endswith("01"): #subtransmission capacity  O18017001
            return True        
        if id_str.startswith("O3") and length == 10 and id_str[5:8] == "103":  #distribution lines upgrades and base electricity price
            return True        
        if id_str.startswith("O2") and length == 8 and id_str.endswith("53"): # O2311253  transformer sup and dem
            try:
                mid = int(id_str[2:5])
            except ValueError:
                return False
            if 0 <= mid <= 299:
                # print("supply transformer")
                return True
            if 300 <= mid <= 600:
                # print("demand transformer")
                return True
            return False

        return False

def process_stage_2(file_path_stage_2_results,second_stage_costing_path="second_stage_output_costings.xlsx"):
    # settings = "pyomo"
    # --- Load source files based on settings ---
    # if settings == "pyomo":
    #     file_path_stage_2 = "pyomo_results.xlsx"
    # elif settings == "pgraph":
    #     file_path_stage_2 = "p_graph_stage2_output.xlsx"
    # else:
    #     raise ValueError("settings must be either 'pyomo' or 'pgraph'")

    # --- Read required sheets ---
    #check costings for all auxillary components
    sheet_name_2 = "Operating Units"
    sheet_name_3 = "Materials"

    df_stage_2_Op = pd.read_excel(file_path_stage_2_results, sheet_name=sheet_name_2)
    df_stage_2_Mat = pd.read_excel(file_path_stage_2_results, sheet_name=sheet_name_3)

    # --- Clean and standardize column names ---
    df_stage_2_Op.columns = df_stage_2_Op.columns.str.strip().str.lower().str.replace(" ", "_")
    df_stage_2_Mat.columns = df_stage_2_Mat.columns.str.strip().str.lower().str.replace(" ", "_")

    # --- Filter Materials sheet (IDs starting with M19) ---
    df_stage_2_Mat['id'] = df_stage_2_Mat['id'].astype(str)
    filtered_df_2_Mat = df_stage_2_Mat[
    (df_stage_2_Mat["id"].astype(str).str.startswith("M10")) &
    (df_stage_2_Mat["id"].astype(str).str.len() == 11) &
    (df_stage_2_Mat["id"].astype(str).str[6:9] == "260")] #bM1001726002 green methanol
    # print(filtered_df_2_Mat)
    
    # --- Apply filter and remove zero-cost rows ---
    mask = df_stage_2_Op.apply(lambda row: bool(filter_id_stage_2(row) or False), axis=1)
    filtered_df_2_Op = df_stage_2_Op[mask]
    filtered_df_2_Op = filtered_df_2_Op[filtered_df_2_Op["cost"].fillna(0) != 0]
    
    # -------------------------------------------------
    # --- Split Operating Units into O18 and others ---
    # -------------------------------------------------
    filtered_df_2_Op["id"] = filtered_df_2_Op["id"].astype(str)

    filtered_df_Op_operating = filtered_df_2_Op[
        filtered_df_2_Op["id"].str.startswith("O18", na=False)
    ]

    filtered_df_Op_capital = filtered_df_2_Op[
        ~filtered_df_2_Op["id"].str.startswith("O18", na=False)
    ]

    filtered_df_Op_operating_cost = filtered_df_Op_operating["cost"].sum()
    filtered_df_Op_capital_cost = filtered_df_Op_capital["cost"].sum()
    
    # print(f"O18 Operating cost: {filtered_df_Op_operating_cost:.2f}")
    # print(f"Capital cost (other Operating Units): {filtered_df_Op_capital_cost:.2f}")
    
    # --- Export filtered data ---
    with pd.ExcelWriter(second_stage_costing_path, engine="openpyxl") as writer:
        filtered_df_2_Op.to_excel(writer, sheet_name="Operating Units", index=False)
        filtered_df_2_Mat.to_excel(writer, sheet_name="Materials", index=False)


    # --- Compute total cost ---
    non_industrial_cost_2022 = 1.01E+10 #refer to thesis writing tables
    operating_cost = filtered_df_Op_operating_cost          # already summed
    capital_cost = filtered_df_Op_capital_cost              # already summed
    materials_cost = filtered_df_2_Mat["cost"].sum()
    # total_cost_2 = operating_cost + 2 * capital_cost - materials_cost + non_industrial_cost_2022
    total_cost_2 = operating_cost + 1* capital_cost - materials_cost + non_industrial_cost_2022

    print(f"Stage 2 Total Cost: {total_cost_2:.2f}")
    
    #upgrade cost sensivity analysis

    return total_cost_2

def converge_two_stages():
    start_time = time.time()
    fraction = 1.0
    coefficient = 1
    iteration = 0
    tolerance = 0.01
    max_iter = 100
    previous_direction = None
    same_direction_count = 0
    max_same_direction = 2

    # ------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------
    MONTE_CARLO_CODES = PROJECT_ROOT / "monte_carlo_codes"
    PGRAPH_TWO_STAGE_ROOT = PROJECT_ROOT.parent / "P-graph-two-stage-optimisation"

    if PGRAPH_TWO_STAGE_ROOT.exists() and str(PGRAPH_TWO_STAGE_ROOT) not in sys.path:
        sys.path.append(str(PGRAPH_TWO_STAGE_ROOT))

    while iteration < max_iter:
        if iteration == 0:
            first_stage_file = MONTE_CARLO_CODES / "backup_test4.py"
            first_stage_input_results1 = PROJECT_ROOT / "pyomo_results.xlsx"
            second_stage_input_file = PROJECT_ROOT / "pyomo_results.xlsx"
            second_stage_input_results = PGRAPH_TWO_STAGE_ROOT / "pyomo_results.xlsx"

            # subprocess.run(["python", str(first_stage_file)], check=True)

            base_sales_electricity_cost = pd.DataFrame()
            fs3.feasible_structures_2(
                base_sales_electricity_cost=base_sales_electricity_cost,
                input_file_path=second_stage_input_file,
            )
            
            total_cost_1 = process_stage_1(file_path_stage_1_results= first_stage_input_results1)    
            total_cost_2 = process_stage_2(file_path_stage_2_results=second_stage_input_results)
            base_sales_electricity_cost = sales_electricity_price_from_first_stage_first_iteration()
            print("Iteration 0 completed.")
        elif iteration == 1:  
            print("Starting iteration 1")   
            materials, operating_units, ME, connections =pf2.extract_data_from_excel(first_stage_input_results1,fraction)
            
            first_stage_input_results1 = f"C:/Users/dc278/OneDrive - The University of Waikato/Documents/GitHub/P-graph-two-stage-optimisation/pipeline_loop/pyomo_results_first{iteration+1}.xlsx"
            second_stage_input_results1 = f"C:/Users/dc278/OneDrive - The University of Waikato/Documents/GitHub/P-graph-two-stage-optimisation/pipeline_loop/pyomo_results_second{iteration+1}.xlsx"

            model,start_time = pyo.build_pyomo_model(materials, operating_units, ME,connections,start_time)
            results = pyo.solve_pyomo_model(model,start_time,output_file_path=first_stage_input_results1)        
            fs3.feasible_structures_2(base_sales_electricity_cost=base_sales_electricity_cost,input_file_path = first_stage_input_results1, output_file_path = second_stage_input_results1)
            total_cost_1 = process_stage_1(file_path_stage_1_results= first_stage_input_results1)    
            total_cost_2 = process_stage_2(file_path_stage_2_results=second_stage_input_results1)            
            print("Iteration 1 completed.")

        
        else:
            materials, operating_units, ME, connections =pf2.extract_data_from_excel(first_stage_input_results1,fraction)
            first_stage_input_results1 = f"pipeline_loop/pyomo_results_first{iteration+1}.xlsx"
            second_stage_input_results1 = f"pipeline_loop/pyomo_results_second{iteration+1}.xlsx"            
            model,start_time = pyo.build_pyomo_model(materials, operating_units, ME,connections,start_time)
            results = pyo.solve_pyomo_model(model,start_time,output_file_path=first_stage_input_results1)
            fs3.feasible_structures_2(base_sales_electricity_cost=base_sales_electricity_cost,input_file_path = first_stage_input_results1, output_file_path = second_stage_input_results1)
            total_cost_1 = process_stage_1(file_path_stage_1_results= first_stage_input_results1)    
            total_cost_2 = process_stage_2(file_path_stage_2_results=second_stage_input_results1)   
            print(f"Iteration {iteration} completed.")

        difference = (total_cost_1 - total_cost_2)/total_cost_1

    
        if abs(difference) < tolerance:
            print("✅ Convergence achieved!")
            break
        elif   abs(difference)> 0.1   :
            if total_cost_1 > total_cost_2:
                direction = 'down'

                fraction = 0.9
                coefficient *= fraction
                # print(fraction)
                print("The assumed cost needs to be lowered")
            else:
                fraction = 1.1
                coefficient *= fraction
                
                direction = 'up'
                # print(fraction)

                print("The assumed cost needs to be increased")            
        else:    
            if total_cost_1 > total_cost_2:
                fraction = 0.99 
                coefficient *= fraction
                
                direction = 'down'

                # print(fraction)
                print("The assumed cost needs to be lowered")
            else:
                fraction = 1.01
                coefficient *= fraction
                
                # print(fraction)
                direction = 'up'
                print("The assumed cost needs to be increased")

        # Directional consistency check
        if direction == previous_direction:
            same_direction_count += 1
        else:
            same_direction_count = 1
            previous_direction = direction

        # if same_direction_count >= max_same_direction:
        #     raise RuntimeError(f"❌ Convergence failed: Direction '{direction}' repeated {same_direction_count} times. Divergence suspected.")

        iteration += 1        
    print("Done")      
    print("The coefficient factor of increase is:", coefficient)

    total_elapsed = time.time() - start_time
    print(f"\n🕒 Total time elapsed: {total_elapsed:.2f} seconds")
    
    
def evaluate_cost_gap( fraction: float,    base_sales_electricity_cost,    first_stage_input_results_prev: str,    iteration_tag: str = "brent",):
    materials, operating_units, ME, connections = pf2.extract_data_from_excel(first_stage_input_results_prev,fraction)
    first_out = f"pipeline_loop/pyomo_results_first_{iteration_tag}_{fraction:.6f}.xlsx"
    second_out = f"pipeline_loop/pyomo_results_second_{iteration_tag}_{fraction:.6f}.xlsx"

    model, _ = pyo.build_pyomo_model(materials, operating_units, ME, connections, time.time())
    _ = pyo.solve_pyomo_model(model, time.time(), output_file_path=first_out)

    # 2) Run feasible structures + second stage
    fs3.feasible_structures_2(base_sales_electricity_cost=base_sales_electricity_cost,input_file_path=first_out,output_file_path=second_out)

    # 3) Compute the two costs
    total_cost_1 = process_stage_1(file_path_stage_1_results=first_out,fraction=fraction)
    total_cost_2 = process_stage_2(file_path_stage_2_results=second_out)

    # Root when equal
    return float((total_cost_1 - total_cost_2)/total_cost_1)

def find_bracket(func, x0=1.0, step=0.05, grow=1.6, max_tries=25):
    a = x0
    fa = func(a)

    b = x0
    fb = func(b)

    # Expand outward alternating low/high
    low = x0
    high = x0

    for _ in range(max_tries):
        low = max(1e-6, low * (1 - step))
        high = high * (1 + step)

        flow = func(low)
        fhigh = func(high)

        if flow == 0:
            return low, low
        if fhigh == 0:
            return high, high

        if np.sign(flow) != np.sign(fhigh):
            return low, high

        step *= grow

    raise RuntimeError("Could not find a sign-changing bracket for brentq/root_scalar.")

def converge_two_stages_with_brentq():
    # Run your iteration 0 exactly once to get base_sales_electricity_cost etc.
    first_stage_file = PROJECT_ROOT / "monte_carlo_codes" / "backup_test4.py"
    first_stage_input_results0 = PROJECT_ROOT / "pyomo_results.xlsx"
    second_stage_input_file = PROJECT_ROOT / "pyomo_results.xlsx"

    # subprocess.run(["python", str(first_stage_file)], check=True)
    
    
    base_sales_electricity_cost = pd.DataFrame()
    fs3.feasible_structures_2(base_sales_electricity_cost=base_sales_electricity_cost,
        input_file_path=second_stage_input_file)
    base_sales_electricity_cost = sales_electricity_price_from_first_stage_first_iteration()

    # Define objective for SciPy
    def f(frac):
        return evaluate_cost_gap(
            fraction=float(frac),base_sales_electricity_cost=base_sales_electricity_cost,first_stage_input_results_prev=first_stage_input_results0, iteration_tag="brent")

    # Find bracket
    a, b = find_bracket(f, x0=1.0)

    # Root solve
    sol = root_scalar(f, bracket=(a, b), method="brentq", xtol=1e-3, rtol=1e-6, maxiter=30)

    if not sol.converged:
        raise RuntimeError("Root solve did not converge")


    print("✅ Converged fraction (alpha):", sol.root)
    print("Relative gap at root:", f(sol.root))
    print("Iterations:", sol.iterations)
    print("Implied electricity proportional_cost change vs base:", (sol.root - 1.0) * 100.0, "%")

    return sol.root




if __name__ == "__main__":
    # #base case for 2022
    # converge_two_stages()
    
    converge_two_stages_with_brentq()