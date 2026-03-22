from pyomo.environ import *
import re
import os
from pyomo.opt import SolverFactory
import time
import pandas as pd
from pyomo.gdp import *
from collections import defaultdict
from collections import defaultdict
from pyomo.environ import *
from pyomo.common.errors import ApplicationError
from datetime import datetime
from pyomo.contrib.fbbt.fbbt import fbbt
# from pyomo.contrib.relaxation import (relax_integrality,linear_relaxation,_relaxation_data)
# from pyomo.contrib.infeasible import log_infeasible_constraints

# os.system('cls')
# Function to parse the P-graph input file and extract materials and operating units

def parse_defaults(file_path):
    # Initialize an empty dictionary to store the parsed defaults
    defaults = {}

    # Open the file and read the lines
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Define the pattern to match key-value pairs
    pattern = re.compile(r'(\w+)=([\w\.\-]+)')
    # Iterate through each line to find matches
    for line in lines:
        match = pattern.match(line.strip())
        if match:
            key = match.group(1)
            value = match.group(2)
            # print("whe",match)
            # print("whe1", key)
            # print("whe2",value)
            
            # Convert the value to the appropriate type
            if value.isdigit():
                value = int(value)
            elif re.match(r'^-?\d+(\.\d+)?([eE][-+]?\d+)?$', value):
                value = float(value)

            # Store in the dictionary
            defaults[key] = value

    # Assign defaults from the parsed values, or use the specified fallback if missing
    return {
        'capacity_lower_bound': defaults.get('operating_unit_capacity_lower_bound', 0),
        'capacity_upper_bound': defaults.get('operating_unit_capacity_upper_bound', 1000000000),
        'fix_cost': defaults.get('operating_unit_fix_cost', 0),
        'proportional_cost': defaults.get('operating_unit_proportional_cost', 0),
        'material_type': defaults.get('material_type', 'raw_material'),
        'flow_rate_lower_bound': defaults.get('material_flow_rate_lower_bound', 0),
        'flow_rate_upper_bound': defaults.get('material_flow_rate_upper_bound', 1000000000),
        'price': defaults.get('material_price', 0)
    }

def parse_pgraph_file(file_path,checking=False):
    start_time = time.time()
    default = parse_defaults(file_path)    
    materials = {}
    operating_units = {}
    material_to_ou_flows = {}
    mutually_exclusive_sets = []
    line_count = 0 
    current_section = None

    # Adjusted pattern with flexible field ordering and truly optional material_type
    pattern_1 = (
                    r'(?P<material_id>\w+):\s*'                  # Matches the material ID
                    r'(?P<material_type>\w+(?:_\w+)*)?'           # Matches the material type, allowing underscores
                    r'(?:,\s*price=(?P<price>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Matches price (optional, with optional decimal/scientific notation)
                    r'(?:,\s*flow_rate_lower_bound=(?P<lower_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Matches lower bound (optional)
                    r'(?:,\s*flow_rate_upper_bound=(?P<upper_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Matches upper bound (optional)
                    )

    pattern_2 = (
        r'(?P<material_id>\w+):\s*(?P<material_type>\w+)?'  # Match material ID and type
        r'(?:,\s*flow_rate_lower_bound=(?P<lower_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Optional lower bound
        r'(?:,\s*flow_rate_upper_bound=(?P<upper_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Optional upper bound
        r'(?:,\s*price=(?P<price>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Optional price
                    )

    
    with open(file_path, 'r') as file:
        
        lines = file.readlines()

        for line in lines:
            line_count +=1
            
            if line_count % 200000 == 0:
                print(f"Processed {line_count} lines")

            line = line.strip()

            # Extract materials
            if line.startswith("materials:"):
                current_section = "materials"
                continue
            elif line.startswith("operating_units:"):
                current_section = "operating_units"
                continue
            elif line.startswith("material_to_operating_unit_flow_rates:"):
                current_section = "flows"
                continue
            elif line.startswith("mutually_exlcusive_sets_of_operating_units:"):
                current_section = "mutually_exclusive"
                continue

            # Parse materials
            if current_section == "materials" and line:
                # match = re.match(
                #     r'(?P<material_id>\w+)\s*:\s*'
                #     r'(?P<material_type>\w+(?:_\w+)*)?'
                #     r'(?:\s*(?:,\s*)?price=(?P<price>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                #     r'(?:\s*(?:,\s*)?flow_rate_lower_bound=(?P<lower_bound>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                #     r'(?:\s*(?:,\s*)?flow_rate_upper_bound=(?P<upper_bound>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                #     ,
                #     line
                # )
                match = re.match(r'(?P<material_id>\w+)\s*:\s*'
                    r'(?P<material_type>\w+(?:_\w+)*)?'
                    r'(?:(?=.*\bprice=(?P<price>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)))?'
                    r'(?:(?=.*\bflow_rate_lower_bound=(?P<lower_bound>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)))?'
                    r'(?:(?=.*\bflow_rate_upper_bound=(?P<upper_bound>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)))?'
                    r'.*',
                    line)


                if match:                   


                    material_id = (match.group("material_id") if match.group("material_id") is not None else default["material_id"])

                    material_type = (match.group("material_type") if match.group("material_type") is not None else default["material_type"])

                    price = (match.group("price") if match.group("price") is not None else default["price"])

                    lower_bound = (match.group("lower_bound") if match.group("lower_bound") is not None else default["flow_rate_lower_bound"])

                    upper_bound = (match.group("upper_bound") if match.group("upper_bound") is not None else default["flow_rate_upper_bound"])
                            
                                    
                    materials[material_id] = {
                            'type': material_type,
                            'lower_bound': float(lower_bound),
                            'upper_bound': float(upper_bound),
                            'price': float(price)
                        }
                    
                    
                #print('test:',materials)
                        
            # Parse operating units
            elif current_section == "operating_units" and line:
                match = re.match(
                    r'(?P<ou_id>\w+)\s*:\s*'
                    r'(?:\s*(?:,\s*)?capacity_lower_bound=(?P<capacity_lower_bound>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                    r'(?:\s*(?:,\s*)?capacity_upper_bound=(?P<capacity_upper_bound>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                    r'(?:\s*(?:,\s*)?fix_cost=(?P<fix_cost>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                    r'(?:\s*(?:,\s*)?proportional_cost=(?P<proportional_cost>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?))?'
                    ,
                    line
                )

                if match:
                    ou_id = match.group("ou_id")

                    capacity_lower_bound = (
                        float(match.group("capacity_lower_bound"))
                        if match.group("capacity_lower_bound") is not None
                        else default["capacity_lower_bound"]
                    )

                    capacity_upper_bound = (
                        float(match.group("capacity_upper_bound"))
                        if match.group("capacity_upper_bound") is not None
                        else default["capacity_upper_bound"]
                    )

                    fix_cost = (
                        float(match.group("fix_cost"))
                        if match.group("fix_cost") is not None
                        else default["fix_cost"]
                    )

                    proportional_cost = (
                        float(match.group("proportional_cost"))
                        if match.group("proportional_cost") is not None
                        else default["proportional_cost"]
                    )

                
                    # Store the parsed data in the dictionary
                    operating_units[ou_id] = {
                        'capacity_lower_bound': capacity_lower_bound,
                        'capacity_upper_bound': capacity_upper_bound,
                        'fix_cost': fix_cost,
                        'proportional_cost': proportional_cost
                    }
                    # print(operating_units)
            # Parse material to operating unit flow rates
            elif current_section == "flows" and line:
                    match = re.match(r'(\w+):\s*(.+)\s*=>\s*(.+)', line)

                    if match:
                        ou_id = match.group(1)
                        inputs_str = match.group(2).strip()
                        outputs_str = match.group(3).strip()
                        # print('ou_id',ou_id)
                        
                        # Parse the inputs and outputs

                        pattern = r'(?:(\d*\.?\d+(?:[eE][-+]?\d+)?)\s*)?(\w+)'
                        inputs = re.findall(pattern, inputs_str)
                        outputs = re.findall(pattern, outputs_str)

                        
                        # Filter empty matches for cleaner output
                        inputs = [(coeff, material) for coeff, material in inputs if material]
                        outputs = [(coeff, material) for coeff, material in outputs if material]
                        # print(inputs)
                        # print(outputs)
                        if ou_id not in material_to_ou_flows:
                            material_to_ou_flows[ou_id] = {'inputs': [], 'outputs': []}

                        # Add parsed inputs
                        for coeff, material in inputs:
                            material_to_ou_flows[ou_id]['inputs'].append({
                                'coeff': float(coeff) if coeff else 1.0, # Default coefficient of 1.0
                                'material': material
                            })

                        # Add parsed outputs
                        for coeff, material in outputs:
                            material_to_ou_flows[ou_id]['outputs'].append({
                                'coeff': float(coeff) if coeff else 1.0,  # Default coefficient of 1.0
                                'material': material
                            })

                        # print(material_to_ou_flows)
            # Parse mutually exclusive sets of operating units
            elif current_section == "mutually_exclusive" and line:
                match = re.match(r'([\w]+)\s*:\s*([\w, ]+)', line)
                if match:
                    ou_set = match.group(2).replace(" ", "").split(',')
                    mutually_exclusive_sets.append(ou_set)   
                    # print("0",match.group(0))
      
        
        ou_to_material_flows = materials_to_ou_convertor(material_to_ou_flows)
        flow_data =  data_flow_coefficient(material_to_ou_flows)
        # print(flow_data)

        if checking==True:
            error_keys = find_empty_input_output_keys(materials,ou_to_material_flows)


    
    
    return materials, operating_units, mutually_exclusive_sets,flow_data,start_time


def materials_to_ou_convertor(material_to_ou_flows):
    ou_to_material_flows = {}

    for op, io_data in material_to_ou_flows.items():
        for entry in io_data['inputs']:
            material = entry['material']
            coeff = entry['coeff']
            # Ensure the material key exists in ou_to_material_flows
            if material not in ou_to_material_flows:
                ou_to_material_flows[material] = {'inputs': [], 'outputs': []}
            # Append the input data for the material
            ou_to_material_flows[material]['inputs'].append({'operating_unit': op, 'coeff': coeff})

        for entry in io_data['outputs']:
            material = entry['material']
            coeff = entry['coeff']
            # Ensure the material key exists in ou_to_material_flows
            if material not in ou_to_material_flows:
                ou_to_material_flows[material] = {'inputs': [], 'outputs': []}
            # Append the output data for the material
            ou_to_material_flows[material]['outputs'].append({'operating_unit': op, 'coeff': coeff})
            
            #print(ou_to_material_flows)
    return ou_to_material_flows


def find_empty_input_output_keys(materials,ou_to_material_flows):
    stream_keys = []
    material_keys = list(materials.keys())
    error_keys=[]

    for key, value in ou_to_material_flows.items():
        stream_keys.append(key)

    for key_2 in material_keys:
        if key_2 not in stream_keys:
            error_keys.append(key_2)
        else:
            pass
    if error_keys:
        print(f'You have some Material nodes that are not linked to anything')
        print(error_keys)  
    else:
        print(f'All good. No errors found for material side')   
    return error_keys



def data_flow_coefficient(material_to_ou_flows):
    # Transforming into the desired format
    data = []

    for ou, flows in material_to_ou_flows.items():
        # Iterate over inputs
        for entry in flows['inputs']:
            material = entry['material']
            coeff = entry['coeff']
            data.append((material, ou, coeff))
        
        # Iterate over outputs
        for entry in flows['outputs']:
            material = entry['material']
            coeff = entry['coeff']
            data.append((ou,material, coeff))

    # Display the transformed data
    return data


# Define the model
def build_pyomo_model(materials, operating_units, mutually_exclusive_sets, flow_data, start_time):
    model = ConcreteModel("P-graph Convertor (GDP)")

    
    # === Sets ===
    model.mutually_exclusive_sets = mutually_exclusive_sets

    
    model.MATERIALS = Set(initialize=materials.keys())
    model.NONPRODUCT_MATERIALS = Set(initialize=[m for m in materials if materials[m]['type'] != 'product'])
    model.OPERATING_UNITS = Set(initialize=operating_units.keys())
    model.Material_Unit_Pairs = Set(initialize=[(m, o) for m, o, _ in flow_data])

    # === Parameters: Materials ===
    # Keep your sign convention for raw materials
    model.material_lower_bound = Param(
        model.MATERIALS,
        initialize={m: (-materials[m]['upper_bound'] if materials[m]['type'] == 'raw_material'
                        else materials[m]['lower_bound']) for m in materials})
    
    model.material_upper_bound = Param(model.MATERIALS,
        initialize={m: (-materials[m]['lower_bound'] if materials[m]['type'] == 'raw_material'
                        else materials[m]['upper_bound']) for m in materials})
    
    model.material_price = Param(model.MATERIALS, initialize={m: materials[m]['price'] for m in materials})
    model.material_type  = Param(model.MATERIALS, initialize={m: materials[m]['type']  for m in materials}, within=Any)

    # === Parameters: Operating Units ===
    model.ou_capacity_lower_bound = Param(model.OPERATING_UNITS, initialize={ou: operating_units[ou]['capacity_lower_bound'] for ou in operating_units})
    model.ou_capacity_upper_bound = Param(model.OPERATING_UNITS, initialize={ou: operating_units[ou]['capacity_upper_bound'] for ou in operating_units})
    model.ou_fix_cost            = Param(model.OPERATING_UNITS, initialize={ou: operating_units[ou]['fix_cost'] for ou in operating_units})
    model.ou_proportional_cost   = Param(model.OPERATING_UNITS, initialize={ou: operating_units[ou]['proportional_cost'] for ou in operating_units})

    # === Parameters: Flow Coefficients ===
    coefficients = {(m, o): coeff for m, o, coeff in flow_data}
    model.Coeff = Param(model.Material_Unit_Pairs, initialize=coefficients)

    # === Variables ===
    # Base bounds must be wide enough to allow both branches;
    # the GDP "ON" branch will re-impose the tight [lower, upper] bounds,
    # the "OFF" branch will fix to 0.
    def mat_bounds(_model, m):
        if _model.material_type[m] == "product":
            # Products: use their normal bounds directly
            lb = float(_model.material_lower_bound[m])
            ub = float(_model.material_upper_bound[m])
        else:
            # Non-products: allow 0 in the base bounds so OFF disjunct is feasible
            lb = min(0.0, float(_model.material_lower_bound[m]))
            ub = max(0.0, float(_model.material_upper_bound[m]))
        return (lb, ub)

    def cap_bounds(_model, o): #this is applied for capacity nodes
        lb = 0.0  # capacity is nonnegative; OFF disjunct needs 0
        ub = float(_model.ou_capacity_upper_bound[o])
        return (lb, ub)

    model.material    = Var(model.MATERIALS, domain=Reals, bounds=mat_bounds)
    model.ou_capacity = Var(model.OPERATING_UNITS, domain=NonNegativeReals, bounds=cap_bounds)
    model.flows       = Var(model.Material_Unit_Pairs, domain=NonNegativeReals, initialize=1.0, bounds=(0, 1e8))
    model.total_cost  = Var(domain=Reals, initialize=0.0)

    # === Flow Constraints ===
    # For each (m,o) pair in flow_data: flow = coeff * capacity[o]
    def flow_constraint_rule(model, m, o):
        return model.flows[m, o] == model.Coeff[m, o] * model.ou_capacity[o] if o in model.OPERATING_UNITS else Constraint.Skip
    model.FlowConstraint = Constraint(model.Material_Unit_Pairs, rule=flow_constraint_rule)

    def flow_constraint_rule_1(model, o, m):
        return model.flows[o, m] == model.Coeff[o, m] * model.ou_capacity[o] if o in model.OPERATING_UNITS else Constraint.Skip
    model.FlowConstraint_1 = Constraint(model.Material_Unit_Pairs, rule=flow_constraint_rule_1)

    # === Precompute flow pairs for balance rule ===
    in_flows  = defaultdict(list)
    out_flows = defaultdict(list)
    for m, o in model.Material_Unit_Pairs.data():
        out_flows[m].append(o)  # m -> o contributes to material m's outflow
        in_flows[o].append(m)   # o -> m contributes to material m's inflow (if such pair exists)

    # === Flow Balance Constraint ===
    def flow_balance_rule(model, m):
        input_flow  = sum(model.flows[o, m] for o in in_flows[m]  if (o, m) in model.Material_Unit_Pairs)
        output_flow = sum(model.flows[m, o] for o in out_flows[m] if (m, o) in model.Material_Unit_Pairs)
        return input_flow == model.material[m] + output_flow
    model.FlowBalanceConstraint = Constraint(model.MATERIALS, rule=flow_balance_rule)

    # === GDP: OU capacity ON/OFF ===
    model.ou_off = Disjunct(model.OPERATING_UNITS)
    model.ou_on  = Disjunct(model.OPERATING_UNITS)

    for o in model.OPERATING_UNITS:
        model.ou_off[o].zero_cap = Constraint(expr=model.ou_capacity[o] == 0.0)
        lb = model.ou_capacity_lower_bound[o]
        ub = model.ou_capacity_upper_bound[o]
        model.ou_on[o].lb_cap = Constraint(expr=model.ou_capacity[o] >= lb)
        model.ou_on[o].ub_cap = Constraint(expr=model.ou_capacity[o] <= ub)

    def ou_choice_rule(model, o):
        return [model.ou_off[o], model.ou_on[o]]
    model.ou_choice = Disjunction(model.OPERATING_UNITS, rule=ou_choice_rule)

    # For each material: OFF => material == 0; ON => lower <= material <= upper
    model.mat_off = Disjunct(model.NONPRODUCT_MATERIALS)
    model.mat_on  = Disjunct(model.NONPRODUCT_MATERIALS)

    for m in model.NONPRODUCT_MATERIALS:
        model.mat_off[m].zero_mat = Constraint(expr=model.material[m] == 0.0)
        lb = model.material_lower_bound[m]
        ub = model.material_upper_bound[m]
        model.mat_on[m].lb_mat = Constraint(expr=model.material[m] >= lb)
        model.mat_on[m].ub_mat = Constraint(expr=model.material[m] <= ub)

    def mat_choice_rule(model, m):
        return [model.mat_off[m], model.mat_on[m]]
    model.mat_choice = Disjunction(model.NONPRODUCT_MATERIALS, rule=mat_choice_rule)

    # === Mutually Exclusive Constraints (at most one OU "ON" per set) ===
    # We can use the indicator_var of the "ON" disjuncts directly.
    # mutually_exclusive_sets is a list of iterables of OU names.
    
    
    def me_rule_factory(me_set):
        # capture the specific set in default argument to avoid late binding
        return Constraint(expr=sum(model.ou_on[o].binary_indicator_var for o in me_set) <= 1)

    for i, me_set in enumerate(mutually_exclusive_sets):
        setattr(model, f"MutuallyExclusive_{i}", me_rule_factory(me_set))

    # === Objective Function ===
    def total_cost_rule(model):
        material_cost  = sum(model.material_price[m] * model.material[m] for m in model.MATERIALS)
        
        operating_cost = sum(model.ou_fix_cost[ou] * model.ou_on[ou].binary_indicator_var +  # pay fix cost only when "ON"
            model.ou_proportional_cost[ou] * model.ou_capacity[ou]
            for ou in model.OPERATING_UNITS)
        return operating_cost - material_cost
    
    model.Objective = Objective(rule=total_cost_rule, sense=minimize)

    # === Apply GDP -> MIP reformulation ===
    # If you want to supply custom Big-Ms, you can pass a dict to bigM=...
    # Here we rely on variable bounds so the transformer can infer reasonable Ms.
    TransformationFactory('gdp.bigm').apply_to(model)

    all_vars = list(model.component_data_objects(Var, active=True))

    n_vars = len(all_vars)
    n_cons = len(list(model.component_data_objects(Constraint, active=True)))
    n_obj  = len(list(model.component_data_objects(Objective, active=True)))

    n_bin = sum(1 for v in all_vars if v.is_binary())
    n_cont = sum(1 for v in all_vars if v.is_continuous())
    n_int = sum(1 for v in all_vars if v.is_integer() and not v.is_binary())

    print("\n=== Model Size ===")
    print("Decision Variables:", n_vars)
    print("Constraints:", n_cons)
    print("Binary Variables:", n_bin)
    print("Continuous Variables:", n_cont)
    print("Integer Variables:", n_int)
    print("Objective Functions:", n_obj)

    return model, start_time


def solve_pyomo_model(model,start_time,output_file_path='pyomo_results.xlsx'):
    # # Now you can solve the model
    algorithm = 'gurobi' #gurobi,cbc,glpk,clp,bonmin,couenne,highs
    
    if algorithm == 'gurobi': #OK
        solver = SolverFactory(algorithm)
        solver.options["timeLimit"]     = 1600  # 1 hour time limit
        # solver.options["PoolSearchMode"] = 2
        # solver.options["PoolSolutions"]  = 10000
        # solver.options["PoolGap"]        = 0        
        solver.options['LogFile']        = 'gurobi.log'
        # solver.options["FeasibilityTol"] = 1e-6
        solver.options["MIPGap"]         = 0.01      # 1% optimality gap

    elif algorithm == 'cbc': #ok
        solver = SolverFactory("asl:cbc")
        solver.options['seconds'] = 800         # time limit in seconds
        solver.options['ratioGap'] = 0.01      # 1% optimality gap
        # solver = SolverFactory("asl:cbc", executable=r"C:\Users\dc278\DCPythonLibrary\ampl.mswin64\cbc.exe")

    elif algorithm == 'glpk': #OK
        solver = SolverFactory(algorithm)
    elif algorithm == 'clp': #OK
        solver = SolverFactory(algorithm)
    elif algorithm == 'bonmin': #OK but slow
        solver = SolverFactory(algorithm)
    elif algorithm == 'couenne': #OK
        solver = SolverFactory(algorithm)
    elif algorithm == 'ipopt': #OK for NLP problems only
        solver = SolverFactory(algorithm)
    elif algorithm == 'cplex': #Not OK
        solver = SolverFactory(algorithm, executable = r"C:\Users\dc278\DCPythonLibrary\ampl.mswin64\cplex.exe")
    elif algorithm == 'highs': #ok
        solver = SolverFactory("asl:highs")
        #solver = SolverFactory("asl:highs", executable= r"C:\Users\dc278\DCPythonLibrary\ampl.mswin64\highs.exe")
    
    try:  
        results = solver.solve(model, tee=True, keepfiles=True, logfile=f"{algorithm}.log", load_solutions=False)
    except Exception as e:
        return f"solve_error:{type(e).__name__}", None
    
    status = results.solver.status
    term = results.solver.termination_condition    
    # 2) Check if there is at least one solution in the results
    has_solution = getattr(results, "solution", None) is not None and len(results.solution) > 0

    label = "optimal_or_feasible" if has_solution else f"no_solution_{term}"

    if has_solution:
        model.solutions.load_from(results)
    
    # Try to evaluate objective safely (only meaningful if solution exists)
    if has_solution:
        try:
            total_cost = value(model.Objective)
            print(f"Total Cost: {total_cost:e}")
        except Exception as e:
            print("Error evaluating objective:", e)
            total_cost = None
    else:
        total_cost = None
    

    # --- Print results ---
    # print("Solver Status:", results.solver.status)
    # print("Solver Termination Condition:", results.solver.termination_condition)
    # print(results)


    # Try to compute elapsed time regardless
    elapsed_time = time.time() - start_time
    print(f"Time elapsed done: {elapsed_time:.2f} seconds\n")

    # --- Build output tables (with safe .value access) ---
    def safe_value(v):
        try:
            return v.value
        except:
            return None

    materials_data = {
        'ID': list(model.MATERIALS),
        'Lower Bound': [model.material_lower_bound[m] for m in model.MATERIALS],
        'Upper Bound': [model.material_upper_bound[m] for m in model.MATERIALS],
        'Price': [model.material_price[m] for m in model.MATERIALS],
        'Type': [model.material_type[m] for m in model.MATERIALS],
        'Flow': [safe_value(model.material[m]) for m in model.MATERIALS]
    }
    materials_data['Cost'] = [
        pc * cm if cm is not None else None
        for pc, cm in zip(materials_data['Price'], materials_data['Flow'])
    ]

    operating_units_data = {
        'ID': list(model.OPERATING_UNITS),
        'Lower Bound': [model.ou_capacity_lower_bound[ou] for ou in model.OPERATING_UNITS],
        'Upper Bound': [model.ou_capacity_upper_bound[ou] for ou in model.OPERATING_UNITS],
        'Proportional Cost': [model.ou_proportional_cost[ou] for ou in model.OPERATING_UNITS],
        'Fix Cost': [model.ou_fix_cost[ou] for ou in model.OPERATING_UNITS],  # ← raw fix cost (no binary)
        'Capacity Multiplier': [safe_value(model.ou_capacity[ou]) for ou in model.OPERATING_UNITS],
    }
    operating_units_data['Cost'] = [
        (pc * cm + fc * safe_value(model.ou_on[ou].binary_indicator_var))
        if cm is not None else None
        for ou, pc, cm, fc in zip(
            model.OPERATING_UNITS,
            operating_units_data['Proportional Cost'],
            operating_units_data['Capacity Multiplier'],
            operating_units_data['Fix Cost']
        )
    ]
    mutually_exclusive_sets = model.mutually_exclusive_sets
    mutually_exclusive_df = pd.DataFrame(
        [(me_set[0], me_set[1]) for me_set in mutually_exclusive_sets],
        columns=["0", "1"]
    )
    
    flows_data = {
        'Source': [m for (m, ou) in model.Material_Unit_Pairs],
        'Destination': [ou for (m, ou) in model.Material_Unit_Pairs],
        'Weight': [model.Coeff[m, ou] for (m, ou) in model.Material_Unit_Pairs],
        'Flow Rate': [safe_value(model.flows[m, ou]) for (m, ou) in model.Material_Unit_Pairs]
    }

    # Convert dictionaries to DataFrames
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    solution_df = pd.DataFrame({
        "Index": ["Objective Value", "Solver Status", "Termination Condition","Timestamp"],
        "Value": [total_cost, str(results.solver.status), str(results.solver.termination_condition), timestamp]
    })

    
    
    materials_df = pd.DataFrame(materials_data)
    operating_units_df = pd.DataFrame(operating_units_data)
    flows_df = pd.DataFrame(flows_data)

    # Export to Excel (always runs)
    if not output_file_path or not str(output_file_path).strip():
        output_file_path = "pyomo_results.xlsx"
    
    # print("Output file path:", output_file_path)

    with pd.ExcelWriter(output_file_path) as writer:
        solution_df.to_excel(writer, sheet_name='Solution', index=False)
        materials_df.to_excel(writer, sheet_name='Materials', index=False)
        operating_units_df.to_excel(writer, sheet_name='Operating Units', index=False)
        flows_df.to_excel(writer, sheet_name='Flows', index=False)
        mutually_exclusive_df.to_excel(writer, sheet_name='ME', index=False)

    print(f"Results exported to {output_file_path}")
    if has_solution:
        return total_cost, results
    else:
        return f"no_solution_{term}", results
    
    
    
def run_file(file_path,output_file_path=None):
    print('Program starts')
    materials, operating_units, mutually_exclusive_sets,flow_data,start_time = parse_pgraph_file(file_path,checking=False)
    print('Parsed Successfully.Now building problem table')
    model,start_time = build_pyomo_model(materials, operating_units, mutually_exclusive_sets,flow_data,start_time)
    print('Pyomo model Succesfully Developed')
    total_cost, results = solve_pyomo_model(model,start_time,output_file_path)
    return total_cost


def check_run(file_path):
    parse_pgraph_file(file_path,checking=False)

    

# Usage
# file_path = 'input.in.txt'


# model = run_file(file_path)

#checking errors in this file:
#file_path ="input.in copy.txt"

# model = check_run(file_path)
