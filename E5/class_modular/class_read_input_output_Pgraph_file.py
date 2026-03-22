
import re
import time
import pandas as pd


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
    #print('default=',default)
    
    materials = {}
    operating_units = {}
    material_to_ou_flows = {}
    mutually_exclusive_sets = []
    line_count = 0 
    current_section = None

    # Adjusted pattern with flexible field ordering and truly optional material_type
    pattern_1 = (
                    r'(?P<material_id>\w+):\s*'        # Matches the material ID
                    r'(?P<material_type>\w+(?:_\w+)*)?'   # Matches the material type, allowing underscores
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
            
            if line_count % 100000 == 0:
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
                match = re.match(
                r'(?P<material_id>\w+):\s*'                  # Matches the material ID
                r'(?P<material_type>\w+(?:_\w+)*)?'           # Matches the material type, allowing underscores
                r'(?:,\s*price=(?P<price>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Matches price (optional, with optional decimal/scientific notation)
                r'(?:,\s*flow_rate_lower_bound=(?P<lower_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Matches lower bound (optional)
                r'(?:,\s*flow_rate_upper_bound=(?P<upper_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Matches upper bound (optional)
                , line
                )

                # print(match)

            # Retrieve values with correct handling of defaults
                if match:

                    
                    if match.group("material_id") is None or match.group("material_type") is None or match.group("price") is None or match.group("lower_bound") is None or match.group("upper_bound") is None:
                        match_2 = re.match(
                        r'(?P<material_id>\w+):\s*(?P<material_type>\w+)?'  # Match material ID and type
                        r'(?:,\s*flow_rate_lower_bound=(?P<lower_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Optional lower bound
                        r'(?:,\s*flow_rate_upper_bound=(?P<upper_bound>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Optional upper bound
                        r'(?:,\s*price=(?P<price>-?\d+(\.\d+)?([eE][-+]?\d+)?))?'  # Optional price
                            ,line
                        )
                        # print(match_2.group("price"))

                    
                        if match.group("material_id") is None:
                            material_id = match_2.group("material_id")
                        else:
                            material_id = match.group("material_id")
                            
                        if match.group("material_type") is None:
                            material_type = match_2.group("material_type") if match_2.group("material_type") else default["material_type"]
                        else:
                            material_type = match.group("material_type") if match.group("material_type") else default["material_type"]                            
                            
                        if match.group("price") is None:
                            price = match_2.group("price") if match_2.group("price") else default["price"]
                        else:
                            price = match.group("price") if match.group("price") else default["price"]                  
                        if match.group("lower_bound") is None:
                            lower_bound = match_2.group("lower_bound") if match_2.group("lower_bound") else default["flow_rate_lower_bound"]
                        else:
                            lower_bound = match.group("lower_bound") if match.group("lower_bound") else default["flow_rate_lower_bound"]
                        if match.group("upper_bound") is None:
                            upper_bound = match_2.group("upper_bound") if match_2.group("upper_bound") else default["flow_rate_upper_bound"]
                        else:
                            upper_bound = match.group("upper_bound") if match.group("upper_bound") else default["flow_rate_upper_bound"]              
                                                             
                        materials[material_id] = {
                            'type': material_type,
                            'lower_bound': float(lower_bound),
                            'upper_bound': float(upper_bound),
                            'price': float(price)
                        }
                    

                        
            # Parse operating units
            elif current_section == "operating_units" and line:
                match = re.match(
        r'(\w+):\s*'
        r'(?:capacity_lower_bound=(-?\d+(\.\d+)?([eE][-+]?\d+)?),\s*)?'
        r'(?:capacity_upper_bound=(-?\d+(\.\d+)?([eE][-+]?\d+)?),\s*)?'
        r'(?:fix_cost=(-?\d+(\.\d+)?([eE][-+]?\d+)?),\s*)?'
        r'(?:proportional_cost=(-?\d+(\.\d+)?([eE][-+]?\d+)?))?',
        line
    )                
                if match:
                    # print(match)
                    ou_id = match.group(1)
                    lower_bound = float(match.group(2)) if match.group(2) else default['capacity_lower_bound']
                    upper_bound = float(match.group(5)) if match.group(5) else default['capacity_upper_bound']
                    fix_cost = float(match.group(8)) if match.group(8) else default['fix_cost']
                    proportional_cost = float(match.group(11)) if match.group(11) else default['proportional_cost']
                    
      
                    # Store the parsed data in the dictionary
                    operating_units[ou_id] = {
                        'capacity_lower_bound': lower_bound,
                        'capacity_upper_bound': upper_bound,
                        'fix_cost': fix_cost,
                        'proportional_cost': proportional_cost
                    }

            # Parse material to operating unit flow rates
            elif current_section == "flows" and line:
                    match = re.match(r'(\w+):\s*(.+)\s*=>\s*(.+)', line)
                    # print('match',match)

                    if match:
                        ou_id = match.group(1)
                        inputs_str = match.group(2).strip()
                        outputs_str = match.group(3).strip()

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

            # Parse mutually exclusive sets of operating units
            elif current_section == "mutually_exclusive" and line:
                match = re.match(r'ME\d+:\s*([\w, ]+)', line)
                if match:
                    ou_set = match.group(1).replace(" ", "").split(',')
                    mutually_exclusive_sets.append(ou_set)   
        
        ou_to_material_flows = materials_to_ou_convertor(material_to_ou_flows)
        flow_data =  data_flow_coefficient(material_to_ou_flows)

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


###################################################### This Model is to extract output values ######################################################

import re
import pandas as pd

# Safe conversion function
def str_to_value(s):
    try:
        if '.' in s or 'e' in s or 'E' in s:
            return float(s)
        else:
            return int(s)
    except ValueError:
        return s  # if it can't convert, return the original string

class FeasibleStructureExtractor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.feasible_structure = None
        self.materials = []
        self.units = []

    def extract_feasible_structure(self):
        with open(self.file_path, "r", encoding="utf-8") as file:
            content = file.read()
        match = re.search(r"Feasible structure #1:(.*)End\.", content, re.DOTALL)
        if match:
            self.feasible_structure = match.group(1).strip()
        else:
            raise ValueError("No feasible structure found.")

    def parse_materials(self):
        materials_section = re.search(r"Materials:(.*?)Operating units:", self.feasible_structure, re.DOTALL)
        if not materials_section:
            return
        materials_text = materials_section.group(1)
        for line in materials_text.splitlines():
            line = line.strip()
            if not line:
                continue
            if 'balanced' in line:
                material_id = line.split(':')[0].strip()
                self.materials.append([material_id, 0, '-', '-', '-'])
            else:
                m = re.match(r"(M\d+) \((-?[\d\.e\+\-]+) ([^\)]+)\): ([\-\d\.e\+]+) ([^\)]+)", line)
                if m:
                    material_id, cost, cost_unit, capacity, capacity_unit = m.groups()
                    self.materials.append([material_id, str_to_value(capacity), capacity_unit, str_to_value(cost), cost_unit])

    def parse_operating_units(self):
        units_section = re.search(r"Operating units:(.*)Total annual cost=", self.feasible_structure, re.DOTALL)
        if not units_section:
            return
        units_text = units_section.group(1)

        merged_lines = []
        buffer = ""
        for line in units_text.splitlines():
            line = line.strip()
            if not line:
                continue
            if '*' in line:
                if buffer:
                    merged_lines.append(buffer)
                buffer = line
            else:
                buffer += " " + line
        if buffer:
            merged_lines.append(buffer)

        for line in merged_lines:
            m = re.search(r"([\d.eE+\-]+)\*(O\d+) \(([\d.eE+\-]+) ([^\)]+)\)", line)
            if m:
                capacity, operating_id, cost, cost_unit = m.groups()
                self.units.append([
                    operating_id,
                    str_to_value(capacity),
                    str_to_value(cost),
                    cost_unit
                ])

    def save_to_excel(self, output_file):
        materials_df = pd.DataFrame(self.materials, columns=['ID', 'Flow', 'Flow Unit', 'Cost', 'Cost Unit'])
        units_df = pd.DataFrame(self.units, columns=['ID', 'Capacity Multiplier', 'Cost', 'Cost Units'])

        # Name the index column
        materials_df.index.name = 'index'
        units_df.index.name = 'index'

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            materials_df.to_excel(writer, sheet_name='Materials', index=True)
            units_df.to_excel(writer, sheet_name='Operating Units', index=True)

    def extract_output_values(self, output_file='feasible_structure.xlsx'):
        self.extract_feasible_structure()
        self.parse_materials()
        self.parse_operating_units()
        self.save_to_excel(output_file)
        print(f"✅ Extraction completed! Saved to {output_file}")

