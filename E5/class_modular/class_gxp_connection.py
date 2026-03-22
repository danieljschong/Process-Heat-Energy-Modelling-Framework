import math

class GXPConnectionProcessor:
    def __init__(self, gxp_connections_dict, output_file, transmission_cost_lookup, time_multiplier):
        self.gxp_connections_dict = gxp_connections_dict
        self.output_file = output_file
        self.transmission_cost_lookup = transmission_cost_lookup
        self.time_multiplier = time_multiplier

    def process_flows(self):
        """Filter output_file for GXP-related IDs and collect flows."""
        filtered = {'ID': [], 'Capacity Multiplier': []}
        for i in range(len(self.output_file['ID'])):
            if (self.output_file['Cost'][i] == 0 and
                len(self.output_file['ID'][i]) == 7 and
                6000 <= int(self.output_file['ID'][i][1:5]) < 7000):
                # print(int(self.output_file['ID'][i][1:5]))
                filtered['ID'].append(self.output_file['ID'][i])
                filtered['Capacity Multiplier'].append(self.output_file['Capacity Multiplier'][i])
        # print(filtered)

        for j in range(len(self.gxp_connections_dict['Op_ID'])):
            gxp_connection_id_str = self.gxp_connections_dict['Op_ID'][j]
            values = []
            for suffix in self.time_multiplier:
                target_id = f"{gxp_connection_id_str}{suffix}"
                found = False
                for i in range(len(filtered['ID'])):
                    if filtered['ID'][i] == target_id:
                        values.append(filtered['Capacity Multiplier'][i])
                        found = True
                        break
                if not found:
                    values.append(0)  # Default to 0 if not found

            self.gxp_connections_dict['flow'].append(values)
            self.gxp_connections_dict['max_flow'].append(max(values) if any(values) else 0)

    def calculate_grid_costs(self):
        """Calculate transmission costs for each GXP connection."""
        for i in range(len(self.gxp_connections_dict['Op_ID'])):
            kv = self.gxp_connections_dict['kV'][i]
            line_type = self.gxp_connections_dict['Line'][i].strip()
            distance_km = self.gxp_connections_dict['Distance'][i] / 1000  # meters to kilometers
            # print(distance_km)
            tac = self.transmission_cost_lookup.get((kv, line_type), None)
            if tac is not None:
                total_cost = tac * distance_km
                self.gxp_connections_dict['cost'].append(round(total_cost, 2))
            else:
                self.gxp_connections_dict['cost'].append(None)
                print(f"No transmission cost match for kV={kv}, Line={line_type}")

    def assign_grid_lines_needed(self):
        """Calculate how many grid lines are needed, and fix even/odd ID cases."""
        for i in range(len(self.gxp_connections_dict['Op_ID'])):
            max_flow = self.gxp_connections_dict['max_flow'][i]
            capacity_per_line = self.gxp_connections_dict['Capacity'][i]
            if capacity_per_line > 0:
                number_grids = math.ceil(max_flow / capacity_per_line)
            else:
                number_grids = 0
            self.gxp_connections_dict['grid_lines_needed'].append(number_grids)

        # Adjust based on even/odd ID relationship
        for i in range(len(self.gxp_connections_dict['Op_ID'])):
            op_id_num = int(self.gxp_connections_dict['Op_ID'][i][1:])  # Remove the 'O'
            if op_id_num % 2 == 1:  # odd
                if self.gxp_connections_dict['grid_lines_needed'][i] == 0 and i > 0:
                    self.gxp_connections_dict['grid_lines_needed'][i] = self.gxp_connections_dict['grid_lines_needed'][i-1]
            elif op_id_num % 2 == 0:  # even
                if self.gxp_connections_dict['grid_lines_needed'][i] == 0 and i + 1 < len(self.gxp_connections_dict['Op_ID']):
                    self.gxp_connections_dict['grid_lines_needed'][i] = self.gxp_connections_dict['grid_lines_needed'][i+1]

    def process_gxp(self):
        """Run the full processing pipeline."""
        self.process_flows()
        # print("GXP flows processed.")        
        self.calculate_grid_costs()
        # print("Grid costs calculated.")
        self.assign_grid_lines_needed()
        # print("GXP Connection Processing Finished.")



    def nodes_update(self, G):
        ME = []
                
        # existing grid lines
        for i in range(len(self.gxp_connections_dict['Op_ID'])):
            for l in range(len(self.time_multiplier)):
                G.add_node(self.gxp_connections_dict['Op_ID'][i]+str(self.time_multiplier[l]),names=self.gxp_connections_dict['Op_ID'][i]+str(self.time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=self.gxp_connections_dict["Capacity"]
        [i], fix_cost=0, proportional_cost=0)
                
                if self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]) not in G.nodes or self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]) not in G.nodes:
                    G.add_node(self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]),names=self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0)
                    G.add_node(self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]),names=self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0)

                G.add_edge(self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]), self.gxp_connections_dict['Op_ID'][i]+str(self.time_multiplier[l]),weight=1)
                G.add_edge(self.gxp_connections_dict['Op_ID'][i]+str(self.time_multiplier[l]), self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]),weight=0.98)
                    
                if int(self.gxp_connections_dict['Op_ID'][i][1:]) % 2 == 1 and int(self.gxp_connections_dict['Op_ID'][i][1:]) >= 6001:
                    ME.append([(self.gxp_connections_dict['Op_ID'][i-1]) + str(self.time_multiplier[l]),(self.gxp_connections_dict['Op_ID'][i]) + str(self.time_multiplier[l])])


        # new grid lines use a for loop, replace str(1) with n. 
        for i in range(len(self.gxp_connections_dict['Op_ID'])):
            for n in range(1,self.gxp_connections_dict['grid_lines_needed'][i]+1):
                if int(self.gxp_connections_dict['Op_ID'][i][1:]) % 2 == 1 and int(self.gxp_connections_dict['Op_ID'][i][1:]) >= 6001:
                    # print(self.gxp_connections_dict['Op_ID'][i-1]+self.gxp_connections_dict['Op_ID'][i][1:] + str(n)+ str(1))
                    G.add_node(self.gxp_connections_dict['Op_ID'][i-1]+self.gxp_connections_dict['Op_ID'][i][1:] + str(n)+ str(1),names=self.gxp_connections_dict['Op_ID'][i-1]+self.gxp_connections_dict['Op_ID'][i][1:] + str(n)+ str(1),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=self.gxp_connections_dict['cost'][i], proportional_cost=0) #capacity

                for l in range(len(self.time_multiplier)):
                    G.add_node(self.gxp_connections_dict['Op_ID'][i] + str(n)+ str(1)+str(self.time_multiplier[l]),names=self.gxp_connections_dict['Op_ID'][i]+ str(n)+ str(1)+str(self.time_multiplier[l]) ,capacity_lower_bound=0, capacity_upper_bound=self.gxp_connections_dict['Capacity'][i], fix_cost=0, proportional_cost=0)
                    # G.add_node(self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]),names=self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0) #upper bound should be zero in big model
                    # G.add_node(self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]),names=self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0) #upper bound should be zero in big model

                    G.add_edge(self.gxp_connections_dict['From'][i]+str(self.time_multiplier[l]), self.gxp_connections_dict['Op_ID'][i]+ str(n)+ str(1)+str(self.time_multiplier[l]),weight=1)
                    G.add_edge(self.gxp_connections_dict['Op_ID'][i]+ str(n)+ str(1)+str(self.time_multiplier[l]), self.gxp_connections_dict['To'][i]+str(self.time_multiplier[l]),weight=0.98)
                    
                    #capacity calculations        
                    if int(self.gxp_connections_dict['Op_ID'][i][1:]) % 2 == 1 and int(self.gxp_connections_dict['Op_ID'][i][1:]) >= 6001:
                        # print(int(self.gxp_connections_dict['Op_ID'][i][1:]))
                        # print(int(self.gxp_connections_dict['Op_ID'][i][1:]) >= 6001)
                        G.add_node("M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+str(n)+str(2)+str(self.time_multiplier[l]),names="M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+str(n)+str(2)+str(self.time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0)
                        G.add_node("M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+str(n)+str(3)+str(self.time_multiplier[l]),names="M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+str(n)+str(3)+str(self.time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0)
                        
                        ME.append([(self.gxp_connections_dict['Op_ID'][i-1]) + str(n)+ str(1)+ str(self.time_multiplier[l]),(self.gxp_connections_dict['Op_ID'][i]) + str(n)+ str(1)+ str(self.time_multiplier[l])])
                        
                        G.add_edge(self.gxp_connections_dict['Op_ID'][i]+ str(n)+ str(1)+str(self.time_multiplier[l]),"M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+str(n)+str(2)+str(self.time_multiplier[l]),weight=1 )
                        G.add_edge(self.gxp_connections_dict['Op_ID'][i-1]+ str(n)+ str(1)+str(self.time_multiplier[l]),"M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+str(n)+str(2)+str(self.time_multiplier[l]),weight=1 )
                        G.add_edge("M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+ str(n)+str(2)+str(self.time_multiplier[l]),self.gxp_connections_dict['Op_ID'][i-1]+self.gxp_connections_dict['Op_ID'][i][1:]+ str(n)+ str(1) , weight=1)
                        
                        G.add_edge(self.gxp_connections_dict['Op_ID'][i-1]+self.gxp_connections_dict['Op_ID'][i][1:]+ str(n)+ str(1),"M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+ str(n)+str(3)+str(self.time_multiplier[l]), weight=1)
                        G.add_edge("M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+ str(n)+str(3)+str(self.time_multiplier[l]),self.gxp_connections_dict['Op_ID'][i]+ str(n)+ str(1)+str(self.time_multiplier[l])  ,weight=1)
                        G.add_edge("M"+self.gxp_connections_dict['Op_ID'][i-1][1:]+self.gxp_connections_dict['Op_ID'][i][1:]+ str(n)+str(3)+str(self.time_multiplier[l]),self.gxp_connections_dict['Op_ID'][i-1]+ str(n)+ str(1)+str(self.time_multiplier[l])  ,weight=1)
                        
        return ME

