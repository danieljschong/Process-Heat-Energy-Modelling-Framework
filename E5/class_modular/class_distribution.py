

class DistributionFlowCalculator:
    def __init__(self, op_out_df, cost_lookup, time_multiplier,time_resolution):
        self.op_out_df = op_out_df
        self.cost_lookup = cost_lookup
        self.time_multiplier = time_multiplier
        self.time_resolution = time_resolution

    def _filter_outputs(self, id_length, prefix, suffix_pos, suffix_val):
        """Filter operation units dataframe based on ID rules."""
        filtered = {'ID': [], 'Capacity Multiplier': []}
        for i in range(len(self.op_out_df['ID'])):
            if (
                len(self.op_out_df['ID'][i]) == id_length and
                self.op_out_df['ID'][i][prefix[0]:prefix[1]] == str(prefix[2]) and
                self.op_out_df['ID'][i][suffix_pos[0]:suffix_pos[1]] == str(suffix_val)):
                
                filtered['ID'].append(self.op_out_df['ID'][i])
                filtered['Capacity Multiplier'].append(self.op_out_df['Capacity Multiplier'][i])
                # print(filtered)
        return filtered

    def _calculate_flows_and_costs(self, nodes, id_builder_func, filter_outputs,time_resolution, distance_field,multiplier_1000=False):
        """General method for demand or generation."""
        for j in range(len(nodes['index'])):
            node_id = id_builder_func(nodes, j)
            values = []
            
            if 'dem_gwh' in nodes:
                demand_nodes = []
                for suffix in self.time_multiplier:
                    target_id = f"{node_id}{suffix}"
                    found = False
                    for i in range(len(filter_outputs['ID'])):
                        
                        if filter_outputs['ID'][i] == target_id:
                            value = filter_outputs['Capacity Multiplier'][i]
                            demand_node = filter_outputs['Capacity Multiplier'][i]
                            if multiplier_1000:
                                value *= 1000
                            
                            if time_resolution == 'monthly':
                                value /= 730
                            elif time_resolution == 'hourly':
                                value = value

                            values.append(value)
                            demand_nodes.append(demand_node)
                            found = True
                            break
                    if not found:
                        values.append(0)
                nodes['flow'].append(values)
                # print('flow',nodes['flow'])
                nodes['demand_node'].append(demand_nodes)
                # print(nodes['demand_node'])
                nodes['max_flow'].append(max(values) if any(values) else 0)
                
            elif 'cf' in nodes:
                cf_value = max(nodes['cf'][j])
                value = nodes['capacity'][j] * cf_value
                # print("vaue",value)
                if time_resolution == 'monthly':
                    value = value
                elif time_resolution == 'hourly':
                    value = value
                values.append(value)
                nodes['flow'].append(values)
                nodes['max_flow'].append(max(values) if any(values) else 0)            
            
        # print(nodes['index'])
        # print(nodes[distance_field])
        # print(nodes['max_flow'])
        # print(nodes)
        
        for i in range(len(nodes['index'])):
            # we will have to cap the distance to be <20km to make it make sense? wip
            # print(nodes[distance_field][i])
            if nodes[distance_field][i] > 10000:
                nodes[distance_field][i] = 10000
            distance_km = nodes[distance_field][i]
            max_flow = nodes['max_flow'][i]
            tac_cost = 0
            for (lower, upper), tac in self.cost_lookup.distribution.items():
                # print(lower,":::",upper) #treat this wip
                if lower <= max_flow <= upper:
                    tac_cost = tac
                    # print(nodes['index'][i],"Matched TAC cost:", tac_cost, "for max_flow:", max_flow)
                    break
            # print('max_flow',nodes['max_flow'][i])
            # print('tac',tac_cost)
            nodes['cost'].append(tac_cost * distance_km)
            # print('cost',nodes['cost'][i])
            # print(tac_cost)
            # print(distance_km)
            # print(i)

    def process_demand(self, dem):
        filter_output_distribution = self._filter_outputs(
            id_length=10, prefix=(5,8,103), suffix_pos=(5,8), suffix_val=103
        )
        # print(filter_output_distribution)
        def demand_id_builder(dem, j):
            return f"O{3000 + dem['dem_ID'][j]}103"

        self._calculate_flows_and_costs(dem, demand_id_builder, filter_output_distribution,self.time_resolution, distance_field='dem_distance',multiplier_1000=True)

    def process_generation(self, gen):
        filter_output_gen = self._filter_outputs(
            id_length=8, prefix=(1,3,18), suffix_pos=(1,3), suffix_val=18
        )
        # print(filter_output_gen)

        def gen_id_builder(gen, j):
            return f"O{8000 + gen['index'][j]}"

        self._calculate_flows_and_costs(gen, gen_id_builder, filter_output_gen, self.time_resolution, distance_field='distance',multiplier_1000=False)


