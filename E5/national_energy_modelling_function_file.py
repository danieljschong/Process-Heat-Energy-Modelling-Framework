from types import SimpleNamespace
import tracemalloc


def text_file_bug_error_2(op_node_as_out_flow,op_node_as_capacity, mat_in_node, mat_out_node, time_period,time_multiplier,G,units):
    op_node_as_capacity = op_node_as_capacity[1:]
    op_node_as_out_flow =op_node_as_out_flow[1:]
    mat_in_node =mat_in_node[1:]
    mat_out_node=mat_out_node[1:]
    
    G.add_edge("M"+mat_in_node, "O"+op_node_as_capacity, weight=1e-7)    

    start_number =0
    increment = 500

    if time_period > increment:
        for l in range(len(time_multiplier)):
            G.add_node("O"+op_node_as_out_flow+str(time_multiplier[l]),names="O"+op_node_as_out_flow+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
        time_slice = round(time_period/increment+0.5)
        for i in range(1,int(time_slice)+1):
            for l in range(start_number,start_number+increment):  
                if l >= len(time_multiplier):  # Ensure l is within the bounds of time_multiplier
                    break
                G.add_node("M"+mat_in_node + str(i),names="M"+mat_in_node + str(i),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e6, price=0,units=units) #top layer
                G.add_node("O"+op_node_as_capacity+ str(i), names = "O"+op_node_as_capacity+ str(i), capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)#top layer
                G.add_edge("M"+mat_in_node + str(i),"O"+op_node_as_capacity+ str(i), weight=1)   
                G.add_edge("O"+op_node_as_capacity+ str(i),"M"+mat_in_node,weight=1)     

                G.add_node("M"+mat_out_node+ str(i),names="M"+mat_out_node+ str(i),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=1e8, price=0,units=units)
                G.add_edge("O"+op_node_as_capacity,"M"+mat_out_node+ str(i),weight=1)
                G.add_edge("M"+mat_out_node+ str(i),"O"+op_node_as_capacity+ str(i),weight=1)
                
                G.add_node("M"+mat_out_node+ str(i)+str(time_multiplier[l]),names="M"+mat_out_node+ str(i)+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=units)
                
                G.add_edge("O"+op_node_as_capacity+ str(i),"M"+mat_out_node+ str(i)+str(time_multiplier[l]),weight=1)
                G.add_edge("M"+mat_out_node+ str(i)+str(time_multiplier[l]),"O"+op_node_as_out_flow+str(time_multiplier[l]),weight=1)
                G.add_edge("O"+op_node_as_out_flow+str(time_multiplier[l]),"M"+mat_in_node+ str(i),weight=1)
                
            start_number+=increment
    else:
        
        for l in range(len(time_multiplier)):
            G.add_node("O"+op_node_as_out_flow+str(time_multiplier[l]),names="O"+op_node_as_out_flow+str(time_multiplier[l]),capacity_lower_bound=0, capacity_upper_bound=1e8, fix_cost=0, proportional_cost=0)
            G.add_node("M"+mat_out_node+str(time_multiplier[l]),names="M"+mat_out_node+str(time_multiplier[l]),type='intermediate', flow_rate_lower_bound=0, flow_rate_upper_bound=0, price=0,units=units)
            G.add_edge("O"+op_node_as_out_flow+str(time_multiplier[l]),"M"+mat_in_node,weight=1)
            G.add_edge("O"+op_node_as_capacity,"M"+mat_out_node+str(time_multiplier[l]),weight=1)
            G.add_edge("M"+mat_out_node+str(time_multiplier[l]),"O"+op_node_as_out_flow+str(time_multiplier[l]),weight=1)

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


def pandas_to_dict(table):
    result_dict = {}

    for index, row in table.iterrows():
        first_column = table.columns[0]
        value_columns = table.columns[1:13]

        key = row[first_column]
        values = row[value_columns].tolist()
        result_dict[key] = values
    keys = list(result_dict.keys())
    return result_dict, keys


def python_dict_dot_notation(panda_keys, sheet, ):
    Pd_dictObj = {}
    for key in panda_keys:
        if key != "Names":
            cleaned_key = key.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("°","_").replace(",", "_").replace(">", "_")
            if cleaned_key[0].isdigit():
                cleaned_key = "_" + cleaned_key
            Pd_dictObj.update({cleaned_key: list(sheet[key])})
        else:
            Pd_dictObj.update(
                {key.replace(" ", "_"): list(sheet['Types of power station'] + "_" + sheet['Name'])})
        # print(Pd_dictObj)
    return SimpleNamespace(**Pd_dictObj)
