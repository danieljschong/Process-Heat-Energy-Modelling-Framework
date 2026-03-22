import matplotlib.pyplot as plt
import numpy as np
import math

class MonteCarloAnalysis:
    def __init__(self,mode):
        self.all_simulation_data  = []
        self.normalised_data = []
        self.mean_values = []    
        self.mode = mode
        
    
    def add_simulation_data(self,input_list):
        simulation_data = []

        for sublist in (input_list):
            if self.mode == 'OP':
                for item_op in sublist:
                    if len(item_op) > 1 and item_op[1] == 'O10013280':
                        
                        self.all_simulation_data.append(float(item_op[0]))
                        return  # Break outer loop to save time
            # elif self.mode == 'OO':
            #     self.all_simulation_data.append(float(sublist))

            elif self.mode == 'OO':
                if isinstance(sublist, (int, float)):
                    if not math.isnan(sublist):
                        self.all_simulation_data.append(float(sublist))

                elif isinstance(sublist, str):
                    try:
                        value = float(sublist)
                        if not math.isnan(value):
                            self.all_simulation_data.append(value)
                    except ValueError:
                        pass
            
    def normalise_data(self):
        if not self.all_simulation_data:
            raise ValueError("No simulation data to normalize. Add data first.")
        
        max_value = np.max(self.all_simulation_data)
        self.normalised_data = [float(value) / float(max_value) for value in self.all_simulation_data]
    
    def calculate_means(self):
        """
        Calculate the mean of normalized data for each simulation.
        """
        if not self.normalised_data:
            self.normalise_data()
        cumulative_sum = 0  # Sum of normalized values so far
        self.mean_values = []  # Reset mean_values to match normalised_data length

        for i, value in enumerate(self.normalised_data, start=1):
            cumulative_sum += value
            self.mean_values.append(cumulative_sum / i)
    
    def plot_results(self):
        if self.mean_values is None:
            self.calculate_means()
        
        # Since we're plotting against simulations, use a single point (mean value)
        # Plot individual simulation normalized values
        if not self.mean_values:
            self.calculate_means()

        # Plot cumulative means
        plt.plot(range(1, len(self.mean_values) + 1), self.mean_values, marker='o', label='Cumulative Means')

        plt.title("Cumulative Mean Values vs. Simulations", fontsize=14)
        plt.xlabel("Simulation Number", fontsize=14)
        plt.ylabel("Cumulative Mean Value", fontsize=14)
        plt.legend()
        plt.grid()
        plt.show()
        
    def analyze(self, input_list):
        """
        Perform the full analysis pipeline from data addition to plotting.
        
        Args:
            goplist (list): List of lists containing simulation data.
        """
        self.add_simulation_data(input_list)
        self.normalise_data()
        self.calculate_means()
        
        




# # Example datasets for multiple iterations
# example_gmatlist_1 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['4.5', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.4', 'O10013280', '0', 'NZD/t']],
# ]

# example_gmatlist_2 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['5.5', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.14', 'O10013280', '0', 'NZD/t']],
# ]

# example_gmatlist_3 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['5.2', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.4', 'O10013280', '0', 'NZD/t']],
# ]

# example_gmatlist_4 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['5.7', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.14', 'O10013280', '0', 'NZD/t']],
# ]
# example_gmatlist_5 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['5.1', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.4', 'O10013280', '0', 'NZD/t']],
# ]

# example_gmatlist_6 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['5', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.14', 'O10013280', '0', 'NZD/t']],
# ]
# example_gmatlist_7 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['5.7', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.4', 'O10013280', '0', 'NZD/t']],
# ]

# example_gmatlist_8 = [
#     [['57.971', 'O10013', '0', 'NZD/t'], ['4.8', 'O10013280', '22492.8', 'NZD/t']],
#     [['0.0223074', 'O30011090001', '0', 'NZD/t'], ['5.14', 'O10013280', '0', 'NZD/t']],
# ]

# oo =[['23.9989'], ['13.8686'], ['20.8166'], ['13.4742'], ['22.6225'], ['16.3832'], ['14.1162'], ['22.4624'], ['16.7575'], ['20.0508'], ['18.8545'], ['26.5464'], ['15.0844'], ['18.3581'], ['20.4746'], ['17.5568'], ['18.9215'], ['17.207'], ['14.4216'], ['13.2935'], ['15.3521'], ['21.2854'], ['27.0881'], ['22.4578'], ['17.7432'], ['18.5723'], ['16.2207'], ['14.7718'], ['14.8093'], ['13.959'], ['17.8298'], ['17.1002'], ['13.8363'], ['13.2786'], ['20.4586'], ['16.9022'], ['16.126'], ['17.0069'], ['18.0461'], ['12.5687'], ['13.5869'], ['16.1779'], ['13.4353'], ['16.1311'], ['15.1102'], ['25.9596'], ['17.5859'], ['15.7293'], ['19.2747'], ['31.3952'], ['22.896'], ['18.7214'], ['19.1555'], ['17.5212'], ['19.9357'], ['27.0115'], ['22.9977'], ['16.7651'], ['21.7308'], ['17.8693'], ['14.8989'], ['13.5721'], ['15.5257'], ['22.4728'], ['22.8838'], ['19.4471'], ['14.5296'], ['15.0116'], ['14.8874'], ['14.734'], ['12.4072'], ['18.7642'], ['22.6866'], ['25.3264'], ['21.4594'], ['20.4108'], ['17.5844'], ['13.0778'], ['13.7419'], ['12.6513'], ['21.2058'], ['18.7387'], ['30.7491'], ['16.0036'], ['24.2697'], ['22.4206'], ['18.8031'], ['17.7755'], ['23.6907'], ['19.8749'], ['22.0968'], ['19.2842'], ['14.5275'], ['17.5618'], ['25.6772'], ['21.7089'], ['19.248'], ['14.3944'], ['16.3446'], ['16.6418']]

# example_gmatlists = [example_gmatlist_1, example_gmatlist_2,example_gmatlist_3, example_gmatlist_4,example_gmatlist_5, example_gmatlist_6,example_gmatlist_7, example_gmatlist_8]

# # if __name__ == "__main__":
# #     # Create an instance of the class
# #     analyzer = MonteCarloAnalysis(mode="OO")

# #     # Analyze datasets
# #     for i, goplist in enumerate(oo):
# #         analyzer.analyze(goplist,)

# #     # Show the final plot after processing all datasets
# #     analyzer.plot_results()

# #     # Access intermediate results if needed
# #     print("\nAll Simulation Data:", analyzer.all_simulation_data)
# #     print("Normalized Data:", analyzer.normalised_data)
# #     print("Mean Values:", analyzer.mean_values)