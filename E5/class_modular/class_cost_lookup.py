import pandas as pd


class CostLookup:
    def __init__(self, pd_distribution=None, pd_transmission=None, pd_transformer=None):
        self.distribution = {}
        self.transmission = {}
        self.transformer = {}

        if pd_distribution is not None:
            self.load_distribution(pd_distribution)
        if pd_transmission is not None:
            self.load_transmission(pd_transmission)
        if pd_transformer is not None:
            self.load_transformer(pd_transformer)

    def load_distribution(self, df):
        self.distribution = {
            (row['Lower Bound'], row['Upper Bound']): row['TAC']
            for _, row in df.iterrows()
        }

    def load_transmission(self, df):
        self.transmission = {
            (int(row['Transmission Cost']), row['Single/Double'].strip()): row['TAC']
            for _, row in df.iterrows()
        }

    def load_transformer(self, df):
        self.transformer = {
            (row['Lower Bound (MVA)'], row['Upper Bound (MVA)']): row['TAC']
            for _, row in df.iterrows()
        }


