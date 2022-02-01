"""
Recommender Server
"""

import ast
import pandas as pd


class RecommenderServer():
    """
    Reads recommendation results into memory for fast retrieval upon input prompt
    """

    def __init__(self, config):
        self.config = config
        self.recommendations_kv = {}

    def get_recommendations_kv_from_source(self):
        """
        Retrieve recommendations from data store and load into key-value store
        """
        try:
            recommendations_df = pd.read_csv(
                self.config.path.recommendations_store)
        except FileNotFoundError:
            print("Please ensure recommendation file is in the proper directory.\n")
            raise

        recommendations_df.set_index("Item1", inplace=True)
        return recommendations_df.to_dict(orient="index")

    def get_recommendations_for_item(self, item_id):
        """
        Display recommendations for item based on key-value store search
        """
        recommendations = []
        if item_id in self.recommendations_kv:
            recommendations = ast.literal_eval(
                self.recommendations_kv[item_id]['Recommendation'])

        print("Item recommendations:")

        if len(recommendations) == 0:
            print("None found")
        for item in recommendations:
            print(item)

    def run(self):
        """
        Execute recommendation model results serving
        """

        self.recommendations_kv = self.get_recommendations_kv_from_source()

        while True:
            print("\nEnter Item ID to get recommendations for:")
            item_id = input()
            self.get_recommendations_for_item(item_id)
