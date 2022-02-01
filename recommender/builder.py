"""
Recommender Builder
"""

import itertools
import json
import dask.bag as db
import dask.dataframe as dd
import pandas as pd


class RecommenderBuilder():
    """
    Computes recommendation model results and persists them to a file store
    """

    def __init__(self, config):
        self.config = config

    @staticmethod
    def get_item_pairs(transaction):
        """
        Helper for generating co-occurence pairs for a transaction record
        """
        for item_pair in itertools.combinations(transaction, 2):
            yield (item_pair[0]['item'], item_pair[1]['item'])

    @staticmethod
    def get_top_value_freq(record, number_of_values=5):
        """
        Helper for computing frequencies values and returning top values
        """
        return record.value_counts().head(number_of_values)

    @staticmethod
    def get_ordered_group_concat(record):
        """
        Helper for group concat of product record attributes into single record
        """
        ordered = record.sort_values('Frequency', ascending=False)
        return list(zip(ordered["Item2"], ordered["ProductName"]))

    def extract_products_to_df(self):
        """
        Retrieve product records from file into dataframe
        """
        try:
            products_dd = dd.read_csv(self.config.path.products_src, sep="\t")
        except FileNotFoundError:
            print("Please ensure products file is in the proper directory.\n")
            raise

        product_df = products_dd.compute()
        product_df.columns = ["ItemId", "CategoryCode", "ProductName"]
        return product_df

    def extract_transactions_to_item_pairs(self):
        """
        Retrieve transaction records from file, compute co-occuring item pairs into dataframe,
        including both A,B and B,A pair variants
        """

        try:
            transactions_bag = db.read_text(
                self.config.path.transactions_src, blocksize=1e6).map(json.loads)
        except FileNotFoundError:
            print("Please ensure transactions file is in the proper directory.\n")
            raise

        item_bag = transactions_bag.map(lambda record: record['itemList'])
        item_pair_bag = item_bag.map(self.get_item_pairs)
        item_pair_flat = item_pair_bag.flatten()

        item_pairs_df = item_pair_flat.to_dataframe(
            meta={"Item1": "object", "Item2": "object"})
        reverse_pairs_df = item_pairs_df[['Item2', 'Item1']].rename(
            columns={"Item2": "Item1", "Item1": "Item2"})
        item_pairs_df = item_pairs_df.append(reverse_pairs_df)

        return item_pairs_df

    def compute_top_cooccurence_items_with_names(self, item_pairs_dd, product_df):
        """
        Compute top co-occurence items by product and
        combine with product names into dataframe
        """

        # compute top frequent co-item values for each item
        freq_df = item_pairs_dd.groupby("Item1")["Item2"].apply(
            self.get_top_value_freq).reset_index()
        freq_df = freq_df.rename(
            columns={'level_1': 'Item2', 'Item2': 'Frequency'})

        # join product dataframe into grouped item dataframe
        freq_name_df = dd.merge(freq_df, product_df,
                                left_on='Item2', right_on="ItemId")

        # group concatenate by co-item - recommended products id and names
        group_freq_df = freq_name_df.groupby("Item1") \
            .apply(self.get_ordered_group_concat,
                   meta=pd.Series(name='Recommendation', dtype='object')).reset_index()

        return group_freq_df.compute()

    def run(self):
        """
        Execute recommendation model building
        """

        # read products into dataframe for later product name join
        product_df = self.extract_products_to_df()

        # read transactions, build persisted co-occurence pairs list
        item_pairs_dd = self.extract_transactions_to_item_pairs().repartition(
            npartitions=(self.config.num_workers * self.config.partitions_per_worker))

        # compute top co-occurring item values with description and output values to store
        cooccurence_df = self.compute_top_cooccurence_items_with_names(
            item_pairs_dd, product_df)
        cooccurence_df.sort_values('Item1').to_csv(
            self.config.path.recommendations_store, index=False)
