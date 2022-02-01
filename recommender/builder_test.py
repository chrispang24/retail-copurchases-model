"""
Baseline Recommender Builder - Test Cases
Should be executed using small sampled data set.
"""
import unittest
from recommender.builder import RecommenderBuilder
from config import cfg


class TestRecommenderBuilder(unittest.TestCase):
    """
    Unit test cases for recommenation model builder
    """

    def setUp(self):
        self.builder = RecommenderBuilder(cfg)

    def test_extract_products_to_df(self):
        self.assertEqual(3, len(self.builder.extract_products_to_df().columns))

    # additional tests to be added below ...


if __name__ == '__main__':
    unittest.main()
