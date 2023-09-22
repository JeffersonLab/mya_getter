import unittest
from datetime import datetime
import os

import pandas as pd
from pandas.testing import assert_frame_equal

from mya_getter.mya import mysampler


DIR = os.path.dirname(__file__)


class TestMysampler(unittest.TestCase):

    query = mysampler.MySamplerQuery(start=datetime.strptime("2023-05-01", "%Y-%m-%d"),
                                        interval='1d',
                                        num_samples=15,
                                        pvlist=["R1M1GMES","R1Q1GMES"],
                                        deployment='history')

    def test_to_web_params(self):
        exp = {'c': 'R1M1GMES,R1Q1GMES', 'b': '2023-05-01T00:00:00', 'n': 15, 'm': 'history', 's': 86400000}
        result = self.query.to_web_params()

        self.assertDictEqual(exp, result)

    def test_mysampler_web(self):

        result = mysampler.mySamplerWeb(query=self.query)
        exp = pd.read_csv(f"{DIR}/test-mysampler-web.csv", index_col=None)

        assert_frame_equal(exp, result)

    def test_mysampler(self):
        result = mysampler.mySampler(query=self.query)
        exp = pd.read_csv(f"{DIR}/test-mysampler-cli.csv", index_col=None)

        assert_frame_equal(exp, result)

