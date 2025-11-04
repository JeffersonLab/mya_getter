import unittest
from datetime import datetime
import os

import pandas as pd
from pandas.testing import assert_frame_equal

from mya_getter.mya import mydata


DIR = os.path.dirname(__file__)


class TestMydata(unittest.TestCase):

    query = mydata.MyDataQuery(begin=datetime.strptime("2024-05-09", "%Y-%m-%d"),
                               end=datetime.strptime("2024-05-09 00:01:00", "%Y-%m-%d %H:%M:%S"),
                               pvlist=["R1M1GMES","R1Q1GMES"],
                               filter=None,
                               ignore_undef=False,
                               time_precision=None,
                               deployment='history')

    query_nans = mydata.MyDataQuery(begin=datetime.strptime("2023-05-09", "%Y-%m-%d"),
                               end=datetime.strptime("2023-05-09 14:59:00", "%Y-%m-%d %H:%M:%S"),
                               pvlist=["R1M1GMES","R1Q1GMES"],
                               filter=None,
                               ignore_undef=False,
                               time_precision=None,
                               deployment='history')

    
    def test_myData(self):
        """Basic test case with only numeric data"""
    
        result = mydata.myData(query=self.query)
        exp = pd.read_csv(f"{DIR}/test-myData.csv", index_col=None)
        exp.Date = pd.to_datetime(exp.Date)

        assert_frame_equal(exp, result)

    def test_myData_nans(self):
        """Basic test case with numeric and '<undefined>' data"""
    
        result = mydata.myData(query=self.query_nans)
        exp = pd.read_csv(f"{DIR}/test-myData-nans.csv", index_col=None)
        exp.Date = pd.to_datetime(exp.Date)

        assert_frame_equal(exp, result)

