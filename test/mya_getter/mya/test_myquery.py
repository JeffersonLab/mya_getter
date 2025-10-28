import os
import unittest
import json
from datetime import datetime
from typing import Dict

import pandas as pd

from mya_getter.mya import myquery


DIR = os.path.dirname(__file__)

def load_interval_data(id: str, pv: str):
    """Load test case data for interval"""
    exp_data = pd.read_csv(f"{DIR}/data/myquery_{id}-data.csv", index_col=0)[pv]
    exp_disconnects = pd.read_csv(f"{DIR}/data/myquery_{id}-disconnects.csv", index_col=0)[pv]
    with open(f"{DIR}/data/myquery_{id}-metadata.csv", "r") as f:
        exp_metadata = json.load(f)
        
    return exp_data, exp_disconnects, exp_metadata


def load_mysampler_data(id: str):
    """Load test case data for mysampler"""
    exp_data = pd.read_csv(f"{DIR}/data/myquery_{id}-data.csv", index_col=0)
    exp_data.index = pd.to_datetime(exp_data.index)
    with open(f"{DIR}/data/myquery_{id}-disconnects.json", "r") as f:
        exp_disconnects = json.load(f)
    
    with open(f"{DIR}/data/myquery_{id}-metadata.json", "r") as f:
        exp_metadata  = json.load(f)
        
    return exp_data, exp_disconnects, exp_metadata


def save_mysampler_data(id: str, data: pd.DataFrame, disconnects: Dict[str, pd.DataFrame], metadata: Dict[str, object]):
    """Convient way to save test case data for mysampler."""
    data.to_csv(f"{DIR}/data/myquery_{id}-data.csv")

    with open(f"{DIR}/data/myquery_{id}-disconnects.json", "w") as f:
        json.dump(myquery.normalize_data(disconnects), f)
        # json.dump(disconnects, f, cls=myquery.MyQueryEncoder)
    
    with open(f"{DIR}/data/myquery_{id}-metadata.json", "w") as f:
        json.dump(myquery.normalize_data(metadata), f)
        # json.dump(metadata, f, cls=myquery.MyQueryEncoder)


def check_interval_result(test_case: unittest.TestCase, exp_data, exp_disconnects, exp_metadata, res_data,
                           res_disconnects, res_metadata):
    """Utility function for checking tes results of interval"""
    test_case.assertTrue(exp_data.equals(res_data), f"\nExp:\n{exp_data}\nResult:\n{res_data}\n")
    test_case.assertTrue(exp_disconnects.equals(res_disconnects),
                         f"\nExp:\n{exp_disconnects}\nResult:\n{res_disconnects}\n")
    test_case.assertDictEqual(exp_metadata, res_metadata, f"\nExp:\n{exp_metadata}\nResult:\n{res_metadata}\n")


def check_mysampler_result(test_case: unittest.TestCase, exp_data, exp_disconnects, exp_metadata, res_data,
                           res_disconnects, res_metadata):
    """Utility function for checking tes results of mysampler"""
    test_case.assertTrue(exp_data.equals(res_data), f"\nExpected:\n{exp_data}\nResult:\n{res_data}\n")
    test_case.assertDictEqual(exp_disconnects, myquery.normalize_data(res_disconnects),
                            f"\nExpected:\n{exp_disconnects}\nResult:\n{res_disconnects}\n")
    test_case.assertDictEqual(exp_metadata, myquery.normalize_data(res_metadata),
                            f"\nExpected:\n{exp_metadata}\nResult:\n{res_metadata}\n")


class TestMyQuery(unittest.TestCase):
    
    def test_get_interval_1(self):
        """Test basic query with lots of default values. (Has NaN)"""
        pv = "R123GSET"
        query = myquery.IntervalQuery(channel=pv,
                                      begin=datetime.strptime("2018-04-24", "%Y-%m-%d"),
                                      end=datetime.strptime("2018-04-25", "%Y-%m-%d"),
                                      deployment="history"
                                      )
        interval = myquery.Interval(query)
        interval.run()
        res_data = interval.data
        res_disconnects = interval.disconnects
        res_metadata = interval.metadata
        
        exp_data, exp_disconnects, exp_metadata = load_interval_data("interval_1", pv=pv)
        check_interval_result(self, exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects, res_metadata)
    
    def test_get_interval_2(self):
        """Test query that uses sampling and some extra options (no NaNs)"""
        pv = "R123GMES"
        query = myquery.IntervalQuery(channel=pv,
                                      begin=datetime.strptime("2018-04-24", "%Y-%m-%d"),
                                      end=datetime.strptime("2018-04-25 01:20:45.002", "%Y-%m-%d %H:%M:%S.%f"),
                                      deployment="history",
                                      bin_limit=5,
                                      sample_type="myget",
                                      frac_time_digits=2,
                                      sig_figs=3,
                                      data_updates_only=False,
                                      prior_point=False,
                                      enums_as_str=False,
                                      unix_timestamps_ms=False,
                                      adjust_time_to_server_offset=False,
                                      integrate=False,
                                      )
        interval = myquery.Interval(query)
        interval.run()
        res_data = interval.data
        res_disconnects = interval.disconnects
        res_metadata = interval.metadata
        
        exp_data, exp_disconnects, exp_metadata = load_interval_data("interval_2", pv=pv)
        check_interval_result(self, exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects, res_metadata)

    def test_get_mysampler_1(self):
        """Test basic query with lots of default values. (No NaN in sample, but in interval)"""
        
        query = myquery.MySamplerQuery(start=datetime.strptime("2024-04-24", "%Y-%m-%d"),
                                       interval=3_600_000,
                                       num_samples=5,
                                       pvlist=["R1Q1GMES", "R1M1GMES"],
                                       deployment="history")
        
        mysampler = myquery.MySampler(query)
        mysampler.run()        
        res_data = mysampler.data
        res_disconnects = mysampler.disconnects
        res_metadata = mysampler.metadata
        
        exp_data, exp_disconnects, exp_metadata = load_mysampler_data("mysampler_1")
        check_mysampler_result(self, exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects,
                               res_metadata)
        
    def test_get_mysampler_2(self):
        """Test basic query with lots of default values. (No NaN in sample, but in interval)"""
        
        query = myquery.MySamplerQuery(start=datetime.strptime("2018-04-24 12:15:00", "%Y-%m-%d %H:%M:%S"),
                                       interval=60_000,
                                       num_samples=15,
                                       pvlist=["R1Q1GMES", "R1M1GMES"],
                                       deployment="history")
        
        mysampler = myquery.MySampler(query)
        mysampler.run()        
        res_data = mysampler.data
        res_disconnects = mysampler.disconnects
        res_metadata = mysampler.metadata     

        save_mysampler_data("mysampler_2", res_data, res_disconnects, res_metadata)
        exp_data, exp_disconnects, exp_metadata = load_mysampler_data("mysampler_2")
        check_mysampler_result(self, exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects,
                               res_metadata)        
