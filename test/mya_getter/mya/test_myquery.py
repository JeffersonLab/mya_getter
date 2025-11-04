import os
import unittest
import json
from datetime import datetime
from typing import Dict

import numpy as np
import pandas as pd

from mya_getter.mya import myquery


DIR = os.path.dirname(__file__)


@staticmethod
def load_multi_pv_data(ident: str):
    """Load test case data for mysampler"""
    exp_data = pd.read_csv(f"{DIR}/data/myquery_{ident}-data.csv", index_col=0)
    exp_data.index = pd.to_datetime(exp_data.index)
    with open(f"{DIR}/data/myquery_{ident}-disconnects.json", "r") as f:
        exp_disconnects = json.load(f)

    with open(f"{DIR}/data/myquery_{ident}-metadata.json", "r") as f:
        exp_metadata = json.load(f)

    return exp_data, exp_disconnects, exp_metadata


class TestInterval(unittest.TestCase):
    s1 = pd.Series(
        [7.930, 0.000, 6.996, 7.930, 7.755, np.nan, 7.755, np.nan],
        index=pd.to_datetime([
            "2018-04-24 00:00:00",
            "2018-04-24 11:12:51",
            "2018-04-24 11:12:55",
            "2018-04-24 11:12:56",
            "2018-04-24 11:18:18",
            "2018-04-24 12:19:44",
            "2018-04-24 12:32:45",
            "2018-04-25 01:20:45",
        ]),
        name="R123GMES",
        dtype="float64",
    )

    s2 = pd.Series(
        [5.911, 0.0, np.nan, 5.66, np.nan, 5.657, 5.657],
        index=pd.to_datetime([
            "2018-04-24 00:00:00",
            "2018-04-24 06:25:01",
            "2018-04-24 06:25:05",
            "2018-04-24 11:18:19",
            "2018-04-24 12:19:44",
            "2018-04-24 12:31:11",
            "2018-04-25 01:20:45"]),
        name="R121GMES",
        dtype="float64",
    )

    df_s1_s2 = pd.DataFrame(
        {
            "R123GMES": [7.930, 7.930, 7.930, 0.000, 6.996, 7.930, 7.755, 7.755, np.nan, np.nan, 7.755, np.nan],
            "R121GMES": [5.911, 0.000, np.nan, np.nan, np.nan, np.nan, np.nan, 5.660, np.nan, 5.657, 5.657, 5.657],
        },
        index=pd.to_datetime([
            "2018-04-24 00:00:00",
            "2018-04-24 06:25:01",
            "2018-04-24 06:25:05",
            "2018-04-24 11:12:51",
            "2018-04-24 11:12:55",
            "2018-04-24 11:12:56",
            "2018-04-24 11:18:18",
            "2018-04-24 11:18:19",
            "2018-04-24 12:19:44",
            "2018-04-24 12:31:11",
            "2018-04-24 12:32:45",
            "2018-04-25 01:20:45",
        ]),
    )

    @staticmethod
    def save_interval_data_parallel(ident: str, data: pd.DataFrame, disconnects: Dict[str, pd.DataFrame],
                            metadata: Dict[str, object]):
        """Convenient way to save test case data for mysampler."""
        data.to_csv(f"{DIR}/data/myquery_{ident}-data.csv")

        with open(f"{DIR}/data/myquery_{ident}-disconnects.json", "w") as f:
            # noinspection PyTypeChecker
            json.dump(myquery.normalize_data(disconnects), f)
            # json.dump(disconnects, f, cls=myquery.MyQueryEncoder)

        with open(f"{DIR}/data/myquery_{ident}-metadata.json", "w") as f:
            # noinspection PyTypeChecker
            json.dump(myquery.normalize_data(metadata), f)

    @staticmethod
    def load_interval_data(ident: str, pv: str):
        """Load test case data for interval"""
        exp_data = pd.read_csv(f"{DIR}/data/myquery_{ident}-data.csv", index_col=0)[pv]
        exp_disconnects = pd.read_csv(f"{DIR}/data/myquery_{ident}-disconnects.csv", index_col=0)[pv]
        with open(f"{DIR}/data/myquery_{ident}-metadata.csv", "r") as f:
            exp_metadata = json.load(f)

        return exp_data, exp_disconnects, exp_metadata

    def check_interval_result(self, exp_data, exp_disconnects, exp_metadata, res_data,
                              res_disconnects, res_metadata):
        """Utility function for checking tes results of interval"""
        self.assertTrue(exp_data.equals(res_data), f"\nExp:\n{exp_data}\nResult:\n{res_data}\n")
        self.assertTrue(exp_disconnects.equals(res_disconnects),
                             f"\nExp:\n{exp_disconnects}\nResult:\n{res_disconnects}\n")
        self.assertDictEqual(exp_metadata, res_metadata, f"\nExp:\n{exp_metadata}\nResult:\n{res_metadata}\n")

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
        
        exp_data, exp_disconnects, exp_metadata = self.load_interval_data("interval_1", pv=pv)
        self.check_interval_result(exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects, res_metadata)
    
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
        
        exp_data, exp_disconnects, exp_metadata = self.load_interval_data("interval_2", pv=pv)
        self.check_interval_result(exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects, res_metadata)

    def test_run_parallel_combined(self):
        exp = self.df_s1_s2

        out = myquery.Interval.run_parallel(pvlist=["R123GMES", "R121GMES"],
                                            begin=datetime.strptime("2018-04-24", "%Y-%m-%d"),
                                            end=datetime.strptime("2018-04-25 01:20:45.002",
                                                                "%Y-%m-%d %H:%M:%S.%f"),
                                            deployment="history",
                                            prior_point=True,)
        res_data, res_disconnects, res_metadata = out

        self.save_interval_data_parallel("interval_parallel_1", res_data, res_disconnects, res_metadata)
        print(res_disconnects)
        print(res_metadata)

        self.fail()


    def test__combine_series(self):
        """Test if internal logic for combining series works."""
        exp = self.df_s1_s2
        result = myquery.Interval._combine_series([self.s1, self.s2])
        self.assertTrue(exp.equals(result), f"\nExp:\n{exp}\nResult:\n{result}\n")


class TestMySampler(unittest.TestCase):

    @staticmethod
    def load_mysampler_data(ident: str):
        """Load test case data for mysampler"""
        exp_data = pd.read_csv(f"{DIR}/data/myquery_{ident}-data.csv", index_col=0)
        exp_data.index = pd.to_datetime(exp_data.index)
        with open(f"{DIR}/data/myquery_{ident}-disconnects.json", "r") as f:
            exp_disconnects = json.load(f)

        with open(f"{DIR}/data/myquery_{ident}-metadata.json", "r") as f:
            exp_metadata = json.load(f)

        return exp_data, exp_disconnects, exp_metadata

    @staticmethod
    def save_mysampler_data(ident: str, data: pd.DataFrame, disconnects: Dict[str, pd.DataFrame],
                            metadata: Dict[str, object]):
        """Convenient way to save test case data for mysampler."""
        data.to_csv(f"{DIR}/data/myquery_{ident}-data.csv")

        with open(f"{DIR}/data/myquery_{ident}-disconnects.json", "w") as f:
            # noinspection PyTypeChecker
            json.dump(myquery.normalize_data(disconnects), f)
            # json.dump(disconnects, f, cls=myquery.MyQueryEncoder)

        with open(f"{DIR}/data/myquery_{ident}-metadata.json", "w") as f:
            # noinspection PyTypeChecker
            json.dump(myquery.normalize_data(metadata), f)

    def check_mysampler_result(self, exp_data, exp_disconnects, exp_metadata, res_data,
                               res_disconnects, res_metadata):
        """Utility function for checking tes results of mysampler"""
        self.assertTrue(exp_data.equals(res_data), f"\nExpected:\n{exp_data}\nResult:\n{res_data}\n")
        self.assertDictEqual(exp_disconnects, myquery.normalize_data(res_disconnects),
                                  f"\nExpected:\n{exp_disconnects}\nResult:\n{res_disconnects}\n")
        self.assertDictEqual(exp_metadata, myquery.normalize_data(res_metadata),
                                  f"\nExpected:\n{exp_metadata}\nResult:\n{res_metadata}\n")

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
        
        exp_data, exp_disconnects, exp_metadata = self.load_mysampler_data("mysampler_1")
        self.check_mysampler_result(exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects,
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

        exp_data, exp_disconnects, exp_metadata = self.load_mysampler_data("mysampler_2")
        self.check_mysampler_result(exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects,
                               res_metadata)
