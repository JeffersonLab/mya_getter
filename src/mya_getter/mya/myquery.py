import re
from typing import List, Optional, Dict, Tuple
from datetime import datetime
import json

import requests
import pandas as pd
import numpy as np

from ._mya import Query


class IntervalQuery(Query):
    def __init__(self, channel: str, begin: datetime, end: datetime,
                 bin_limit: Optional[int] = None,
                 sample_type: Optional[str] = None,
                 deployment: str = "ops",
                 frac_time_digits: int = 0,
                 sig_figs: int = 6,
                 data_updates_only: bool = False,
                 prior_point: bool = False,
                 enums_as_str: bool = False,
                 unix_timestamps_ms: bool = False,
                 adjust_time_to_server_offset: bool = False,
                 integrate: bool = False,
                 **kwargs
                 ):
        """Construct a query to the myquery interval service.
        
        Args:
            channel: The name of the PV to query
            begin: The start time of the query
            end: The end time of the query
            bin_limit: How many points returned from MYA before sampling kicks in
            sample_type: What sampling algorithm should be used.  [graphical, eventsimple, myget, mysampler]
            frac_time_digits: How many digits should be displayed for fractional seconds
            sig_figs: How many significant figures should be reported in the PV values
            data_updates_only: Should the response include updates that only include value changes (not disconnects?)
            prior_point: Should the query use the most recent update prior to the start to give a value at the start of
                         the query.
            enums_as_str:  Should enum PV values be returned as their named strings instead of ints
            unix_timestamps_ms:  Should timestamps be returned as millis since unix epoch
            adjust_time_to_server_offset: Should the timestamp be localized to the myquery server
            integrate: Should the values be integrated (ony supported for float PVs)
            kwargs: Any extra parameters to be supplied to the interval web end point.
        """
        self.channel = channel
        self.begin = begin
        self.end = end
        self.bin_limit = bin_limit
        self.sample_type = sample_type
        self.deployment = deployment
        self.frac_time_digits = frac_time_digits
        self.sig_figs = sig_figs
        self.data_updates_only = data_updates_only
        self.prior_point = prior_point
        self.enums_as_str = enums_as_str
        self.unix_timestamps_ms = unix_timestamps_ms
        self.adjust_time_to_server_offset = adjust_time_to_server_offset
        self.integrate = integrate
        self.extra_opts = kwargs

    def to_web_params(self):
        """
        Convert the query to web parameters.  
        
        Based on myquery v6.2, but use of kwargs makes this more flexible.
        
        Example URL that we're targeting
        https://epicsweb.jlab.org/myquery/interval?c=R1M1GMES&b=2023-05-09&e=2023-05-09+15%3A59%3A00&l=&t=graphical&m=history&f=0&v=6&d=on&p=on&s=on&u=on&a=on&i=on
        """
        ts_fmt = "%Y-%m-%dT%H:%M:%S"
        out = {'c': self.channel, 
               'b': self.begin.strftime(ts_fmt),
               'e': self.end.strftime(ts_fmt),
               'm': self.deployment,
               'f': self.frac_time_digits,
               'v': self.sig_figs,
               }

        # It looks like the form keeps the 'l' param with "" passed if not specified
        if self.bin_limit is None:
            out['l'] = ""
        else:
            out['l'] = self.bin_limit

        # myquery app assumes it's own default if 't' is missing.  Don't need to send anything.
        if self.sample_type is not None:
            out['t'] = self.sample_type

        # API takes presence of some params to mean == true, and the web form uses 'on' instead of a boolean.
        if self.data_updates_only:
            out['d'] = 'on'
        if self.prior_point:
            out['p'] = 'on'
        if self.enums_as_str:
            out['s'] = 'on'
        if self.unix_timestamps_ms:
            out['u'] = 'on'
        if self.adjust_time_to_server_offset:
            out['a'] = 'on'
        # only valid for float events, but that's left to the user
        if self.integrate:
            out['i'] = 'on'
        
        # Allow the user to add extra options if they so choose.
        if self.extra_opts is not None:
            out.update(self.extra_opts)
        
        return out


class MySamplerQuery(Query):
    """A class for containing the arguments needed by mySampler."""

    def __init__(self, start: datetime, interval: int, num_samples: int, pvlist: List[str],
                  deployment: Optional[str] = None, **kwargs):
        self.start = start.replace(microsecond=0).isoformat().replace("T", " ")
        self.interval = interval
        self.num_samples = num_samples
        self.pvlist = pvlist
        self.deployment = deployment
        self.extra_opts = kwargs

    @staticmethod
    def from_config(start: str, interval: str, num_samples: str, pvlist: List[str], **kwargs):
        return MySamplerQuery(start=datetime.strptime(start, "%Y-%m-%d %H:%M:%S"),
                              interval=int(interval),
                              num_samples=int(num_samples),
                              pvlist=pvlist, **kwargs)
    
    def to_web_params(self) -> Dict[str, str]:
        """Convert the objects command line parameters to their web counter parts"""
        out = {}
        out['c'] = ",".join(self.pvlist)
        out['b'] = self.start.replace(" ", "T")
        out['n'] = self.num_samples
        out['m'] = self.deployment
        out['s'] = self.interval

        if self.extra_opts is not None:
            out.update(self.extra_opts)

        return out      


class Interval:
    """A class for running calls to myquery's interval endpoint.
    
    Values of the PV updates are stored as a pandas Series object in the data
    field.  Non-update events are stored as None to allow better automatic type
    detection by pandas.  Diconnect events are stored as a pandas Series in the
    disconnects field.  Other response metadata is available in the metadata
    field.
    
    The interval endpoint is intended for retrieving the mya events over the
    requested time interval.
    """
    
    def __init__(self, query: IntervalQuery, url: str = "https://epicsweb.jlab.org/myquery/interval"):
        """Construct an instance for running a myquery interval.
        
        Args:
            query: The query to run
            url: The location of the myquery/interval endpoint
        """
        self.query = query
        self.url = url
        
        self.data: Optional[pd.Series] = None
        self.disconnects: Optional[pd.Series] = None
        self.metadata: Optional[Dict[str, object]] = None

    def run(self):
        """Run a web-based myquery interval query.  This supports querying only one PV at a time.

        Args:
            query:  A query object that contains information needed by mySampler
            mysampler_url:  The base URL for the mysampler query
            options: A dictionary of key/value pairs to be passed as HTTP parameters

        Raises:
            RequestException when a problem making the query has occurred            
        """

        # Combine the two sources of options.  query object takes precendent
        opts =  self.query.to_web_params()
        r = requests.get(self.url, params=opts)
        
        if r.status_code != 200:
            raise requests.RequestException(f"Error contacting server. status={r.status_code}")

        content = r.json()
        values = []
        ts = []
        disconnect_ts = []
        disconnect_values = []
        for item in content['data']:
            if 'x' in item:
                disconnect_values.append(item['t'])
                disconnect_ts.append(item['d'])
                ts.append(item['d'])
                values.append(None)
            else:
                ts.append(item['d'])
                values.append(item['v'])
            
        data = pd.Series(values, index=ts, name=self.query.channel)
        
        # Default value for empty series is in flux.  Future will have dtype of object.  This skips a deprecation warning.
        if len(disconnect_values) == 0:
            disconnects = pd.Series(disconnect_values, index=disconnect_ts, name=self.query.channel, dtype=object)
        else:
            disconnects = pd.Series(disconnect_values, index=disconnect_ts, name=self.query.channel)
                
        metadata = {}
        for key, value in content.items():
            if key != "data":
                metadata[key] = value
        
        self.data = data
        self.disconnects = disconnects
        self.metadata = metadata


class MySampler:
    """A class for running a myquery mysampler request and holding the results.
    
    Data from all PVs are stored the data field as a single DataFrame as they
    share a common time index.  Non-update events are stored as None in the
    data field.  This should allow pandas automatic type detection to work
    in the case of non-update events.
    
    The diconnects field contains a dictionary that is keyed on each PV with
    values that are a Series of only the disconnect events.  The values of this
    Series contain the original text associated with the non-update events.
    
    Additional metadata from the myquery/mysampler response is contained in a
    dictionary under the metadata field.
    
    The mysampler endpoint is intended to provide the value of a set of PVs at
    regularly spaced time intervals.
    """

    def __init__(self, query: MySamplerQuery, url: str = "https://epicsweb.jlab.org/myquery/mysampler"):
        """Construct an instance for running a mysampler query.
        
        Args:
            query: The query to run
            url: The location of the mysampler endpoint
        """
        self.query = query
        self.url = url
        
        self.data: Optional[pd.DataFrame] = None
        self.disconnects: Optional[Dict[str, pd.Series]] = None
        self.metadata: Optional[Dict[str, object]] = None

    def run(self):
        """Run a web-based mysampler query.
        
        Results will be stored in the data, disconnects, and metadata fields.

        Raises:
            RequestException when a problem making the query has occurred
        """

        # Make the request
        opts =  self.query.to_web_params()
        r = requests.get(self.url, params=opts)
        
        # Check if we have any errors
        if r.status_code != 200:
            raise requests.RequestException(f"Error contacting server. status={r.status_code}")

        # Single top level key is channels
        channels = r.json()['channels']
        
        # This will hold the information for the data, disconnects, and metadata fields respectively
        samples = {'Date': []}
        disconnects = {}
        metadata = {}
        
        # Process the response for each channel
        for idx, channel in enumerate(channels.keys()):
            for key in channels[channel].keys():
                if key == "data":
                    v = []
                    dv = []
                    dts = []
                    for sample in channels[channel]['data']:
                        # Grab only one datetime series
                        if idx == 0:
                            samples['Date'].append(sample['d'])

                        # Handle disconnect events
                        if 't' in sample.keys():
                            v.append(None)
                            dts.append(sample['d'])
                            dv.append(sample['t'])
                        else:
                            v.append(sample['v'])
                    samples[channel] = v

                    if len(dts) > 0:
                        disconnects[channel] = pd.Series(dv, index=dts, name=channel)
                
                else:
                    if channel not in metadata.keys():
                        metadata[channel] = {}
                    metadata[channel][key] = channels[channel][key]

        data = pd.DataFrame(samples)
        data.Date = pd.to_datetime(data.Date)
        data = data.set_index("Date")

        # Update the object with the processed response
        self.data = data
        self.disconnects = disconnects
        self.metadata = metadata
       
    
def normalize_data(obj):
    """Prepare data for use by json.dump.
    
    Pandas won't json encode nicely, and I want to save numpy numbers directly"""
    if isinstance(obj, pd.Series):
        # Does not support "split".  => {"__type__: "series", idx1: val1, idx2:val2, ...}
        return {"__type__": "series", **obj.to_dict()}
    if isinstance(obj, pd.DataFrame):
        # split => {"index": [], "columns": [], "data": []}
        return {"__type__": "dataframe", **obj.to_dict(orient="split")}
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    if isinstance(obj, dict):
        # ensure JSON-safe keys and normalized values
        return {str(k): normalize_data(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_data(v) for v in obj]

    return obj    
    