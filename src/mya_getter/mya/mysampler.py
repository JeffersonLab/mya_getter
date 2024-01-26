import re
import io
from typing import List, Optional, Dict
from datetime import datetime
import logging

import subprocess
import requests
import pandas as pd

from ._mya import Query

# Time-related constants
MILLIS_PER_SECOND = 1_000
MILLIS_PER_MINUTE = 60_000
MILLIS_PER_HOUR = 3_600_000
MILLIS_PER_DAY = 86_400_000
MILLIS_PER_WEEK = 604_800_000


class MySamplerQuery(Query):
    """A class for containing the arguments needed by mySampler."""

    def __init__(self, start: datetime, interval: str, num_samples: int, pvlist: List[str],
                  deployment: Optional[str] = None):
        self.start = start.replace(microsecond=0).isoformat().replace("T", " ")
        self.interval = interval.strip()
        self.num_samples = num_samples
        self.pvlist = pvlist
        self.deployment = deployment

    @staticmethod
    def from_config(start: str, interval: str, num_samples: str, pvlist: List[str], **kwargs):
        return MySamplerQuery(start=datetime.strptime(start, "%Y-%m-%d %H:%M:%S"),
                              interval=interval,
                              num_samples=int(num_samples),
                              pvlist=pvlist, **kwargs)
    
    def to_web_params(self) -> Dict[str, str]:
        """Convert the objects command line parameters to their web counter parts"""
        out = {}
        out['c'] = ",".join(self.pvlist)
        out['b'] = self.start.replace(" ", "T")
        out['n'] = self.num_samples
        out['m'] = self.deployment
        
        # Need milliseconds between samples.  Have to parse variety of strings.
        time_pattern = '^(\d+)(\D)$'
        if re.match(time_pattern, self.interval):
            match = re.match(time_pattern, self.interval)
            multiplier = int(match.group(1))  # mySample CLI does not support floats
            unit = match.group(2)

            if unit == 's':
                millis = MILLIS_PER_SECOND
            elif unit == 'm':
                millis = MILLIS_PER_MINUTE
            elif unit == 'h':
                millis = MILLIS_PER_WEEK
            elif unit == 'd':
                millis = MILLIS_PER_DAY
            elif unit == 'w':
                millis = MILLIS_PER_WEEK
            else:
                raise ValueError(f"Unsupported time specification '{unit}")
            out['s'] = int(millis) * multiplier
        else:
            raise ValueError(f"Unsupported time specification '{self.interval}")

        return out      


def mySamplerWeb(query: MySamplerQuery, mysampler_url: str = "https://epicsweb.jlab.org/myquery/mysampler",
                 options: Optional[Dict[str,str]] = None) -> pd.DataFrame:
    """Run a web-based mysampler query.

    Args:
        query:  A query object that contains information needed by mySampler
        mysampler_url:  The base URL for the mysampler query
        options: A dictionary of key/value pairs to be passed as HTTP parameters

    Raises:
        RequestException when a problem making the query has occurred
    """

    # Combine the two sources of options.  query object takes precendent
    q_opts =  query.to_web_params()

    opts = q_opts
    if options is not None:
        opts = options
        for key in q_opts.keys():
            opts[key] = q_opts[key]

    r = requests.get(mysampler_url, params=opts)
    
    if r.status_code != 200:
        raise requests.RequestException(f"Error contacting server. status={r.status_code}")

    channels = r.json()['channels']
    samples = {'Date': []}
    # If a channel has a disconnect event, then the mySampler CLI would have returned "<undefined>"
    # which would have made the entire series a str type.  Enforce that behavior for consistency, even though I
    # could query the metadata to figure out if <undefined> should be switched NaN and keep the numbers.
    types = {}
    for idx, channel in enumerate(channels.keys()):
        v = []
        for sample in channels[channel]['data']:
            # Grab only one datetime series
            if idx == 1:
                samples['Date'].append(sample['d'])

            if 't' in sample.keys():
                # The mySampler CLI simply returns <undefined>.  Better to match that than make users
                # think about why there are incosistencies.
                v.append("<undefined>")
                types[channel] = 'str'
            else:
                v.append(sample['v'])
        samples[channel] = v
    
    df = pd.DataFrame(samples)
    df.Date = df.Date.str.replace("T", "_")
    df = df.astype(types)

    return df

# noinspection PyPep8Naming
def mySampler(query: MySamplerQuery, mysampler_cmd: str = '/usr/csite/certified/bin/mySampler',
              options: Optional[List[str]] = None) -> pd.DataFrame:
    """Run mySampler with the specified arguments and return a single row DataFrame.

    The results for each PV is saved as a column with of tuples.  The tuples contain the individual samples in order.

    Args:
        query:  A query object that contains information needed by mySampler
        mysampler_cmd: The path the to mySampler command

    Raises:
        SubprocessError when something goes wrong with mySampler call
    """

    # Run the mySampler command to get samples at 1s intervals
    args = [mysampler_cmd, '-b', query.start, '-s', query.interval, '-n', str(query.num_samples)]
    if query.deployment is not None:
        args.append('-m')
        args.append(query.deployment)

    if options is not None:
        args = args + options
    args = args + query.pvlist

    logging.info(f"Starting {args[:7]} + {query.pvlist[0]}, ... ({len(query.pvlist)} PVs)")
    output = subprocess.run(args=args, check=True, capture_output=True)
    logging.info(f"Finished {args[:7]} + {query.pvlist[0]}, ... ({len(query.pvlist)} PVs)")
    lines = output.stdout.decode('UTF-8').split('\n')
    date_pattern = re.compile(r'^(\d\d\d\d-\d\d-\d\d) (\d.*)')
    space_pattern = re.compile(r'(\s+)')

    # mySampler returns a human readable format.  We want a CSV for pandas.
    processed_lines = []
    for line in lines:
        # Remove trailing or leading whitespace
        line = line.strip()

        # Modify the timestamp format
        line = re.sub(date_pattern, r"\1_\2", line)

        # Convert multiple spaces to a single comma to be CSV compatible
        line = re.sub(space_pattern, ',', line)

        processed_lines.append(line)

    # Create a single CSV string, then read it into a DataFrame
    csv_out = "\n".join(processed_lines)
    df = pd.read_csv(io.StringIO(csv_out))

    return df

