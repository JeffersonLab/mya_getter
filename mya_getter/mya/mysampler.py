import re
import io
from typing import List

from datetime import datetime
import pandas as pd
import logging
import subprocess
from . import Query


class MySamplerQuery(Query):
    """A class for containing the arguments needed by mySampler."""

    def __init__(self, start: datetime, interval: str, num_samples: int, pvlist: List[str]):
        self.start = start.isoformat().replace("T", " ")
        self.interval = interval
        self.num_samples = num_samples
        self.pvlist = pvlist


# noinspection PyPep8Naming
def mySampler(query: MySamplerQuery, mysampler_cmd: str = '/usr/csite/certified/bin/mySampler') -> pd.DataFrame:
    """Run mySampler with the specified arguments and return a single row DataFrame.

    The results for each PV is saved as a column with of tuples.  The tuples contain the individual samples in order.

    Args:
        query:  A query object that contains information needed by mySampler
        mysampler_cmd: The path the to mySampler command

    Raises:
        SubprocessError when something goes wrong with mySampler call
    """

    # Run the mySampler command to get samples at 1s intervals
    args = [mysampler_cmd, '-b', query.start, '-s', query.interval, '-n', str(query.num_samples)] + query.pvlist
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

