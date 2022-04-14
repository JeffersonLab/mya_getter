import pandas as pd
import subprocess
from io import StringIO
from datetime import datetime
from typing import List
from ._mya import Query


class MyDataQuery(Query):
    """A class for containing the arguments needed for a call to myData."""

    def __init__(self, begin: datetime, end: datetime, pvlist: List[str]):
        self.begin = begin.isoformat().replace("T", " ")[:-7]
        self.end = end.isoformat().replace("T", " ")[:-7]
        self.pv_list = pvlist


# noinspection PyPep8Naming
def myData(query: MyDataQuery, mydata_cmd: str = '/usr/csite/certified/bin/myData') -> pd.DataFrame:
    """A wrapper on the myData command line application.

    Args:
        query: The object containing the query information.
        mydata_cmd: The path to the myData command
"""

    args = [mydata_cmd, '-b', query.begin, '-e', query.end, ] + query.pv_list
    output = subprocess.run(args=args, check=True, capture_output=True)
    lines = output.stdout.decode('UTF-8').split('\n')

    header = ';'.join(lines[0].strip().split())
    out = [header]
    for line in lines[1:]:
        # Get date and time explicitly, the rest go into values
        if len(line) == 0:
            continue

        date, time, *values = line.strip().split()
        values = ';'.join(values)
        out.append(f"{date}T{time};{values}")

    df = pd.read_csv(StringIO('\n'.join(out)), sep=';')
    df.Date = pd.to_datetime(df.Date)

    return df
