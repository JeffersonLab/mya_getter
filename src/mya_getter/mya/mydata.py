import pandas as pd
import subprocess
from io import StringIO
from datetime import datetime
from typing import List, Optional
from ._mya import Query


class MyDataQuery(Query):
    """A class for containing the arguments needed for a call to myData."""

    def __init__(self, begin: datetime, end: datetime, pvlist: List[str],
                 filter: Optional[str] = None, ignore_undef: bool = False,
                 time_precision: Optional[int] = None,
                 deployment: Optional[str] = None):
        self.begin = begin.replace(microsecond=0).isoformat().replace("T", " ")
        self.end = end.replace(microsecond=0).isoformat().replace("T", " ")
        self.pv_list = pvlist
        self.filter = filter
        self.ignore_undef = ignore_undef
        self.time_precision = time_precision
        self.deployment = deployment


    @staticmethod
    def from_config(begin: str, end: str, pvlist: List[str], **kwargs):
        return MyDataQuery(begin=datetime.strptime(begin, "%Y-%m-%d %H:%M:%S"),
                           end=datetime.strptime(end, "%Y-%m-%d %H:%M:%S"),
                           pvlist=pvlist, **kwargs)


# noinspection PyPep8Naming
def myData(query: MyDataQuery, mydata_cmd: str = '/usr/csite/certified/bin/myData',
           options: Optional[List[str]] = None) -> pd.DataFrame:
    """A wrapper on the myData command line application.

    Args:
        query: The object containing the query information.
        mydata_cmd: The path to the myData command
        options: A list of command line options to be applied to the query other
                 the the start, end, and PV list of the query.  Think deployment.
"""

    args = [mydata_cmd, '-b', query.begin, '-e', query.end]
    if query.filter is not None:
        args.append("-f")
        args.append(query.filter)
    if query.ignore_undef:
        args.append("-i")
    if query.time_precision is not None:
        args.append("-p")
        args.append(f"{query.time_precision}")
    if query.deployment is not None:
        args.append("-m")
        args.append(query.deployment)
        
    if options is None:
        args = args + query.pv_list
    else:
        args = args + options + query.pv_list
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
