"""
Integration step for S Bahn Berlin data. Run this file to load and convert S Bahn data in nearby
files accordingly.
"""
import json
import pandas as pd
from pandas.io.json import json_normalize
import converting


def main():
    """
    Execution of integration step.
    :return:
    """
    with open('../data/traffic_data.json') as file:
        for index, line in enumerate(file):
            parts = line.split("|")
            timestamp = parts[0]
            suspected_json = "|".join(parts[1:])
            data_set = json.loads(suspected_json)
            entries = data_set["t"]
            df = converting(json_normalize(entries))



class JSONParseException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


if __name__ == '__main__':
    main()
