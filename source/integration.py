"""
Integration step for S Bahn Berlin data. Run this file to load and convert S Bahn data in nearby
files accordingly.
"""
import json
from pandas.io.json import json_normalize


def main():
    """
    Execution of integration step.
    :return:
    """
    with open('../data/traffic_data.json') as file:
        lines = file.readlines()
        data = [json.loads(object) for object in lines]
        frame = json_normalize(data)


if __name__ == '__main__':
    main()
