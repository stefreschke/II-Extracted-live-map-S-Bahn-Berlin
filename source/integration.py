"""
Integration step for S Bahn Berlin data. Run this file to load and convert S Bahn data in nearby
files accordingly.
"""
import json


def main():
    with open('../data/tweets_SBahnBerlin.json') as f:
        line = f.readline()
        DATA = json.loads(line)
        print(DATA)


if __name__ == '__main__':
    main()
