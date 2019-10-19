"""
Integration step for S Bahn Berlin data. Run this file to load and convert S Bahn data in nearby
files accordingly.
"""
import json

if __name__ == '__main__':
    with open('tweets_SBahnBerlin.json') as f:
        DATA = json.load(f)
        print(DATA)
