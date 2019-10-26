import unittest

class Stuff(unittest.TestCase):
    def test_number_of_lines(self):
        with open('../data/traffic_data.json') as file:
            print("file is {} lines long".format(len(file.readlines())))
