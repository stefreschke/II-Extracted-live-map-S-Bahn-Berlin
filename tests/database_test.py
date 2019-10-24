import unittest
import storing


class StoringTest(unittest.TestCase):
    def test_database_generation(self):
        storing.create_connection("test.db")


if __name__ == '__main__':
    unittest.main()
