import unittest
import storing


class StoringTest(unittest.TestCase):
    def test_database_generation(self):
        storing.write_dataframe_to_sql("test.db")

    def test_read_frames(self):
        storing.read_data_frames()

    def test_read_and_sql(self):
        storing.read_data_frame_and_convert()


if __name__ == '__main__':
    unittest.main()
