import unittest


class WholeExecution(unittest.TestCase):
    @unittest.skip("Main test, do not execute in build")
    def test_execution(self):
        import extracting
        extracting.main()


if __name__ == '__main__':
    unittest.main()
