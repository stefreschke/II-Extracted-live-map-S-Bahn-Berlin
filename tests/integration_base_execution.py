import unittest


class WholeExecution(unittest.TestCase):
    def test_execution(self):
        import integration
        integration.main()


if __name__ == '__main__':
    unittest.main()
