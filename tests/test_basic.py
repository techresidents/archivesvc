import unittest

from testbase import IntegrationTestCase

class BasicTest(IntegrationTestCase):

    @classmethod
    def setUpClass(cls):
        IntegrationTestCase.setUpClass()

    @classmethod
    def tearDownClass(cls):
        IntegrationTestCase.tearDownClass()

    def test_getName(self):
        result = self.service_proxy.getName(self.request_context)
        self.assertEqual(result, self.service_name)
    
    def test_getVersion(self):
        result = self.service_proxy.getVersion(self.request_context)
        self.assertIsInstance(result, basestring)

    def test_getBuildNumber(self):
        result = self.service_proxy.getBuildNumber(self.request_context)
        self.assertIsInstance(result, basestring)
    
    def test_getStatus(self):
        result = self.service_proxy.getStatus(self.request_context)
        self.assertIsInstance(result, int)

    def test_getCounter(self):
        result = self.service_proxy.getCounter(self.request_context, "open_requests")
        self.assertIsInstance(result, int)
        self.assertEqual(result, 1)

    def test_getCounters(self):
        result = self.service_proxy.getCounters(self.request_context)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["open_requests"], 1)

    def test_getOptions(self):
        result = self.service_proxy.getOptions(self.request_context)
        self.assertIsInstance(result, dict)

if __name__ == '__main__':
    unittest.main()
