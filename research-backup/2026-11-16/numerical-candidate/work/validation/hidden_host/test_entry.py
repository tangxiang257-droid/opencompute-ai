import unittest,importlib.util
class Entry(unittest.TestCase):
 def test_existing_receipt_host_entry(self):
  self.assertIsNotNone(importlib.util.find_spec('private_b_hidden_host'),'missing concrete B data accept/install/receive entry')
if __name__=='__main__':unittest.main()
