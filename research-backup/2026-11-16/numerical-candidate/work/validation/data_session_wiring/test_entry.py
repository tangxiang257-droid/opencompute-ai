import unittest,importlib.util
class Entry(unittest.TestCase):
 def test_installed_A_session_entry_exists(self):
  self.assertIsNotNone(importlib.util.find_spec('cluster_p1.hidden_session'),'missing original installed A data-session assembly')
if __name__=='__main__':unittest.main()
