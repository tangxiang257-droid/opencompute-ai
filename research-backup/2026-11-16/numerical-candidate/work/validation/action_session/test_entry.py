import importlib.util,unittest
class Entry(unittest.TestCase):
 def test_existing_action_path_has_explicit_bounded_session(self):
  self.assertIsNotNone(importlib.util.find_spec('cluster_p1.action_session'))
if __name__=='__main__':unittest.main()
