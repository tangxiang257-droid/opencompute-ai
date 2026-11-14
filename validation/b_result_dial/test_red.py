import unittest
from private_c_result_host import CResultHost
class Red(unittest.TestCase):
 def test_pending_transport_preparation_is_available(self):self.assertTrue(callable(getattr(CResultHost,'prepare',None)),'actual connection generation cannot precede B operation with ACTIVE-only C host')
if __name__=='__main__':unittest.main()
