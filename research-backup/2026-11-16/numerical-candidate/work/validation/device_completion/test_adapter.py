"""AST only, symbolic tensors; verifies production slice re-entry check."""
import unittest
from validation.one_token import test_projection_adapter as fixture
class Tests(fixture.Tests):
 def test_projection_rechecks_scope_before_next_linear(self):
  adapter,handle=self.prepare()
  self.completion.open=False
  with self.assertRaises(ValueError):adapter.project_slice('scope',handle,0,8192)
  self.assertFalse(any(x[0]=='linear' for x in self.log))
if __name__=='__main__':unittest.main()
