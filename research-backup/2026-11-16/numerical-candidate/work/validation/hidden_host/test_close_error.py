import unittest
from validation.hidden_host.test_pipeline import Flow

def leaves(error):
 pending=[error];seen=set();out=[]
 while pending:
  e=pending.pop()
  if id(e) in seen:continue
  seen.add(id(e))
  if isinstance(e,BaseExceptionGroup):pending.extend(e.exceptions)
  else:out.append(e)
  pending.extend(x for x in (e.__cause__,e.__context__) if x is not None)
 return out
class CloseError(Flow):
 async def test_original_wait_closed_error_is_retained(self):
  h=self.hidden_host;l=h.launcher;w=h.slot.writer;original=w.wait_closed
  marker=OSError('LAB hidden wait_closed failure');observed=[]
  async def failed():raise marker
  w.wait_closed=failed
  try:
   for owner in (h,l):
    try:await owner.close()
    except BaseException as error:
     observed.append(error);actual=leaves(error)
     self.assertTrue(actual);self.assertTrue(all(e is marker for e in actual),'unexpected cleanup failure must remain a test failure')
    else:self.fail('injected close failure was swallowed')
    self.assertTrue(owner.cleanup.done() if owner is h else owner._close_task.done())
    self.owned.remove(owner) # exact observed terminal error acknowledged above, not suppressed
   self.assertEqual(h.state,'UNKNOWN');self.assertEqual(h.resources.query(h.ticket.reservation_id)['state'],'UNKNOWN')
   self.assertTrue(w.closed);self.assertTrue(h.prepared.channel._task.done())
   try:h.cleanup.result()
   except BaseException as error:self.assertIs(error,observed[0])
   else:self.fail('retained cleanup task lost its original failure')
  finally:w.wait_closed=original
  self.observed_hidden_close_errors=tuple(observed)
def load_tests(*a):return unittest.TestSuite([CloseError('test_original_wait_closed_error_is_retained')])
if __name__=='__main__':unittest.main()
