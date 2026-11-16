import asyncio,unittest,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):self.borrowed=[]
 async def asyncTearDown(self):
  for task in self.borrowed:
   if not task.done():task.cancel()
  await asyncio.gather(*self.borrowed,return_exceptions=True)
 def api(self):
  import startup_supervisor
  return startup_supervisor
 def dependency(self):
  event=asyncio.Event();task=asyncio.create_task(event.wait());self.borrowed.append(task);return event,task
 async def bounded(self,awaitable):return await asyncio.wait_for(awaitable,5)
 async def test_success_borrows_reader(self):
  api=self.api();_,reader=self.dependency();value=object()
  async def body():return value
  self.assertIs(await self.bounded(api.supervise_startup(body,(reader,))),value);self.assertFalse(reader.done())
 async def test_already_ended_never_calls_factory(self):
  api=self.api();event,reader=self.dependency();event.set();await reader;calls=[]
  def factory():calls.append(1);raise AssertionError
  with self.assertRaises(api.DependencyEnded):await self.bounded(api.supervise_startup(factory,(reader,)))
  self.assertEqual(calls,[])
 async def test_dependency_end_joins_child(self):
  api=self.api();event,reader=self.dependency();_,other=self.dependency();entered=asyncio.Event();settled=asyncio.Event()
  async def body():
   try:entered.set();await asyncio.Event().wait()
   finally:settled.set()
  work=asyncio.create_task(api.supervise_startup(body,(reader,other)))
  try:
   await self.bounded(entered.wait());event.set()
   with self.assertRaises(api.DependencyEnded):await self.bounded(work)
   self.assertTrue(settled.is_set());self.assertFalse(other.done())
  finally:
   if not work.done():work.cancel()
   await asyncio.gather(work,return_exceptions=True)
 async def test_child_failure_identity(self):
  api=self.api();_,reader=self.dependency();error=ValueError('fixture')
  async def body():raise error
  with self.assertRaises(ValueError) as got:await self.bounded(api.supervise_startup(body,(reader,)))
  self.assertIs(got.exception,error);self.assertFalse(reader.done())
 async def test_simultaneous_reader_and_child_denies(self):
  api=self.api();event,reader=self.dependency()
  async def body():event.set();await reader;return 'not-admitted'
  with self.assertRaises(api.DependencyEnded):await self.bounded(api.supervise_startup(body,(reader,)))
 async def test_repeated_cancel_waits_cleanup_and_preserves_error(self):
  api=self.api();_,reader=self.dependency();entered=asyncio.Event();cleaning=asyncio.Event();release=asyncio.Event();finished=asyncio.Event();failure=RuntimeError('fixture cleanup')
  async def body():
   try:entered.set();await asyncio.Event().wait()
   finally:
    cleaning.set();await release.wait();finished.set();raise failure
  work=asyncio.create_task(api.supervise_startup(body,(reader,)))
  try:
   await self.bounded(entered.wait());work.cancel();await self.bounded(cleaning.wait());work.cancel()
   checkpoint=asyncio.get_running_loop().create_future()
   asyncio.get_running_loop().call_soon(checkpoint.set_result,None)
   await self.bounded(checkpoint)
   self.assertFalse(work.done());self.assertFalse(finished.is_set());self.assertFalse(reader.done());release.set()
   with self.assertRaises(asyncio.CancelledError) as got:await self.bounded(work)
   seen=set();pending=[got.exception];found=False
   while pending:
    e=pending.pop()
    if id(e) in seen:continue
    seen.add(id(e));found=found or e is failure
    pending.extend(x for x in (e.__cause__,e.__context__) if x is not None)
    if isinstance(e,BaseExceptionGroup):pending.extend(e.exceptions)
   self.assertTrue(found);self.assertTrue(finished.is_set());self.assertFalse(reader.done())
  finally:
   release.set()
   if not work.done():work.cancel()
   await asyncio.gather(work,return_exceptions=True)
 async def test_cancel_before_supervisor_entry_no_factory(self):
  api=self.api();_,reader=self.dependency();calls=[]
  async def body():calls.append(1)
  work=asyncio.create_task(api.supervise_startup(body,(reader,)));work.cancel()
  with self.assertRaises(asyncio.CancelledError):await self.bounded(work)
  self.assertEqual(calls,[]);self.assertFalse(reader.done())
 async def test_child_self_cancel_propagates(self):
  api=self.api();_,reader=self.dependency()
  async def body():raise asyncio.CancelledError()
  with self.assertRaises(asyncio.CancelledError):await self.bounded(api.supervise_startup(body,(reader,)))
  self.assertFalse(reader.done())
 async def test_simultaneous_child_error_retained(self):
  api=self.api();event,reader=self.dependency();failure=RuntimeError('fixture child')
  async def body():event.set();await reader;raise failure
  try:await self.bounded(api.supervise_startup(body,(reader,)))
  except BaseException as error:
   pending=[error];seen=set();found=False
   while pending:
    e=pending.pop()
    if id(e) in seen:continue
    seen.add(id(e));found=found or e is failure
    pending.extend(x for x in (e.__cause__,e.__context__) if x is not None)
    if isinstance(e,BaseExceptionGroup):pending.extend(e.exceptions)
   self.assertTrue(found)
  else:self.fail('dependency end not rejected')
 async def test_reader_ends_during_success_settlement_denies(self):
  api=self.api();event,reader=self.dependency();loop=asyncio.get_running_loop()
  async def body():
   loop.call_soon(loop.call_soon,event.set)
   return 'not-admitted'
  with self.assertRaises(api.DependencyEnded):await self.bounded(api.supervise_startup(body,(reader,)))
  self.assertTrue(reader.done())
if __name__=='__main__':unittest.main()
