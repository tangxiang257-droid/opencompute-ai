import asyncio,sqlite3,struct,tempfile,unittest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from test_p1_tile_contract_v2 import fixture
from cluster_p1.local_attempt import LocalAttemptJournal
from cluster_p1.store import Conflict
@contextmanager
def auth():yield 'owner'
class Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'db';self.args=fixture();self.allowed=True
        @contextmanager
        def execution(spec):
            if not self.allowed:raise PermissionError('revoked')
            yield 'owner'
        self.j=LocalAttemptJournal.create(self.path,*self.args[:3],authorize=auth,execution_authorize=execution)
        self.j.register(self.args[3],hidden_size=24,vocab_size=32,event_id='plan')
        self.inputs=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5))
        self.calls=[]
    def owner(self):
        def op(value,**kw):self.calls.append(value);return b'\0'*(len(value)//8*24*4)
        return self.j.make_owner(op,self.inputs,instance_id='instance')
    async def test_bytes_commit_and_no_second_permission(self):
        o=self.owner()
        try:out=await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        r=self.j.query(0);self.assertEqual(r['state'],'OUTPUT_COMMITTED');self.assertEqual(r['output'],out)
        self.assertFalse(r['execution_allowed'])
        o=self.owner()
        try:
            with self.assertRaises(Conflict):await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        self.assertEqual(len(self.calls),1)
    async def test_revocation_no_admission(self):
        o=self.owner();self.allowed=False
        try:
            with self.assertRaises(PermissionError):await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        self.assertIsNone(self.j.query(0));self.assertEqual(self.calls,[])
    async def test_cancel_after_operation_blocks_output(self):
        def op(value,**kw):self.j.cancel(event_id='cancel');return b'\0'*192
        o=self.j.make_owner(op,self.inputs,instance_id='instance')
        try:
            with self.assertRaises(Conflict):await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        r=self.j.query(0);self.assertEqual(r['state'],'ADMITTED');self.assertIsNone(r['output'])
    async def test_admission_event_failure_rolls_back_bytes(self):
        o=self.owner()
        try:
            with patch.object(self.j,'_record_event',side_effect=RuntimeError('fault')):
                with self.assertRaises(RuntimeError):await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        self.assertIsNone(self.j.query(0));self.assertEqual(self.calls,[])
    async def test_wrong_input_or_count_no_write(self):
        for value,count in [(b'\0'*16,2),(self.inputs[0],1)]:
            o=self.owner()
            try:
                with self.assertRaises(Conflict):await o.call(value,token_start=0,token_count=count)
            finally:await o.close()
        self.assertIsNone(self.j.query(0));self.assertEqual(self.calls,[])
    async def test_output_event_failure_rolls_back_output_only(self):
        original=self.j._record_event
        def record(c,event,*args):
            if event.startswith('local:output:'):raise RuntimeError('output fault')
            return original(c,event,*args)
        o=self.owner()
        try:
            with patch.object(self.j,'_record_event',side_effect=record):
                with self.assertRaises(RuntimeError):await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        self.assertEqual(self.j.query(0)['state'],'ADMITTED');self.assertIsNone(self.j.query(0)['output'])
    async def test_post_commit_execution_exit_unknown_query_resolves(self):
        calls=[0]
        @contextmanager
        def exit_fault(spec):
            calls[0]+=1
            yield 'owner'
            if calls[0]==2:raise RuntimeError('exit after commit')
        self.j._execution=exit_fault;o=self.owner()
        try:
            with self.assertRaises(RuntimeError):await o.call(self.inputs[0],token_start=0,token_count=2)
            self.assertEqual(o.state.commit,'unconfirmed')
        finally:await o.close()
        self.assertEqual(self.j.query(0)['state'],'OUTPUT_COMMITTED')
    async def test_post_admission_exit_never_executes_but_keeps_attempt(self):
        @contextmanager
        def exit_fault(spec):
            yield 'owner'
            raise RuntimeError('exit after admission')
        self.j._execution=exit_fault;o=self.owner()
        try:
            with self.assertRaises(RuntimeError):await o.call(self.inputs[0],token_start=0,token_count=2)
        finally:await o.close()
        self.assertEqual(self.j.query(0)['state'],'ADMITTED');self.assertEqual(self.calls,[])
    async def test_two_owners_one_admission(self):
        a=self.owner();b=self.owner()
        try:
            results=await asyncio.gather(a.call(self.inputs[0],token_start=0,token_count=2),
                b.call(self.inputs[0],token_start=0,token_count=2),return_exceptions=True)
            self.assertEqual(sum(type(x) is bytes for x in results),1)
            self.assertEqual(sum(isinstance(x,Conflict) for x in results),1)
        finally:await a.close();await b.close()
        self.assertEqual(len(self.calls),1)
    async def test_full_prefix_and_invalid_output(self):
        o=self.owner()
        try:
            for value,start,count in zip(self.inputs,(0,2,4),(2,2,1)):
                await o.call(value,token_start=start,token_count=count)
        finally:await o.close()
        self.assertEqual(o.state.next_token,5)
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('select sum(length(input)),sum(length(output)) from p1_local_attempts').fetchone(),(40,480))
    async def test_existing_path_never_reopened(self):
        before=self.path.read_bytes()
        with self.assertRaises(FileExistsError):
            LocalAttemptJournal.create(self.path,*self.args[:3],authorize=auth,execution_authorize=lambda spec:auth())
        self.assertEqual(self.path.read_bytes(),before)
    async def test_suppressing_execution_cannot_repeat_operation(self):
        @contextmanager
        def swallowing(spec):
            try:yield 'owner'
            except BaseException:pass
        self.j._execution=swallowing
        first=self.owner()
        try:await first.call(self.inputs[0],token_start=0,token_count=2)
        finally:await first.close()
        second=self.owner()
        try:
            with self.assertRaises(Conflict):
                await second.call(self.inputs[0],token_start=0,token_count=2)
        finally:await second.close()
        self.assertEqual(len(self.calls),1)
    async def test_suppressing_owner_cannot_repeat_operation(self):
        @contextmanager
        def swallowing():
            try:yield 'owner'
            except BaseException:pass
        self.j._authorize=swallowing
        first=self.owner()
        try:await first.call(self.inputs[0],token_start=0,token_count=2)
        finally:await first.close()
        second=self.owner()
        try:
            with self.assertRaises(Conflict):await second.call(self.inputs[0],token_start=0,token_count=2)
        finally:await second.close()
        self.assertEqual(len(self.calls),1)
    async def test_suppression_never_confirms_failed_output_transaction(self):
        @contextmanager
        def swallowing(*args):
            try:yield 'owner'
            except BaseException:pass
        self.j._execution=swallowing;self.j._authorize=swallowing
        original=self.j._record_event
        def record(c,event,*args):
            if event.startswith('local:output:'):raise Conflict('injected output mismatch')
            return original(c,event,*args)
        o=self.owner()
        try:
            with patch.object(self.j,'_record_event',side_effect=record):
                with self.assertRaises(Conflict):await o.call(self.inputs[0],token_start=0,token_count=2)
            self.assertEqual(o.state.commit,'unconfirmed');self.assertEqual(o.state.status,'retired')
        finally:await o.close()
        self.assertEqual(self.j.query(0)['state'],'ADMITTED');self.assertIsNone(self.j.query(0)['output'])
    async def test_suppression_preserves_all_base_exception_instances(self):
        from cluster_p1.local_attempt import _non_suppressing
        for error in (Conflict('x'),RuntimeError('x'),KeyboardInterrupt('x'),asyncio.CancelledError('x')):
            @contextmanager
            def swallowing():
                try:yield 'owner'
                except BaseException:pass
            try:
                with _non_suppressing(swallowing()),_non_suppressing(swallowing()):raise error
            except BaseException as caught:self.assertIs(caught,error)
            else:self.fail('exception suppressed')
    async def test_execution_exit_error_not_swallowed_by_owner(self):
        @contextmanager
        def owner():
            try:yield 'owner'
            except BaseException:pass
        error=RuntimeError('exit error')
        @contextmanager
        def execution(spec):
            yield 'owner'
            raise error
        self.j._authorize=owner;self.j._execution=execution;o=self.owner()
        try:
            with self.assertRaises(RuntimeError) as cm:await o.call(self.inputs[0],token_start=0,token_count=2)
            self.assertIs(cm.exception,error)
        finally:await o.close()
        self.assertEqual(self.calls,[]);self.assertEqual(self.j.query(0)['state'],'ADMITTED')
    async def test_initialization_suppression_keeps_original_failure(self):
        @contextmanager
        def swallowing():
            try:yield 'owner'
            except BaseException:pass
        error=RuntimeError('bootstrap')
        from cluster_p1.store import P1Store
        with patch.object(P1Store,'bind_session',side_effect=error):
            with self.assertRaises(RuntimeError) as cm:
                LocalAttemptJournal.create(Path(self.tmp.name)/'partial',*self.args[:3],
                    authorize=swallowing,execution_authorize=lambda spec:auth())
        self.assertIs(cm.exception,error)
    async def test_commit_attempt_mismatch_not_suppressed(self):
        @contextmanager
        def swallowing(*args):
            try:yield 'owner'
            except BaseException:pass
        self.j._authorize=swallowing;self.j._execution=swallowing
        def corrupt(value,**kw):
            with sqlite3.connect(self.path) as c:
                c.execute("UPDATE p1_local_attempts SET instance='different'")
            return b'\0'*192
        o=self.j.make_owner(corrupt,self.inputs,instance_id='instance')
        try:
            with self.assertRaises(Conflict):await o.call(self.inputs[0],token_start=0,token_count=2)
            self.assertEqual(o.state.commit,'unconfirmed')
        finally:await o.close()
        self.assertIsNone(self.j.query(0)['output'])
