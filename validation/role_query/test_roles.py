"""Separate local role/controller tasks, real memory Channels, invented providers."""
import asyncio,tempfile,unittest
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
from cluster_p1.channel import Channel,Grant
from cluster_p1.control_exchange import ControlClient,ControlService
from cluster_p1.control_journal import ControlJournal,Conflict
from cluster_p1.projection_driver import Spec
from validation.control_exchange.test_transport import Holds
from validation.role_bridge.test_transport import streams,config
try:from cluster_p1.role_control import RegisteredRole,CoordinatorControl,RoleDescriptor
except ImportError:RegisteredRole=CoordinatorControl=RoleDescriptor=None

class Completion:
    def __init__(self):self.calls=[];self.fail=None
    def check(self,binding):self.calls.append(('check',None))
    def begin(self,binding,scope):
        self.calls.append(('begin',scope))
        if self.fail=='begin':raise RuntimeError('partial begin')
    def finish(self,binding,scope):
        self.calls.append(('finish',scope))
        if self.fail=='finish':raise RuntimeError('unknown completion')
    def quarantine(self,binding,scope):self.calls.append(('quarantine',scope))

class Tests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(RegisteredRole,'split role registration missing')
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.resources=Holds();self.authority=Holds();self.identity=Holds();self.completion=Completion()
        self.journal=ControlJournal(Path(self.tmp.name)/'journal',1024,resources=self.resources);self.addCleanup(self.journal.close)
        self.spec=Spec('owner','task','session',1,*['a'*64]*6,4,8,('b'*64,))
        cr,cw,sr,sw=streams('controller','a');grant=Grant('task','session',1,0,(0,1),'a'*64,2)
        self.channels=(Channel(cr,cw,config('a'),{'a':{grant}},capacity_bytes=32768,capacity_tiles=1),
                       Channel(sr,sw,config('controller'),{'controller':{grant}},capacity_bytes=32768,capacity_tiles=1))
        self.client=ControlClient(self.channels[0])
        self.service=ControlService(self.channels[1],self.journal,stage=0,node='a',coordinator_node='controller',device='device',instance='instance',identity=self.identity,authorize=self.authority)
        await asyncio.gather(*(c.start() for c in self.channels));self.tasks=[];self.calls=[]
    async def asyncTearDown(self):
        for t in getattr(self,'tasks',[]):
            if not t.done():t.cancel()
        await asyncio.gather(*getattr(self,'tasks',[]),return_exceptions=True)
        await asyncio.gather(*(c.close() for c in getattr(self,'channels',())),return_exceptions=True)
    def register(self,operation=None,completion=True):
        def body(scope):
            self.calls.append(scope)
            self.assertEqual((self.identity.active,self.authority.active,self.resources.active),(0,0,0))
            return {'ordinary':'bytes'}
        return RegisteredRole.register(self.service,self.spec,operation or body,declaration_digest='c'*64,charge=64,completion=self.completion if completion else None)
    async def exchange(self,role,controller,requests,method='dispatch'):
        async def serve():
            for _ in range(requests):await role.serve_once()
        server=asyncio.create_task(serve());client=asyncio.create_task(getattr(controller,method)())
        self.tasks.extend((server,client))
        result=await asyncio.wait_for(asyncio.gather(server,client),3)
        return result[1]
    async def test_independent_role_and_controller_tasks_execute_once(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        receipt=await self.exchange(role,controller,2)
        self.assertEqual(receipt['state'],'COMMITTED');self.assertFalse(receipt['execution_allowed'])
        self.assertEqual(len(self.calls),1)
        self.assertEqual([x[0] for x in self.completion.calls],['check','begin','finish'])
        self.assertEqual(self.completion.calls[1][1],self.calls[0])
        self.assertEqual(await self.exchange(role,controller,1),receipt)
        self.assertEqual(len(self.calls),1)
    async def test_coordinator_has_no_service_body_or_service_task(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        self.assertFalse(hasattr(controller,'__dict__'))
        for name in ('service','_service','body','_operation','_tasks'):self.assertFalse(hasattr(controller,name))
        with self.assertRaises(TypeError):await controller.dispatch(lambda:None)
    async def test_reconstructed_coordinator_cannot_restore_first_dispatch(self):
        role=self.register();CoordinatorControl(self.client,role.descriptor)
        with self.assertRaises(ValueError):CoordinatorControl(self.client,role.descriptor)
        with self.assertRaises(ValueError):CoordinatorControl(ControlClient(self.client.channel),role.descriptor)
    async def test_completion_absent_denies_before_local_bind(self):
        with self.assertRaises(ValueError):self.register(completion=False)
        self.assertIsNone(self.service._operation);self.assertEqual(self.calls,[])
    async def test_missing_identity_authority_resource_denies_before_bind(self):
        for obj,name in ((self.service,'identity'),(self.service,'authorize'),(self.journal,'resources')):
            old=getattr(obj,name);setattr(obj,name,None)
            try:
                with self.assertRaises((ValueError,Conflict)):self.register()
                self.assertIsNone(self.service._operation)
            finally:setattr(obj,name,old)
    async def test_registration_is_permanently_once(self):
        self.register()
        with self.assertRaises(ValueError):self.register()
    async def test_descriptor_is_copied_immutable_and_fixed_peer(self):
        role=self.register();d=role.descriptor;b=d.binding;b['device']='other'
        self.assertEqual(d.binding['device'],'device')
        with self.assertRaises((ValueError,TypeError)):replace(d,charge=True)
        wrong=replace(d,role_node='other')
        with self.assertRaises(ValueError):CoordinatorControl(self.client,wrong)
    async def test_completion_failure_unknown_retained_no_replay(self):
        self.completion.fail='finish';role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        receipt=await self.exchange(role,controller,2)
        self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(receipt['charge'],64)
        self.assertEqual(len(self.calls),1);self.assertIn('quarantine',[x[0] for x in self.completion.calls])
        self.completion.fail=None
        self.assertEqual((await self.exchange(role,controller,1))['state'],'UNKNOWN');self.assertEqual(len(self.calls),1)
    async def test_partial_begin_failure_quarantines_without_body(self):
        self.completion.fail='begin';role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        self.assertEqual((await self.exchange(role,controller,2))['state'],'UNKNOWN')
        self.assertEqual(self.calls,[]);self.assertIn('quarantine',[x[0] for x in self.completion.calls])
    async def test_invalid_result_quarantines_before_completion(self):
        role=self.register(lambda scope: {'not_json':b'bytes'});controller=CoordinatorControl(self.client,role.descriptor)
        self.assertEqual((await self.exchange(role,controller,2))['state'],'UNKNOWN')
        self.assertNotIn('finish',[x[0] for x in self.completion.calls]);self.assertIn('quarantine',[x[0] for x in self.completion.calls])
    async def test_query_before_any_dispatch_does_not_create_reservation(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        with self.assertRaises(ValueError):await controller.query()
        self.assertIsNone(self.journal.query(role.descriptor.command_id))
    async def test_completion_boolean_is_not_device_proof(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        with patch.object(self.completion,'finish',return_value=True):
            self.assertEqual((await self.exchange(role,controller,2))['state'],'UNKNOWN')
        self.assertIn('quarantine',[x[0] for x in self.completion.calls])
    async def test_changed_descriptor_cannot_rebind_role_operation(self):
        from cluster_p1.role_control import _ids
        from cluster_p1.control_exchange import _json
        role=self.register();binding=role.descriptor.binding;binding['device']='other'
        command,reservation=_ids(binding)
        changed=replace(role.descriptor,binding_json=_json(binding),command_id=command,reservation_id=reservation)
        controller=CoordinatorControl(self.client,changed)
        with self.assertRaises(ValueError):await self.exchange(role,controller,1)
        self.assertEqual(self.calls,[]);self.assertIsNone(self.journal.query(command))
    async def test_cancelled_role_quarantines_scope_and_never_rebinds(self):
        entered=asyncio.Event();exited=asyncio.Event()
        async def body(scope):
            self.calls.append(scope);entered.set()
            try:await asyncio.Future()
            finally:exited.set()
        role=self.register(body);controller=CoordinatorControl(self.client,role.descriptor)
        async def serve():
            await role.serve_once();await role.serve_once()
        server=asyncio.create_task(serve());client=asyncio.create_task(controller.dispatch());self.tasks.extend((server,client))
        await asyncio.wait_for(entered.wait(),3);server.cancel()
        with self.assertRaises(asyncio.CancelledError):await server
        client.cancel()
        with self.assertRaises(asyncio.CancelledError):await client
        self.assertTrue(exited.is_set());self.assertEqual(len(self.calls),1)
        receipt=self.journal.query(role.descriptor.command_id)
        self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(receipt['charge'],64)
        self.assertIn('quarantine',[x[0] for x in self.completion.calls])
        with self.assertRaises(ValueError):self.register(body)
    async def test_commit_failure_after_finish_quarantines(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        with patch.object(self.journal,'complete',side_effect=RuntimeError('commit unknown')):
            self.assertEqual((await self.exchange(role,controller,2))['state'],'UNKNOWN')
        phases=[x[0] for x in self.completion.calls]
        self.assertLess(phases.index('finish'),phases.index('quarantine'))
    async def test_cancelled_client_joins_owned_shutdown_despite_second_cancel(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        sent=asyncio.Event();closing=asyncio.Event();release=asyncio.Event()
        channel=self.client.channel;send=channel.send;close=channel.close
        async def send_checked(*args,**kwargs):
            await send(*args,**kwargs);sent.set()
        async def close_blocked():
            closing.set();await release.wait();await close()
        with patch.object(channel,'send',side_effect=send_checked),patch.object(channel,'close',side_effect=close_blocked):
            client=asyncio.create_task(controller.dispatch());self.tasks.append(client)
            await asyncio.wait_for(sent.wait(),3);client.cancel()
            await asyncio.wait_for(closing.wait(),3);client.cancel()
            self.assertFalse(client.done());release.set()
            with self.assertRaises(asyncio.CancelledError):await client
        self.assertTrue(channel._closed);self.assertTrue(channel._task.done());self.assertEqual(self.calls,[])
    async def test_writer_close_failure_still_joins_local_reader(self):
        role=self.register();controller=CoordinatorControl(self.client,role.descriptor)
        with patch.object(self.client.channel.w,'close',side_effect=RuntimeError('writer close failed')):
            with self.assertRaises(RuntimeError):await controller.close()
        self.assertTrue(self.client.channel._task.done())
    async def test_role_descriptor_cannot_be_replaced_after_binding(self):
        role=self.register()
        with self.assertRaises(AttributeError):role.descriptor=replace(role.descriptor,charge=65)

if __name__=='__main__':unittest.main()
