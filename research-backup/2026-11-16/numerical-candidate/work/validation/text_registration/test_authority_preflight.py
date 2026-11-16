"""Real Authority API with TEST-ONLY explicit v12 assembly; ordinary bytes only.
No production factory flag, tokenizer, model, worker or node is introduced.
"""
from dataclasses import replace
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
import controlled_delivery_authority as control
from ledger_database import PathTrust
from result_artifacts import Limits
from text_input_ledger import TextInputLedger
from token_input_snapshot import InputBinding,publish_input

class Checks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        for name in ('db','inputs','results'):(self.root/name).mkdir(mode=0o700)
        self.fds=[]
        for path in (self.root,self.root/'inputs',self.root/'results'):
            fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY);self.fds.append(fd);self.addCleanup(os.close,fd)
        self.a=None
        self.binding=InputBinding('alice','request','a'*64,'internal-test-only','c'*64,100)
    async def asyncTearDown(self):
        if self.a is not None:await self.a.close()
    def open(self,v12):
        kwargs=dict(deployment=control.ControlledDeployment(True),recipient='closed-local',certificate_pins={'unused':'a'*64},initialize=True,enable_unified=True)
        args=(PathTrust(self.fds[0],('db',)),self.fds[1],self.fds[2],Limits(2097152,4194304,8388608,16777216,100,4))
        if v12:
            # Deliberately isolated test assembler, NOT a supported factory option.
            with patch.object(control,'_UnifiedLedger',TextInputLedger):self.a=control.open_authority(*args,**kwargs)
        else:self.a=control.open_authority(*args,**kwargs)
        return self.a
    async def source(self,v12):
        a=self.open(v12);l=a._store._managed._ledger
        raw=b'ordinary text';ref=publish_input(self.fds[1],self.binding,(1,2,3))
        async with a._lock:
            with l._transaction():
                draft=l._create_draft('alice','1'*32,raw)['draft_id']
                if v12:task=l.commit_text(l._db,'alice','source-key',draft,1,raw,'c'*64,'d'*64,self.binding,ref)
                else:task=l._commit_draft_input(l._db,'alice','source-key',draft,1,raw,'c'*64,self.binding,ref)
        return task
    async def test_v12_same_key_identical_input_is_conflict_before_early_return(self):
        task=await self.source(True)
        with self.assertRaises(control.RegistrationConflict):
            await self.a.create('alice','source-key',self.binding,(1,2,3))
        self.assertFalse(self.a._poisoned)
        l=self.a._store._managed._ledger
        self.assertEqual(l._db.execute('SELECT task_id FROM stored_tasks').fetchall(),[(task.task_id,)])
        # Expected conflict must not make the authority unusable.
        ordinary=replace(self.binding,request_id='other-request')
        first=await self.a.create('alice','ordinary-key',ordinary,(4,5))
        self.assertEqual(await self.a.create('alice','ordinary-key',ordinary,(4,5)),first)
    async def test_v11_original_source_replay_stays_unchanged(self):
        task=await self.source(False)
        self.assertEqual(await self.a.create('alice','source-key',self.binding,(1,2,3)),task)
        self.assertFalse(self.a._poisoned)
        self.assertEqual(self.a._store._managed._ledger._db.execute('PRAGMA user_version').fetchone(),(11,))
    async def test_v12_new_ordinary_key_keeps_old_replay_semantics(self):
        self.open(True)
        task=await self.a.create('alice','ordinary-key',self.binding,(1,2,3))
        self.assertEqual(await self.a.create('alice','ordinary-key',self.binding,(1,2,3)),task)
        self.assertFalse(self.a._poisoned)

    async def test_distinct_request_ids_allow_intentional_equal_payload_submissions(self):
        self.open(True)
        first=await self.a.create('alice','first-key',self.binding,(1,2,3))
        other=replace(self.binding,request_id='second-request')
        second=await self.a.create('alice','second-key',other,(1,2,3))
        self.assertNotEqual(first.task_id,second.task_id)
        self.assertEqual(await self.a.create('alice','second-key',other,(1,2,3)),second)
        self.assertEqual(self.a._store._managed._ledger._db.execute('SELECT count(*) FROM stored_tasks').fetchone(),(2,))

    async def test_same_request_id_under_different_key_conflicts_without_poison(self):
        self.open(True)
        first=await self.a.create('alice','first-key',self.binding,(1,2,3))
        with self.assertRaises(control.RegistrationConflict):
            await self.a.create('alice','different-key',self.binding,(1,2,3))
        self.assertFalse(self.a._poisoned)
        self.assertEqual(await self.a.create('alice','first-key',self.binding,(1,2,3)),first)

if __name__=='__main__':unittest.main(verbosity=2)
