"""Owner syntax is representation, never identity or execution permission."""
import unittest,hashlib,json,re,tempfile,struct
from pathlib import Path
from dataclasses import replace
from task_input_api import Principal,_owner,RejectAllUsers
from cluster_p1.projection_driver import Spec,Journal,Driver,Policy
from cluster_p1.byte_spool import Binding,Plan,Book,Rejected
from cluster_p1.control_journal import _binding
from cluster_p1.control_exchange import _json,_parse
from cluster_p1.role_control import _ids

def spec(owner):return Spec(owner,'request','session',1,*(['a'*64]*6),1,3,('b'*64,))
def binding(owner):return Binding(owner,'request','session','attempt',1,'connection',*(['a'*64]*6))
def control(owner):return dict(owner=owner,task='request',session='session',epoch=1,stage=0,input_snapshot='a'*64,model='a'*64,layout='a'*64,plan='a'*64,input_digest='a'*64,device='device',instance='instance',peer='a'*64,declaration_digest='a'*64,token_digest='a'*64,preprocessing='a'*64)

class Tests(unittest.TestCase):
    def setUp(self):
        self.owner=_owner(Principal('synthetic-owner-design','alice'))
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
    def test_canonical_generated_owner_preserved(self):
        self.assertEqual(len(self.owner),68)
        self.assertEqual(spec(self.owner).owner,self.owner)
        p=Plan(binding(self.owner),(1,),3)
        self.assertEqual(json.loads(p.encoded())['binding']['owner'],self.owner)
    def test_legacy_golden_bytes_and_digests(self):
        owners=['owner','A_0-9','x'*64,'usr_'+'A'*60]
        shas=['22503ec30b12b5915a992398bbf0ab5d1cfe49b5fdc15ea0c2cf38ced1d5f300','15d4dc856f0b1d29067e6e8ea3c2f9d6b34f2a2b57fea636163ad5e8d1765c30','02384effa8c4331e9ff227463d152193b0176a86c122ecb4fd16d4b45f57f11f','3dac13684d387746f14693f4ac1c9c754a41f6319aeb7167ab78e7d0c98b44b9']
        plans=['07946287c4fecbbe1da45afebce999abd626a1ef29f7ed70317ed4bfc6472d23','70138f8558fe20838409d3b9005d4a7ba9b1dd81f65625a97df9a722c913a2f4','93d481d190e785c108f80f1891eef453b27be20fcc22729592f99f6202b7bbdb','f4d231e3b61d14314061613d919e93d878e010de4ede3bae0c6c48ced5a306a7']
        for o,s,p in zip(owners,shas,plans):
            self.assertEqual(spec(o).digest,s)
            self.assertEqual(hashlib.sha256(Plan(binding(o),(1,),3).encoded().encode()).hexdigest(),p)
    def test_invalid_owner_matrix_rejected(self):
        class Sub(str):pass
        bad=['','x'*65,'x'*68,'usr_'+'a'*63,'usr_'+'a'*65,'usr_'+'A'*64,'usr_'+'g'*64,self.owner+'\n',self.owner+' ',self.owner[:-1]+'é',b'owner',True,None,Sub('owner'),Sub(self.owner)]
        for value in bad:
            for make in (spec,binding):
                with self.subTest(value=repr(value),factory=make.__name__),self.assertRaises(ValueError):make(value)
    def test_non_owner_fields_keep_old_bounds(self):
        for name in ('request','session'):
            with self.assertRaises(ValueError):replace(spec('owner'),**{name:self.owner})
        for name in ('task','session_nonce','attempt','connection'):
            with self.assertRaises(ValueError):replace(binding('owner'),**{name:self.owner})
        book=Book(self.root/'book',2097152);self.addCleanup(book.close)
        with self.assertRaises(ValueError):book.reserve(self.owner,Plan(binding('owner'),(1,),3),'source')
    def test_old_records_reopen_exact_no_new_consumption(self):
        path=self.root/'book';p=Plan(binding('usr_'+'A'*60),(1,),3)
        book=Book(path,2097152);book.reserve('old',p,'source');book.transition('old',p,'source','RESERVED','ADMITTED')
        before=list(book.db.iterdump());used=book.used();book.close()
        book=Book(path,2097152);self.addCleanup(book.close);book.reserve('old',p,'source')
        self.assertEqual(before,list(book.db.iterdump()));self.assertEqual(book.used(),used)
        with self.assertRaises(ValueError):book.transition('old',p,'source','RESERVED','ADMITTED')
        self.assertEqual(before,list(book.db.iterdump()))
    def test_new_owner_no_truncation_or_alias_in_book(self):
        book=Book(self.root/'book',2097152);self.addCleanup(book.close)
        p=Plan(binding(self.owner),(1,),3);short=Plan(binding(self.owner[:64]),(1,),3)
        book.reserve('full',p,'source');before=list(book.db.iterdump())
        with self.assertRaises(ValueError):book.confirm('full',short,'source')
        with self.assertRaises(ValueError):book.reserve('full',short,'source')
        self.assertEqual(before,list(book.db.iterdump()))
        book.reserve('short',short,'source')
        self.assertEqual(book.used(),p.charge+short.charge)
        self.assertNotEqual(book.record('full')['request_key'],book.record('short')['request_key'])
        self.assertEqual(json.loads(book.record('full')['binding'])['binding']['owner'],self.owner)
    def test_control_wire_identity_does_not_alias(self):
        full=control(self.owner);short=control(self.owner[:64]);other=control(_owner(Principal('synthetic-owner-design','bob')))
        self.assertEqual(_parse(_json(full)),full);self.assertEqual(json.loads(_binding(full))['owner'],self.owner)
        self.assertNotEqual(_ids(full),_ids(short));self.assertNotEqual(_ids(full),_ids(other))
    def test_default_authority_denies_canonical_owner(self):
        self.assertIsNone(RejectAllUsers().verify(self.owner))
        s=replace(spec(self.owner),inputs=(hashlib.sha256(struct.pack('<f',1)).hexdigest(),));j=Journal(self.root/'journal',s);self.addCleanup(j.close)
        d=Driver(j,s,Policy(100,200,300,1000000,262144,2097152,2097152))
        with self.assertRaises(ValueError):d.run(0,struct.pack('<f',1))
        self.assertTrue(j.retired())
        with self.assertRaises(FileExistsError):Journal(self.root/'journal',s)
    def test_new_owner_history_retains_unknown_charge(self):
        book=Book(self.root/'book',2097152);p=Plan(binding(self.owner),(1,),3)
        book.reserve('new',p,'source');book.isolate('new');before=book.record('new');used=book.used();book.close()
        book=Book(self.root/'book',2097152);self.addCleanup(book.close)
        book.reserve('new',p,'source');self.assertEqual(book.record('new'),before);self.assertEqual(book.used(),used)
        with self.assertRaises(ValueError):book.confirm('new',p,'source')
    def test_mixed_version_old_grammar_rejects_without_fallback(self):
        value=spec(self.owner).owner
        self.assertIsNone(re.fullmatch('[A-Za-z0-9_-]{1,64}',value))
        self.assertEqual(value,self.owner)
        with self.assertRaises(ValueError):Plan(binding(self.owner),(1,),524289)

if __name__=='__main__':unittest.main()
