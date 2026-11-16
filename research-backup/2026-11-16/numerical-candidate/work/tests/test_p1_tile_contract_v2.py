"""Versioned declarations only: no tensors, executors, sockets or databases."""
import copy
import json
import unittest
from unittest.mock import patch
from cluster_p1.contracts import Task, Session, Stage, Tile
from cluster_p1.tile_contract_v2 import validate_prefill_plan, assess_record, ContractRejected, TileV2

H='a'*64

def fixture():
    task=Task(task_id='t',owner_id='owner',model_hash=H,total_layers=3,payload_ref='not-read')
    session=Session(session_id='s',task_id='t',owner_id='owner',epoch=1,model_hash=H,layout_hash=H,expected_tokens=5,selected_nodes=('a','b'))
    stages=(Stage(session_id='s',stage_id=0,layer_start=0,layer_end=1,model_hash=H,layout_hash=H,assigned_node='a'),Stage(session_id='s',stage_id=1,layer_start=1,layer_end=3,model_hash=H,layout_hash=H,assigned_node='b'))
    tiles=[]
    for tid,start,count in [(0,0,2),(1,2,2),(2,4,1)]:
        for stage in (0,1):
            inp={'role':'token_ids','dtype':'int64','shape':(1,count)} if stage==0 else {'role':'hidden_states','dtype':'float32','shape':(1,count,24)}
            out={'role':'hidden_states','dtype':'float32','shape':(1,count,24)} if stage==0 else {'role':'full_logits','dtype':'float32','shape':(1,count,32)}
            tiles.append(dict(schema_version=2,task_id='t',owner_id='owner',session_id='s',epoch=1,model_hash=H,layout_hash=H,stage_id=stage,tile_id=tid,token_start=start,token_count=count,input=inp,output=out))
    return task,session,stages,tuple(tiles)

def check(args):return validate_prefill_plan(*args,hidden_size=24,vocab_size=32)
class Tests(unittest.TestCase):
    def test_token_hidden_logits_and_tail(self):
        args=fixture();before=copy.deepcopy(args[3]);r=check(args)
        self.assertEqual(r.status,'declarations_consistent')
        self.assertEqual(r.tiles_per_stage,3);self.assertFalse(r.execution_allowed)
        self.assertFalse(r.payload_verified);self.assertEqual(args[3],before)
    def test_adjacent_boundary_not_equal_outputs(self):
        args=fixture();tiles=list(args[3]);tiles[1]=copy.deepcopy(tiles[1]);tiles[1]['input']['shape']=(1,2,32)
        with self.assertRaises(ContractRejected):check((*args[:3],tuple(tiles)))
        # Original stage outputs differ (24 hidden -> 32 vocabulary), legitimately.
        self.assertEqual(check(args).tiles_per_stage,3)
    def test_role_dtype_batch_and_token_axis(self):
        for desc,key,value in [('input','dtype','float32'),('input','shape',(2,2)),('input','shape',(1,3)),('output','role','full_logits'),('output','shape',(1,2,23)),('output','dtype','float16')]:
            args=fixture();tiles=copy.deepcopy(args[3]);tiles[0][desc][key]=value
            with self.subTest(desc=desc,key=key),self.assertRaises(ContractRejected):check((*args[:3],tiles))
    def test_identity_epoch_stage_and_types(self):
        for key,value in [('task_id','other'),('owner_id','other'),('session_id','other'),('epoch',2),('model_hash','b'*64),('layout_hash','b'*64),('stage_id',True),('token_count',True),('tile_id',1.0),('schema_version',2.0),('schema_version',1)]:
            args=fixture();tiles=copy.deepcopy(args[3]);tiles[0][key]=value
            with self.subTest(key=key),self.assertRaises(ContractRejected):check((*args[:3],tiles))
    def test_missing_duplicate_gap_overlap_and_reordered(self):
        args=fixture()
        for variant in (args[3][:-1],args[3]+(args[3][0],),tuple(reversed(args[3]))):
            with self.assertRaises(ContractRejected):check((*args[:3],variant))
        for start in (1,3):
            tiles=copy.deepcopy(args[3]);tiles[2]['token_start']=start
            with self.assertRaises(ContractRejected):check((*args[:3],tiles))
    def test_binding_and_layer_coverage(self):
        args=fixture()
        for session in (args[1].model_copy(update={'owner_id':'other'}),args[1].model_copy(update={'task_id':'other'})):
            with self.assertRaises(ContractRejected):check((args[0],session,*args[2:]))
        stage=args[2][1].model_copy(update={'layer_start':2})
        with self.assertRaises(ContractRejected):check((args[0],args[1],(args[2][0],stage),args[3]))
    def test_legacy_classification_no_inference(self):
        raw=Tile(model_hash=H,layout_hash=H,session_id='s',epoch=1,stage_id=0,tile_id=0,token_start=0,token_count=2,shape=(1,2,24),dtype='float32').model_dump_json().encode()
        before=bytes(raw);r=assess_record(raw)
        self.assertEqual(r.status,'legacy_unchanged');self.assertFalse(r.automatic_migration_allowed)
        self.assertFalse(r.execution_allowed);self.assertEqual(raw,before)
        self.assertEqual(assess_record(json.dumps(fixture()[3][0]).encode()).status,'v2_structure_only')
    def test_preflight_unknown_malformed_and_duplicates(self):
        for raw in (b'{}',b'{"schema_version":3}',b'{"schema_version":2,"schema_version":2}',b'[]',b'\xff',b' '*65537):
            self.assertEqual(assess_record(raw).status,'unsupported')
    def test_v2_never_implicitly_enters_legacy_tile(self):
        value=TileV2.model_validate(fixture()[3][0])
        with self.assertRaises(ValueError):Tile.model_validate_json(value.model_dump_json())
        legacy=Tile(model_hash=H,layout_hash=H,session_id='s',epoch=1,stage_id=0,tile_id=0,token_start=0,token_count=2,shape=(1,2,24),dtype='float32')
        with self.assertRaises(ValueError):TileV2.model_validate_json(legacy.model_dump_json())
    def test_caps_unknown_fields_and_no_payload_claim(self):
        args=fixture()
        with self.assertRaises(ContractRejected):validate_prefill_plan(*args,hidden_size=True,vocab_size=32)
        with self.assertRaises(ContractRejected):check((*args[:3],args[3]*22))
        tiles=copy.deepcopy(args[3]);tiles[0]['computed']=True
        with self.assertRaises(ContractRejected):check((*args[:3],tiles))
        raw=json.dumps(fixture()[3][0]).encode()
        with patch('builtins.open',side_effect=AssertionError('I/O')),patch('os.open',side_effect=AssertionError('I/O')):
            self.assertEqual(assess_record(raw).status,'v2_structure_only')
        for version in (True,2.0,3):
            obj=copy.deepcopy(fixture()[3][0]);obj['schema_version']=version
            self.assertEqual(assess_record(json.dumps(obj).encode()).status,'unsupported')

    def test_no_io_and_immutable_report(self):
        args=fixture()
        with patch('builtins.open',side_effect=AssertionError('I/O')),patch('os.open',side_effect=AssertionError('I/O')),patch('socket.socket',side_effect=AssertionError('network')):
            r=check(args)
        with self.assertRaises((AttributeError,TypeError)):r.execution_allowed=True

if __name__=='__main__':unittest.main()
