import math
import struct
import unittest
from dataclasses import replace
import test_p1_handoff as hf
from test_p1_tile_contract_v2 import fixture
from cluster_p1.tile_contract_v2 import decode_input_payload
from cluster_p1.handoff import Receiver
from cluster_p1.store import Conflict

class Tests(unittest.TestCase):
    def token(self,payload,**kw):return decode_input_payload(fixture()[3][0],payload,layout_version=kw.pop('layout_version',2),vocab_size=kw.pop('vocab_size',32),**kw)
    def test_int64_golden_and_edges(self):
        r=self.token(bytes.fromhex('00000000000000001f00000000000000'))
        self.assertEqual(r.values,(0,31));self.assertEqual(r.shape,(1,2));self.assertFalse(r.execution_allowed)
        for pair in ((-1,0),(0,32),(2**63-1,0)):
            with self.assertRaises(ValueError):self.token(struct.pack('<qq',*pair))
    def test_version_not_inferred(self):
        for v in (None,1,3,True,2.0):
            with self.subTest(v=v),self.assertRaises(ValueError):self.token(b'\0'*16,layout_version=v)
    def test_lengths_mutability_and_cap(self):
        for p in (b'\0'*15,b'\0'*17,bytearray(16),memoryview(b'\0'*16),b'\0'*65537):
            with self.assertRaises(ValueError):self.token(p)
    def test_float_row_major_golden_and_tail(self):
        tile=fixture()[3][1];values=tuple(float(x) for x in range(48));r=decode_input_payload(tile,struct.pack('<48f',*values),layout_version=2,vocab_size=32)
        self.assertEqual(r.values,values);self.assertEqual(r.shape,(1,2,24))
        tail=fixture()[3][-1];r=decode_input_payload(tail,struct.pack('<24f',*values[:24]),layout_version=2,vocab_size=32)
        self.assertEqual(r.shape,(1,1,24));self.assertEqual(len(r.values),24)
    def test_nonfinite_rejected(self):
        for value in (float('nan'),float('inf'),-float('inf')):
            with self.assertRaises(ValueError):decode_input_payload(fixture()[3][1],struct.pack('<48f',value,*([0.0]*47)),layout_version=2,vocab_size=32)
    def test_endian_is_declared_not_guessed(self):
        with self.assertRaises(ValueError):self.token(struct.pack('>qq',1,2))
        payload=struct.pack('>48f',*([1.0]*48))
        r=decode_input_payload(fixture()[3][1],payload,layout_version=2,vocab_size=32)
        self.assertNotEqual(r.values,(1.0,)*48)  # finite reversed bytes are not identifiable as mislabelled
    def test_shape_and_numeric_type(self):
        tile=dict(fixture()[3][1]);tile['input']=dict(tile['input'],shape=(1,3,24))
        with self.assertRaises(ValueError):decode_input_payload(tile,b'\0'*192,layout_version=2,vocab_size=32)
        for v in (True,32.0,0):
            with self.assertRaises(ValueError):self.token(b'\0'*16,vocab_size=v)
    def test_receiver_optin_rejects_nan_before_write(self):
        f=hf.Tests('test_receive_and_duplicate');f.setUp();self.addCleanup(f.doCleanups)
        from test_p1_handoff import peer
        b=replace(f.binding,layout_version=2)
        r=Receiver(f.j,b,authenticate_peer=peer,input_layout_version=2,input_layout_wire_version=2)
        with self.assertRaises(ValueError):r.receive_bytes(f.frame(layout_version=2,payload=struct.pack('<48f',float('nan'),*([0.0]*47))))
        self.assertFalse(r.receive_bytes(f.frame(layout_version=2))['duplicate'])
    def test_opaque_old_mode_not_silently_upgraded(self):
        f=hf.Tests('test_receive_and_duplicate');f.setUp();self.addCleanup(f.doCleanups)
        from test_p1_handoff import peer
        with self.assertRaises(ValueError):Receiver(f.j,f.binding,authenticate_peer=peer,input_layout_version=2,input_layout_wire_version=2)
        b=replace(f.binding,layout_version=2)
        Receiver(f.j,b,authenticate_peer=peer).receive_bytes(f.frame(layout_version=2))
        with self.assertRaises(Conflict):Receiver(f.j,b,authenticate_peer=peer,input_layout_version=2,input_layout_wire_version=2).receive_bytes(f.frame(layout_version=2))
    def test_exact_budget_and_immutable_result(self):
        from dataclasses import FrozenInstanceError
        tile=dict(fixture()[3][1],token_count=16)
        tile['input']={'role':'hidden_states','dtype':'float32','shape':(1,16,1024)}
        tile['output']={'role':'full_logits','dtype':'float32','shape':(1,16,32)}
        r=decode_input_payload(tile,b'\0'*65536,layout_version=2,vocab_size=32)
        self.assertEqual(len(r.values),16384)
        with self.assertRaises(FrozenInstanceError):r.execution_allowed=True
        with self.assertRaises(ValueError):decode_input_payload(tile,b'\0'*65540,layout_version=2,vocab_size=32)
    def test_wire_version_mapping_is_explicit_and_independent(self):
        f=hf.Tests('test_receive_and_duplicate');f.setUp();self.addCleanup(f.doCleanups)
        from test_p1_handoff import peer
        b=replace(f.binding,layout_version=7)
        with self.assertRaises(ValueError):Receiver(f.j,b,authenticate_peer=peer,input_layout_version=2)
        r=Receiver(f.j,b,authenticate_peer=peer,input_layout_version=2,input_layout_wire_version=7)
        self.assertFalse(r.receive_bytes(f.frame(layout_version=7))['duplicate'])
    def test_signed_zero_and_subnormal_are_finite(self):
        payload=bytes.fromhex('0000008001000000')+b'\0'*(192-8)
        r=decode_input_payload(fixture()[3][1],payload,layout_version=2,vocab_size=32)
        self.assertEqual(math.copysign(1,r.values[0]),-1)
        self.assertGreater(r.values[1],0)
