import dataclasses
import unittest
from cluster_p1.envelope import Envelope, EnvelopeError, decode


def sample(**kw):
    fields = dict(task_id='task', session_id='session', execution_epoch=2, stage_id=3,
                  tile_id=4, token_range=(0, 2), model_hash='ab'*32, layout_version=1,
                  tensor_shape=(2,), dtype='float32', payload=b'12345678')
    fields.update(kw)
    return Envelope(**fields)


class EnvelopeTests(unittest.TestCase):
    def test_roundtrip_and_context(self):
        e = sample()
        assert decode(e.encode(), expected=('task','session',2)) == e
        for ctx in [('other','session',2),('task','other',2),('task','session',3)]:
            with self.assertRaises(EnvelopeError): decode(e.encode(), expected=ctx)
    
    
    def test_bounds_tamper_truncation(self):
        b = sample().encode()
        for bad in [b[:-1], b+b'x', b[:20]+bytes([b[20]^1])+b[21:], b'X'+b[1:]]:
            with self.assertRaises(EnvelopeError): decode(bad)
        for kw in [dict(dtype='object'),dict(tensor_shape=(3,)),dict(token_range=(2,1)),dict(tensor_shape=(2**60,)),dict(execution_epoch=-1), dict(execution_epoch=0), dict(layout_version=0)]:
            with self.assertRaises(EnvelopeError): sample(**kw).encode()
        with self.assertRaises(EnvelopeError): decode(b, max_payload=4)
