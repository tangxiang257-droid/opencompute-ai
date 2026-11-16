"""AST-loaded production definitions with SYMBOLIC tensors, never numerical proof.

No torch/transformers/model import, real forward, device, node or network.
"""
import ast
import contextlib
import hashlib
import inspect
from pathlib import Path
import re
import struct
import threading
from types import SimpleNamespace
import unittest


SOURCE = Path(__file__).resolve().parents[2] / 'compute/qwen2_projection_adapter.py'


class Tensor:
    def __init__(self, shape, device='cpu', dtype='fp32', finite=True):
        self.shape = tuple(shape)
        self.device = device
        self.dtype = dtype
        self.finite = finite

    def reshape(self, *shape):
        return Tensor(shape, self.device, self.dtype, self.finite)

    def __getitem__(self, key):
        assert isinstance(key, slice) and key.step is None
        return Tensor((key.stop - key.start, self.shape[1]), self.device, self.dtype)


class Stage:
    pass


class Completion:
    def __init__(self):
        self.open = True
        self.pending = None
        self.finished = False
        self.owner = None
        self.scope = 'scope'

    def require_open(self, scope, spec, owner):
        if not self.open or scope != self.scope:
            raise ValueError('scope unavailable')
        self.owner = owner

    def register_pending(self, scope, owner, pending):
        if scope != self.scope or owner is not self.owner or self.pending is not None:
            raise ValueError('single pending')
        self.pending = pending

    def require_materialized(self, scope, owner):
        if scope != self.scope or owner is not self.owner or self.pending is not None:
            raise ValueError('materialization unproved')

    def require_finished(self, scope, owner):
        self.require_materialized(scope, owner)
        if not self.finished:
            raise ValueError('finish unproved')

    def materialize_symbolically(self):
        # Test-only acknowledgment. No real value or device completion is proved.
        pending = self.pending
        self.pending = None
        pending.tensor = None


class Tests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SOURCE.is_file(), 'adapter source missing')
        tree = ast.parse(SOURCE.read_text())
        definitions = ast.Module(body=[n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))], type_ignores=[])
        symbols = dict(contextmanager=contextlib.contextmanager, hashlib=hashlib, inspect=inspect,
                       re=re, struct=struct, threading=threading)
        exec(compile(definitions, str(SOURCE), 'exec'), symbols)
        self.log = []
        def linear(x, weight):
            self.log.append(('linear', tuple(weight.shape)))
            return Tensor((1, 1, weight.shape[0]))
        def forward(x, layers, caches, meta):
            self.log.append(('forward', tuple(x.shape)))
            return x, [('candidate-k', 'candidate-v')]
        def norm(x, weight, eps):
            self.log.append(('norm', tuple(x.shape)))
            return x
        torch = SimpleNamespace(Tensor=Tensor, float32='fp32', int64='i64', inference_mode=contextlib.nullcontext,
                                tensor=lambda values, **kw: Tensor((len(values),), kw['device'], kw['dtype']),
                                isfinite=lambda x: SimpleNamespace(all=lambda: x.finite))
        symbols['_dependencies'] = lambda: (torch, SimpleNamespace(linear=linear), forward, norm, Stage)
        self.adapter_type = symbols['Qwen2ProjectionAdapter']
        self.stage0_type = symbols.get('Stage0ByteAdapter')
        self.value = struct.pack('<2f', 1, 2)
        self.spec = SimpleNamespace(hidden=2, vocab=8193, inputs=(hashlib.sha256(self.value).hexdigest(),),
                                    model='a'*64, layout='b'*64, digest='c'*64)
        self.stage = Stage()
        self.stage.stage_id = 1
        self.stage.layer_range = (1, 2)
        self.stage.meta = dict(hidden=2, vocab=8193, layers=2, positions=32, workers=1, rms_eps=1e-6)
        self.stage.device = 'cpu'
        self.stage.head = Tensor((8193, 2))
        self.stage.norm = Tensor((2,))
        self.stage.layers = [object()]
        self.stage.caches = [None]
        self.stage.cache_length = 0
        self.stage.batch_size = None
        self.completion = Completion()

    def make(self, **kw):
        return self.adapter_type(self.stage, self.spec, completion=self.completion,
                                 expected_model='a'*64, expected_layout='b'*64,
                                 expected_layer_range=(1, 2), **kw)

    def fresh_symbolic_stage(self):
        # Independent fake loaded instance for unrelated negative cases.
        stage=Stage()
        stage.__dict__.update({k:v for k,v in vars(self.stage).items()
                               if k!='_private_one_token_owner'})
        self.stage=stage

    def prepare(self):
        adapter = self.make()
        adapter.check_binding(self.spec)
        return adapter, adapter.prepare_token('scope', self.value, 0)

    def test_symbolic_slices_finish_and_single_cache_install(self):
        adapter, handle = self.prepare()
        self.assertEqual(self.stage.cache_length, 0)
        first = adapter.project_slice('scope', handle, 0, 8192)
        self.assertEqual((first.scope, first.owner, first.column, first.count), ('scope', adapter, 0, 8192))
        self.assertEqual(first.tensor.shape, (1, 1, 8192))
        self.completion.materialize_symbolically()
        last = adapter.project_slice('scope', handle, 8192, 1)
        self.assertEqual(last.tensor.shape, (1, 1, 1))
        self.completion.materialize_symbolically()
        self.completion.finished = True
        adapter.install_cache('scope', handle)
        self.assertEqual((self.stage.cache_length, self.stage.batch_size), (1, 1))
        self.assertEqual(self.log, [('forward', (1,1,2)), ('norm', (1,1,2)),
                                   ('linear', (8192,2)), ('linear', (1,2))])
        with self.assertRaises(ValueError):
            adapter.install_cache('scope', handle)

    def test_unmaterialized_pending_prevents_second_projection(self):
        adapter, handle = self.prepare()
        adapter.project_slice('scope', handle, 0, 8192)
        with self.assertRaises(ValueError):
            adapter.project_slice('scope', handle, 8192, 1)
        self.assertEqual(sum(n[0]=='linear' for n in self.log), 1)

    def test_cache_requires_finish_and_full_vocabulary(self):
        adapter, handle = self.prepare()
        self.completion.finished = True
        with self.assertRaises(ValueError):
            adapter.install_cache('scope', handle)
        self.assertEqual(self.stage.cache_length, 0)

    def test_complete_slices_without_finish_cannot_install(self):
        adapter, handle = self.prepare()
        for col, count in ((0,8192), (8192,1)):
            adapter.project_slice('scope', handle, col, count)
            self.completion.materialize_symbolically()
        with self.assertRaises(ValueError):
            adapter.install_cache('scope', handle)
        self.assertEqual(self.stage.cache_length, 0)

    def test_no_prepare_without_registered_scope_or_binding(self):
        adapter = self.make()
        with self.assertRaises(ValueError):
            adapter.prepare_token('scope', self.value, 0)
        self.assertEqual(self.log, [])
        self.fresh_symbolic_stage()
        adapter = self.make()
        adapter.check_binding(self.spec)
        self.completion.open = False
        with self.assertRaises(ValueError):
            adapter.prepare_token('scope', self.value, 0)
        self.assertEqual(self.log, [])

    def test_actual_input_hash_type_finiteness_and_token(self):
        for value, token in ((self.value,1), (bytes(8),0), (memoryview(self.value),0)):
            self.fresh_symbolic_stage()
            adapter = self.make(); adapter.check_binding(self.spec)
            with self.assertRaises(ValueError):
                adapter.prepare_token('scope', value, token)
        self.value = struct.pack('<II', 0x7f800000, 0)
        self.spec.inputs = (hashlib.sha256(self.value).hexdigest(),)
        self.fresh_symbolic_stage()
        adapter = self.make(); adapter.check_binding(self.spec)
        with self.assertRaises(ValueError):
            adapter.prepare_token('scope', self.value, 0)
        self.assertEqual(self.log, [])

    def test_fresh_stage_and_pinned_binding_required(self):
        self.stage.cache_length = 1
        with self.assertRaises(ValueError):self.make()
        self.stage.cache_length = 0
        self.spec.model = 'd'*64
        with self.assertRaises(ValueError):self.make()

    def test_wrong_handle_scope_order_and_oversized_slice(self):
        for scope, foreign, col, count in (('other',False,0,8192), ('scope',True,0,8192),
                                         ('scope',False,1,8192), ('scope',False,0,8193)):
            self.fresh_symbolic_stage()
            adapter, handle = self.prepare()
            with self.assertRaises(ValueError):
                adapter.project_slice(scope, object() if foreign else handle, col, count)
        self.assertFalse(any(n[0]=='linear' for n in self.log))

    def test_one_prepare_failure_is_not_retryable(self):
        adapter, handle = self.prepare()
        with self.assertRaises(ValueError):adapter.prepare_token('scope', self.value, 0)
        with self.assertRaises(ValueError):adapter.project_slice('scope', handle, 0, 8192)
        self.assertEqual(sum(n[0]=='forward' for n in self.log), 1)

    def test_thread_ownership(self):
        adapter, handle = self.prepare(); errors=[]
        def other():
            try:adapter.project_slice('scope', handle, 0, 8192)
            except ValueError:errors.append('denied')
        thread=threading.Thread(target=other);thread.start();thread.join(5)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,['denied'])
        self.assertFalse(any(n[0]=='linear' for n in self.log))

    def test_completion_awaitable_is_not_evidence(self):
        async def bad(*args):pass
        self.completion.require_open=bad
        adapter=self.make();adapter.check_binding(self.spec)
        with self.assertRaises(ValueError):adapter.prepare_token('scope',self.value,0)
        self.assertEqual(self.log,[])

    def test_same_stage_cannot_be_rewrapped_even_after_failed_prepare(self):
        adapter=self.make()
        with self.assertRaises(ValueError):self.make()
        adapter.check_binding(self.spec)
        with self.assertRaises(ValueError):adapter.prepare_token('scope',bytes(8),0)
        with self.assertRaises(ValueError):self.make()

    def test_retire_during_open_fences_forward(self):
        entered=threading.Event();release=threading.Event();errors=[]
        original=self.completion.require_open
        def blocked(*args):
            entered.set();self.assertTrue(release.wait(5));return original(*args)
        self.completion.require_open=blocked
        adapter=self.make()
        def work():
            try:
                adapter.check_binding(self.spec);adapter.prepare_token('scope',self.value,0)
            except ValueError:errors.append('retired')
        thread=threading.Thread(target=work);thread.start()
        try:
            self.assertTrue(entered.wait(5));adapter.retire(self.spec)
        finally:release.set();thread.join(5)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,['retired'])
        self.assertEqual(self.log,[])

    def test_source_is_lazy_and_never_uses_prefill_or_full_head(self):
        tree=ast.parse(SOURCE.read_text())
        for node in tree.body:
            if isinstance(node,ast.Import):
                self.assertFalse(any(n.name.startswith(('torch','transformers')) for n in node.names))
            if isinstance(node,ast.ImportFrom):
                self.assertFalse((node.module or '').startswith(('torch','transformers','compute.qwen2')))
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
        self.assertFalse(any(isinstance(n.func,ast.Attribute) and n.func.attr in ('tolist','synchronize') for n in calls))
        stage1=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Qwen2ProjectionAdapter')
        self.assertFalse(any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                             and n.func.attr=='prefill' for n in ast.walk(stage1)))
        self.assertEqual(sum(isinstance(n.func,ast.Attribute) and n.func.attr=='linear' for n in calls),1)

    def stage0(self):
        self.assertIsNotNone(self.stage0_type, 'stage0 adapter missing')
        self.value=struct.pack('<q',1)
        self.spec.inputs=(hashlib.sha256(self.value).hexdigest(),)
        self.stage.stage_id=0;self.stage.layer_range=(0,1)
        self.stage.head=self.stage.norm=None
        self.stage.embed=Tensor((self.spec.vocab,self.spec.hidden))
        def prefill(tensor, *, token_start, stage_id):
            self.log.append(('prefill',tensor.shape,tensor.dtype,token_start,stage_id))
            self.stage.cache_length=1;self.stage.batch_size=1
            return Tensor((1,1,self.spec.hidden))
        self.stage.prefill=prefill
        def materialize(scope,pending,size):
            self.log.append(('materialize',size))
            self.completion.materialize_symbolically()
            return bytes(size)
        def finish(scope):
            self.log.append(('finish',scope));self.completion.finished=True
        self.completion.materialize=materialize;self.completion.finish_scope=finish
        return self.stage0_type(self.stage,self.spec,completion=self.completion)

    def test_stage0_materializes_finishes_and_returns_exact_hidden(self):
        adapter=self.stage0()
        self.assertEqual(adapter.operation('scope',self.value),bytes(8))
        self.assertEqual(self.log,[('prefill',(1,1),'i64',0,0),('materialize',8),('finish','scope')])
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)

    def test_stage0_scope_missing_prevents_prefill(self):
        adapter=self.stage0();self.completion.open=False
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)
        self.assertEqual(self.log,[])

    def test_stage0_bad_materialization_retires_advanced_cache(self):
        adapter=self.stage0()
        self.completion.materialize=lambda *args:memoryview(bytes(8))
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)
        self.assertEqual(self.stage.cache_length,1)
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)
        self.assertEqual(sum(n[0]=='prefill' for n in self.log),1)

    def test_stage0_unfinished_device_cannot_return_bytes(self):
        adapter=self.stage0();self.completion.finish_scope=lambda scope:None
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)

    def test_stage0_rejects_nonfinite_hidden_bytes_and_invalid_token(self):
        adapter=self.stage0()
        self.completion.materialize=lambda *args:struct.pack('<II',0x7f800000,0)
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)
        self.stage.cache_length=0;self.stage.batch_size=None
        self.fresh_symbolic_stage()
        adapter=self.stage0();self.value=struct.pack('<q',-1)
        self.spec.inputs=(hashlib.sha256(self.value).hexdigest(),)
        with self.assertRaises(ValueError):adapter.operation('scope',self.value)


if __name__ == '__main__':
    unittest.main()
