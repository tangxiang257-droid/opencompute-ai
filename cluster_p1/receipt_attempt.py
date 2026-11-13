"""Opt-in fresh serial receipt-source seam; external real authentication required.

No sockets, credentials, loader, public entrypoint, recovery or default allow.
"""
import hashlib
import json
import math
import re
from dataclasses import dataclass,asdict
from contextlib import contextmanager
from pydantic import TypeAdapter
from .contracts import Identifier,Digest,Stage
from .handoff import EdgeBinding,Receiver,MAX_FRAME_BYTES
from .envelope import decode
from .local_attempt import LocalAttemptJournal,_non_suppressing,MAX_RESERVED
from .store import Conflict,canonical
from .tile_contract_v2 import decode_input_payload,INPUT_LAYOUT_V2

@dataclass(frozen=True,slots=True)
class PeerIdentity:
    node_id: str
    certificate_sha256: str
    connection_generation: str
    def __post_init__(self):
        TypeAdapter(Identifier).validate_python(self.node_id,strict=True)
        TypeAdapter(Digest).validate_python(self.certificate_sha256,strict=True)
        TypeAdapter(Identifier).validate_python(self.connection_generation,strict=True)

class ReceiptAttemptJournal(LocalAttemptJournal):
    @classmethod
    def create(cls,path,task,session,stages,*,authorize,execution_authorize,
               receipt_binding,expected_peer,peer_authorize,local_node,
               expected_config_sha256,enable_receipt_stage1=False):
        if enable_receipt_stage1 is not True:raise Conflict('explicit receipt-stage opt-in required')
        if type(receipt_binding)is not EdgeBinding or type(expected_peer)is not PeerIdentity:
            raise TypeError('fixed edge and expected peer required')
        edge=EdgeBinding(**asdict(receipt_binding));peer=PeerIdentity(**asdict(expected_peer))
        TypeAdapter(Digest).validate_python(expected_config_sha256,strict=True)
        # Trusted provisioner chooses a fresh random session nonce BEFORE registration.
        # Syntax is checked here; entropy/uniqueness across journals are external duties.
        if not re.fullmatch('[0-9a-f]{32}',edge.session_id):raise ValueError('nonce session required')
        if (edge.session_id!=session.session_id or edge.task_id!=task.task_id
            or edge.epoch!=session.epoch or edge.layout_hash!=session.layout_hash
            or edge.layout_version<2 or edge.sender_node!=peer.node_id
            or type(local_node)is not str or edge.receiver_node!=local_node):
            raise Conflict('receipt trust context mismatch')
        fixed_stages=tuple(Stage.model_validate(x) for x in stages)
        if tuple(x.assigned_node for x in fixed_stages)!=(edge.sender_node,edge.receiver_node):
            raise Conflict('initial stage endpoints mismatch')
        stage_body=canonical([json.loads(x.model_dump_json()) for x in fixed_stages])
        if not callable(peer_authorize) or not callable(execution_authorize):raise TypeError('live fences required')
        @contextmanager
        def peer_fence():
            with _non_suppressing(peer_authorize(peer)) as actual:
                if type(actual)is not PeerIdentity or actual!=peer:raise PermissionError('authenticated peer mismatch')
                yield actual.node_id
        @contextmanager
        def execute(spec):
            with peer_fence(),_non_suppressing(execution_authorize(spec)) as principal:
                yield principal
        # Peer context remains held through fresh bootstrap and persistent binding.
        with peer_fence():
            obj=super().create(path,task,session,fixed_stages,authorize=authorize,execution_authorize=execute,enable_stage1=True)
            obj._receipt_edge=edge;obj._receipt_peer=peer;obj._receipt_fence=peer_fence
            obj._receipt_stages=stage_body
            obj._receipt_context=canonical({'edge':asdict(edge),'peer':asdict(peer),
                'local_node':local_node,'config_sha256':expected_config_sha256,'stages':json.loads(stage_body)})
            try:
                with obj._authorized(),obj._store._tx() as c:
                    c.execute('CREATE TABLE p1_receipt_context(id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL)')
                    c.execute('INSERT INTO p1_receipt_context VALUES(1,?)',(obj._receipt_context,))
                    c.execute('CREATE TABLE p1_received_frames(tile INTEGER PRIMARY KEY,frame BLOB NOT NULL CHECK(typeof(frame)=\'blob\' AND length(frame) BETWEEN 1 AND 65536))')
            except BaseException:
                obj.close();raise
        return obj

    def make_receiver(self):
        return Receiver(self,self._receipt_edge,authenticate_peer=self._receipt_fence,
                        input_layout_version=2,input_layout_wire_version=self._receipt_edge.layout_version,
                        retain_payload=True)

    def make_owner(self,*args,**kwargs):
        raise Conflict('receipt journal cannot execute stage 0')

    def _context(self,c):
        if c.execute('SELECT body FROM p1_receipt_context WHERE id=1').fetchone()!=(self._receipt_context,):
            raise Conflict('receipt context changed')
        stages=[Stage.model_validate_json(r[0]) for r in c.execute(
            'SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(self._session_id,))]
        if canonical([json.loads(x.model_dump_json()) for x in stages])!=self._receipt_stages:
            raise Conflict('complete stage declarations changed')

    def _used(self,c):
        return super()._used(c)+c.execute('SELECT coalesce(sum(length(frame)),0) FROM p1_received_frames').fetchone()[0]

    def _retain_frame(self,c,frame,binding,*,duplicate):
        self._context(c)
        if binding!=self._receipt_edge:raise Conflict('receipt edge changed')
        e=decode(frame,max_payload=MAX_FRAME_BYTES)
        old=c.execute('SELECT frame FROM p1_received_frames WHERE tile=?',(e.tile_id,)).fetchone()
        if old:
            if not duplicate:raise Conflict('orphan retained frame; no receipt repair')
            if old!=(frame,):raise Conflict('conflicting retained frame')
            return
        if duplicate:raise Conflict('missing retained payload; no receipt repair')
        if self._used(c)+len(frame)>MAX_RESERVED:raise Conflict('retained bytes budget')
        if c.execute('SELECT count(*) FROM p1_received_frames').fetchone()[0]>=64:raise Conflict('receipt count bound')
        c.execute('INSERT INTO p1_received_frames VALUES(?,?)',(e.tile_id,frame))

    def _sources(self,c,owner):
        self._context(c)
        s,plan,tiles,vocab=self._plan(c,owner,active=True,stage_id=1)
        b=self._receipt_edge
        if (s.epoch,s.session_id,plan)!=(b.epoch,b.session_id,b.plan_digest):raise Conflict('receipt plan changed')
        stages=tuple(Stage.model_validate_json(r[0]) for r in c.execute(
            'SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(s.session_id,)))
        if tuple(x.assigned_node for x in stages)!=(b.sender_node,b.receiver_node):
            raise Conflict('receipt endpoints changed')
        edge=asdict(b);edge['payload_encoding']={'name':INPUT_LAYOUT_V2,'input_version':2,'wire_version':b.layout_version}
        if c.execute('SELECT body FROM p1_v2_edges WHERE plan_digest=?',(plan,)).fetchone()!=(canonical(edge),):
            raise Conflict('receipt edge mismatch')
        rows=c.execute('SELECT tile,frame FROM p1_received_frames ORDER BY tile').fetchall()
        if len(rows)!=len(tiles):raise Conflict('complete receipt inputs required')
        values=[]
        for (tid,frame),t in zip(rows,tiles):
            e=decode(frame,expected=(b.task_id,b.session_id,b.epoch),max_payload=MAX_FRAME_BYTES)
            if (tid,e.tile_id,e.stage_id,e.model_hash,e.layout_version,e.token_range,e.tensor_shape,e.dtype)!=(
                t.tile_id,t.tile_id,0,t.model_hash,b.layout_version,(t.token_start,t.token_start+t.token_count),t.input.shape,t.input.dtype):
                raise Conflict('receipt descriptor mismatch')
            digest=hashlib.sha256(frame).hexdigest()
            if c.execute('SELECT digest FROM p1_v2_receipts WHERE plan_digest=? AND tile_id=?',(plan,tid)).fetchone()!=(digest,):
                raise Conflict('receipt digest mismatch')
            event='v2:receipt:'+hashlib.sha256(canonical([plan,tid]).encode()).hexdigest()
            if c.execute('SELECT payload FROM p1_events WHERE id=?',(event,)).fetchone()!=(canonical(['receipt_v2',plan,tid,digest]),):
                raise Conflict('receipt event mismatch')
            decode_input_payload(t,e.payload,layout_version=2,vocab_size=vocab)
            values.append(e.payload)
        return s,plan,tiles,tuple(values)

    def make_stage1_owner(self,operation,*,instance_id):
        with self._receipt_fence():
            with self._authorized() as owner,self._store._tx() as c:
                s,plan,tiles,inputs=self._sources(c,owner)
                if c.execute('SELECT 1 FROM p1_local_stage1_attempts LIMIT 1').fetchone():
                    raise Conflict('prior attempt; no new-owner continuation')
                need=sum(len(v)+4*math.prod(t.output.shape) for t,v in zip(tiles,inputs))
                if self._used(c)+need>MAX_RESERVED:raise Conflict('complete stage retained budget')
            return self._make_owner(operation,inputs,instance_id=instance_id,stage_id=1,
                                   source_snapshot=(s.epoch,plan,inputs))
