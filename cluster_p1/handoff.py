"""Experimental 0->1 hidden-byte receipt; not verification or execution authority.

No Channel/Delivery ownership here: caller retains its one release obligation.
Both injected authentication fences must remain held for this synchronous call.
"""
from dataclasses import dataclass, asdict
import hashlib
import json
from pydantic import TypeAdapter
from .contracts import Identifier, Digest, Task, Session, Stage, State
from .store import Conflict, canonical
from .tile_contract_v2 import validate_prefill_plan, TileV2
from .envelope import decode

MAX_FRAME_BYTES=65536

@dataclass(frozen=True, slots=True)
class EdgeBinding:
    task_id: str
    session_id: str
    epoch: int
    plan_digest: str
    layout_hash: str
    layout_version: int
    sender_node: str
    receiver_node: str

    def __post_init__(self):
        for v in (self.task_id,self.session_id,self.sender_node,self.receiver_node):
            TypeAdapter(Identifier).validate_python(v,strict=True)
        for v in (self.plan_digest,self.layout_hash):TypeAdapter(Digest).validate_python(v,strict=True)
        for v in (self.epoch,self.layout_version):
            if type(v) is not int or not 1<=v<2**31:raise ValueError('invalid version')
        if self.sender_node==self.receiver_node:raise ValueError('distinct stage endpoints required')

class Receiver:
    def __init__(self,journal,binding,*,authenticate_peer):
        if type(binding) is not EdgeBinding or not callable(authenticate_peer):
            raise TypeError('explicit trusted binding and peer context required')
        # Revalidate even a caller-mutated frozen instance; keep an independent copy.
        self._binding=EdgeBinding(**asdict(binding))
        self._journal=journal;self._peer=authenticate_peer

    def receive_bytes(self,frame):
        if type(frame) is not bytes or not 1<=len(frame)<=MAX_FRAME_BYTES:
            raise ValueError('bounded immutable frame required')
        b=self._binding;j=self._journal
        e=decode(frame,expected=(b.task_id,b.session_id,b.epoch),max_payload=MAX_FRAME_BYTES)
        if e.stage_id!=0 or e.layout_version!=b.layout_version:
            raise Conflict('edge/version mismatch')
        digest=hashlib.sha256(frame).hexdigest()
        with j._authorized() as owner, self._peer() as peer, j._store._tx() as c:
            TypeAdapter(Identifier).validate_python(peer,strict=True)
            if peer!=b.sender_node:raise PermissionError('peer mismatch')
            s=j._store._load(c,'p1_sessions',b.session_id,Session)
            t=j._store._load(c,'p1_tasks',b.task_id,Task)
            if owner!=s.owner_id or owner!=t.owner_id:raise PermissionError('owner mismatch')
            if (b.session_id!=j._session_id or b.task_id!=j._task_id or s.task_id!=t.task_id
                or s.epoch!=b.epoch or s.layout_hash!=b.layout_hash
                or s.state!=State.PREPARING or t.state!=State.PREPARING):
                raise Conflict('inactive/stale binding')
            row=c.execute('SELECT body,digest FROM p1_v2_plans WHERE session_id=? AND epoch=?',(b.session_id,b.epoch)).fetchone()
            if row is None or row[1]!=b.plan_digest:raise Conflict('plan binding mismatch')
            plan=json.loads(row[0])
            if hashlib.sha256(canonical(['register_v2',plan]).encode()).hexdigest()!=b.plan_digest:
                raise Conflict('plan digest mismatch')
            stages=tuple(Stage.model_validate_json(x[0]) for x in c.execute('SELECT body FROM p1_stages WHERE session_id=? ORDER BY stage_id',(b.session_id,)))
            tiles=tuple(TileV2.model_validate_json(canonical(x)) for x in plan['tiles'])
            validate_prefill_plan(t,s,stages,tiles,hidden_size=plan['hidden_size'],vocab_size=plan['vocab_size'])
            if (stages[0].assigned_node,stages[1].assigned_node)!=(b.sender_node,b.receiver_node):
                raise Conflict('stage endpoint mismatch')
            matches=[x for x in tiles if x.stage_id==0 and x.tile_id==e.tile_id]
            if len(matches)!=1:raise Conflict('unknown tile')
            tile=matches[0];desc=tile.output
            if (e.model_hash,e.token_range,e.tensor_shape,e.dtype)!=(tile.model_hash,(tile.token_start,tile.token_start+tile.token_count),desc.shape,desc.dtype):
                raise Conflict('descriptor mismatch')
            binding=canonical(asdict(b))
            edge=c.execute('SELECT body FROM p1_v2_edges WHERE plan_digest=?',(b.plan_digest,)).fetchone()
            if edge and edge[0]!=binding:raise Conflict('edge binding already fixed')
            old=c.execute('SELECT digest FROM p1_v2_receipts WHERE plan_digest=? AND tile_id=?',(b.plan_digest,e.tile_id)).fetchone()
            if old and old[0]!=digest:raise Conflict('conflicting delivery')
            if not old:
                if edge is None:c.execute('INSERT INTO p1_v2_edges VALUES(?,?)',(b.plan_digest,binding))
                c.execute('INSERT INTO p1_v2_receipts VALUES(?,?,?)',(b.plan_digest,e.tile_id,digest))
                event='v2:receipt:'+hashlib.sha256(canonical([b.plan_digest,e.tile_id]).encode()).hexdigest()
                if c.execute('SELECT 1 FROM p1_events WHERE id=?',(event,)).fetchone():raise Conflict('event identity conflict')
                j._record_event(c,event,canonical(['receipt_v2',b.plan_digest,e.tile_id,digest]),{'recorded':True,'execution_allowed':False})
            return {'frame_digest':digest,'duplicate':bool(old),'execution_allowed':False,'computed':False}
