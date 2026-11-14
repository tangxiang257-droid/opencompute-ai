"""Private C-purpose history over supplied authenticated Channels, never a grant.

Trusted local store selection and external current-principal authorization are
required. Original connection/incarnation fields are historical data only. A
separate read-only connection performs no reservation, retirement or recovery.
Same-host filesystem rollback/cloning and remote revocation remain out of scope.
"""
import hashlib
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from .c_receiving import ReceivingStore, descriptor, actual
from .control_exchange import _json, _parse, _need, _nothing
from .history_query import _HistoryEndpoint, _context, _schema

_STATES={'PENDING','BINDING','ACTIVE','RECEIVING','RECEIVED','REVOKE_PENDING','REVOKED','UNKNOWN'}
def _scope(d):
    return dict(task=d['task'],session=d['session'],epoch=d['epoch'],stage=1,model=d['model'])
def _receipt(value,d):
    if value is None:return None
    _need(type(value)is dict and set(value)=={'descriptor','connection','challenge','actual','state','charge','recipient','execution_allowed','receiving_allowed','resource_verified'})
    _need(_json(descriptor(value['descriptor']))==_json(d) and value['state'] in _STATES
          and type(value['charge'])is int and value['charge']==d['charge']
          and value['execution_allowed'] is False and value['receiving_allowed'] is False and value['resource_verified'] is False)
    for k in ('connection','challenge'):_need(type(value[k])is str and re.fullmatch('[a-f0-9]{64}',value[k]) is not None)
    _need(value['recipient'] is None or (type(value['recipient'])is str and re.fullmatch('[a-f0-9]{64}',value['recipient']) is not None))
    if value['actual'] is not None:actual(value['actual'],d)
    return value

class ReceivingHistoryClient(_HistoryEndpoint):
    async def query(self,expected_descriptor):
        _need(not self._busy and not self._closed);self._busy=True;delivery=None
        try:
            self.current();d=descriptor(expected_descriptor);seq=self._sequence;self._sequence+=1
            raw=_json(dict(version=1,purpose='result-receive',action='query',sequence=seq,descriptor=d))
            await self.channel.send(self.envelope(_scope(d),seq,raw),max_frame_bytes=65536)
            delivery=await self.channel.receive();self.current();self.validate(delivery.envelope,_scope(d),seq)
            reply=_parse(delivery.envelope.payload)
            _need(type(reply)is dict and set(reply)=={'version','purpose','sequence','reply_to','history_only','receipt'}
                  and type(reply['version'])is int and reply['version']==1
                  and type(reply['sequence'])is int and reply['sequence']==seq
                  and reply['purpose']=='result-receive' and reply['history_only'] is True
                  and reply['reply_to']==hashlib.sha256(raw).hexdigest())
            result=_receipt(reply['receipt'],d)
            await delivery.release();self.current();return result
        except BaseException as error:await self._failed(error)
        finally:
            if delivery is not None:delivery.envelope=None
            self._busy=False

class ReceivingHistoryService(_HistoryEndpoint):
    """Query local C store only; issuer-store querying is deliberately unsupported.

    identity.hold(current_identity) -> current principal; authorize.hold(principal,
    current_identity, original_descriptor, 'query') -> None. Both are mandatory.
    No resource provider is called, and no returned state qualifies any work.
    """
    def __init__(self,channel,store,*,identity=None,authorize=None):
        super().__init__(channel)
        _need(type(store)is ReceivingStore and store.authority is None)
        self.identity=identity;self.authorize=authorize;self._history=None;self._pid=os.getpid()
        with store.lock:
            store._check();self._path=store.path.resolve(strict=True);self._parent=self._path.parent
            self._file=self._inode(self._path);self._directory=self._inode(self._parent)
            self._schema=_schema(store.db)
            self._incarnation=store.db.execute('SELECT incarnation FROM c_receiving_identity').fetchall()
            _need(len(self._incarnation)==1 and self._incarnation[0][0]==store.incarnation)
        try:
            self._history=sqlite3.connect(self._path.as_uri()+'?mode=ro',uri=True,isolation_level=None)
            self._history.execute('PRAGMA query_only=ON');self._store_current()
        except BaseException:
            if self._history is not None:self._history.close();self._history=None
            raise
    @staticmethod
    def _inode(path):
        s=path.stat();return s.st_dev,s.st_ino
    def _store_current(self):
        _need(os.getpid()==self._pid and self._inode(self._parent)==self._directory and self._inode(self._path)==self._file)
        _need(self._history is not None and self._history.execute('PRAGMA query_only').fetchone()==(1,))
        _need(_schema(self._history)==self._schema and self._history.execute('SELECT incarnation FROM c_receiving_identity').fetchall()==self._incarnation)
    @contextmanager
    def _authorized(self,d,expected=None):
        _need(not self._closed);self.current();self._store_current()
        _need(callable(getattr(self.identity,'hold',None)) and callable(getattr(self.authorize,'hold',None)))
        with self.channel.identity_fence(self._identity),_context(self.identity.hold(self._identity)) as principal:
            _need(type(principal)is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',principal) is not None)
            if expected is not None:_need(principal==expected)
            row=self._history.execute('SELECT descriptor,connection,challenge,actual,state,charge,recipient FROM c_receiving WHERE owner=? AND task=?',(d['owner'],d['task'])).fetchone()
            original=d if row is None else descriptor(_parse(row[0].encode()))
            _need(_json(original)==_json(d))
            with _context(self.authorize.hold(principal,self._identity,_parse(_json(original)),'query')) as permitted:
                _nothing(permitted)
                receipt=None if row is None else dict(descriptor=original,connection=row[1],challenge=row[2],actual=_parse(row[3].encode()) if row[3] else None,state=row[4],charge=row[5],recipient=row[6],execution_allowed=False,receiving_allowed=False,resource_verified=False)
                _receipt(receipt,d);yield principal,receipt;self._store_current()
        self.current();self._store_current()
    async def serve_once(self):
        _need(not self._busy and not self._closed);self._busy=True;delivery=None
        try:
            self.current();delivery=await self.channel.receive();self.current()
            raw=delivery.envelope.payload;q=_parse(raw);seq=self._sequence;self._sequence+=1
            _need(type(q)is dict and set(q)=={'version','purpose','action','sequence','descriptor'})
            d=descriptor(q['descriptor'])
            _need(type(q['version'])is int and q['version']==1 and type(q['sequence'])is int and q['sequence']==seq and q['purpose']=='result-receive' and q['action']=='query')
            self.validate(delivery.envelope,_scope(d),seq)
            with self._authorized(d) as (principal,_):pass
            await delivery.release();delivery.envelope=None;delivery=None
            with self._authorized(d,principal) as (_,receipt):pass
            reply=_json(dict(version=1,purpose='result-receive',sequence=seq,reply_to=hashlib.sha256(raw).hexdigest(),history_only=True,receipt=receipt))
            @contextmanager
            def write_fence():
                with self._authorized(d,principal) as (_,current):
                    # A changing record can be retried as history, never sent as a stale grant.
                    _need(_json(current)==_json(receipt));yield None
            await self.channel.send(self.envelope(_scope(d),seq,reply),max_frame_bytes=65536,write_fence=write_fence)
        except BaseException as error:await self._failed(error)
        finally:
            if delivery is not None:delivery.envelope=None
            self._busy=False
    async def _failed(self,error):
        try:await super()._failed(error)
        finally:self._close_db()
    def _close_db(self):
        if self._history is not None:
            connection=self._history;self._history=None;connection.close()
    async def close(self):
        try:await super().close()
        finally:self._close_db()
