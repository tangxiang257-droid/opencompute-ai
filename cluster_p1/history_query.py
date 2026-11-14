"""Private authenticated, query-only control history over supplied Channels.

The client holds no service, operation or scheduler. Reopened control records are
read through an independent SQLite mode=ro/query_only connection. No execution,
resource verification, allocation, prepare, consume, fail or recovery operation is
provided. Current authenticated identity authorizes access to an ORIGINAL binding;
its old peer hash is never rewritten into a new connection qualification.
"""
import asyncio
from contextlib import contextmanager
import hashlib
import inspect
from pathlib import Path
import re
import sqlite3

from .control_exchange import _Endpoint, _json, _parse, _receipt, _need, _nothing
from .control_journal import ControlJournal, _binding, _identifier
from .local_attempt import _non_suppressing
from .delivery_receipt import _with_cleanup


def _context(value):
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value) and inspect.getcoroutinestate(value)==inspect.CORO_CREATED:value.close()
        raise TypeError('history providers must be synchronous contexts')
    return _non_suppressing(value)


def _schema(connection):
    return connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name").fetchall()


class _HistoryEndpoint(_Endpoint):
    def __init__(self,channel):
        super().__init__(channel);self._closed=False;self._busy=False
    async def _close_transport(self):
        self._closed=True
        async def cleanup():
            try:await self.channel.close()
            finally:
                # A writer.close failure must not leave Channel's reader running.
                if not self.channel._task.done():self.channel._task.cancel()
                await asyncio.gather(self.channel._task,return_exceptions=True)
        task=asyncio.create_task(cleanup());cancelled=None
        while not task.done():
            try:await asyncio.shield(task)
            except asyncio.CancelledError as exc:cancelled=exc
            except BaseException:break
        try:task.result()
        except BaseException as error:
            if cancelled is not None:raise _with_cleanup(cancelled,error)
            raise
        if cancelled is not None:raise cancelled
    async def _failed(self,error):
        try:await self._close_transport()
        except BaseException as cleanup:raise _with_cleanup(error,cleanup)
        raise error
    async def close(self):await self._close_transport()


class HistoryClient(_HistoryEndpoint):
    """Only a Channel and a query codec. No body/service/execute API."""
    async def query(self,command_id,expected_binding):
        _need(not self._busy and not self._closed);self._busy=True;delivery=None
        try:
            self.current();_identifier(command_id);_binding(expected_binding)
            binding=_parse(_json(expected_binding))
            sequence=self._sequence;self._sequence+=1
            raw=_json(dict(version=1,sequence=sequence,action='query',command_id=command_id,binding=binding))
            await self.channel.send(self.envelope(binding,sequence,raw),max_frame_bytes=65536)
            delivery=await self.channel.receive();self.current()
            self.validate(delivery.envelope,binding,sequence)
            reply=_parse(delivery.envelope.payload)
            _need(type(reply)is dict and set(reply)=={'version','sequence','reply_to','history_only','receipt'}
                  and type(reply['version'])is int and reply['version']==1
                  and type(reply['sequence'])is int and reply['sequence']==sequence
                  and reply['history_only'] is True and reply['reply_to']==hashlib.sha256(raw).hexdigest())
            receipt=reply['receipt']
            if receipt is not None:
                _need(type(receipt)is dict)
                _identifier(receipt['reservation_id']);_binding(receipt['binding'])
                _need(type(receipt['charge'])is int and receipt['charge']>0)
                _receipt(receipt,command_id,receipt['reservation_id'],binding,receipt['charge'])
            await delivery.release()
            return receipt
        except BaseException as error:await self._failed(error)
        finally:
            if delivery is not None:delivery.envelope=None
            self._busy=False


class HistoryService(_HistoryEndpoint):
    """Read authorization belongs to REAL external synchronous providers.

    identity.hold(current ChannelIdentity) yields a validated principal string.
    authorize.hold(principal,current_identity,original_binding,'query') yields
    exactly None. The old binding's peer is historical; no old-key==new-peer test
    grants access. Test providers prove no TLS, principal or deployment policy.

    Only an exact already-reopened ControlJournal is accepted. Its connection is
    used during construction to locate/compare the trusted store; requests never
    contain filesystem paths. Runtime reads use a separate read-only connection.
    Same-UID hostile mutation, rollback/copies and durable distributed revocation
    remain outside this private trusted-filesystem boundary.
    """
    def __init__(self,channel,journal,*,identity=None,authorize=None):
        super().__init__(channel)
        _need(type(journal)is ControlJournal and journal._writable is False and not journal._closed)
        self.identity=identity;self.authorize=authorize;self._history=None
        with journal._lock:
            databases=journal._db.execute('PRAGMA database_list').fetchall()
            _need(len(databases)==1 and databases[0][1]=='main' and bool(databases[0][2]))
            self._path=Path(databases[0][2]).resolve(strict=True)
            info=self._path.stat();self._file_identity=(info.st_dev,info.st_ino)
            self._policy=journal._db.execute('SELECT version,capacity FROM control_policy').fetchall()
            self._schema=_schema(journal._db)
        _need(len(self._policy)==1 and self._policy[0][0]==1 and type(self._policy[0][1])is int and self._policy[0][1]>0)
        _need({(row[0],row[1]) for row in self._schema}=={
            ('table','control_calls'),('table','control_policy'),('trigger','control_binding_immutable')})
        try:
            self._history=sqlite3.connect(self._path.as_uri()+'?mode=ro',uri=True,isolation_level=None)
            self._history.execute('PRAGMA query_only=ON')
            self._store_current()
        except BaseException:
            if self._history is not None:self._history.close()
            raise

    def _store_current(self):
        info=self._path.stat();_need((info.st_dev,info.st_ino)==self._file_identity)
        _need(self._history is not None and self._history.execute('PRAGMA query_only').fetchone()==(1,))
        _need(self._history.execute('SELECT version,capacity FROM control_policy').fetchall()==self._policy
              and _schema(self._history)==self._schema)

    def _ready(self):
        _need(not self._closed);self.current()
        _need(self.identity is not None and callable(getattr(self.identity,'hold',None))
              and self.authorize is not None and callable(getattr(self.authorize,'hold',None)))

    def _authorized_snapshot(self,command,binding,*,principal_expected=None):
        self._ready()
        with (self.channel.identity_fence(self._identity),
              _context(self.identity.hold(self._identity)) as principal):
            _need(type(principal)is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',principal) is not None)
            if principal_expected is not None:_need(principal==principal_expected)
            self._store_current()
            row=self._history.execute('SELECT command_id,reservation_id,binding,charge,state,output_digest,retired FROM control_calls WHERE command_id=?',(command,)).fetchone()
            receipt=ControlJournal._receipt(row)
            original=binding if receipt is None else receipt['binding']
            _binding(original);_need(_json(original)==_json(binding))
            # Providers see isolated ordinary copies, never a mutable stored map.
            with _context(self.authorize.hold(principal,self._identity,_parse(_json(original)),'query')) as allowed:
                _nothing(allowed)
                if receipt is not None:
                    _identifier(receipt['reservation_id'])
                    _need(type(receipt['charge'])is int and receipt['charge']>0)
                    _receipt(receipt,command,receipt['reservation_id'],binding,receipt['charge'])
                self._store_current()
        return principal,receipt

    async def serve_once(self):
        _need(not self._busy and not self._closed);self._busy=True;delivery=None
        try:
            self._ready();delivery=await self.channel.receive();self.current()
            sequence=self._sequence;self._sequence+=1
            raw=delivery.envelope.payload;request=_parse(raw)
            _need(type(request)is dict and set(request)=={'version','sequence','action','command_id','binding'})
            command=request['command_id'];binding=request['binding'];_identifier(command);_binding(binding)
            _need(_json(request)==_json(dict(version=1,sequence=sequence,action='query',command_id=command,binding=binding)))
            self.validate(delivery.envelope,binding,sequence)
            principal,_=self._authorized_snapshot(command,binding)
            await delivery.release();delivery.envelope=None;delivery=None
            # Release may await. Recheck permission and history afterward, without
            # a lock across either network wait. This is a read-admission point,
            # not a promise to recall bytes after send has already begun.
            _,receipt=self._authorized_snapshot(command,binding,principal_expected=principal)
            reply=_json(dict(version=1,sequence=sequence,reply_to=hashlib.sha256(raw).hexdigest(),history_only=True,receipt=receipt))
            await self.channel.send(self.envelope(binding,sequence,reply),max_frame_bytes=65536)
        except BaseException as error:await self._failed(error)
        finally:
            if delivery is not None:delivery.envelope=None
            self._busy=False

    async def close(self):
        try:await self._close_transport()
        finally:
            if self._history is not None:
                connection,self._history=self._history,None;connection.close()
