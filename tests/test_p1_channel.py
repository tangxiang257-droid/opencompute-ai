import asyncio
import hashlib
import ssl
import subprocess
import unittest
import tempfile
from pathlib import Path
from cluster_p1.channel import Channel, Grant, ChannelError, TLSConfig, connect, accept
from test_p1_envelope import sample

def tls(tmp_path):
    def make(name):
        key=tmp_path/(name+'.key'); cert=tmp_path/(name+'.crt')
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        return key,cert
    ak,ac=make('a'); bk,bc=make('b')
    def cfg(key,cert,peer):
        fp=hashlib.sha256(ssl.PEM_cert_to_DER_cert(peer.read_text())).hexdigest()
        return TLSConfig(str(cert),str(key),str(peer),{'peer':fp})
    return cfg(ak,ac,bc),cfg(bk,bc,ac)

async def pair(tls, grants=None):
    a,b=tls; fut=asyncio.get_running_loop().create_future()
    async def handler(r,w):
        try: fut.set_result(await accept(r,w,b,grants if grants is not None else {'peer':{Grant('task','session',2,3,(0,2),'ab'*32,1)}},capacity_bytes=8,capacity_tiles=1))
        except Exception as e: fut.set_exception(e)
    server=await asyncio.start_server(handler,'127.0.0.1',0,ssl=b.context(server=True))
    try:
        client=await connect('127.0.0.1',server.sockets[0].getsockname()[1],a,{'peer':{Grant('task','session',2,3,(0,2),'ab'*32,1)}},server_hostname='localhost',capacity_bytes=8,capacity_tiles=1)
    except BaseException:
        server.close(); await server.wait_closed()
        try:
            peer=await asyncio.wait_for(fut,1); await peer.close()
        except Exception: pass
        raise
    return server,client,await fut


class ChannelTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.tls=tls(Path(self.tmp.name))

    def test_persistent_credit_and_disconnect(self):
        tls=self.tls
        async def run():
            server,a,b=await pair(tls)
            try:
                await a.send(sample()); received=await b.receive()
                waiting=asyncio.create_task(a.send(sample(tile_id=5)))
                await asyncio.sleep(.05); assert not waiting.done()
                await received.release(); await waiting
                second=await b.receive(); assert second.envelope.tile_id==5
                with self.assertRaises(ChannelError): await received.release()
                blocked=asyncio.create_task(a.send(sample(tile_id=6)))
                await asyncio.sleep(.02); await b.close()
                with self.assertRaises(ChannelError): await blocked
                with self.assertRaises(ChannelError): await a.receive()
            finally:
                await a.close(); await b.close(); server.close(); await server.wait_closed()
        asyncio.run(run())
    
    
    def test_authorization_and_cancellation(self):
        tls=self.tls
        async def run():
            server,a,b=await pair(tls)
            try:
                for fields in [dict(stage_id=4),dict(model_hash='cd'*32),dict(layout_version=2),dict(token_range=(2,4))]:
                    with self.assertRaises(ChannelError): await a.send(sample(**fields))
                with self.assertRaises(ChannelError): await a.send(sample(execution_epoch=1))
                await a.send(sample()); item=await b.receive()
                task=asyncio.create_task(a.send(sample(tile_id=7))); await asyncio.sleep(.02); task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task
                await item.release(); await a.send(sample(tile_id=8)); assert (await b.receive()).envelope.tile_id==8
            finally:
                await a.close(); await b.close(); server.close(); await server.wait_closed()
        asyncio.run(run())

    def test_trusted_peer_without_grant_is_rejected(self):
        async def run():
            server,a,b=await pair(self.tls,grants={})
            try:
                await a.send(sample())
                with self.assertRaises(ChannelError): await b.receive()
            finally:
                await a.close(); await b.close(); server.close(); await server.wait_closed()
        asyncio.run(run())

    def test_wrong_certificate_fingerprint(self):
        async def run():
            a,b=self.tls
            bad=TLSConfig(a.certfile,a.keyfile,a.cafile,{'peer':'00'*32})
            with self.assertRaises(ChannelError): await pair((bad,b))
        asyncio.run(run())

    def test_duplicate_credit_and_oversized_frame_close_connection(self):
        import struct
        async def run():
            for kind,data in [(b'C',struct.pack('!QIQ',8,1,1)),(b'D',b'x'*(8+4096+100))]:
                server,a,b=await pair(self.tls)
                try:
                    await b._frame(kind,data)
                    with self.assertRaises(ChannelError): await a.receive()
                finally:
                    await a.close(); await b.close(); server.close(); await server.wait_closed()
        asyncio.run(run())

    def test_truncated_and_tampered_data_disconnect(self):
        import struct
        async def run():
            for tamper in (False,True):
                server,a,b=await pair(self.tls)
                try:
                    encoded=sample().encode()
                    if tamper:
                        await a._frame(b'D',encoded[:-1]+bytes([encoded[-1]^1]))
                    else:
                        a.w.write(b'D'+struct.pack('!I',len(encoded))+encoded[:20])
                        await a.w.drain(); await a.close()
                    with self.assertRaises(ChannelError): await b.receive()
                finally:
                    await a.close(); await b.close(); server.close(); await server.wait_closed()
        asyncio.run(run())

    def test_cancel_release_waiting_for_write_lock_aborts(self):
        async def run():
            server, sender, receiver = await pair(self.tls)
            try:
                await sender.send(sample())
                delivery = await receiver.receive()
                await receiver._write.acquire()
                try:
                    release = asyncio.create_task(delivery.release())
                    # Let release commit accounting and block on the real lock.
                    await asyncio.sleep(0)
                    self.assertFalse(release.done())
                    release.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await release
                    self.assertTrue(receiver._closed)
                finally:
                    receiver._write.release()
                with self.assertRaises(ChannelError):
                    await sender.send(sample(tile_id=9))
            finally:
                await sender.close()
                await receiver.close()
                server.close()
                await server.wait_closed()
        asyncio.run(run())

    def test_custom_insecure_tls_contexts_are_rejected(self):
        async def run_case(side, mode, check_hostname=True):
            client_config, server_config = self.tls
            server_context = server_config.context(server=True)
            client_context = client_config.context()
            if side == 'server':
                server_context.verify_mode = mode
            else:
                client_context.check_hostname = check_hostname
                client_context.verify_mode = mode
            accepted = asyncio.get_running_loop().create_future()
            async def handler(reader, writer):
                accepted.set_result((reader, writer))
            server = await asyncio.start_server(
                handler, '127.0.0.1', 0, ssl=server_context)
            client_writer = server_writer = None
            channel = None
            try:
                client_reader, client_writer = await asyncio.open_connection(
                    '127.0.0.1', server.sockets[0].getsockname()[1],
                    ssl=client_context, server_hostname='localhost')
                server_reader, server_writer = await asyncio.wait_for(accepted, 2)
                reader, writer, config = (
                    (server_reader, server_writer, server_config)
                    if side == 'server' else
                    (client_reader, client_writer, client_config))
                with self.assertRaises(ChannelError):
                    channel = await accept(reader, writer, config, {})
            finally:
                if channel is not None:
                    await channel.close()
                for writer in (client_writer, server_writer):
                    if writer is not None:
                        writer.close()
                        try:
                            await writer.wait_closed()
                        except (ssl.SSLError, ConnectionError):
                            pass
                server.close()
                await server.wait_closed()
        async def run():
            await run_case('server', ssl.CERT_OPTIONAL)
            await run_case('client', ssl.CERT_NONE, False)
            await run_case('client', ssl.CERT_REQUIRED, False)
        asyncio.run(run())
