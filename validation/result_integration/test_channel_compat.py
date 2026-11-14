"""Real baseline Channel, paired memory streams, invented SSL metadata. NOT TLS."""
import asyncio,hashlib,ssl,unittest
from types import SimpleNamespace
from cluster_p1.channel import Channel,Grant,TLSConfig,ChannelError
from cluster_p1.envelope import Envelope
from isolated_tls_delivery import ReceiveBinding,Scope
from framed_result_transport import ObjectReceiver,payload
class Writer:
 def __init__(self,peer,cert,server):
  self.peer=peer;self.cert=cert;self.server_side=server;self.context=SimpleNamespace(verify_mode=ssl.CERT_REQUIRED,check_hostname=not server);self.closed=False;self.frames=[]
 def get_extra_info(self,name):return self if name=='ssl_object' else None
 def getpeercert(self,binary_form=False):return self.cert
 def get_channel_binding(self,name):return b'fake-unique-not-TLS'
 def version(self):return 'TLSv1.2'
 def is_closing(self):return self.closed
 def write(self,data):
  if self.closed:raise ConnectionError()
  self.frames.append(data);self.peer.feed_data(data)
 async def drain(self):await asyncio.sleep(0)
 def close(self):
  if not self.closed:self.closed=True;self.peer.feed_eof()
 async def wait_closed(self):pass
class Checks(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):self.channels=[];self.receivers=[]
 async def asyncTearDown(self):
  for receiver in self.receivers:await receiver.close()
  await asyncio.gather(*(c.close() for c in self.channels))
 def streams(self):
  sr,rr=asyncio.StreamReader(),asyncio.StreamReader();return sr,Writer(rr,b'b',False),rr,Writer(sr,b'a',True)
 def config(self,peer):return TLSConfig('unused','unused','unused',{peer:hashlib.sha256(peer.encode()).hexdigest()})
 def grant(self):return Grant('task','session',1,1,(0,1),'a'*64,1)
 def envelope(self,data):return Envelope('task','session',1,1,0,(0,1),'a'*64,1,(len(data),),'uint8',data)
 async def test_default_public_api_and_credit_reuse(self):
  sr,sw,rr,rw=self.streams();g=self.grant()
  a=Channel(sr,sw,self.config('b'),{'b':{g}},capacity_bytes=1024,capacity_tiles=1,timeout=1)
  b=Channel(rr,rw,self.config('a'),{'a':{g}},capacity_bytes=1024,capacity_tiles=1,timeout=1);self.channels.extend((a,b))
  await asyncio.gather(a.start(),b.start())
  for data in (b'ordinary',b'next'):
   await a.send(self.envelope(data));d=await b.receive();self.assertEqual(d.envelope.payload,data);await d.release()
   with self.assertRaises(ChannelError):await d.release()
  self.assertEqual([f[:1] for f in rw.frames],[b'H',b'C',b'C'])
  await asyncio.gather(a.close(),b.close());self.assertTrue(sw.closed and rw.closed)
 async def test_existing_object_receiver_default_channel_calls(self):
  sr,sw,rr,rw=self.streams();scope=Scope('task',1,'1'*64,'2'*64,'stage1->owner','logits','session','attempt','a',hashlib.sha256(b'a').hexdigest(),'pre')
  binding=ReceiveBinding(scope,1,'a'*64,1,'uint8',((0,1,(1,)),))
  receiver=ObjectReceiver(rr,rw,self.config('a'),binding,'3'*64,4,hashlib.sha256(b'data').hexdigest());self.receivers.append(receiver)
  sender=Channel(sr,sw,self.config('b'),{'b':{self.grant()}},capacity_bytes=1024,capacity_tiles=1,timeout=1);self.channels.append(sender)
  await asyncio.gather(sender.start(),receiver._channel.start())
  frame=payload(receiver.context,0,0,b'data');await sender.send(self.envelope(frame))
  delivery=await receiver._channel.receive();self.assertEqual(receiver.validate(delivery),b'data')
  receiver.held=delivery;receiver.written=4;receiver.frames=1
  await receiver.release_written_frame();self.assertTrue(delivery._released)
  self.assertEqual([f[:1] for f in rw.frames],[b'H'])
 async def test_existing_accept_receiver_one_tile_and_revoke(self):
  import time
  from isolated_tls_delivery import accept_receiver,LocalAuthority,frame_payload,Denied
  sr,sw,rr,rw=self.streams();scope=Scope('task',1,'1'*64,'2'*64,'stage1->owner','logits','session','attempt','a',hashlib.sha256(b'a').hexdigest(),'pre')
  binding=ReceiveBinding(scope,1,'a'*64,1,'uint8',((0,1,(4,)),));authority=LocalAuthority()
  receiver=await accept_receiver(rr,rw,self.config('a'),authority=authority,binding=binding,deadline=time.monotonic()+5,capacity=16);self.receivers.append(receiver)
  sender=Channel(sr,sw,self.config('b'),{'b':{self.grant()}},capacity_bytes=1024,capacity_tiles=1,timeout=1);self.channels.append(sender);await sender.start()
  await sender.send(self.envelope(frame_payload(receiver._binding,b'data')))
  receipt=await asyncio.wait_for(receiver.claim(0),1);self.assertEqual(receipt.payload,b'data');await receipt.release()
  self.assertEqual([f[:1] for f in rw.frames],[b'H',b'C'])
  await receiver.revoke(authority);self.assertTrue(rw.closed)
  with self.assertRaises(Denied):await receiver.claim(0)
