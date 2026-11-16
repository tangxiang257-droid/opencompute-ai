"""Ordinary persistent journal tests; no model or real authorization."""
import unittest,tempfile
from pathlib import Path
from cluster_p1.control_journal import ControlJournal
from validation.control_exchange.test_transport import Holds
def binding():
 return dict(owner="owner",task="task",session="nonce",epoch=1,stage=1,input_snapshot="a"*64,model="b"*64,layout="c"*64,plan="d"*64,input_digest="e"*64,device="device",instance="instance",peer="f"*64,declaration_digest="1"*64,token_digest="2"*64,preprocessing="3"*64)
class Tests(unittest.TestCase):
 def test_missing_C_ack_cannot_consume(self):
  with tempfile.TemporaryDirectory() as root:
   j=ControlJournal(Path(root)/'j',1024,resources=Holds(),require_receiving_gate=True)
   self.addCleanup(j.close);b=binding();b['stage']=1
   j.prepare('command','resource',b,32)
   with self.assertRaises(Exception):j.consume('command','resource',b)
   self.assertEqual(j.query('command')['state'],'RESERVED')
 def make(self):
  tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);p=Path(tmp.name)/'j'
  j=ControlJournal(p,1024,resources=Holds(),require_receiving_gate=True);self.addCleanup(j.close)
  b=binding();j.prepare('command','resource',b,32)
  j.acknowledge_receiver('command','resource',b,'a'*64,'b'*64)
  return j,b,p
 def test_ack_and_consume_exactly_once(self):
  j,b,_=self.make();self.assertEqual(j.consume('command','resource',b)['state'],'CONSUMED')
  self.assertEqual(j._db.execute('SELECT state FROM control_receiving_gate').fetchone(),('CONSUMED',))
  with self.assertRaises(Exception):j.consume('command','resource',b)
  self.assertEqual(j.query('command')['charge'],32)
 def test_duplicate_ACK_does_not_replace(self):
  j,b,_=self.make()
  with self.assertRaises(Exception):j.acknowledge_receiver('command','resource',b,'c'*64,'d'*64)
  self.assertEqual(j._db.execute('SELECT nonce,recipient FROM control_receiving_gate').fetchone(),('a'*64,'b'*64))
 def test_atomic_consume_rollback(self):
  from unittest.mock import patch
  j,b,_=self.make()
  with patch.object(j,'_commit',side_effect=OSError('before commit')):
   with self.assertRaises(BaseException):j.consume('command','resource',b)
  self.assertEqual(j.query('command')['state'],'RESERVED')
  self.assertEqual(j._db.execute('SELECT state FROM control_receiving_gate').fetchone(),('ACKED',))
 def test_consumed_commit_unknown_is_not_retryable(self):
  from unittest.mock import patch
  j,b,_=self.make();old=j._commit
  def lost(c):old(c);raise OSError('after durable commit')
  with patch.object(j,'_commit',lost):
   with self.assertRaises(BaseException):j.consume('command','resource',b)
  self.assertTrue(j.query('command')['retired'])
  self.assertEqual(j._db.execute('SELECT state FROM control_receiving_gate').fetchone(),('CONSUMED',))
  with self.assertRaises(Exception):j.consume('command','resource',b)
 def test_reopen_ACK_is_history_only(self):
  j,b,p=self.make();j.close()
  other=ControlJournal(p,1024,resources=Holds(),require_receiving_gate=True);self.addCleanup(other.close)
  with self.assertRaises(Exception):other.consume('command','resource',b)
  self.assertEqual(other.query('command')['state'],'RESERVED')
 def test_no_silent_v1_to_v2_migration(self):
  with tempfile.TemporaryDirectory() as root:
   p=Path(root)/'j';j=ControlJournal(p,1024,resources=Holds());j.close()
   with self.assertRaises(Exception):ControlJournal(p,1024,resources=Holds(),require_receiving_gate=True)
if __name__=='__main__':unittest.main()
