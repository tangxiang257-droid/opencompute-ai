import sys,importlib.abc,unittest
class Guard(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if any(x in fullname.lower().split('.') for x in ('torch','transformers','safetensors','tensorflow','worker')):raise RuntimeError('forbidden import '+fullname)
def audit(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system','os.fork','os.posix_spawn'):raise RuntimeError('forbidden operation '+event)
sys.meta_path.insert(0,Guard());sys.addaudithook(audit)
suite=unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:]);r=unittest.TextTestRunner(verbosity=2).run(suite);sys.exit(not r.wasSuccessful())
