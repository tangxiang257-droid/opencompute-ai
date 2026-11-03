import os,time,uuid,hashlib,requests
BASE=os.getenv('API_URL','http://api:8000'); KEY=os.getenv('ADMIN_KEY','dev-only-change-me'); NAME=os.getenv('WORKER_NAME','worker'); ID=os.getenv('WORKER_ID',NAME+'-'+str(uuid.uuid4())[:8]); HEAD={'X-API-Key':KEY}
def post(path,data=None,params=None):
    r=requests.post(BASE+path,json=data,params=params,headers=HEAD,timeout=10);r.raise_for_status();return r.json()
while True:
    try:
        post('/nodes/register',{'id':ID,'name':NAME,'capabilities':['sum','sha256']})
        while True:
            post('/nodes/'+ID+'/heartbeat')
            t=post('/tasks/claim',params={'node_id':ID})['task']
            if not t:time.sleep(2);continue
            if t['kind']=='sum':result={'value':sum(t['payload']['numbers'])}
            elif t['kind']=='sha256':result={'digest':hashlib.sha256(t['payload']['text'].encode()).hexdigest()}
            else:continue
            post('/tasks/'+t['id']+'/complete',{'node_id':ID,'lease_token':t['lease_token'],'result':result})
    except Exception as exc:
        print(NAME,'retrying:',exc,flush=True);time.sleep(3)
