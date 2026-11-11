const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {JSDOM} = require('jsdom');
const root = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const embedded = html.match(/<script>([\s\S]*?)<\/script>/);
const script = embedded ? embedded[1] : fs.readFileSync(path.join(root, 'dashboard.js'), 'utf8');
const cluster = {nodes:[], tasks:[], counts:{queued:0,leased:0,completed:0,failed:0,cancelled:0}};
const tick = () => new Promise(r => setTimeout(r, 20));

function setup({url='http://localhost:8000', storageBlocked=false, handler}={}) {
  const dom = new JSDOM(html, {url, runScripts:'outside-only'});
  const w = dom.window;
  if (storageBlocked) Object.defineProperty(w,'sessionStorage',{get(){throw new Error('blocked storage')}});
  w.fetch = handler || (async p => ({ok:true,status:200,json:async()=> p==='/v0.3/status'?cluster:p==='/v0.4/models'?{models:[]}:{enabled:false,running:false}}));
  w.eval(script);
  return {w, doc:w.document, close:()=>w.close()};
}
function connect(doc) {doc.getElementById('adminKey').value='test-admin-key-0123456789';doc.getElementById('connectBtn').click();}

test('blocked session storage still allows connection and disconnect clears data', async()=>{
  const d=setup({storageBlocked:true});
  try {
    connect(d.doc); await tick();
    assert.equal(d.doc.getElementById('connectionText').textContent,'集群在线');
    d.doc.getElementById('disconnectBtn').click();
    assert.equal(d.doc.getElementById('connectionText').textContent,'已断开');
    assert.equal(d.doc.getElementById('kpiNodes').textContent,'—');
  } finally {d.close()}
});

test('a late response cannot reconnect a disconnected session',async()=>{
  let release;
  const d=setup({handler: p=> p==='/v0.3/status'?new Promise(r=>release=r):Promise.resolve({ok:true,status:200,json:async()=>({})})});
  try {
    connect(d.doc); await tick();
    d.doc.getElementById('disconnectBtn').click();
    release({ok:true,status:200,json:async()=>cluster});await tick();
    assert.equal(d.doc.getElementById('connectionText').textContent,'已断开');
    assert.equal(d.w.sessionStorage.getItem('ocAdminKey'),null);
  } finally {d.close()}
});

test('non-local HTTP never receives an administrator key', async()=>{
  let sensitive=0;
  const d=setup({url:'http://example.test',handler:async(p,o)=>{if(o?.headers?.['X-Admin-Key'])sensitive++;return {ok:true,status:200,json:async()=>({})}}});
  try {connect(d.doc);await tick();assert.equal(sensitive,0);assert.match(d.doc.getElementById('connectionFeedback').textContent,/HTTPS/)}finally{d.close()}
});

test('worker commands use current origin and exported, quoted variables',()=>{
  const d=setup({url:'https://compute.example.test'});
  try {
    d.doc.getElementById('workerName').value="node'; echo surprise";
    d.doc.getElementById('workerName').dispatchEvent(new d.w.Event('input'));
    const text=d.doc.getElementById('workerCommand').textContent;
    assert.match(text,/export OPENCOMPUTE_API_URL='https:\/\/compute.example.test'/);
    assert.match(text,/--name 'node'"'"'; echo surprise'/);
  }finally{d.close()}
});

test('empty sum fields cannot silently turn into zero',async()=>{
  let submitted=0;
  const d=setup({handler:async p=>{if(p==='/v0.3/tasks')submitted++;return {ok:true,status:200,json:async()=>p==='/v0.3/status'?cluster:{models:[]}}}});
  try {
    connect(d.doc);await tick();d.doc.getElementById('sumInput').value='1,,2';
    d.doc.getElementById('taskForm').dispatchEvent(new d.w.Event('submit',{cancelable:true}));await tick();
    assert.equal(submitted,0);assert.match(d.doc.getElementById('taskFeedback').textContent,/有效数字/);
  }finally{d.close()}
});

test('queue status uses returned active request counters and disables unavailable inference',async()=>{
  const d=setup();
  try {connect(d.doc);await tick();assert.equal(d.doc.getElementById('submitInference').disabled,true)}finally{d.close()}
});

test('transient connection failure can retry without retyping the active key',async()=>{
  let failed=false;
  const d=setup({handler:async p=>{if(p==='/v0.3/status'&&failed)throw new Error('offline');return {ok:true,status:200,json:async()=>p==='/v0.3/status'?cluster:{models:[],enabled:false,running:false}}}});
  try {
    connect(d.doc);await tick();failed=true;d.doc.getElementById('refreshTasks').click();await tick();
    assert.equal(d.doc.getElementById('connectionText').textContent,'连接中断');
    failed=false;d.doc.getElementById('connectBtn').click();await tick();
    assert.equal(d.doc.getElementById('connectionText').textContent,'集群在线');
  }finally{d.close()}
});

test('older model status cannot overwrite a newer disabled status',async()=>{
  let qcount=0,release;
  const d=setup({handler:async p=>{
    if(p==='/v0.3/status')return {ok:true,status:200,json:async()=>cluster};
    if(p==='/v0.5/status'){
      if(++qcount===2)return new Promise(r=>release=r);
      return {ok:true,status:200,json:async()=>({enabled:qcount===1,running:qcount===1})};
    }
    return {ok:true,status:200,json:async()=>({models:[]})};
  }});
  try {
    connect(d.doc);await tick();d.doc.getElementById('refreshInference').click();await tick();
    d.doc.getElementById('refreshInference').click();await tick();
    release({ok:true,status:200,json:async()=>({enabled:true,running:true})});await tick();
    assert.equal(d.doc.getElementById('submitInference').disabled,true);
    assert.equal(d.doc.getElementById('qwenBadge').textContent,'未配置模型包');
  }finally{d.close()}
});

test('older system diagnostics cannot replace newer degraded state',async()=>{
  let n=0,release;
  const report=state=>({version:'0.6.0',uptime_seconds:10,state,database:'ok',workers:{online:1,registered:1,cpu_cores:2},tasks:{queued:0},qwen2:{enabled:false,running:false}});
  const d=setup({handler:async p=>{
    if(p==='/v0.6/system'){if(++n===1)return new Promise(r=>release=r);return {ok:true,status:200,json:async()=>report('degraded')}}
    return {ok:true,status:200,json:async()=>p==='/v0.3/status'?cluster:{models:[],enabled:false,running:false}};
  }});
  try {
    connect(d.doc);await tick();d.doc.getElementById('refreshSystem').click();await tick();
    d.doc.getElementById('refreshSystem').click();await tick();
    release({ok:true,status:200,json:async()=>report('ready')});await tick();
    assert.equal(d.doc.getElementById('systemState').textContent,'需要处理');
  }finally{d.close()}
});

test('old inference cleanup cannot unlock a newer in-flight request',async()=>{
  const pending=[];
  const d=setup({handler:async p=>{
    if(p==='/v0.5/requests')return new Promise(r=>pending.push(r));
    return {ok:true,status:200,json:async()=>p==='/v0.3/status'?cluster:{models:[],enabled:true,running:true}};
  }});
  const submit=()=>d.doc.getElementById('inferenceForm').dispatchEvent(new d.w.Event('submit',{cancelable:true}));
  try {
    connect(d.doc);await tick();submit();await tick();d.doc.getElementById('disconnectBtn').click();
    connect(d.doc);await tick();submit();await tick();
    pending[0]({ok:true,status:200,json:async()=>({generated_ids:[1]})});await tick();
    assert.equal(d.doc.getElementById('submitInference').disabled,true);
  }finally{d.close()}
});

test('GPU queue submits text to the selected Qwen2 model and renders the full output as text',async()=>{
  const model='a'.repeat(64),output='<em>模型输出</em> '+ '继续生成 '.repeat(20);
  const submitted=[];
  const d=setup({handler:async(p,o)=>{
    let data={};
    if(p==='/v0.3/status')data=cluster;
    if(p==='/v0.7/gpus')data={devices:[{online:true,qwen2_model_id:model,qwen2_vram_mb:4096}]};
    if(p==='/v0.7/gpu-tasks'&&o?.method==='POST'){
      submitted.push(JSON.parse(o.body));
      data={id:'qwen-task',status:'queued',required_vram_mb:4096};
    }else if(p==='/v0.7/gpu-tasks'&&submitted.length){
      data={tasks:[{id:'qwen-task',kind:'qwen2_generate',status:'completed',result:{text:output},required_vram_mb:4096,max_attempts:3}]};
    }
    return {ok:true,status:200,json:async()=>data};
  }});
  try {
    connect(d.doc);await tick();
    const kind=d.doc.getElementById('gpuTaskKind');
    assert.ok(kind);
    kind.value='qwen2_generate';kind.dispatchEvent(new d.w.Event('change'));
    d.doc.getElementById('gpuQwenModel').value=model;
    d.doc.getElementById('gpuQwenPrompt').value='你好';
    d.doc.getElementById('gpuQwenTokens').value='16';
    d.doc.getElementById('gpuTaskForm').dispatchEvent(new d.w.Event('submit',{cancelable:true}));
    await tick();
    assert.deepEqual(submitted,[{kind:'qwen2_generate',payload:{model_id:model,prompt:'你好',max_new_tokens:16}}]);
    const result=d.doc.querySelector('#gpuTaskRows details pre');
    assert.equal(result.textContent,output);
    assert.equal(result.querySelector('em'),null);
    d.doc.getElementById('disconnectBtn').click();
    assert.equal(d.doc.getElementById('gpuQwenPrompt').value,'');
  }finally{d.close()}
});

test('Qwen2 mode hides matrix fields and disables submission without an online model',async()=>{
  const d=setup();
  try {
    connect(d.doc);await tick();
    const kind=d.doc.getElementById('gpuTaskKind');
    kind.value='qwen2_generate';kind.dispatchEvent(new d.w.Event('change'));
    const model=d.doc.getElementById('gpuQwenModel');
    assert.equal(model.value,'');
    assert.equal(model.disabled,true);
    assert.equal(d.doc.getElementById('submitGpuTask').disabled,true);
    assert.equal(d.w.getComputedStyle(d.doc.getElementById('gpuMatrixFields')).display,'none');
  }finally{d.close()}
});

test('public admission works before administrator sign-in and uses a keyless command',async()=>{
  const calls=[];
  const d=setup({url:'https://compute.example.test',handler:async(p,o)=>{
    calls.push([p,o]);
    return {ok:true,status:200,json:async()=>({enabled:true,cpu_workers:2,gpu_workers:1,verified_checks:7})};
  }});
  try {
    d.doc.querySelector('[data-page="connect"]').click();
    await tick();
    assert.equal(d.doc.getElementById('publicPoolControls').hidden,false);
    assert.match(d.doc.getElementById('publicPoolMetrics').textContent,/在线 GPU 节点1/);
    assert.equal(d.doc.getElementById('downloadPublicWorker').getAttribute('href'),'/v0.8/public/worker.py');
    assert.equal(calls[0][0],'/v0.8/public/status');
    assert.equal(calls[0][1].headers,undefined);
    assert.equal(calls[0][1].credentials,'omit');
    d.doc.getElementById('publicWorkerShell').value='powershell';
    const device=d.doc.getElementById('publicWorkerDevice');
    device.value='cuda';device.dispatchEvent(new d.w.Event('change'));
    assert.equal(d.doc.getElementById('publicWorkerCommand').textContent,
      'python opencompute-worker.py --url "https://compute.example.test" --device cuda');
  }finally{d.close()}
});
