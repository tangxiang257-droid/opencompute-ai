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
