const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {JSDOM} = require('jsdom');
const root = path.join(__dirname, '..');
const workerSource = fs.readFileSync(path.join(root, 'browser-worker.js'), 'utf8');
const clientSource = fs.readFileSync(path.join(root, 'join-client.js'), 'utf8');
const joinHtml = fs.readFileSync(path.join(root, 'join.html'), 'utf8');
const dashHtml = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
function expectedChecksum(seed) {
  const n = 48, bytes = Buffer.from(seed,'hex');
  const [a,b,c,d] = [bytes[0]%30+1, bytes[1], bytes[2]%30+1, bytes[3]];
  let sum=0;
  for(let k=0;k<n;k++)for(let i=0;i<n;i++)for(let j=0;j<n;j++)
    sum += (((i*n+k)*a+b)%31) * (((k*n+j)*c+d)%31);
  return sum;
}
test('browser checksum exactly matches independent matrix computation', () => {
  const sandbox = {self:{postMessage(){}}, setTimeout(){}, clearTimeout(){}, fetch(){}};
  vm.runInNewContext(workerSource, sandbox);
  for(const seed of ['0'.repeat(32),'f'.repeat(32),'01ab12ef'+'4'.repeat(24)]) {
    assert.equal(vm.runInNewContext('checksum('+JSON.stringify(seed)+')',sandbox),expectedChecksum(seed));
  }
});
test('worker registers, heartbeats, leaves and never posts bearer token to page',async()=>{
  const seed='1234567890abcdef1234567890abcdef',next='abcdef1234567890abcdef1234567890';
  const calls=[],messages=[],timers=[];
  const response=(data)=>({ok:true,status:200,json:async()=>data});
  const sandbox = {
    self:{postMessage:m=>messages.push(m),close(){}},
    setTimeout:cb=>{timers.push(cb);return timers.length;},
    clearTimeout(){},
    AbortController,fetch:async(path,opts)=>{
      calls.push([path,opts]);
      if(path.endsWith('/challenge'))return response({seed,resource:'cpu',expires:999999999,signature:'a'.repeat(64)});
      if(path.endsWith('/join'))return response({node_id:seed,token:'private-bearer-token',seed:next,heartbeat_seconds:20});
      if(path.endsWith('/heartbeat'))return response({seed});
      if(path.endsWith('/leave'))return response({ok:true});
      throw Error(path);
    }
  };
  vm.runInNewContext(workerSource,sandbox);
  await sandbox.self.onmessage({data:{type:'start',resources:{cores:4,ram_mb:4096,vram_mb:0}}});
  assert.equal(calls[0][0],'/v0.8/public/challenge');
  assert.equal(calls[1][0],'/v0.8/public/join');
  assert.equal(JSON.parse(calls[1][1].body).checksum,expectedChecksum(seed));
  assert.ok(messages.some(m=>m.phase==='online'));
  assert.equal(JSON.stringify(messages).includes('private-bearer-token'),false);
  await timers.at(-1)();
  assert.equal(calls[2][0],'/v0.8/public/heartbeat');
  assert.equal(calls[2][1].headers.Authorization,'Bearer private-bearer-token');
  await sandbox.self.onmessage({data:{type:'stop'}});
  assert.equal(calls.at(-1)[0],'/v0.8/public/leave');
  assert.equal(calls.at(-1)[1].headers.Authorization,'Bearer private-bearer-token');
});
test('invalid worker capability reports error without attempting network admission',async()=>{
  let requested=0,results=[];
  const sandbox={self:{postMessage:m=>results.push(m)},setTimeout,clearTimeout,AbortController,
    fetch:async()=>{requested++;throw Error('unexpected');}};
  vm.runInNewContext(workerSource,sandbox);
  await sandbox.self.onmessage({data:{type:'start',resources:{cores:1,ram_mb:1024,vram_mb:0}}});
  assert.equal(requested,0);
  assert.equal(results[0].phase,'error');
});
test('public join page starts only from explicit click and supports stopping',()=>{
  const dom = new JSDOM(joinHtml,{url:'https://example.test/join',runScripts:'outside-only'});
  const w=dom.window,messages=[],workers=[];
  Object.defineProperty(w,'isSecureContext',{value:true,configurable:true});
  Object.defineProperty(w.navigator,'hardwareConcurrency',{value:4,configurable:true});
  Object.defineProperty(w.navigator,'deviceMemory',{value:8,configurable:true});
  w.Worker=class {constructor(url){this.url=url;workers.push(this)}postMessage(m){messages.push(m)}terminate(){}};
  w.eval(clientSource);
  assert.equal(workers.length,0,'no silent opt-in');
  w.document.querySelector('[data-browser-join]').click();
  assert.equal(workers.length,1);
  assert.equal(workers[0].url,'/assets/browser-worker.js');
  assert.deepEqual(JSON.parse(JSON.stringify(messages[0].resources)),{cores:4,ram_mb:8192,vram_mb:0});
  workers[0].onmessage({data:{type:'status',phase:'online',message:'connected'}});
  assert.match(w.document.querySelector('[data-browser-status]').textContent,/connected/);
  w.document.querySelector('[data-browser-leave]').click();
  assert.equal(messages.at(-1).type,'stop');
  w.close();
});
test('dashboard includes direct opt-in and shared join client',()=>{
  assert.match(dashHtml,/data-browser-join/);
  assert.match(dashHtml,/\/assets\/join-client.js/);
  assert.match(dashHtml,/href="\/join"/);
});
test('browser cannot claim unknown RAM as 2GiB',()=>{
  const dom=new JSDOM(joinHtml,{url:'https://example.test/join',runScripts:'outside-only'});
  const w=dom.window;
  Object.defineProperty(w,'isSecureContext',{value:true});
  Object.defineProperty(w.navigator,'hardwareConcurrency',{value:4});
  w.Worker=class{constructor(){throw Error('must not create worker')}};
  w.eval(clientSource);
  w.document.querySelector('[data-browser-join]').click();
  assert.match(w.document.querySelector('[data-browser-status]').textContent,/未开放内存估计/);
  w.close();
});
