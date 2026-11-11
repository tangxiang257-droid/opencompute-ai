
(function(){
'use strict';
const $=id=>document.getElementById(id);
const state={key:'',cluster:null,qwen:null,models:[],page:'overview',timer:null,epoch:0,controllers:new Set(),clusterBusy:false,inferenceBusy:false,detailId:null,qwenSequence:0,systemSequence:0,gpuSequence:0,gpus:null,gpuTasks:null,gpuTaskSequence:0};
const titles={overview:['算力总览','OVERVIEW'],devices:['设备节点','DEVICES'],tasks:['任务队列','TASK QUEUE'],models:['模型中心','MODEL HUB'],inference:['推理实验','INFERENCE LAB'],system:['连接诊断','DIAGNOSTICS'],connect:['设备接入与 API','DEVELOPER']};
function toast(message){const el=$('toast');el.textContent=message;el.classList.add('show');clearTimeout(toast.timer);toast.timer=setTimeout(()=>el.classList.remove('show'),3000)}
function text(v){return String(v===undefined||v===null?'—':v)}
function el(tag,cls,value){const n=document.createElement(tag);if(cls)n.className=cls;if(value!==undefined)n.textContent=value;return n}
function cell(row,value,cls){const td=el('td',cls,value);row.appendChild(td);return td}
function statusTag(value){const map={online:['在线','good'],queued:['排队中','wait'],leased:['执行中','good'],completed:['已完成','good'],failed:['失败','bad'],offline:['离线','bad'],cancelled:['已取消','']};const item=map[value]||[text(value),''];return el('span','status '+item[1],item[0])}
function bytesMb(v){const n=Number(v)||0;return n>=1024?(n/1024).toFixed(1)+' GB':Math.round(n)+' MB'}
function ago(ts){if(!ts)return '—';const sec=Math.max(0,Math.floor(Date.now()/1000-Number(ts)));if(sec<5)return '刚刚';if(sec<60)return sec+' 秒前';if(sec<3600)return Math.floor(sec/60)+' 分钟前';if(sec<86400)return Math.floor(sec/3600)+' 小时前';return Math.floor(sec/86400)+' 天前'}
function when(ts){return ts?new Date(Number(ts)*1000).toLocaleString():'—'}
function requireKey(){if(!state.key){toast('请先连接管理员密钥');$('adminKey').focus();return false}return true}
function storage(action,value){try{if(action==='read')return sessionStorage.getItem('ocAdminKey');if(action==='write')sessionStorage.setItem('ocAdminKey',value);else if(action==='clear')sessionStorage.removeItem('ocAdminKey');return true}catch(_){return null}}
function banner(message){$('connectionFeedback').textContent=message||'';$('connectionFeedback').classList.toggle('hidden',!message)}
function stale(){const error=new Error('连接已变更，请重试');error.stale=true;return error}
async function api(path,options={}){
  const epoch=state.epoch,controller=new AbortController(),headers=Object.assign({},options.headers||{});
  if(state.key)headers['X-Admin-Key']=state.key;
  if(options.body)headers['Content-Type']='application/json';
  const timeout=setTimeout(()=>controller.abort(),options.timeoutMs||15000);state.controllers.add(controller);
  try{
    const response=await fetch(path,Object.assign({},options,{headers,signal:controller.signal}));
    let data;try{data=await response.json()}catch(_){data={detail:response.statusText}}
    if(epoch!==state.epoch)throw stale();
    if(!response.ok){
      if(response.status===401&&state.key){disconnect('身份验证失效');banner('管理员密钥无效，请重新输入并连接。');throw stale()}
      const detail=data&&data.detail;throw new Error(response.status===429?'请求过于频繁，请稍后重试':typeof detail==='string'?detail:JSON.stringify(detail||response.statusText));
    }
    return data;
  }catch(error){if(epoch!==state.epoch||error.stale)throw stale();if(error.name==='AbortError')throw new Error('请求超时，请检查网络后重试');throw error}
  finally{clearTimeout(timeout);state.controllers.delete(controller)}
}
function disconnect(message='已断开'){
  state.epoch++;clearTimeout(state.timer);state.controllers.forEach(c=>c.abort());state.controllers.clear();
  state.key='';state.cluster=null;state.qwen=null;state.gpus=null;state.gpuSequence++;state.gpuTasks=null;state.gpuTaskSequence++;state.clusterBusy=false;state.inferenceBusy=false;state.detailId=null;storage('clear');
  $('adminKey').value='';$('disconnectBtn').disabled=true;$('connectBtn').disabled=false;$('submitInference').disabled=true;
  ['kpiNodes','kpiCpu','kpiRam','kpiRunning'].forEach(id=>$(id).textContent='—');
  ['resourceBars','taskBars','taskSummary','qwenStats','gpuSummary','gpuTaskSummary'].forEach(id=>$(id).replaceChildren(el('div','empty','连接后读取实时数据')));
  [['overviewNodes',4],['overviewTasks',3],['deviceRows',8],['taskRows',8],['gpuTaskRows',8]].forEach(([id,n])=>{const row=el('tr');cell(row,'请先连接集群','muted').colSpan=n;$(id).replaceChildren(row)});
  $('taskDetailPanel').classList.add('hidden');$('taskDetail').textContent='';$('taskFeedback').textContent='';$('inferenceFeedback').textContent='';$('inferenceResult').textContent='尚未提交请求';
  $('qwenBadge').textContent='未检测';$('qwenBadge').className='status';$('taskLiveStatus').textContent='未连接';$('taskLiveStatus').className='status';$('gpuTaskLiveStatus').textContent='未连接';$('gpuTaskLiveStatus').className='status';$('gpuTaskFeedback').textContent='';
  $('deviceCount').textContent='等待连接';$('gpuPoolState').textContent='等待数据';$('gpuPoolState').className='status wait';const gpuRow=el('tr');cell(gpuRow,'请先连接集群','muted').colSpan=8;$('gpuRows').replaceChildren(gpuRow);$('kpiTaskFoot').textContent='等待集群连接';$('kpiNodesFoot').textContent='等待集群连接';
  $('systemChecks').replaceChildren();$('systemState').textContent='未检测';$('systemState').className='status';$('systemHint').textContent='连接管理员密钥后读取服务器状态';
  connection(false,message);banner('');
}
async function poll(){
  const epoch=state.epoch;if(!state.key)return;
  if(!document.hidden)await Promise.all([loadCluster(),loadQwen(),loadGpuInventory(),loadGpuTasks(),state.page==='system'?loadSystem():Promise.resolve()]);
  if(epoch===state.epoch&&state.key)state.timer=setTimeout(poll,12000);
}
function go(page){if(!titles[page])return;state.page=page;document.querySelectorAll('.page').forEach(n=>n.classList.toggle('active',n.id==='page-'+page));document.querySelectorAll('[data-page]').forEach(n=>n.classList.toggle('active',n.dataset.page===page));$('pageTitle').textContent=titles[page][0];$('crumb').textContent=titles[page][1];$('sidebar').classList.remove('open');$('menuBtn').setAttribute('aria-expanded','false');if(page==='models')loadModels();if(page==='inference'&&state.key)loadQwen();if((page==='devices'||page==='tasks')&&state.key)loadCluster();if(page==='devices'&&state.key)loadGpuInventory();if(page==='tasks'&&state.key)loadGpuTasks();if(page==='system'&&state.key)loadSystem()}
document.querySelectorAll('[data-page]').forEach(n=>n.addEventListener('click',()=>go(n.dataset.page)));
document.querySelectorAll('[data-goto]').forEach(n=>n.addEventListener('click',()=>go(n.dataset.goto)));
$('menuBtn').addEventListener('click',()=>{$('sidebar').classList.toggle('open');$('menuBtn').setAttribute('aria-expanded',String($('sidebar').classList.contains('open')))});
function connection(connected,message){$('connectionDot').className='dot'+(connected?' on':'');$('sideDot').className='dot'+(connected?' on':'');$('connectionText').textContent=message;$('sideState').textContent=connected?'集群已连接':'尚未连接集群';$('sideHint').textContent=connected?'数据已从 API 读取':'输入管理员密钥以读取实时数据'}
function renderResources(online,allNodes,counts){const host=$('resourceBars');host.replaceChildren();const cores=online.reduce((s,n)=>s+(Number(n.cpu_cores)||0),0),ram=online.reduce((s,n)=>s+(Number(n.ram_mb)||0),0),free=online.reduce((s,n)=>s+(Number(n.free_ram_mb)||0),0),load=cores?online.reduce((s,n)=>s+(Number(n.cpu_cores)||0)*(Number(n.cpu_load_pct)||0),0)/cores:0,ratio=allNodes.length?online.length/allNodes.length*100:0;[['CPU 平均负载',load.toFixed(1)+'%',load],['可用内存',bytesMb(free)+' / '+bytesMb(ram),ram?free/ram*100:0],['在线设备',online.length+' / '+allNodes.length,ratio]].forEach(x=>{const row=el('div','resource-row');row.append(el('span','',x[0]));const bar=el('div','bar'),span=el('span');span.style.width=Math.max(0,Math.min(100,x[2]))+'%';bar.append(span);row.append(bar,el('strong','',x[1]));host.append(row)});const taskHost=$('taskBars');taskHost.replaceChildren();const total=Math.max(1,(counts.queued||0)+(counts.leased||0)+(counts.completed||0)+(counts.failed||0));[['排队中',counts.queued||0,'#f0bd68'],['执行中',counts.leased||0,'#63d9a2'],['已完成',counts.completed||0,'#78a9ff'],['失败',counts.failed||0,'#f08086']].forEach(x=>{const row=el('div','resource-row');row.append(el('span','',x[0]));const bar=el('div','bar'),span=el('span');span.style.width=(x[1]/total*100)+'%';span.style.background=x[2];bar.append(span);row.append(bar,el('strong','',String(x[1])));taskHost.append(row)})}
function renderKpis(data){const nodes=data.nodes||[],tasks=data.tasks||[],online=nodes.filter(n=>n.online),cpu=online.reduce((s,n)=>s+(Number(n.cpu_cores)||0),0),ram=online.reduce((s,n)=>s+(Number(n.ram_mb)||0),0),counts=data.counts||{};$('kpiNodes').textContent=online.length+' / '+nodes.length;$('kpiNodesFoot').textContent='在线节点 / 已注册节点';$('kpiCpu').textContent=cpu;$('kpiRam').textContent=bytesMb(ram);$('kpiRunning').textContent=counts.leased||0;$('kpiTaskFoot').textContent=(counts.queued||0)+' 个排队 · '+(counts.completed||0)+' 个已完成';renderResources(online,nodes,counts);renderOverviewTables(online,tasks)}
function renderOverviewTables(nodes,tasks){const nr=$('overviewNodes');nr.replaceChildren();if(!nodes.length){const tr=el('tr'),td=cell(tr,'暂无在线设备','muted');td.colSpan=4;nr.append(tr)}else nodes.slice(0,5).forEach(n=>{const tr=el('tr');cell(tr,n.name||String(n.id).slice(0,8),'node-name');cell(tr,(n.cpu_cores||0)+' 核 · '+bytesMb(n.ram_mb));cell(tr,(Number(n.cpu_load_pct)||0).toFixed(0)+'%');const td=el('td');td.append(statusTag('online'));tr.append(td);nr.append(tr)});const trh=$('overviewTasks');trh.replaceChildren();if(!tasks.length){const tr=el('tr'),td=cell(tr,'暂无任务','muted');td.colSpan=3;trh.append(tr)}else tasks.slice(0,5).forEach(t=>{const tr=el('tr');cell(tr,String(t.id||'').slice(0,10),'node-name');cell(tr,t.kind);const td=el('td');td.append(statusTag(t.status));tr.append(td);trh.append(tr)})}
function renderDevices(nodes){$('deviceCount').textContent=nodes.length+' 台已注册设备';const body=$('deviceRows');body.replaceChildren();if(!nodes.length){const tr=el('tr'),td=cell(tr,'暂无设备。前往“设备接入与 API”添加 Worker。','muted');td.colSpan=8;body.append(tr);return}nodes.forEach(n=>{const tr=el('tr'),a=el('td');a.append(el('div','node-name',n.name||n.id),el('div','sub',String(n.id).slice(0,16)));tr.append(a);cell(tr,n.platform);cell(tr,String(n.cpu_cores)+' 核');cell(tr,bytesMb(n.free_ram_mb)+' / '+bytesMb(n.ram_mb));cell(tr,(Number(n.cpu_load_pct)||0).toFixed(1)+'%');cell(tr,(n.capabilities||[]).join(', '));cell(tr,ago(n.last_seen));const st=el('td');st.append(statusTag(n.online?'online':'offline'));tr.append(st);body.append(tr)})}
function renderTasks(tasks){
  const body=$('taskRows'),filter=$('taskFilter').value,query=$('taskSearch').value.trim().toLowerCase();body.replaceChildren();
  const visible=tasks.filter(t=>(filter==='all'||t.status===filter)&&(!query||(t.id+' '+t.kind).toLowerCase().includes(query)));
  if(!visible.length){const tr=el('tr');cell(tr,tasks.length?'没有符合条件的任务':'暂无任务。创建一个 CPU 测试任务开始体验。','muted').colSpan=8;body.append(tr)}
  visible.forEach(t=>{const tr=el('tr');cell(tr,String(t.id).slice(0,12),'node-name');cell(tr,t.kind);const st=el('td');st.append(statusTag(t.status));tr.append(st);
    cell(tr,t.assigned_node?String(t.assigned_node).slice(0,10):'—');cell(tr,(t.attempts||0)+' / '+t.max_attempts);cell(tr,when(t.created));cell(tr,t.result?JSON.stringify(t.result).slice(0,70):(t.error||'—'));
    const actions=el('td'),group=el('div','task-actions');
    const detail=el('button','btn secondary small','详情');detail.addEventListener('click',()=>showTask(t.id));group.append(detail);
    if(['queued','leased','failed','cancelled'].includes(t.status)){const action=['queued','leased'].includes(t.status)?'cancel':'retry',button=el('button','btn secondary small',action==='cancel'?'取消':'重试');button.addEventListener('click',()=>changeTask(t.id,action,button));group.append(button)}
    actions.append(group);tr.append(actions);body.append(tr);
  });
  const counts=state.cluster?.counts||{},host=$('taskSummary');host.replaceChildren();
  [['排队中',counts.queued||0],['执行中',counts.leased||0],['已完成',counts.completed||0],['失败',counts.failed||0],['已取消',counts.cancelled||0]].forEach(([name,count])=>{const row=el('div','resource-row');row.append(el('span','',name),el('span'),el('strong','',String(count)));host.append(row)});
  $('taskLiveStatus').textContent=visible.length+' / '+tasks.length+' 条记录';$('taskLiveStatus').className='status good';
}
async function showTask(id){
  state.detailId=id;$('taskDetailPanel').classList.remove('hidden');$('taskDetail').textContent='正在读取任务详情…';
  try{const data=await api('/v0.3/tasks/'+encodeURIComponent(id));if(state.detailId===id)$('taskDetail').textContent=JSON.stringify(data,null,2)}catch(e){if(!e.stale&&state.detailId===id)$('taskDetail').textContent='读取失败：'+e.message}
}
async function changeTask(id,action,button){
  button.disabled=true;
  try{const data=await api('/v0.3/tasks/'+encodeURIComponent(id)+'/'+action,{method:'POST'});$('taskFeedback').textContent=action==='cancel'?'任务已取消，旧租约结果将被拒绝。':'任务已重新排队。';await loadCluster();if(state.detailId===id)$('taskDetail').textContent=JSON.stringify(data,null,2)}
  catch(e){if(!e.stale){$('taskFeedback').textContent='操作失败：'+e.message;await loadCluster()}}
  finally{button.disabled=false}
}
async function loadCluster(){
  if(!state.key||state.clusterBusy)return;state.clusterBusy=true;const epoch=state.epoch;
  try{const d=await api('/v0.3/status');state.cluster=d;renderKpis(d);renderDevices(d.nodes||[]);renderTasks(d.tasks||[]);connection(true,'集群在线');banner('')}
  catch(e){if(!e.stale){connection(false,'连接中断');banner('集群读取失败：'+e.message+'。可点击连接重试；输入内容已保留。')}}
  finally{if(epoch===state.epoch)state.clusterBusy=false}
}
function renderGpuInventory(data){
  const summary=data.summary||{},devices=data.devices||[],host=$('gpuSummary');
  host.replaceChildren();
  const online=Number(summary.online)||0,registered=Number(summary.registered)||0;
  $('gpuPoolState').textContent=registered?online+' / '+registered+' 台 GPU 在线':'未发现 GPU';
  $('gpuPoolState').className='status '+(online?'good':registered?'wait':'');
  if(!registered){
    host.append(el('div','empty','尚无 GPU 节点。安装 NVIDIA 驱动并运行带 GPU 检测功能的 Worker 后，资源会自动出现在这里。'));
  }else{
    [['在线 GPU',online+' / '+registered],['在线总显存',bytesMb(summary.total_vram_mb)],['可用显存',bytesMb(summary.free_vram_mb)],['离线 GPU',String(summary.offline||0)]].forEach(([label,value])=>{
      const row=el('div','resource-row');row.append(el('span','',label),el('span'),el('strong','',value));host.append(row);
    });
  }
  const body=$('gpuRows');body.replaceChildren();
  if(!devices.length){const tr=el('tr');cell(tr,'暂未检测到 NVIDIA GPU。','muted').colSpan=8;body.append(tr);return}
  devices.forEach(gpu=>{
    const tr=el('tr');
    cell(tr,(gpu.node_name||'未知节点')+' · '+String(gpu.node_id||'').slice(0,8),'node-name');
    cell(tr,'GPU '+gpu.gpu_index+' · '+gpu.name);
    cell(tr,(gpu.vendor||'—').toUpperCase()+' / '+(gpu.backend||'—').toUpperCase());
    cell(tr,bytesMb(gpu.free_memory_mb)+' / '+bytesMb(gpu.total_memory_mb));
    cell(tr,Number(gpu.utilization_pct||0).toFixed(1)+'%');
    cell(tr,gpu.compute_capability||'—');
    cell(tr,gpu.runtime==='pytorch_cuda'?'PyTorch CUDA '+(gpu.runtime_version||''):'未就绪');
    const status=el('td');status.append(statusTag(gpu.online?'online':'offline'));tr.append(status);body.append(tr);
  });
}
async function loadGpuInventory(){
  if(!state.key)return;
  const sequence=++state.gpuSequence;
  try{
    const data=await api('/v0.7/gpus');
    if(sequence!==state.gpuSequence)return;
    state.gpus=data;renderGpuInventory(data);
  }catch(error){
    if(!error.stale&&sequence===state.gpuSequence){
      $('gpuPoolState').textContent='读取失败';$('gpuPoolState').className='status bad';
      $('gpuSummary').replaceChildren(el('div','empty','GPU 资源读取失败：'+error.message));
      const row=el('tr');cell(row,'请检查服务端是否已部署 v0.7 GPU API。','muted').colSpan=7;$('gpuRows').replaceChildren(row);
    }
  }
}
function renderGpuTasks(data){
  const tasks=data.tasks||[],counts=data.counts||{},host=$('gpuTaskSummary');
  host.replaceChildren();
  [['排队中',counts.queued||0],['执行中',counts.leased||0],['已完成',counts.completed||0],['失败',counts.failed||0],['已取消',counts.cancelled||0],['当前预留显存',bytesMb(data.reserved_vram_mb||0)]].forEach(([label,value])=>{
    const row=el('div','resource-row');row.append(el('span','',label),el('span'),el('strong','',String(value)));host.append(row);
  });
  const body=$('gpuTaskRows');body.replaceChildren();
  if(!tasks.length){const tr=el('tr');cell(tr,'暂无 GPU 任务。提交一个矩阵校验任务开始体验。','muted').colSpan=8;body.append(tr)}
  tasks.forEach(task=>{
    const tr=el('tr');
    cell(tr,String(task.id||'').slice(0,12),'node-name');
    cell(tr,task.kind);
    const status=el('td');status.append(statusTag(task.status));tr.append(status);
    cell(tr,task.assigned_node?(String(task.assigned_node).slice(0,8)+' · GPU '+task.assigned_gpu_index):'等待 GPU');
    cell(tr,bytesMb(task.required_vram_mb));
    cell(tr,(task.attempts||0)+' / '+task.max_attempts);
    cell(tr,task.result?JSON.stringify(task.result).slice(0,72):(task.error||'—'));
    const actions=el('td'),group=el('div','task-actions');
    if(['queued','leased','failed','cancelled'].includes(task.status)){
      const action=['queued','leased'].includes(task.status)?'cancel':'retry';
      const button=el('button','btn secondary small',action==='cancel'?'取消':'重试');
      button.addEventListener('click',()=>changeGpuTask(task.id,action,button));group.append(button);
    }
    actions.append(group);tr.append(actions);body.append(tr);
  });
  $('gpuTaskLiveStatus').textContent=tasks.length+' 条记录';
  $('gpuTaskLiveStatus').className='status '+(tasks.length?'good':'wait');
}
async function loadGpuTasks(){
  if(!state.key)return;
  const sequence=++state.gpuTaskSequence;
  try{
    const data=await api('/v0.7/gpu-tasks');
    if(sequence!==state.gpuTaskSequence)return;
    state.gpuTasks=data;renderGpuTasks(data);
  }catch(error){
    if(!error.stale&&sequence===state.gpuTaskSequence){
      $('gpuTaskLiveStatus').textContent='读取失败';
      $('gpuTaskLiveStatus').className='status bad';
      $('gpuTaskSummary').replaceChildren(el('div','empty','GPU 任务队列读取失败：'+error.message));
      const row=el('tr');cell(row,'请确认服务端已部署 v0.7.1 GPU 调度 API。','muted').colSpan=8;$('gpuTaskRows').replaceChildren(row);
    }
  }
}
async function changeGpuTask(id,action,button){
  button.disabled=true;
  try{
    const data=await api('/v0.7/gpu-tasks/'+encodeURIComponent(id)+'/'+action,{method:'POST'});
    $('gpuTaskFeedback').textContent=action==='cancel'?'GPU 任务已取消，原租约已失效。':'GPU 任务已重新排队。';
    await loadGpuTasks();
  }catch(error){
    if(!error.stale)$('gpuTaskFeedback').textContent='GPU 任务操作失败：'+error.message;
  }finally{button.disabled=false}
}
async function loadModels(){
  try{const d=await api('/v0.4/models');state.models=d.models||[];const host=$('modelCards');host.replaceChildren();if(!state.models.length){host.append(el('div','empty','模型目录暂无条目'));return}
    state.models.forEach(m=>{const card=el('article','model-card'),ready=m.capability==='experimental';card.append(el('span','tag '+(ready?'ready':'planned'),ready?'实验引擎已实现':m.capability==='research'?'研究阶段':'待适配'),el('h3','',m.display_name||m.id),el('p','',m.description||'暂无说明'));const meta=el('div','model-meta');meta.append(el('span','tag',m.architecture||m.family||'模型'));(m.modes||[]).forEach(mode=>meta.append(el('span','tag',mode)));card.append(meta);host.append(card)})
  }catch(e){if(!e.stale)$('modelCards').replaceChildren(el('div','empty','模型目录读取失败：'+e.message))}
}
async function loadQwen(){
  if(!state.key)return;const sequence=++state.qwenSequence;
  try{const q=await api('/v0.5/status');if(sequence!==state.qwenSequence)return;state.qwen=q;const ready=!!q.enabled&&!!q.running,badge=$('qwenBadge');badge.textContent=ready?'服务运行中':q.enabled?'服务已停止':'未配置模型包';badge.className='status '+(ready?'good':'wait');$('submitInference').disabled=!ready||state.inferenceBusy;
    const host=$('qwenStats');host.replaceChildren();[['服务状态',q.enabled?'已启用':'未配置模型包'],['运行状态',q.running?'运行中':'未运行'],['排队请求',q.queue_pending??'—'],['队列容量',q.queue_capacity??'—'],['活动请求',q.active_requests??'—'],['已完成批次',q.batches_completed??'—']].forEach(([label,value])=>{const row=el('div','resource-row');row.append(el('span','',label),el('span'),el('strong','',String(value)));host.append(row)});if(q.last_error)host.append(el('div','notice warn','最近错误：'+q.last_error));
  }catch(e){if(!e.stale&&sequence===state.qwenSequence){$('submitInference').disabled=true;$('qwenBadge').textContent='读取失败';$('qwenBadge').className='status bad';$('qwenStats').replaceChildren(el('div','empty',e.message))}}
}
async function loadSystem(){
  if(!state.key)return;const sequence=++state.systemSequence;
  try{const d=await api('/v0.6/system');if(sequence!==state.systemSequence)return;$('systemHint').textContent='v'+d.version+' · 已运行 '+Math.floor(d.uptime_seconds)+' 秒';$('systemState').textContent=d.state==='ready'?'CPU 节点就绪':d.state==='waiting_for_workers'?'等待设备':'需要处理';$('systemState').className='status '+(d.state==='ready'?'good':'wait');const host=$('systemChecks');host.replaceChildren();
    [['API 地址',location.origin],['数据库',d.database==='ok'?'读写正常':'不可用，请检查服务器日志及数据卷'],['设备',d.workers?d.workers.online+' 台在线 / '+d.workers.registered+' 台注册':'无法读取'],['在线 CPU',d.workers?d.workers.cpu_cores+' 核':'—'],['待执行任务',d.tasks?String(d.tasks.queued):'—'],['Qwen2 推理',d.qwen2.running?'运行中':d.qwen2.enabled?'已配置，服务未运行':'未配置模型包；CPU 任务仍可使用']].forEach(([label,value])=>{const row=el('div');row.append(el('dt','',label),el('dd','',value));host.append(row)})
  }catch(e){if(!e.stale&&sequence===state.systemSequence){$('systemHint').textContent='检测失败：'+e.message;$('systemState').textContent='检测失败'}}
}
async function connect(){
  const key=$('adminKey').value.trim()||state.key;
  if(!key){banner('请输入管理员密钥后连接。');$('adminKey').focus();return}
  if(location.protocol!=='https:'&&!['localhost','127.0.0.1','[::1]'].includes(location.hostname)){banner('此地址未使用 HTTPS。请使用 HTTPS 地址连接，以保护管理员密钥。');return}
  disconnect();state.key=key;const epoch=state.epoch;$('connectBtn').disabled=true;$('disconnectBtn').disabled=false;connection(false,'正在验证…');
  try{const d=await api('/v0.3/status');state.cluster=d;storage('write',key);renderKpis(d);renderDevices(d.nodes||[]);renderTasks(d.tasks||[]);connection(true,'集群在线');banner('');await Promise.all([loadQwen(),loadModels(),loadGpuInventory(),loadGpuTasks()]);if(epoch!==state.epoch)return;if(state.page==='system')await loadSystem();state.timer=setTimeout(poll,12000)}
  catch(e){if(!e.stale){disconnect('连接失败');banner('连接失败：'+e.message+'。请检查服务地址和管理员密钥。')}}
  finally{if(epoch===state.epoch)$('connectBtn').disabled=false}
}
$('disconnectBtn').addEventListener('click',()=>disconnect());$('connectBtn').addEventListener('click',connect);$('adminKey').addEventListener('keydown',e=>{if(e.key==='Enter')connect()});
$('taskKind').addEventListener('change',()=>{const k=$('taskKind').value;$('sumField').classList.toggle('hidden',k!=='sum');$('textField').classList.toggle('hidden',k!=='sha256');$('matrixField').classList.toggle('hidden',k!=='matmul_checksum')});
$('taskForm').addEventListener('submit',async e=>{e.preventDefault();if(!requireKey())return;const kind=$('taskKind').value;let payload={};try{if(kind==='sum'){const raw=$('sumInput').value.split(',').map(x=>x.trim());if(raw.some(x=>!x))throw new Error('请输入有效数字，不要留空项');const values=raw.map(Number);if(!values.length||values.some(x=>!Number.isFinite(x))||values.length>10000)throw new Error('请输入不超过 10000 个有效数字');payload={numbers:values}}else if(kind==='sha256'){const value=$('hashInput').value;if(!value||value.length>100000)throw new Error('请输入 1–100000 个字符');payload={text:value}}else{const size=Number($('matrixSize').value);if(!Number.isInteger(size)||size<1||size>64)throw new Error('矩阵边长必须为 1–64');payload={size:size}}const cpu=Number($('taskCpu').value),ram=Number($('taskRam').value);if(!Number.isInteger(cpu)||cpu<1||cpu>128||!Number.isInteger(ram)||ram<1||ram>1048576)throw new Error('资源需求超出允许范围');$('submitTask').disabled=true;const result=await api('/v0.3/tasks',{method:'POST',body:JSON.stringify({kind:kind,payload:payload,required_cpu:cpu,required_memory_mb:ram})});$('taskFeedback').textContent='任务已提交：'+result.id+' · 状态：'+result.status;toast('任务已加入队列');await loadCluster()}catch(err){if(err.stale)return;$('taskFeedback').textContent='提交失败：'+err.message;toast('提交失败：'+err.message)}finally{$('submitTask').disabled=false}});
$('gpuTaskForm').addEventListener('submit',async e=>{
  e.preventDefault();if(!requireKey())return;
  const size=Number($('gpuTaskSize').value),vram=Number($('gpuTaskVram').value);
  const estimated=64+Math.ceil(3*size*size*4/1048576);
  if(!Number.isInteger(size)||size<16||size>512){$('gpuTaskFeedback').textContent='矩阵边长必须为 16–512';return}
  if(!Number.isInteger(vram)||vram<Math.max(64,estimated)||vram>1048576){$('gpuTaskFeedback').textContent='预留显存不足，至少需要 '+Math.max(64,estimated)+' MB';return}
  $('submitGpuTask').disabled=true;
  try{
    const result=await api('/v0.7/gpu-tasks',{method:'POST',body:JSON.stringify({
      kind:'matmul_checksum',payload:{size:size},required_vram_mb:vram
    })});
    $('gpuTaskFeedback').textContent='GPU 任务已提交：'+result.id+' · 预留 '+bytesMb(vram);
    toast('GPU 任务已加入队列');await loadGpuTasks();
  }catch(error){
    if(!error.stale){$('gpuTaskFeedback').textContent='提交失败：'+error.message;toast('GPU 任务提交失败：'+error.message)}
  }finally{$('submitGpuTask').disabled=false}
});
$('inferenceForm').addEventListener('submit',async e=>{e.preventDefault();if(!requireKey()||!state.qwen?.enabled||!state.qwen?.running||state.inferenceBusy)return;const epoch=state.epoch;try{const client=$('clientId').value.trim(),raw=$('promptIds').value.split(',').map(x=>x.trim());if(!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$/.test(client))throw new Error('客户端标识须为 1–64 位安全 ASCII 字符');if(!raw.length||raw.length>128||raw.some(x=>!/^\d+$/.test(x)))throw new Error('请输入 1–128 个逗号分隔的 Token ID');const ids=raw.map(Number);if(ids.some(x=>!Number.isSafeInteger(x)||x<0||x>2147483647))throw new Error('Token ID 超出有效范围');state.inferenceBusy=true;$('submitInference').disabled=true;const result=await api('/v0.5/requests',{method:'POST',headers:{'X-Client-ID':client},timeoutMs:130000,body:JSON.stringify({prompt_ids:ids})});$('inferenceResult').textContent=JSON.stringify(result,null,2);$('inferenceFeedback').textContent='推理完成 · 批次大小 '+(result.batch_size||'—');await loadQwen()}catch(err){if(err.stale)return;$('inferenceFeedback').textContent='请求失败：'+err.message;$('inferenceResult').textContent=err.message}finally{if(epoch===state.epoch){state.inferenceBusy=false;$('submitInference').disabled=!state.qwen?.enabled||!state.qwen?.running}}});
$('refreshDevices').addEventListener('click',()=>{loadCluster();loadGpuInventory()});$('refreshTasks').addEventListener('click',()=>{loadCluster();loadGpuTasks()});$('refreshModels').addEventListener('click',loadModels);$('refreshInference').addEventListener('click',loadQwen);
function updateCommand(){
  const name=$('workerName').value.trim()||'my-cpu-worker',mode=$('workerShell').value;
  try{const url=new URL($('serverUrl').value.trim());if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.search||url.hash||url.pathname!=='/')throw new Error('请输入不含账户信息的完整服务源地址');
    const quote=value=>"'"+value.replaceAll("'",mode==='powershell'?"''":"'\"'\"'")+"'";
    $('workerCommand').textContent=(mode==='powershell'?"$env:OPENCOMPUTE_API_URL = ":'export OPENCOMPUTE_API_URL=')+quote(url.origin)+'\n'+(mode==='powershell'?"$env:OPENCOMPUTE_JOIN_KEY = ":'export OPENCOMPUTE_JOIN_KEY=')+quote('替换为服务端加入密钥')+'\npython -m cluster_v03.worker --name '+quote(name);
  }catch(e){$('workerCommand').textContent='地址无效：'+e.message}
}
$('serverUrl').value=location.origin;updateCommand();$('workerShell').addEventListener('change',updateCommand);$('serverUrl').addEventListener('input',updateCommand);$('workerName').addEventListener('input',updateCommand);$('copyWorker').addEventListener('click',async()=>{try{await navigator.clipboard.writeText($('workerCommand').textContent);toast('接入命令已复制')}catch(_){toast('浏览器不允许自动复制，请手动选择命令')}});
$('taskFilter').addEventListener('change',()=>{if(state.cluster)renderTasks(state.cluster.tasks||[])});
$('taskSearch').addEventListener('input',()=>{if(state.cluster)renderTasks(state.cluster.tasks||[])});
$('closeTaskDetail').addEventListener('click',()=>{state.detailId=null;$('taskDetailPanel').classList.add('hidden');$('taskDetail').textContent=''});
$('refreshSystem').addEventListener('click',()=>{if(requireKey())loadSystem()});
$('apiOrigin').textContent=location.origin;
window.addEventListener('pagehide',()=>{clearTimeout(state.timer);state.controllers.forEach(c=>c.abort())});
const saved=storage('read');if(saved){$('adminKey').value=saved;connect()}else connection(false,'等待连接');
})();
