/* OpenCompute AI public browser CPU verification client.
 * No admin key, local file access, background service, GPU admission or model inference.
 * The current public CPU pool verifies nodes only; it does not schedule user tasks.
 */
'use strict';
const API = '/v0.8/public';
const SIZE = 48;
let active = false;
let session = null;
let heartbeatTimer = null;
let resourceReport = null;
function report(phase, details = {}) {
  self.postMessage({type: 'status', phase, ...details});
}
function checksum(seed) {
  if (!/^[0-9a-f]{32}$/.test(seed)) throw Error('无效的校验任务');
  const a = parseInt(seed.slice(0, 2), 16) % 30 + 1;
  const b = parseInt(seed.slice(2, 4), 16);
  const c = parseInt(seed.slice(4, 6), 16) % 30 + 1;
  const d = parseInt(seed.slice(6, 8), 16);
  let result = 0;
  for (let k = 0; k < SIZE; k++) {
    let column = 0, row = 0;
    for (let i = 0; i < SIZE; i++) column += ((i * SIZE + k) * a + b) % 31;
    for (let j = 0; j < SIZE; j++) row += ((k * SIZE + j) * c + d) % 31;
    result += column * row;
  }
  return result;
}
async function post(path, body, token = '') {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(API + path, {
      method: 'POST', credentials: 'omit', cache: 'no-store',
      referrerPolicy: 'no-referrer', signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...(token ? {Authorization: 'Bearer ' + token} : {})
      }, body: JSON.stringify(body)
    });
    if (!response.ok) {
      const error = Error('请求失败（HTTP ' + response.status + '）');
      error.status = response.status;
      throw error;
    }
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}
async function leave() {
  const current = session;
  session = null;
  if (!current) return;
  try { await post('/leave', {node_id: current.node_id}, current.token); }
  catch (_) { /* Backend expires the session after the existing 90s TTL. */ }
}
function later(seconds) {
  clearTimeout(heartbeatTimer);
  if (active) heartbeatTimer = setTimeout(heartbeat, Math.max(1000, seconds * 1000));
}
async function join() {
  if (!active) return;
  report('checking', {message: '正在进行自动校验…'});
  const challenge = await post('/challenge', {resource: 'cpu'});
  if (!active) return;
  const joined = await post('/join', {
    challenge,
    resources: resourceReport,
    checksum: checksum(challenge.seed)
  });
  session = joined; // Token remains only inside this Worker.
  if (!active) { await leave(); return; }
  report('online', {nodeShort: joined.node_id.slice(0, 8), verifiedChecks: 1,
    message: '已加入 CPU 验证池；保持网页打开即可在线'});
  later(joined.heartbeat_seconds || 20);
}
async function heartbeat() {
  if (!active || !session) return;
  const current = session;
  try {
    const answer = await post('/heartbeat', {
      node_id: current.node_id, seed: current.seed,
      checksum: checksum(current.seed)
    }, current.token);
    if (!active || session !== current) return;
    current.seed = answer.seed;
    report('online', {nodeShort: current.node_id.slice(0, 8),
      message: '节点在线 · 最近一次校验成功'});
    later(current.heartbeat_seconds || 20);
  } catch (error) {
    if (!active) return;
    if (error.status === 401 || error.status === 409) {
      session = null;
      report('reconnecting', {message: '会话已过期，正在重新接入…'});
      try { await join(); } catch (failure) {
        report('reconnecting', {message: '网络连接中断，稍后重新尝试：' + failure.message});
        later(10);
      }
    } else {
      report('reconnecting', {message: '心跳暂时失败，正在重试…'});
      later(error.status === 429 ? 12 : 10);
    }
  }
}
async function stop() {
  if (!active && !session) return;
  active = false;
  clearTimeout(heartbeatTimer);
  await leave();
  report('stopped', {message: '已退出；无需手动清理密钥'});
  self.close();
}
self.onmessage = async event => {
  const message = event.data || {};
  if (message.type === 'stop') { await stop(); return; }
  if (message.type !== 'start' || active) return;
  const {cores, ram_mb: ramMb, vram_mb: vramMb} = message.resources || {};
  if (!Number.isInteger(cores) || cores < 2 || cores > 1024 ||
      !Number.isInteger(ramMb) || ramMb < 2048 || ramMb > 4194304 ||
      vramMb !== 0) {
    report('error', {message: '浏览器资源不符合自动接入要求'});
    return;
  }
  resourceReport = {cores, ram_mb: ramMb, vram_mb: 0};
  active = true;
  try { await join(); }
  catch (error) {
    if (active) {
      active = false;
      report('error', {message: '接入失败：' + error.message});
    }
  }
};
