/* Browser-only, opt-in one-click CPU verification. Never start on page load. */
(() => {
  'use strict';
  const joinButtons = [...document.querySelectorAll('[data-browser-join]')];
  const leaveButtons = [...document.querySelectorAll('[data-browser-leave]')];
  const statuses = [...document.querySelectorAll('[data-browser-status]')];
  if (!joinButtons.length) return;
  let worker = null;
  let phase = 'idle';
  const update = (next, message) => {
    phase = next;
    for (const element of statuses) {
      element.textContent = message;
      element.setAttribute('data-phase', next);
    }
    for (const button of joinButtons) button.disabled = !['idle','stopped','error'].includes(next);
    for (const button of leaveButtons) button.disabled = !['online','checking','reconnecting'].includes(next);
  };
  const stop = () => {
    if (!worker) return;
    const stopping = worker;
    worker = null;
    stopping.postMessage({type:'stop'});
    // The Worker attempts a token-authenticated leave; expiry provides fallback.
    setTimeout(() => stopping.terminate(), 3000);
    update('stopped', '已停止。服务端将立即撤销会话，网络中断时最多约 90 秒显示离线。');
  };
  const start = () => {
    if (worker || !['idle','stopped','error'].includes(phase)) return;
    if (!window.isSecureContext && !['localhost','127.0.0.1'].includes(location.hostname)) {
      update('error', '请通过 HTTPS 网站访问算力池。');
      return;
    }
    if (typeof Worker === 'undefined') {
      update('error', '此浏览器不支持 Web Worker，无法使用网页模式。');
      return;
    }
    const cores = navigator.hardwareConcurrency;
    const ramGiB = navigator.deviceMemory;
    if (!Number.isInteger(cores) || cores < 2) {
      update('error', '浏览器未报告至少 2 个可用逻辑处理器。');
      return;
    }
    if (!Number.isFinite(ramGiB)) {
      update('error', '浏览器未开放内存估计，无法按现有 2 GB 门槛自动验证；请使用桌面 Worker。');
      return;
    }
    if (ramGiB < 2) {
      update('error', '浏览器报告的估计内存不足 2 GB，暂时不符合接入门槛。');
      return;
    }
    try {
      const current = new Worker('/assets/browser-worker.js');
      worker = current;
      current.onmessage = event => {
        if (worker !== current || event.data?.type !== 'status') return;
        const {phase: next, message} = event.data;
        if (next === 'error') {
          worker = null;
          current.terminate();
        }
        update(next, message);
      };
      current.onerror = () => {
        if (worker !== current) return;
        worker = null;
        current.terminate();
        update('error', '浏览器工作线程启动失败，请重试或使用桌面 Worker。');
      };
      update('checking', '正在检测资源并完成自动注册…');
      current.postMessage({
        type:'start',
        resources:{
          cores: Math.min(cores, 1024),
          ram_mb: Math.min(Math.floor(ramGiB * 1024), 4194304),
          vram_mb: 0
        }
      });
    } catch (_) {
      update('error', '浏览器不允许启动计算线程。');
    }
  };
  for (const button of joinButtons) button.addEventListener('click', start);
  for (const button of leaveButtons) button.addEventListener('click', stop);
  window.addEventListener('pagehide', stop);
  update('idle', '尚未加入。仅用于 CPU 接入校验，暂不领取其他用户的计算任务。');
})();
