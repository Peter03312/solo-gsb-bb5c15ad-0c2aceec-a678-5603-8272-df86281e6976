'use strict';

/**
 * 站场状态变化审计流水 HTTP 端到端测试：
 *   临时库启动真实服务 → 走完开放/走行/解除、等待兑现、延时取消、封锁、
 *   暂停/恢复、检查点回退、站场重置、命名副本 → 逐项验证审计流水：
 *     1) 每条含发生时刻、操作者/系统来源、关联编号、变更类型与前后状态摘要；
 *     2) 业务变更与流水同成同败（失败操作/幂等重传/无变化扫描不产生条目）；
 *     3) 按当前站场序号分页、按类型及关联编号筛选，非法筛选 400 拒绝；
 *     4) 回退/重置保留此前流水并写明界标；
 *     5) 副本创建后审计表为空，只记录自身新变化，跨站场不串看。
 *
 * 运行：node test/audit-http.cjs
 */
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PORT = process.env.TEST_PORT || 3929;
const DB_PATH = process.env.SMOKE_DB_AUDIT || path.join(__dirname, '..', 'data', 'audit-test.db');
const BASE = `http://127.0.0.1:${PORT}`;

let passed = 0;
function ok(name, cond, extra) {
  if (!cond) throw new Error(name + (extra ? `（${JSON.stringify(extra)}）` : ''));
  console.log('  ✓', name);
  passed++;
}
function eq(name, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${name}: 期望 ${JSON.stringify(expected)}，实际 ${JSON.stringify(actual)}`);
  }
  console.log('  ✓', name);
  passed++;
}

let server = null;
function startServer(keepDb = false) {
  return new Promise((resolve, reject) => {
    if (!keepDb) {
      for (const ext of ['', '-wal', '-shm']) {
        try { fs.unlinkSync(DB_PATH + ext); } catch (_) {}
      }
      fs.rmSync(path.join(path.dirname(DB_PATH),
        path.basename(DB_PATH, path.extname(DB_PATH)) + '.yards'),
        { recursive: true, force: true });
    }
    server = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'server.js')], {
      env: { ...process.env, PORT: String(PORT), DB_PATH, SEED_DEMO: '1' },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    let out = '';
    const timer = setTimeout(() => reject(new Error('服务启动超时: ' + out)), 10000);
    server.stdout.on('data', (d) => {
      out += d.toString();
      if (out.includes('已启动')) { clearTimeout(timer); resolve(); }
    });
    server.stderr.on('data', (d) => { out += d.toString(); });
    server.on('exit', (code) => { if (code !== 0) reject(new Error('服务异常退出: ' + out)); });
  });
}
function stopServer() {
  return new Promise((resolve) => {
    if (!server) return resolve();
    server.on('exit', () => { server = null; resolve(); });
    server.kill('SIGTERM');
    setTimeout(() => { try { server.kill('SIGKILL'); } catch (_) {} }, 3000);
  });
}

async function api(pathName, { token, method = 'GET', body } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = 'Bearer ' + token;
  const resp = await fetch(BASE + pathName, {
    method, headers, body: body ? JSON.stringify(body) : undefined
  });
  let data = {};
  try { data = await resp.json(); } catch (_) {}
  return { status: resp.status, data };
}
async function login(role, nameOrPwd) {
  const body = role === 'admin' ? { password: nameOrPwd || 'admin123' } : { name: nameOrPwd };
  const { status, data } = await api(`/api/auth/login/${role}`, { method: 'POST', body });
  if (status !== 200) throw new Error('登录失败: ' + data.error);
  return data.token;
}
async function logs(token, query = '', asAdmin = false) {
  const prefix = asAdmin ? '/api/admin/audit-logs' : '/api/audit-logs';
  const r = await api(`${prefix}${query}`, { token });
  return r;
}
async function selectYard(token, yardId) {
  await api('/api/auth/yard', { token, method: 'PUT', body: { yardId } });
}
async function openRoute(token, routeId = 'R1') {
  const r = await api(`/api/routes/${routeId}/open`, {
    token, method: 'POST', body: { switchPositions: { SW7: 'N', SW8: 'N' } }
  });
  if (r.status !== 200) throw new Error('开放失败: ' + JSON.stringify(r.data));
  return r.data.instance.id;
}
async function event(token, inst, seq, sectionId, action) {
  return api(`/api/instances/${inst}/events`, {
    token, method: 'POST', body: { seq, sectionId, action }
  });
}
async function walkR1(token, inst) {
  // R1 区段顺序 1,3,5
  const secs = ['1', '3', '5'];
  let seq = 1;
  for (let i = 0; i < secs.length; i++) {
    await event(token, inst, seq++, secs[i], 'occupy');
    if (i >= 1) await event(token, inst, seq++, secs[i - 1], 'clear');
  }
  await event(token, inst, seq++, secs[secs.length - 1], 'clear');
}
async function waitForLog(token, predicate, { timeout = 6000, asAdmin = true } = {}) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const r = await logs(token, '?pageSize=200', asAdmin);
    const found = r.data.entries.find(predicate);
    if (found) return found;
    if (Date.now() > deadline) throw new Error('等待审计条目超时');
    await new Promise((res) => setTimeout(res, 200));
  }
}

async function main() {
  console.log('[审计流水] 启动服务…');
  await startServer();

  try {
    const admin = await login('admin');
    const dispA = await login('dispatcher', '审计值班员甲');

    console.log('[1] 开放→走行→正常解除，逐条记录且字段齐全');
    const total0 = (await logs(admin,"",true)).data.total;
    eq('初始流水为 0', total0, 0);
    const inst = await openRoute(dispA);
    await walkR1(dispA, inst);

    const r1 = await logs(admin,'?pageSize=200',true);
    const types = r1.data.entries.map((e) => e.type);
    eq('流水条数=开放1+占用3+清空3+解除1=8', r1.data.entries.length, 8);
    eq('第1条为 route_open', types[0], 'route_open');
    eq('末条为 route_release', types[types.length - 1], 'route_release');
    eq('走行事件数=6', types.filter((t) => t === 'movement_event').length, 6);

    const openE = r1.data.entries[0];
    ok('route_open 序号1', openE.seq === 1);
    ok('route_open 关联实例', openE.refType === 'instance' && openE.refId === String(inst));
    eq('route_open 操作者', openE.actor, '审计值班员甲');
    eq('route_open 人工', openE.actorKind, 'human');
    eq('route_open 来源 manual', openE.source, 'manual');
    ok('route_open 有发生时刻', typeof openE.occurredAt === 'number' && openE.occurredAt > 0);
    ok('route_open 有前后状态', openE.before && openE.after && openE.after.status === 'open'
      && openE.after.signalState === 'green');
    ok('route_open after 含锁闭区段', Array.isArray(openE.after.lockedSections));

    const occupy1 = r1.data.entries.find((e) => e.type === 'movement_event'
      && e.detail.action === 'occupy' && e.detail.sectionId === '1');
    ok('首段占用后信号回红在 after', occupy1.after.signalState === 'red');
    eq('首段占用 signalTurnedRed', occupy1.detail.signalTurnedRed, true);
    const clearMid = r1.data.entries.find((e) => e.type === 'movement_event'
      && e.detail.action === 'clear' && e.detail.sectionId === '1');
    ok('中间清空 after 区段空闲', clearMid.after.sectionState === 'free');
    const releaseE = r1.data.entries[r1.data.entries.length - 1];
    eq('正常解除原因 walkthrough', releaseE.detail.reason, 'walkthrough');
    eq('解除后状态 released', releaseE.after.status, 'released');

    console.log('[2] 失败操作 / 幂等重传 / 无变化扫描不产生条目');
    const inst2 = await openRoute(dispA);
    const beforeDup = (await logs(admin,'?pageSize=200',true)).data.total;
    // 幂等重传：占用首段后重发同序号同内容
    await event(dispA, inst2, 1, '1', 'occupy');
    const idem = await event(dispA, inst2, 1, '1', 'occupy');
    eq('幂等重传 HTTP 200', idem.status, 200);
    // 失败：跳号
    const gap = await event(dispA, inst2, 99, '1', 'clear');
    eq('跳号 HTTP 409', gap.status, 409);
    // 失败：开放被锁资源（再开 R1 必失败，区段1被占用）
    const blocked = await api('/api/routes/R1/open', {
      token: dispA, method: 'POST', body: { switchPositions: { SW7: 'N', SW8: 'N' } }
    });
    ok('冲突开放被拒', blocked.status === 409, blocked.status);
    // 无变化的只读/扫描：等待全员查询、封锁列表
    await api('/api/waiting?scope=all', { token: dispA });
    await api('/api/blockades', { token: dispA });
    await new Promise((res) => setTimeout(res, 1200)); // 等系统扫描周期
    const afterDup = (await logs(admin,'?pageSize=200',true)).data.total;
    // 基线在 inst2 开放之后：仅多出首段占用 1 条（幂等重传/跳号/冲突开放/扫描均不产生）
    eq('幂等/失败/扫描不新增流水', afterDup - beforeDup, 1);

    console.log('[3] 延时取消：申请→到期解除（系统来源）');
    // 先把 inst2 走完释放，再开一条干净实例做延时取消
    // inst2 当前：已占用区段1。直接继续走完即可
    await event(dispA, inst2, 2, '1', 'clear');
    await event(dispA, inst2, 3, '3', 'occupy');
    await event(dispA, inst2, 4, '3', 'clear');
    await event(dispA, inst2, 5, '5', 'occupy');
    await event(dispA, inst2, 6, '5', 'clear');
    await api('/api/admin/settings/cancel-protection', {
      token: admin, method: 'PUT', body: { protectionSeconds: 1 }
    });
    const inst3 = await openRoute(dispA);
    const cancelResp = await api(`/api/instances/${inst3}/cancel-delayed`, {
      token: dispA, method: 'POST'
    });
    eq('取消申请 201', cancelResp.status, 201);
    const cReq = await waitForLog(admin,
      (e) => e.type === 'cancellation_released' && e.instanceId === inst3);
    eq('到期解除来源为系统定时', cReq.actorKind, 'system');
    ok('到期解除 source 含 timer', cReq.source.includes('timer'));
    const cCancelRequested = (await logs(admin,'?pageSize=200',true)).data.entries.find(
      (e) => e.type === 'cancellation_requested' && e.instanceId === inst3);
    ok('取消申请人工记录存在', cCancelRequested && cCancelRequested.actorKind === 'human');
    const cRelease = (await logs(admin,'?pageSize=200',true)).data.entries.find(
      (e) => e.type === 'route_release' && e.instanceId === inst3);
    eq('延时解除原因 delayed_cancellation', cRelease.detail.reason, 'delayed_cancellation');

    console.log('[4] 封锁：发起(待生效)→转已生效→结束');
    // 重置让区段空闲，封锁空闲区段直接已生效
    await api('/api/admin/reset/training', { token: admin, method: 'POST' });
    const bResp = await api('/api/admin/blockades', {
      token: admin, method: 'POST', body: { sectionId: '3', reason: '审计施工' }
    });
    eq('封锁 201', bResp.status, 201);
    const bid = bResp.data.blockade.id;
    const bCreated = await waitForLog(admin,
      (e) => e.type === 'blockade_created' && e.refId === String(bid));
    const bActive = await waitForLog(admin,
      (e) => e.type === 'blockade_activated' && e.refId === String(bid));
    eq('封锁转已生效系统来源', bActive.actorKind, 'system');
    ok('封锁创建人工', bCreated.actorKind === 'human');
    await api(`/api/admin/blockades/${bid}/end`, { token: admin, method: 'POST' });
    const bEnded = await waitForLog(admin,
      (e) => e.type === 'blockade_ended' && e.refId === String(bid));
    eq('封锁结束状态', bEnded.after.status, 'ended');

    console.log('[5] 等待申请兑现（系统兑现，含 route_open 系统来源）');
    await api('/api/admin/reset/training', { token: admin, method: 'POST' });
    await api('/api/sections/1/state', {
      token: dispA, method: 'POST', body: { action: 'occupy' }
    });
    const wResp = await api('/api/routes/R1/waiting', {
      token: dispA, method: 'POST', body: { switchPositions: { SW7: 'N', SW8: 'N' } }
    });
    eq('等待提交 201', wResp.status, 201);
    const wid = wResp.data.waiting.id;
    eq('提交后为 waiting', wResp.data.waiting.status, 'waiting');
    await api('/api/sections/1/state', {
      token: dispA, method: 'POST', body: { action: 'clear' }
    });
    const wf = await waitForLog(admin,
      (e) => e.type === 'waiting_fulfilled' && e.refId === String(wid));
    eq('等待兑现系统动作', wf.actorKind, 'system');
    eq('等待兑现来源 waiting', wf.source, 'waiting');
    ok('等待兑现 after 有实例号', wf.after.instanceId > 0);
    const wOpen = (await logs(admin,'?pageSize=200',true)).data.entries.find(
      (e) => e.type === 'route_open' && e.instanceId === wf.after.instanceId);
    ok('兑现开放记录来源 waiting', wOpen && wOpen.source === 'waiting');

    console.log('[6] 站场重置：保留此前流水并写界标');
    const beforeReset = (await logs(admin,"",true)).data.total;
    await api('/api/admin/reset/state', { token: admin, method: 'POST' });
    const resetE = await waitForLog(admin, (e) => e.type === 'yard_reset'
      && e.after && e.after.mode === 'reset_state');
    ok('重置界标 before 有计数', resetE.before && typeof resetE.before.openInstances === 'number');
    eq('重置后区段未知', resetE.after.sectionState, 'unknown');
    const afterReset = (await logs(admin,"",true)).data.total;
    eq('重置只新增 1 条界标', afterReset - beforeReset, 1);
    // 此前流水仍在
    const firstStill = (await logs(admin,'?pageSize=1',true)).data.entries[0];
    eq('最早流水仍为 seq 1', firstStill.seq, 1);

    console.log('[7] 分页：序号升序、page/pageSize、边界非法');
    const pg = await logs(admin,'?page=1&pageSize=5',true);
    eq('分页第1页5条', pg.data.entries.length, 5);
    eq('第1页序号1..5', pg.data.entries.map((e) => e.seq).join(','), '1,2,3,4,5');
    eq('hasPrev false', pg.data.hasPrev, false);
    ok('hasNext true', pg.data.hasNext);
    const pg2 = await logs(admin,'?page=2&pageSize=5',true);
    eq('第2页序号从6开始', pg2.data.entries[0].seq, 6);
    for (const [q, code] of [
      ['?page=0', 400], ['?page=-1', 400], ['?pageSize=0', 400],
      ['?pageSize=99999', 400], ['?page=abc', 400]
    ]) {
      const rr = await logs(admin, q, true);
      eq(`非法分页 ${q} -> ${code}`, rr.status, code);
    }

    console.log('[8] 筛选：类型 / 关联编号；非法筛选明确拒绝');
    const onlyOpen = await logs(admin,'?type=route_open',true);
    ok('type 筛选全为 route_open', onlyOpen.data.entries.every((e) => e.type === 'route_open'));
    const multi = await logs(admin,'?type=route_open,route_release',true);
    ok('多类型筛选', multi.data.entries.every(
      (e) => e.type === 'route_open' || e.type === 'route_release'));
    const byRef = await logs(admin,'?refType=blockade&refId=1',true);
    ok('refType+refId 筛选', byRef.data.entries.length > 0
      && byRef.data.entries.every((e) => e.refType === 'blockade' && e.refId === '1'));
    for (const [q, code] of [
      ['?type=not_a_type', 400],
      ['?type=route_open,hacked', 400],
      ['?refType=ufo&refId=1', 400],
      ['?refType=instance', 400],
      ['?refId=1', 400]
    ]) {
      const rr = await logs(admin, q, true);
      eq(`非法筛选 ${q} -> ${code}`, rr.status, code);
      eq(`非法筛选 ${q} 错误码`, rr.data.code, 'AUDIT_FILTER_INVALID');
    }

    console.log('[9] 权限：值班员可查；未登录 401；管理员/值班员同口径');
    const dispView = await logs(dispA, '', false);
    eq('值班员可查 200', dispView.status, 200);
    const noAuth = await api('/api/audit-logs');
    eq('未登录 401', noAuth.status, 401);
    const noAuthAdmin = await api('/api/admin/audit-logs');
    eq('管理接口未登录 401', noAuthAdmin.status, 401);

    console.log('[10] 暂停/恢复 + 检查点回退：界标与流水保留');
    // 创建副本并切到副本
    const yResp = await api('/api/admin/yards', {
      token: admin, method: 'POST', body: { name: '审计副本' }
    });
    eq('创建副本 201', yResp.status, 201);
    const yid = yResp.data.yard.id;
    await selectYard(admin, yid);
    // 副本审计表为空（旧数据不补造）
    const copyEmpty = await logs(admin,"",true);
    eq('副本创建后审计为空', copyEmpty.data.total, 0);

    await api('/api/admin/yards/current/pause', { token: admin, method: 'POST' });
    const cpResp = await api('/api/admin/yards/current/checkpoints', {
      token: admin, method: 'POST', body: { name: '审计基线' }
    });
    const cpid = cpResp.data.checkpoint.id;
    await api(`/api/admin/yards/current/checkpoints/${cpid}/rollback`, {
      token: admin, method: 'POST'
    });
    await api('/api/admin/yards/current/resume', { token: admin, method: 'POST' });

    const copyLogs = await logs(admin,'?pageSize=200',true);
    const copyTypes = copyLogs.data.entries.map((e) => e.type);
    eq('副本有 暂停/回退/恢复 三条', copyTypes.join(','),
      'yard_paused,checkpoint_rollback,yard_resumed');
    const rb = copyLogs.data.entries.find((e) => e.type === 'checkpoint_rollback');
    eq('回退界标关联检查点', rb.refType, 'checkpoint');
    eq('回退界标 refId', rb.refId, String(cpid));
    ok('回退界标注明检查点名称', rb.note.includes('审计基线'));
    ok('回退 detail 有时间平移', typeof rb.detail.timeShiftMs === 'number');

    console.log('[11] 站场隔离：不能串看其他站场，序号各自递增');
    // 副本序号从 1 开始
    eq('副本首条 seq=1', copyLogs.data.entries[0].seq, 1);
    // 切回原站场，看到的是原站场数据，不含副本三条
    await selectYard(admin, 1);
    const origLogs = await logs(admin,'?pageSize=200',true);
    ok('原站场不含 yard_paused（副本的）',
      !origLogs.data.entries.some((e) => e.type === 'yard_paused'));
    ok('原站场仍有自己的 yard_reset',
      origLogs.data.entries.some((e) => e.type === 'yard_reset'));
    // 再切回副本，仍看到副本三条
    await selectYard(admin, yid);
    const copyAgain = await logs(admin,"",true);
    eq('再次切回副本仍 3 条', copyAgain.data.total, 3);

    console.log('[12] 暂停期间只读审计仍可查询');
    await api('/api/admin/yards/current/pause', { token: admin, method: 'POST' });
    const pausedView = await logs(admin,"",true);
    eq('暂停副本可查审计 200', pausedView.status, 200);
    await api('/api/admin/yards/current/resume', { token: admin, method: 'POST' });

    console.log(`\n审计流水全部 ${passed} 项断言通过 ✅`);
  } finally {
    await stopServer();
  }
}

main().catch((e) => {
  console.error('测试失败:', e);
  process.exit(1);
});
