'use strict';

/**
 * 站场状态变化审计流水。
 *
 * 每个站场业务库各持一张 yard_audit_log（见 schema-yard.sql），序号 id 在
 * 本站场内严格递增。审计原则：
 *  - 只记录真正改变当前站场推演状态的变化（见 TYPES）；
 *  - 业务变更与流水【同成同败】：recordInTxn 必须在调用方业务事务内调用，
 *    业务事务回滚则流水一并回滚；不会出现"业务成功但无流水"或反之；
 *  - 失败操作（抛错回滚）天然不产生流水；幂等重传、未改变任何状态的扫描
 *    （等待兑现扫描、封锁复核、到期扫描）由调用点在状态确有变化时才调用本模块，
 *    因此不产生新条目；
 *  - 回退（检查点）与重置保留此前全部流水，并以明确的界标类型
 *    （checkpoint_rollback / yard_reset）另写一条；
 *  - 命名副本创建时本表为空（复制白名单不含本表），之后只记录本副本自身的
 *    新变化，原站场旧数据不补造历史；
 *  - 审计流水表不参与检查点快照替换（与交接单/检查点同属"过程记录"）。
 *
 * 查询（listEntries）只作用于当前站场业务库，天然不能串看其他站场；
 * type/refType 必须命中白名单，非法筛选明确拒绝（400）。
 */

const { getDb } = require('./db');
const { InterlockError } = require('./interlocking');

/* ------------------------------ 变更类型定义 ------------------------------ */

/**
 * 细粒度变更类型 → 大类。大类仅用于返回展示，筛选只接受细粒度 type。
 */
const TYPE_CATEGORY = {
  // 进路开放与解除
  route_open: 'route',
  route_release: 'route',
  // 走行事件（占用 / 清空）
  movement_event: 'movement',
  // 等待申请
  waiting_fulfilled: 'waiting',
  // 延时取消
  cancellation_requested: 'cancellation',
  cancellation_released: 'cancellation',
  cancellation_aborted: 'cancellation',
  // 临时封锁
  blockade_created: 'blockade',
  blockade_activated: 'blockade',
  blockade_ended: 'blockade',
  // 副本暂停 / 恢复（流水落在被操作副本的业务库）
  yard_paused: 'yard',
  yard_resumed: 'yard',
  // 检查点回退界标
  checkpoint_rollback: 'checkpoint',
  // 站场重置界标
  yard_reset: 'reset'
};

const TYPES = Object.keys(TYPE_CATEGORY);
const TYPES_SET = new Set(TYPES);
const CATEGORIES = [...new Set(Object.values(TYPE_CATEGORY))];

/** 关联对象类别白名单（refType 筛选时必须命中其一；'none' 表示无具体关联） */
const REF_TYPES = [
  'instance',   // 进路实例
  'route',      // 进路定义
  'waiting',    // 等待申请
  'cancellation', // 延时取消申请
  'blockade',   // 封锁申请
  'yard',       // 站场（暂停/恢复/重置）
  'checkpoint', // 检查点
  'none'
];
const REF_TYPES_SET = new Set(REF_TYPES);

/* ------------------------------ 写入 ------------------------------ */

const SYSTEM_ACTOR = '系统';

/**
 * 规范化一条待写入的流水。供 recordInTxn / record 共用。
 * @param {Object} e
 * @param {string} e.type 细粒度变更类型（必须命中 TYPES）
 * @param {string} [e.actor] 操作者姓名；系统动作可省略（记为"系统"）
 * @param {string} [e.actorKind] 'human' | 'system'
 * @param {string} [e.source] 系统来源标记；人工为 'manual'
 * @param {string} [e.refType] 关联对象类别（默认按类型推断）
 * @param {string|number} [e.refId] 关联编号（默认 '-'）
 * @param {string} [e.routeId] 进路编号
 * @param {number} [e.instanceId] 进路实例编号
 * @param {Object} [e.before] 变更前状态摘要（可 JSON 序列化）
 * @param {Object} [e.after] 变更后状态摘要
 * @param {Object} [e.detail] 结构化补充信息
 * @param {string} [e.note] 文字说明（界标原因等）
 */
function normalizeEntry(e) {
  const type = e && e.type;
  if (!TYPES_SET.has(type)) {
    throw new Error(`非法审计类型: ${type}（不在白名单内）`);
  }
  const actorKind = e.actorKind === 'human' ? 'human' : 'system';
  const actor = e.actor || SYSTEM_ACTOR;
  const source = e.source || (actorKind === 'system' ? 'system' : 'manual');

  const refType = e.refType || DEFAULT_REF_TYPE[type] || 'none';
  const refId = e.refId == null ? '-' : String(e.refId);

  return {
    occurredAt: Number.isInteger(e.occurredAt) ? e.occurredAt : Date.now(),
    type,
    category: TYPE_CATEGORY[type],
    actor,
    actorKind,
    source,
    refType,
    refId,
    routeId: e.routeId == null ? null : String(e.routeId),
    instanceId: e.instanceId == null ? null : Number(e.instanceId),
    before: e.before == null ? null : JSON.stringify(e.before),
    after: e.after == null ? null : JSON.stringify(e.after),
    detail: e.detail == null ? null : JSON.stringify(e.detail),
    note: e.note == null ? null : String(e.note)
  };
}

const DEFAULT_REF_TYPE = {
  route_open: 'instance',
  route_release: 'instance',
  movement_event: 'instance',
  waiting_fulfilled: 'waiting',
  cancellation_requested: 'cancellation',
  cancellation_released: 'cancellation',
  cancellation_aborted: 'cancellation',
  blockade_created: 'blockade',
  blockade_activated: 'blockade',
  blockade_ended: 'blockade',
  yard_paused: 'yard',
  yard_resumed: 'yard',
  checkpoint_rollback: 'checkpoint',
  yard_reset: 'yard'
};

const insertStmt = `
  INSERT INTO yard_audit_log
    (occurred_at,type,category,actor,actor_kind,source,ref_type,ref_id,
     route_id,instance_id,before_state,after_state,detail,note)
  VALUES
    (@occurredAt,@type,@category,@actor,@actorKind,@source,@refType,@refId,
     @routeId,@instanceId,@before,@after,@detail,@note)`;

/**
 * 在调用方【当前事务】内写一条审计流水。必须从 db.transaction 体内调用，
 * 以保证业务变更与流水同成同败。返回新流水序号。
 */
function recordInTxn(entry) {
  const db = getDb();
  const row = normalizeEntry(entry);
  const info = db.prepare(insertStmt).run(row);
  return Number(info.lastInsertRowid);
}

/**
 * 用独立事务写一条审计流水。仅用于流水本身就是一次完整业务动作
 * （例如暂停/恢复，其业务变更在主库注册表，流水落在副本业务库）的场景。
 */
function record(entry) {
  return getDb().transaction(() => recordInTxn(entry))();
}

/* ------------------------------ 站场重置摘要 ------------------------------ */

/**
 * 站场重置前统计受影响对象数量（在重置事务内、删除/改写之前调用），
 * 作为 yard_reset 界标的 before 摘要。
 */
function resetCountersInTxn() {
  const db = getDb();
  const count = (sql) => db.prepare(sql).get().n;
  return {
    openInstances: count(`SELECT COUNT(*) AS n FROM route_instances WHERE status='open'`),
    totalInstances: count(`SELECT COUNT(*) AS n FROM route_instances`),
    sections: count(`SELECT COUNT(*) AS n FROM sections`),
    occupiedOrUnknownSections: count(
      `SELECT COUNT(*) AS n FROM sections WHERE occ_state IN ('occupied','unknown')`),
    waiting: count(`SELECT COUNT(*) AS n FROM waiting_applications WHERE status='waiting'`),
    pendingCancellations: count(
      `SELECT COUNT(*) AS n FROM cancellation_applications WHERE status='pending'`),
    unfinishedBlockades: count(
      `SELECT COUNT(*) AS n FROM blockade_applications WHERE status IN ('pending','active')`),
    activeMovementPlans: count(`SELECT COUNT(*) AS n FROM movement_plans WHERE status='active'`),
    activeHandovers: count(`SELECT COUNT(*) AS n FROM handover_documents WHERE status='active'`),
    activeExams: count(`SELECT COUNT(*) AS n FROM exams WHERE status='in_progress'`)
  };
}

/* ------------------------------ 查询 ------------------------------ */

const MAX_PAGE_SIZE = 200;
const DEFAULT_PAGE_SIZE = 50;

function parseJsonOrRaw(v) {
  if (v == null) return null;
  try { return JSON.parse(v); } catch (_) { return v; }
}

function rowToEntry(row) {
  return {
    seq: row.id,                     // 站场内递增流水序号
    occurredAt: row.occurred_at,
    occurredAtText: new Date(Number(row.occurred_at))
      .toLocaleString('zh-CN', { hour12: false }),
    type: row.type,
    category: row.category,
    actor: row.actor,
    actorKind: row.actor_kind,       // human / system
    source: row.source,
    refType: row.ref_type,
    refId: row.ref_id,
    routeId: row.route_id,
    instanceId: row.instance_id,
    before: parseJsonOrRaw(row.before_state),
    after: parseJsonOrRaw(row.after_state),
    detail: parseJsonOrRaw(row.detail),
    note: row.note
  };
}

/**
 * 校验筛选参数。非法 type/refType 或分页参数明确拒绝（抛 400）。
 */
function validateFilter(filter = {}) {
  const out = {};

  if (filter.type !== undefined && filter.type !== null && filter.type !== '') {
    const values = String(filter.type).split(',').map((s) => s.trim()).filter(Boolean);
    for (const v of values) {
      if (!TYPES_SET.has(v)) {
        throw new InterlockError(400, 'AUDIT_FILTER_INVALID',
          `非法的变更类型筛选：${v}；允许的取值为 ${TYPES.join('、')}`);
      }
    }
    if (new Set(values).size !== values.length) {
      throw new InterlockError(400, 'AUDIT_FILTER_INVALID',
        '变更类型筛选存在重复取值');
    }
    out.types = values;
  }

  if (filter.refType !== undefined && filter.refType !== null && filter.refType !== '') {
    const rt = String(filter.refType).trim();
    if (!REF_TYPES_SET.has(rt)) {
      throw new InterlockError(400, 'AUDIT_FILTER_INVALID',
        `非法的关联对象类别筛选：${rt}；允许的取值为 ${REF_TYPES.join('、')}`);
    }
    out.refType = rt;
    if (filter.refId === undefined || filter.refId === null || String(filter.refId).trim() === '') {
      throw new InterlockError(400, 'AUDIT_FILTER_INVALID',
        '按关联对象类别 refType 筛选时必须同时提供关联编号 refId');
    }
    out.refId = String(filter.refId).trim();
  } else if (filter.refId !== undefined && filter.refId !== null && String(filter.refId).trim() !== '') {
    throw new InterlockError(400, 'AUDIT_FILTER_INVALID',
      '按关联编号 refId 筛选时必须同时提供关联对象类别 refType');
  }

  let page = filter.page == null ? 1 : Number(filter.page);
  if (!Number.isInteger(page) || page < 1) {
    throw new InterlockError(400, 'AUDIT_PAGE_INVALID',
      `分页 page 必须是从 1 开始的正整数（收到 ${filter.page}）`);
  }
  let pageSize = filter.pageSize == null ? DEFAULT_PAGE_SIZE : Number(filter.pageSize);
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
    throw new InterlockError(400, 'AUDIT_PAGE_INVALID',
      `每页条数 pageSize 必须是 1~${MAX_PAGE_SIZE} 之间的整数（收到 ${filter.pageSize}）`);
  }
  out.page = page;
  out.pageSize = pageSize;
  return out;
}

/**
 * 按当前站场查询审计流水：序号（id）升序分页，可按细粒度 type（逗号分隔多个）
 * 与关联对象（refType + refId）筛选。只读当前站场业务库，绝不串看其他站场。
 */
function listEntries(filter = {}) {
  const f = validateFilter(filter);
  const db = getDb();

  const where = [];
  const params = {};
  if (f.types) {
    const marks = f.types.map((_, i) => `@t${i}`).join(',');
    f.types.forEach((t, i) => { params[`t${i}`] = t; });
    where.push(`type IN (${marks})`);
  }
  if (f.refType) {
    where.push(`ref_type=@refType AND ref_id=@refId`);
    params.refType = f.refType;
    params.refId = f.refId;
  }
  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

  const total = db.prepare(`SELECT COUNT(*) AS n FROM yard_audit_log ${whereSql}`).get(params).n;

  // 序号升序（按发生先后）；分页 LIMIT/OFFSET
  const rows = db.prepare(
    `SELECT * FROM yard_audit_log ${whereSql} ORDER BY id ASC
      LIMIT @limit OFFSET @offset`
  ).all({ ...params, limit: f.pageSize, offset: (f.page - 1) * f.pageSize });

  const totalPages = Math.max(1, Math.ceil(total / f.pageSize));
  return {
    entries: rows.map(rowToEntry),
    page: f.page,
    pageSize: f.pageSize,
    total,
    totalPages,
    hasPrev: f.page > 1,
    hasNext: f.page < totalPages
  };
}

module.exports = {
  TYPES,
  TYPES_SET,
  CATEGORIES,
  TYPE_CATEGORY,
  REF_TYPES,
  SYSTEM_ACTOR,
  recordInTxn,
  record,
  resetCountersInTxn,
  listEntries,
  validateFilter
};
