/* 离线车队回执中继 —— 控制台（原生 JS） */
"use strict";

const KIND_LABEL = {departed: "已离场", not_departed: "未能出发", arrived: "到达确认"};
const STATUS_LABEL = {
  waiting: "等待回执", awaiting_gap: "等待补齐缺口", retrying: "瞬时失败·待重试",
  forwarding: "转发中", flowing: "转发推进中", caught_up: "已全部转发", blocked: "终态拒绝·屏障",
};
let STATE = {packages: [], adapter: "?", draft: [], downloaded: null, mock: null};

const $ = (id) => document.getElementById(id);

async function api(path, opts = {}) {
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers: opts.body ? {"Content-Type": "application/json"} : undefined,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch (_) { data = {}; }
  if (!res.ok && data && data.detail) throw Object.assign(new Error(data.detail), {data});
  return data;
}

let toastTimer = null;
function toast(text, ms = 3200) {
  const t = $("toast");
  t.textContent = text;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), ms);
}
function setMsg(el, text, cls = "") {
  el.textContent = text;
  el.className = "msg " + cls;
}
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g,
    (c) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}
function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toLocaleString("zh-CN", {hour12: false});
}

/* ------------------------------------------------------------- 页签 */
document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tabpane").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    $("tab-" + btn.dataset.tab).classList.add("active");
    refreshAll();
  });
});

/* ------------------------------------------------------------- 数据加载 */
async function loadHealth() {
  try {
    const h = await api("/api/health");
    STATE.adapter = h.adapter;
    $("adapterBadge").textContent = "适配器：" + h.adapter +
      (h.recovered_on_start ? `（启动恢复 ${h.recovered_on_start} 条）` : "");
  } catch (_) {}
}

async function loadPackages() {
  const data = await api("/api/packages");
  STATE.packages = data.packages || [];
  renderPackages();
  populateBatchSelects();
  renderQueueOverview();
  renderCurrentBatchDetail();
}

function allBatches() {
  const out = [];
  STATE.packages.forEach((p) => p.batches.forEach((b) => out.push(b)));
  return out;
}

function batchMap() {
  const m = {};
  allBatches().forEach((b) => { m[b.batch_id] = b; });
  return m;
}

/* ------------------------------------------------------------- ① 执行包 */
function renderPackages() {
  const box = $("packageList");
  if (!STATE.packages.length) {
    box.innerHTML = '<div class="hint">还没有执行包。在左侧载入示例并导入。</div>';
    return;
  }
  box.innerHTML = STATE.packages.map((p) => {
    const batches = p.batches.map((b) => `
      <div class="batchline" data-batch="${esc(b.batch_id)}">
        <div>
          <b>${esc(b.batch_id)}</b>
          ${esc(b.vehicle)} · ${esc(b.origin)} → ${esc(b.destination)}
          <div class="meta">已收 ${b.received_count} 条 · 游标 ${b.next_seq}
            ${b.blocked_seq != null ? `· <span style="color:#a02424">屏障 #${b.blocked_seq}</span>` : ""}
          </div>
        </div>
        <span class="st st-${b.derived_status}">${STATUS_LABEL[b.derived_status] || b.derived_status}</span>
      </div>`).join("");
    return `<div class="pkg">
      <div class="head">
        <div class="title">${esc(p.summary)} <span class="meta">(${esc(p.package_id)})</span></div>
      </div>
      <div class="meta">批次 ${p.batch_count} · 指纹 ${esc(p.content_hash.slice(0, 12))}…
        · 发布 ${fmtTime(p.created_at)} · 导入 ${fmtTime(p.imported_at)}</div>
      ${batches}
    </div>`;
  }).join("");
  box.querySelectorAll(".batchline").forEach((el) =>
    el.addEventListener("click", () => {
      document.querySelector('.tab[data-tab="queue"]').click();
      $("detailBatch").value = el.dataset.batch;
      renderCurrentBatchDetail();
    }));
}

function samplePackage() {
  const now = Math.floor(Date.now() / 1000);
  return {
    package_id: "EVAC-" + new Date().toISOString().slice(0, 10) + "-01",
    summary: "北岭火线 第 2 波撤离令",
    issued_at: now,
    commander: "张指",
    batches: [
      {batch_id: "B-N2-01", wave: 2, vehicle: "V3 大巴(无障碍)", origin: "G2 北岭村",
       destination: "S1 县体育馆", route: ["G2", "r2", "G1", "r1", "S1"],
       households: ["H-101", "H-102(轮椅)", "H-103"], expected_kinds: ["departed", "arrived"]},
      {batch_id: "B-N2-02", wave: 2, vehicle: "V5 中巴", origin: "G2 北岭村",
       destination: "S3 三中(可携宠)", route: ["G2", "r3", "S3"],
       households: ["H-104(2犬)", "H-105"], expected_kinds: ["departed", "arrived"]},
      {batch_id: "B-N2-03", wave: 2, vehicle: "V1 中巴", origin: "G4 东坡",
       destination: "S1 县体育馆", route: ["G4", "r5", "G1", "r1", "S1"],
       households: ["H-201", "H-202"], expected_kinds: ["departed", "not_departed", "arrived"]},
    ],
  };
}

$("loadSampleBtn").addEventListener("click", () => {
  $("pkgInput").value = JSON.stringify(samplePackage(), null, 2);
  setMsg($("importMsg"), "已载入示例，可修改后导入", "info");
});
$("pkgFile").addEventListener("change", async (e) => {
  const f = e.target.files[0];
  if (!f) return;
  $("pkgInput").value = await f.text();
});
$("importBtn").addEventListener("click", async () => {
  let pkg;
  try { pkg = JSON.parse($("pkgInput").value); }
  catch (e) { return setMsg($("importMsg"), "JSON 解析失败：" + e.message, "err"); }
  try {
    const r = await api("/api/packages", {body: pkg});
    if (r.idempotent_replay) {
      setMsg($("importMsg"), `↩ 幂等重放：该包已存在（${r.batch_count} 批次），返回首次导入结果`, "info");
      toast("完全相同的再次导入：返回原结果");
    } else {
      setMsg($("importMsg"), `✔ 已导入 ${r.batch_count} 个批次（指纹 ${r.content_hash.slice(0, 10)}…）`, "ok");
      toast("执行包已持久化，现场终端可下载");
    }
    await loadPackages();
  } catch (e) {
    setMsg($("importMsg"), "✖ " + e.message, "err");
  }
});

/* ------------------------------------------------------------- ② 终端 */
function populateBatchSelects() {
  const batches = allBatches();
  const opts = (withEmpty) =>
    (withEmpty ? '<option value="">— 选择批次 —</option>' : "") +
    batches.map((b) => `<option value="${esc(b.batch_id)}">${esc(b.batch_id)}：
      ${esc(b.vehicle)} ${esc(b.origin)}→${esc(b.destination)}</option>`).join("");
  ["rcptBatch", "detailBatch", "mockBatch"].forEach((id) => {
    const el = $(id), cur = el.value;
    el.innerHTML = opts(id !== "rcptBatch" ? false : !batches.length);
    if (cur) el.value = cur;
  });
  const dl = $("dlPackage");
  const dlcur = dl.value;
  dl.innerHTML = STATE.packages.map((p) =>
    `<option value="${esc(p.package_id)}">${esc(p.summary)}（${esc(p.package_id)}）</option>`).join("");
  if (dlcur) dl.value = dlcur;
}

$("downloadBtn").addEventListener("click", async () => {
  const pid = $("dlPackage").value;
  if (!pid) return;
  const dl = await api(`/api/packages/${encodeURIComponent(pid)}/download`);
  STATE.downloaded = dl;
  $("downloadOut").textContent = JSON.stringify(dl, null, 2);
  toast("已下载不可变执行包（含批次摘要指纹）");
});

function currentDraftBatch() { return batchMap()[$("rcptBatch").value]; }

function buildReceipt(seq) {
  const b = currentDraftBatch();
  if (!b) { toast("请先选择批次"); return null; }
  let details = {};
  const raw = $("rcptDetail").value.trim();
  if (raw) {
    try { details = JSON.parse(raw); }
    catch (_) { details = {note: raw}; }
  }
  return {
    batch_id: b.batch_id,
    batch_summary: b.batch_summary,
    seq: seq,
    receipt_no: $("rcptNo").value.trim() ||
      `RC-${b.batch_id}-${seq}-${Date.now().toString(36)}`,
    kind: $("rcptKind").value,
    occurred_at: Math.floor(Date.now() / 1000),
    details,
  };
}

$("addRcptBtn").addEventListener("click", () => {
  const seq = parseInt($("rcptSeq").value, 10);
  if (!seq) return toast("请填写序号");
  const r = buildReceipt(seq);
  if (r) { STATE.draft.push(r); renderDraft(); }
});

function demoOutOfOrder() {
  const b = currentDraftBatch();
  if (!b) { toast("请先选择批次（建议选尚未上传过的批次）"); return; }
  const base = Date.now().toString(36);
  [2, 3].forEach((seq) => {
    const r = buildReceipt(seq);
    r.receipt_no = `RC-${b.batch_id}-${seq}-${base}`;
    STATE.draft.push(r);
  });
  renderDraft();
  toast("已加入 #2、#3（#1 留缺口，将持久缓冲不转发）");
}
$("fillGapDemoBtn").addEventListener("click", demoOutOfOrder);

$("fillSeq1Btn").addEventListener("click", () => {
  const b = currentDraftBatch();
  if (!b) return toast("请先选择批次");
  const r = buildReceipt(1);
  r.receipt_no = `RC-${b.batch_id}-1-${Date.now().toString(36)}`;
  STATE.draft.push(r);
  renderDraft();
  toast("已补入 #1，上传后缓冲的 #2/#3 将连续转发");
});

function renderDraft() {
  $("bulkDraft").textContent = JSON.stringify(STATE.draft, null, 2);
}

$("bulkSendBtn").addEventListener("click", async () => {
  if (!STATE.draft.length) return toast("待传清单为空");
  const receipts = STATE.draft;
  STATE.draft = [];
  renderDraft();
  try {
    const r = await api("/api/receipts/bulk", {body: {receipts}});
    renderBulkResult(r);
    const c = r.counts;
    setMsg($("bulkMsg"),
      `新收 ${c.accepted} · 幂等重放 ${c.replay} · 冲突拒绝 ${c.conflict} · 无效 ${c.invalid}`,
      c.conflict || c.invalid ? "err" : "ok");
    await loadPackages();
  } catch (e) {
    setMsg($("bulkMsg"), "✖ " + e.message, "err");
    STATE.draft = receipts;
    renderDraft();
  }
});

function renderBulkResult(r) {
  $("bulkResult").innerHTML = '<ul class="tight">' + r.results.map((x) => {
    const color = x.ok ? (x.status === "replay" ? "#8a6100" : "#1b7f3b") : "#c0392b";
    return `<li style="color:${color}">
      ${x.status === "accepted" ? "✔ 已缓冲" : x.status === "replay" ? "↩ 幂等重放" : "✖ 拒绝"}
      ：批次 ${esc(x.batch_id || "?")} #${esc(x.seq)} ${esc(x.receipt_no || "")}
      ${x.detail ? "— " + esc(x.detail) : ""} ${x.state ? "（状态:" + x.state + "）" : ""}
    </li>`;
  }).join("") + "</ul>";
}

/* ------------------------------------------------------------- ③ 队列 */
function renderQueueOverview() {
  const box = $("queueOverview");
  const batches = allBatches();
  if (!batches.length) { box.innerHTML = '<div class="hint">暂无批次。</div>'; return; }
  box.innerHTML = batches.map((b) => {
    const s = b.states;
    return `<div class="batchline" data-qbatch="${esc(b.batch_id)}" style="cursor:pointer">
      <div>
        <b>${esc(b.batch_id)}</b> <span class="meta">(${esc(b.package_id)})</span><br>
        <span class="pill forwarded">已转发 ${s.forwarded}</span>
        <span class="pill buffered">缓冲 ${s.buffered}</span>
        <span class="pill retry">待重试 ${s.retry}</span>
        <span class="pill rejected">终态拒绝 ${s.rejected}</span>
        ${b.gaps && b.gaps.length ? `<span class="pill" style="background:#f3f6fa;color:#52606d">缺口 ${b.gaps.join(",")}</span>` : ""}
        ${b.blocked_seq != null ? `<span class="pill rejected">屏障 #${b.blocked_seq}：${esc(b.blocked_reason)}</span>` : ""}
      </div>
      <span class="st st-${b.derived_status}">${STATUS_LABEL[b.derived_status] || b.derived_status}</span>
    </div>`;
  }).join("");
  box.querySelectorAll("[data-qbatch]").forEach((el) =>
    el.addEventListener("click", () => {
      $("detailBatch").value = el.dataset.qbatch;
      renderCurrentBatchDetail();
    }));
}

async function renderCurrentBatchDetail() {
  const bid = $("detailBatch") && $("detailBatch").value;
  const box = $("batchDetail");
  if (!bid) { box.innerHTML = '<div class="hint">选择批次查看详情。</div>'; return; }
  const d = await api("/api/batches/" + encodeURIComponent(bid));
  if (d.error) { box.innerHTML = ""; return; }

  const highest = d.highest_seq;
  const maxN = Math.max(highest, d.next_seq, d.blocked_seq || 0);
  const cells = [];
  for (let i = 1; i <= maxN; i++) {
    const r = d.receipts.find((x) => x.seq === i);
    if (!r) cells.push(`<div class="seq gap"><span class="n">#${i}</span>缺口</div>`);
    else cells.push(`<div class="seq ${r.state}" title="${esc(r.last_error || "")}">
      <span class="n">#${i}</span>${KIND_LABEL[r.kind] || r.kind}<br>${stateLabel(r.state)}</div>`);
  }
  const held = d.receipts.filter((r) =>
    r.seq > d.next_seq && r.state === "buffered" &&
    (d.blocked_seq == null || r.seq < d.blocked_seq));

  let banner = "";
  if (d.blocked_seq != null) {
    banner = `<div class="alert block">⛔ <b>终态屏障 #${d.blocked_seq}</b>：
      ${esc(d.blocked_reason_label)}（${esc(d.blocked_reason)}）。${esc(d.blocked_detail)}
      主系统已将该回执记为终态拒绝；#${d.blocked_seq} 之后的回执
      <b>不会、也永远不能越过该项转发</b>。请在主系统侧处理后重新下发执行包。</div>`;
  } else if (d.gaps.length) {
    banner = `<div class="alert gap">⏳ <b>序号缺口：${d.gaps.join(", ")}</b>。
      缺口之后的回执已持久缓冲，补齐后按序连续转发（游标 next_seq=${d.next_seq}）。</div>`;
  } else if (d.states.retry) {
    banner = `<div class="alert info">🔁 游标处瞬时失败，可安全重试。回调按回执编号幂等，
      不会在主系统重复生效。</div>`;
  }

  const rows = d.receipts.map((r) => `
    <tr>
      <td>#${r.seq}</td>
      <td>${r.kind_label}</td>
      <td>${esc(r.receipt_no)}</td>
      <td><span class="pill ${r.state}">${stateLabel(r.state)}</span></td>
      <td>${r.attempts}</td>
      <td>${r.forwarded_at ? fmtTime(r.forwarded_at) : "—"}</td>
      <td>${r.main_ref ? esc(r.main_ref) : (r.last_error ? esc(r.last_error) : "—")}</td>
    </tr>`).join("");

  box.innerHTML = `
    ${banner}
    <dl class="kv">
      <dt>执行包</dt><dd>${esc(d.package_summary)}（${esc(d.package_id)}）</dd>
      <dt>车辆/路线</dt><dd>${esc(d.vehicle)} · ${esc(d.origin)} → ${esc(d.destination)}</dd>
      <dt>批次摘要</dt><dd><code>${esc(d.batch_summary)}</code>（终端回执必须携带）</dd>
      <dt>游标</dt><dd>next_seq=${d.next_seq}
        ${d.blocked_seq != null ? "· blocked_seq=" + d.blocked_seq : ""}</dd>
      <dt>缓冲项</dt><dd>${held.length ? held.map((r) => "#" + r.seq).join(", ") +
        "（等待队首/缺口，先持久化不转发）" : "无"}</dd>
    </dl>
    <h3>序号轨道</h3>
    <div class="seqgrid">${cells.join("") || '<span class="hint">尚无回执</span>'}</div>
    <h3>回执与转发结果</h3>
    <table class="table">
      <thead><tr><th>序号</th><th>类型</th><th>回执编号</th><th>状态</th><th>尝试次数</th>
        <th>转发时间</th><th>主系统引用 / 失败原因</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="7" class="hint">尚无回执</td></tr>'}</tbody>
    </table>
    <details style="margin-top:8px"><summary class="hint">批次原始执行单元 JSON</summary>
      <pre class="jsonview">${esc(JSON.stringify(d.payload, null, 2))}</pre></details>`;
}

function stateLabel(s) {
  return {buffered: "缓冲", forwarding: "转发中", forwarded: "已转发",
          retry: "待重试", rejected: "终态拒绝"}[s] || s;
}

$("retryAllBtn").addEventListener("click", async () => {
  const r = await api("/api/retry", {method: "POST", body: {}});
  toast("已触发全批次安全重试");
  setTimeout(loadPackages, 300);
});
$("retryBatchBtn").addEventListener("click", async () => {
  const bid = $("detailBatch").value;
  if (!bid) return;
  const r = await api("/api/retry", {method: "POST", body: {batch_id: bid}});
  toast("重试结果：" + JSON.stringify(r.result.status));
  await loadPackages();
});

/* ------------------------------------------------------------- ④ 审计 */
async function loadAudit() {
  const data = await api("/api/audit?limit=300");
  const ACTION = {
    package_imported: "执行包导入", receipt_received: "回执接收",
    forwarded: "转发成功", retry_scheduled: "瞬时失败·安排重试",
    terminal_rejected: "终态拒绝·建屏障", recovered: "崩溃恢复",
  };
  $("auditBody").innerHTML = (data.events || []).map((e) => `
    <tr>
      <td>${fmtTime(e.ts)}</td>
      <td><b>${ACTION[e.action] || e.action}</b></td>
      <td>${esc(e.package_id)}</td>
      <td>${esc(e.batch_id)}${e.seq != null ? " #" + e.seq : ""}</td>
      <td>${e.seq != null ? e.seq : ""}</td>
      <td><code>${esc(e.detail)}</code></td>
    </tr>`).join("");
}

/* ------------------------------------------------------------- 🧪 mock */
$("mockApplyBtn").addEventListener("click", async () => {
  const bid = $("mockBatch").value;
  if (!bid) return toast("请选择批次");
  const cfg = {};
  const n = parseInt($("mockTransient").value, 10);
  const termSeq = parseInt($("mockTermSeq").value, 10);
  if (!isNaN(n) && n > 0) cfg.transient_times = {[bid]: n};
  if (termSeq > 0) cfg.terminal = {[`${bid}:${termSeq}`]: $("mockTermReason").value};
  if (!Object.keys(cfg).length) return toast("请填写故障次数或终态序号");
  await applyMock(cfg);
});
$("mockResetBtn").addEventListener("click", () => applyMock({reset: true}));
$("mockJsonBtn").addEventListener("click", async () => {
  try { await applyMock(JSON.parse($("mockJson").value)); }
  catch (e) { toast("JSON 解析失败：" + e.message); }
});
async function applyMock(cfg) {
  try {
    const r = await api("/api/mock/config", {method: "POST", body: cfg});
    if (!r.ok) return toast(r.detail || "配置失败");
    toast("模拟主系统配置已生效");
    await loadMock();
  } catch (e) { toast(e.message); }
}
async function loadMock() {
  const m = await api("/api/mock/config");
  STATE.mock = m;
  const rows = (m.accepted || []).map((a) =>
    `<tr><td>${esc(a.batch_id)}</td><td>${esc(a.receipt_no)}</td><td>${esc(a.ref)}</td></tr>`).join("");
  const attempts = Object.keys(m.attempts || {}).map((no) =>
    `<li>${esc(no)}：到达主系统 <b>${m.attempts[no]}</b> 次` +
    (m.attempts[no] > 1 ? "（重复到达，但主系统只受理一次）" : "") + "</li>").join("");
  $("mockState").innerHTML = `
    <dl class="kv">
      <dt>瞬时故障</dt><dd>${esc(JSON.stringify(m.transient_times))}</dd>
      <dt>终态规则</dt><dd>${esc(JSON.stringify(m.terminal))} ${esc(JSON.stringify(m.terminal_by_no))}</dd>
      <dt>模拟延迟</dt><dd>${m.latency}s</dd>
      <dt>受理回执</dt><dd><b>${m.accepted_count}</b> 条（按回执编号去重后的唯一受理）</dd>
    </dl>
    <h3>主系统实际受理明细</h3>
    <table class="table"><thead><tr><th>批次</th><th>回执编号</th><th>受理引用</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="3" class="hint">暂无</td></tr>'}</tbody></table>
    <h3>回调到达次数（恰好一次观测）</h3>
    <ul class="tight">${attempts || '<li class="hint">暂无</li>'}</ul>`;
}

/* ------------------------------------------------------------- 刷新 */
async function refreshAll() {
  const tab = document.querySelector(".tab.active").dataset.tab;
  try {
    if (tab === "packages") await loadPackages();
    else if (tab === "terminal") { await loadPackages(); }
    else if (tab === "queue") { await loadPackages(); await renderCurrentBatchDetail(); }
    else if (tab === "audit") await loadAudit();
    else if (tab === "mock") await loadMock();
  } catch (e) { /* 网络中断时静默，下轮再试 */ }
}
$("refreshBtn").addEventListener("click", refreshAll);
setInterval(() => { if ($("autoRefresh").checked) refreshAll(); }, 3000);
setInterval(loadHealth, 10000);

loadHealth().then(loadPackages);
