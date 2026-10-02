"""中继模块持久化层（SQLite，WAL，synchronous=FULL）。

崩溃/进程重启一致性目标：
  * 批次包、回执、转发结果、游标全部落盘后才算生效；
  * 转发 HTTP 调用永远在事务提交之后进行（不会出现"调用了但库里没记录"）；
  * 调用结果通过 (batch_id, seq) 落盘，恰好一次效果不依赖内存状态；
  * 崩溃点若发生在"调用返回后、写库前"，启动恢复会把 forwarding 行重置为 pending，
    回调适配器按回执编号(receipt_no)幂等去重，因此主系统最多收到一次生效调用。
"""
import os
import sqlite3
import threading
import time

SCHEMA = """
-- 不可变的出站执行包（撤离编排系统导出，中继导入）
CREATE TABLE IF NOT EXISTS packages(
  package_id   TEXT PRIMARY KEY,           -- 幂等键：完全相同的再次导入返回原结果
  content_hash TEXT NOT NULL,              -- 规范化 JSON 的 SHA-256（同 id 不同内容 => 409）
  payload      TEXT NOT NULL,              -- 原始执行包 JSON，原样供现场终端下载
  summary      TEXT NOT NULL DEFAULT '',   -- 包标题/说明（展示用）
  batch_count  INTEGER NOT NULL,
  revoked      INTEGER NOT NULL DEFAULT 0, -- 主系统通知整包撤回（预留；批次级撤回走回调终态）
  created_at   REAL NOT NULL,
  imported_at  REAL NOT NULL
);

-- 执行包内的批次（不可变）。游标 next_seq 与终态屏障随转发推进
CREATE TABLE IF NOT EXISTS batches(
  batch_id      TEXT PRIMARY KEY,
  package_id    TEXT NOT NULL REFERENCES packages(package_id),
  vehicle       TEXT NOT NULL DEFAULT '',
  origin        TEXT NOT NULL DEFAULT '',
  destination   TEXT NOT NULL DEFAULT '',
  payload       TEXT NOT NULL,             -- 批次在包内的原始 JSON
  batch_summary TEXT NOT NULL,             -- 批次摘要（回执必须携带，内容指纹校验）
  expected_kinds TEXT NOT NULL DEFAULT '', -- 逗号分隔（如 departed,arrived），仅展示
  next_seq      INTEGER NOT NULL DEFAULT 1,-- 游标：下一个允许连续转发的序号
  blocked_seq   INTEGER,                   -- 终态屏障：该序号被主系统终态拒绝，后续不得越过
  blocked_reason TEXT NOT NULL DEFAULT '', -- withdrawn | vehicle_changed | destination_changed
  blocked_detail TEXT NOT NULL DEFAULT '',
  created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_package ON batches(package_id);

-- 回执（append-only 逻辑；冲突回执拒绝、不落库、不推进游标）
CREATE TABLE IF NOT EXISTS receipts(
  batch_id     TEXT NOT NULL REFERENCES batches(batch_id),
  seq          INTEGER NOT NULL,           -- 批次内严格递增 1..N
  receipt_no   TEXT NOT NULL,              -- 全局唯一回执编号（现场终端生成）
  kind         TEXT NOT NULL,              -- departed(已离场) | not_departed(未能出发) | arrived(到达确认)
  batch_summary TEXT NOT NULL,             -- 回执声称的批次摘要，必须与批次指纹一致
  occurred_at  REAL NOT NULL DEFAULT 0,
  payload_hash TEXT NOT NULL,              -- 回执内容指纹：同 receipt_no/同 seq 内容不同 => 409
  payload      TEXT NOT NULL,             -- 原始回执 JSON
  state        TEXT NOT NULL,             -- buffered | forwarding | forwarded | retry | rejected
  attempts     INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT NOT NULL DEFAULT '',
  forwarded_at REAL,
  main_ref     TEXT NOT NULL DEFAULT '',   -- 主系统返回的受理引用
  created_at   REAL NOT NULL,
  PRIMARY KEY (batch_id, seq)
);
-- receipt_no 全局唯一：同一编号重复用于不同内容/序号必须拒绝
CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_no ON receipts(receipt_no);
CREATE INDEX IF NOT EXISTS idx_receipts_state ON receipts(batch_id, state);

-- 审计流（所有关键动作追加记录）
CREATE TABLE IF NOT EXISTS audit_log(
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts         REAL NOT NULL,
  action     TEXT NOT NULL,
  package_id TEXT NOT NULL DEFAULT '',
  batch_id   TEXT NOT NULL DEFAULT '',
  seq        INTEGER,
  detail     TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);

CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

# 主系统回调终态分类（不可重试；置为终态拒绝屏障）
TERMINAL_REASONS = ("withdrawn", "vehicle_changed", "destination_changed")
TERMINAL_LABEL = {
    "withdrawn": "批次已撤回",
    "vehicle_changed": "车辆已变化",
    "destination_changed": "目的地已变化",
}

VALID_KINDS = ("departed", "not_departed", "arrived")
KIND_LABEL = {
    "departed": "已离场",
    "not_departed": "未能出发",
    "arrived": "到达确认",
}


class DB:
    """单连接 + RLock 串行化所有写事务。

    后台转发线程与 HTTP 工作线程共用本对象；回调等慢操作必须在锁外执行
    （见 relay.py 的 pump_batch），避免网络等待阻塞导入/上传接口。
    """

    def __init__(self, path):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        with self.lock:
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    # ------------------------------------------------------------- 基础封装
    def execute(self, sql, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur

    def query(self, sql, params=()):
        with self.lock:
            return self.conn.execute(sql, params).fetchall()

    def query_one(self, sql, params=()):
        with self.lock:
            return self.conn.execute(sql, params).fetchone()

    def get_meta(self, key, default=None):
        row = self.query_one("SELECT value FROM meta WHERE key=?", (key,))
        return row["value"] if row else default

    def set_meta(self, key, value):
        self.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)))

    def audit(self, action, detail="", package_id="", batch_id="", seq=None, commit=True):
        """追加审计；commit=False 时并入调用方当前事务。"""
        with self.lock:
            self.conn.execute(
                "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                "VALUES(?,?,?,?,?,?)",
                (time.time(), action, package_id, batch_id, seq, detail))
            if commit:
                self.conn.commit()

    # ------------------------------------------------------------- 崩溃恢复
    def recover_inflight(self):
        """启动恢复：把崩溃时停留在 forwarding 的行安全回退到 pending(retry/buffered)。

        这些行的回调可能：(a) 根本没发出；(b) 已发出但结果丢失。
        由于适配器契约要求按 receipt_no 幂等，重放是安全的；
        已 forwarded 的行不受影响，绝不会重复回调。
        """
        with self.lock:
            rows = self.conn.execute(
                "SELECT batch_id, seq FROM receipts WHERE state='forwarding'").fetchall()
            if rows:
                self.conn.execute(
                    "UPDATE receipts SET state='retry', last_error="
                    "'崩溃恢复：上次转发结果未落盘，按回执编号幂等重放' "
                    "WHERE state='forwarding'")
                for r in rows:
                    self.conn.execute(
                        "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                        "VALUES(?,?,?,?,?,?)",
                        (time.time(), "recovered", "", r["batch_id"], r["seq"],
                         "启动恢复：重置未落盘的转发为待重试"))
            self.conn.commit()
            return len(rows)

    def close(self):
        with self.lock:
            self.conn.close()
