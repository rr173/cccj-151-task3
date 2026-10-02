"""中继核心：执行包导入、回执接收、缺口缓冲、顺序转发、终态屏障。

顺序与恰好一次的关键机制（每批次一条独立序号流，游标 next_seq 从 1 开始）：

  1. 回执按 (batch_id, seq) 去重，receipt_no 全局唯一；
     序号乱序到达时全部持久化为 buffered，但只有 seq == next_seq 的队首会被转发。
  2. 队首转发：先在事务里把 buffered/retry -> forwarding（attempts+1）并提交，
     锁外执行回调（慢网络不阻塞其它批次/接口），结果再落盘：
       ack       -> forwarded，next_seq += 1，继续处理新队首；
       transient -> retry，游标不动，后台/手动安全重试（回调按 receipt_no 幂等）；
       terminal  -> rejected，写 blocked_seq 屏障，本批次转发永久停在该序号，
                    后续序号即使已缓冲也绝不越过。
  3. 完全相同的再次导入/再次上传返回首次结果（幂等重放）；
     同 receipt_no 或同 seq 内容不同 -> 拒绝（409），不插入、不推进任何游标。
"""
import json
import threading
import time

from db import TERMINAL_LABEL, TERMINAL_REASONS, VALID_KINDS
from models import ValidationError, batch_summary_of, canonical, receipt_fingerprint, sha256_hex


class RelayService:
    def __init__(self, db, adapter, pump_interval=2.0):
        self.db = db
        self.adapter = adapter
        self.pump_interval = pump_interval
        self._batch_locks = {}
        self._locks_guard = threading.Lock()
        self._stop = threading.Event()
        self._worker = None

    # ----------------------------------------------------------- 后台泵
    def _lock_for(self, batch_id):
        with self._locks_guard:
            lk = self._batch_locks.get(batch_id)
            if lk is None:
                lk = threading.Lock()
                self._batch_locks[batch_id] = lk
            return lk

    def start_background(self):
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()

        def loop():
            while not self._stop.wait(self.pump_interval):
                try:
                    self.pump_all()
                except Exception:
                    pass  # 单次轮询异常不影响后续轮询；细节已在审计/失败原因里

        self._worker = threading.Thread(target=loop, name="relay-pump", daemon=True)
        self._worker.start()

    def stop_background(self):
        self._stop.set()

    def active_batches(self):
        return [r["batch_id"] for r in self.db.query(
            "SELECT batch_id FROM batches WHERE blocked_seq IS NULL ORDER BY created_at")]

    def pump_all(self):
        """转发所有仍在流动的批次；各批次独立，互不阻塞。"""
        out = {}
        for bid in self.active_batches():
            out[bid] = self.pump_batch(bid)
        return out

    def kick(self, batch_id):
        """异步触发一次批次转发（回执上传后立即调用，不必等后台轮询）。"""
        if not self.db.query_one("SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)):
            return

        def run():
            try:
                self.pump_batch(batch_id)
            except Exception:
                pass

        threading.Thread(target=run, name=f"kick-{batch_id[:8]}", daemon=True).start()

    def pump_batch(self, batch_id):
        """把一个批次的队首回执连续转发，直到：缺口 / 瞬时失败 / 终态屏障。

        用非阻塞批次锁保证同一批次不会被多个线程并发转发；
        已在转发中则跳过（后台下一轮自然继续）。
        """
        lk = self._lock_for(batch_id)
        if not lk.acquire(blocking=False):
            return {"batch_id": batch_id, "status": "busy"}
        forwarded_now = []
        try:
            while True:
                # ---- 事务内：读取游标与队首，决定是否进入转发 ----
                with self.db.lock:
                    batch = self.db.conn.execute(
                        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
                    if batch is None:
                        return {"batch_id": batch_id, "status": "unknown_batch"}
                    if batch["blocked_seq"] is not None:
                        return {"batch_id": batch_id, "status": "blocked",
                                "blocked_seq": batch["blocked_seq"],
                                "reason": batch["blocked_reason"],
                                "forwarded_now": forwarded_now}
                    head = self.db.conn.execute(
                        "SELECT * FROM receipts WHERE batch_id=? AND seq=?",
                        (batch_id, batch["next_seq"])).fetchone()
                    if head is None:
                        return {"batch_id": batch_id, "status": "gap_or_idle",
                                "cursor": batch["next_seq"],
                                "forwarded_now": forwarded_now}
                    if head["state"] == "forwarded":
                        # 理论上不会发生（游标只在 forwarded 后推进）；防御性对齐
                        self.db.conn.execute(
                            "UPDATE batches SET next_seq=? WHERE batch_id=?",
                            (batch["next_seq"] + 1, batch_id))
                        self.db.conn.commit()
                        continue
                    if head["state"] not in ("buffered", "retry", "forwarding"):
                        return {"batch_id": batch_id, "status": f"head_{head['state']}",
                                "forwarded_now": forwarded_now}
                    # 进入转发并落盘（崩溃后可凭 forwarding 恢复，且不会丢序号）
                    self.db.conn.execute(
                        "UPDATE receipts SET state='forwarding', attempts=attempts+1 "
                        "WHERE batch_id=? AND seq=?", (batch_id, head["seq"]))
                    self.db.conn.commit()
                    env = {
                        "batch_id": batch_id,
                        "package_id": batch["package_id"],
                        "seq": head["seq"],
                        "receipt_no": head["receipt_no"],
                        "kind": head["kind"],
                        "batch_summary": head["batch_summary"],
                        "occurred_at": head["occurred_at"],
                        "payload": json.loads(head["payload"]),
                    }

                # ---- 锁外：调用主系统（网络等待不阻塞其它事务）----
                try:
                    result = self.adapter.callback(env)
                except Exception as e:
                    from adapters import CallbackResult
                    result = CallbackResult("transient", "adapter_exception", str(e))

                # ---- 结果落盘 ----
                with self.db.lock:
                    if result.outcome == "ack":
                        self.db.conn.execute(
                            "UPDATE receipts SET state='forwarded', last_error='', "
                            "forwarded_at=?, main_ref=? WHERE batch_id=? AND seq=?",
                            (time.time(), result.ref, batch_id, head["seq"]))
                        self.db.conn.execute(
                            "UPDATE batches SET next_seq=? WHERE batch_id=?",
                            (head["seq"] + 1, batch_id))
                        self.db.conn.execute(
                            "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                            "VALUES(?,?,?,?,?,?)",
                            (time.time(), "forwarded", batch["package_id"], batch_id,
                             head["seq"], json.dumps(
                                 {"receipt_no": head["receipt_no"], "ref": result.ref,
                                  "attempts": head["attempts"] + 1}, ensure_ascii=False)))
                        self.db.conn.commit()
                        forwarded_now.append(head["seq"])
                        continue

                    if result.outcome == "terminal":
                        detail = result.detail or TERMINAL_LABEL.get(result.reason, "终态拒绝")
                        self.db.conn.execute(
                            "UPDATE receipts SET state='rejected', last_error=?, "
                            "main_ref=? WHERE batch_id=? AND seq=?",
                            (f"{result.reason}: {detail}", result.ref,
                             batch_id, head["seq"]))
                        self.db.conn.execute(
                            "UPDATE batches SET blocked_seq=?, blocked_reason=?, "
                            "blocked_detail=? WHERE batch_id=?",
                            (head["seq"], result.reason, detail, batch_id))
                        self.db.conn.execute(
                            "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                            "VALUES(?,?,?,?,?,?)",
                            (time.time(), "terminal_rejected", batch["package_id"],
                             batch_id, head["seq"], json.dumps(
                                 {"receipt_no": head["receipt_no"],
                                  "reason": result.reason, "detail": detail},
                                 ensure_ascii=False)))
                        self.db.conn.commit()
                        return {"batch_id": batch_id, "status": "blocked",
                                "blocked_seq": head["seq"], "reason": result.reason,
                                "forwarded_now": forwarded_now}

                    # transient：游标不动，等待后台/手动重试
                    self.db.conn.execute(
                        "UPDATE receipts SET state='retry', last_error=? "
                        "WHERE batch_id=? AND seq=?",
                        (f"{result.reason}: {result.detail}", batch_id, head["seq"]))
                    self.db.conn.execute(
                        "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                        "VALUES(?,?,?,?,?,?)",
                        (time.time(), "retry_scheduled", batch["package_id"],
                         batch_id, head["seq"], json.dumps(
                             {"receipt_no": head["receipt_no"],
                              "reason": result.reason, "detail": result.detail,
                              "attempts": head["attempts"] + 1}, ensure_ascii=False)))
                    self.db.conn.commit()
                    return {"batch_id": batch_id, "status": "retry",
                            "seq": head["seq"], "reason": result.reason,
                            "detail": result.detail, "forwarded_now": forwarded_now}
        finally:
            lk.release()

    # ----------------------------------------------------------- 执行包导入
    def import_package(self, payload):
        """导入（或幂等重放）一个出站执行包。包与批次一旦写入不可变。"""
        now = time.time()
        if not isinstance(payload, dict):
            raise ValidationError("执行包必须是 JSON 对象")
        package_id = payload.get("package_id")
        if not package_id or not isinstance(package_id, str):
            raise ValidationError("缺少 package_id（字符串）")
        batches_obj = payload.get("batches")
        if not isinstance(batches_obj, list) or not batches_obj:
            raise ValidationError("执行包必须包含非空 batches 数组")
        seen = set()
        for i, b in enumerate(batches_obj):
            if not isinstance(b, dict):
                raise ValidationError(f"batches[{i}] 必须是对象")
            bid = b.get("batch_id") or b.get("id")
            if not bid:
                raise ValidationError(f"batches[{i}] 缺少 batch_id")
            if bid in seen:
                raise ValidationError(f"包内 batch_id 重复: {bid}")
            seen.add(bid)

        content_hash = sha256_hex(canonical(payload))
        existing = self.db.query_one(
            "SELECT * FROM packages WHERE package_id=?", (package_id,))
        if existing is not None:
            if existing["content_hash"] != content_hash:
                raise ValidationError(
                    f"package_id={package_id} 已存在但内容不同（已存指纹 "
                    f"{existing['content_hash'][:12]}… / 本次 {content_hash[:12]}…），"
                    "执行包不可变，拒绝覆盖", code="package_id_reused")
            return {"idempotent_replay": True, "package_id": package_id,
                    "content_hash": content_hash,
                    "batch_count": existing["batch_count"],
                    "imported_at": existing["imported_at"]}

        summary = str(payload.get("summary") or payload.get("name") or package_id)
        with self.db.lock:
            self.db.conn.execute(
                "INSERT INTO packages(package_id,content_hash,payload,summary,"
                "batch_count,created_at,imported_at) VALUES(?,?,?,?,?,?,?)",
                (package_id, content_hash, json.dumps(payload, ensure_ascii=False),
                 summary, len(batches_obj),
                 float(payload.get("issued_at") or now), now))
            for b in batches_obj:
                bid = b.get("batch_id") or b.get("id")
                kinds = b.get("expected_kinds") or []
                if isinstance(kinds, list):
                    kinds_s = ",".join(str(k) for k in kinds)
                else:
                    kinds_s = str(kinds)
                self.db.conn.execute(
                    "INSERT INTO batches(batch_id,package_id,vehicle,origin,destination,"
                    "payload,batch_summary,expected_kinds,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (bid, package_id,
                     str(b.get("vehicle") or b.get("vehicle_code") or ""),
                     str(b.get("origin") or b.get("grid") or ""),
                     str(b.get("destination") or b.get("shelter") or ""),
                     json.dumps(b, ensure_ascii=False), batch_summary_of(b),
                     kinds_s, now))
            self.db.conn.execute(
                "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                "VALUES(?,?,?,?,?,?)",
                (now, "package_imported", package_id, "", None,
                 json.dumps({"summary": summary, "batch_count": len(batches_obj),
                             "content_hash": content_hash}, ensure_ascii=False)))
            self.db.conn.commit()
        return {"idempotent_replay": False, "package_id": package_id,
                "content_hash": content_hash, "batch_count": len(batches_obj),
                "imported_at": now}

    # ----------------------------------------------------------- 回执接收
    def receive_receipt(self, item):
        """处理单条回执，返回结果字典（不抛异常 —— 批量接口逐条报告）。

        status:
          accepted  新回执已持久化（可能在缓冲）；
          replay    完全相同的再次提交，返回原状态，不推进游标；
          invalid   校验失败（未知批次/摘要不符/字段缺失）；
          conflict  同 seq 或同 receipt_no 内容不同，拒绝且不推进游标。
        """
        def fail(status, reason, detail="", http=422):
            return {"ok": False, "status": status, "reason": reason,
                    "detail": detail, "http_status": http}

        if not isinstance(item, dict):
            return fail("invalid", "not_object", "回执必须是 JSON 对象")
        batch_id = item.get("batch_id")
        seq = item.get("seq")
        receipt_no = item.get("receipt_no")
        kind = item.get("kind")
        batch_summary = item.get("batch_summary")
        details = item.get("details") or {}
        try:
            occurred_at = float(item.get("occurred_at") or time.time())
        except (TypeError, ValueError):
            return fail("invalid", "bad_occurred_at", "occurred_at 必须是数字时间戳")

        if not batch_id or not isinstance(batch_id, str):
            return fail("invalid", "missing_field", "缺少 batch_id")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
            return fail("invalid", "missing_field", "seq 必须是 >=1 的整数")
        if not receipt_no or not isinstance(receipt_no, str):
            return fail("invalid", "missing_field", "缺少 receipt_no")
        if kind not in VALID_KINDS:
            return fail("invalid", "bad_kind",
                        f"kind 必须是 { '/'.join(VALID_KINDS) } 之一")
        if not batch_summary or not isinstance(batch_summary, str):
            return fail("invalid", "missing_field", "缺少 batch_summary（批次摘要）")

        batch = self.db.query_one(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        if batch is None:
            return fail("invalid", "unknown_batch",
                        f"中继中没有批次 {batch_id}，请先下载执行包")
        if batch["batch_summary"] != batch_summary:
            return fail("invalid", "summary_mismatch",
                        "批次摘要不一致：终端持有的批次视图（车辆/目的地/搭载名单）"
                        "与中继保存的不可变执行包不符，可能主系统已撤回或改写该批次", 422)

        fp = receipt_fingerprint(kind, occurred_at, details)
        payload_json = json.dumps(item, ensure_ascii=False)

        with self.db.lock:
            same_seq = self.db.conn.execute(
                "SELECT * FROM receipts WHERE batch_id=? AND seq=?",
                (batch_id, seq)).fetchone()
            if same_seq is not None:
                if (same_seq["receipt_no"] == receipt_no
                        and same_seq["payload_hash"] == fp):
                    return {"ok": True, "status": "replay",
                            "idempotent_replay": True, "batch_id": batch_id,
                            "seq": seq, "receipt_no": receipt_no,
                            "state": same_seq["state"],
                            "detail": "完全相同的回执已存在，返回首次处理结果"}
                return fail("conflict", "seq_conflict",
                            f"批次 {batch_id} 的序号 {seq} 已有不同内容"
                            f"（原回执编号 {same_seq['receipt_no']}），拒绝且游标不推进",
                            409)

            same_no = self.db.conn.execute(
                "SELECT * FROM receipts WHERE receipt_no=?", (receipt_no,)).fetchone()
            if same_no is not None:
                # 即使内容完全相同，同一回执编号绑到另一个序号也属于冲突性复用，
                # 必须拒绝（同序号的完全重放已在上面作为 replay 放行）。
                return fail("conflict", "receipt_no_reused",
                            f"回执编号 {receipt_no} 已用于批次 {same_no['batch_id']} "
                            f"序号 {same_no['seq']}，内容/序号不同，拒绝且游标不推进", 409)

            # 终态屏障后的新序号也允许持久化（现场可能已上传），但永远不会被转发；
            # 页面会明确标注其卡在屏障之后。
            self.db.conn.execute(
                "INSERT INTO receipts(batch_id,seq,receipt_no,kind,batch_summary,"
                "occurred_at,payload_hash,payload,state,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (batch_id, seq, receipt_no, kind, batch_summary, occurred_at,
                 fp, payload_json, "buffered", time.time()))
            self.db.conn.execute(
                "INSERT INTO audit_log(ts,action,package_id,batch_id,seq,detail) "
                "VALUES(?,?,?,?,?,?)",
                (time.time(), "receipt_received", batch["package_id"], batch_id, seq,
                 json.dumps({"receipt_no": receipt_no, "kind": kind,
                             "fingerprint": fp[:12]}, ensure_ascii=False)))
            self.db.conn.commit()

        return {"ok": True, "status": "accepted", "batch_id": batch_id,
                "seq": seq, "receipt_no": receipt_no, "state": "buffered"}

    def receive_many(self, items):
        if not isinstance(items, list) or not items:
            raise ValidationError("receipts 必须是非空数组")
        results = []
        affected = set()
        for item in items:
            r = self.receive_receipt(item)
            results.append(r)
            if r.get("status") == "accepted":
                affected.add(r["batch_id"])
        for bid in affected:
            self.kick(bid)
        counts = {"accepted": 0, "replay": 0, "conflict": 0, "invalid": 0}
        for r in results:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        return {"results": results, "counts": counts,
                "all_ok": all(r["ok"] for r in results)}

    # ----------------------------------------------------------- 视图
    def _batch_stats(self, b):
        rows = self.db.query(
            "SELECT seq,state FROM receipts WHERE batch_id=?", (b["batch_id"],))
        received = {r["seq"]: r["state"] for r in rows}
        highest = max(received) if received else 0
        cursor = b["next_seq"]
        blocked = b["blocked_seq"]
        gaps = [s for s in range(1, highest + 1) if s not in received]
        held = sorted(s for s, st in received.items()
                      if st in ("buffered", "retry") and s > cursor and
                      (blocked is None or s < blocked))
        states = {"buffered": 0, "forwarding": 0, "forwarded": 0,
                  "retry": 0, "rejected": 0}
        for st in received.values():
            states[st] = states.get(st, 0) + 1
        if blocked is not None:
            status = "blocked"
        elif gaps or (received and cursor <= highest and cursor not in received):
            status = "awaiting_gap"
        elif states["retry"]:
            status = "retrying"
        elif states["forwarding"]:
            status = "forwarding"
        elif received:
            status = "caught_up" if cursor > highest else "flowing"
        else:
            status = "waiting"
        return {"received_count": len(received), "highest_seq": highest,
                "gaps": gaps, "buffered_behind_cursor": held, "states": states,
                "derived_status": status}

    def list_packages(self):
        out = []
        for p in self.db.query(
                "SELECT * FROM packages ORDER BY imported_at DESC, package_id"):
            batches = []
            for b in self.db.query(
                    "SELECT * FROM batches WHERE package_id=? ORDER BY rowid",
                    (p["package_id"],)):
                bd = dict(b)
                bd.pop("payload", None)
                bd.update(self._batch_stats(b))
                batches.append(bd)
            out.append({
                "package_id": p["package_id"], "summary": p["summary"],
                "content_hash": p["content_hash"], "batch_count": p["batch_count"],
                "revoked": bool(p["revoked"]), "created_at": p["created_at"],
                "imported_at": p["imported_at"], "batches": batches,
            })
        return out

    def package_detail(self, package_id):
        p = self.db.query_one(
            "SELECT * FROM packages WHERE package_id=?", (package_id,))
        if p is None:
            return None
        return {"package_id": p["package_id"], "summary": p["summary"],
                "content_hash": p["content_hash"], "revoked": bool(p["revoked"]),
                "created_at": p["created_at"], "imported_at": p["imported_at"],
                "payload": json.loads(p["payload"])}

    def package_download(self, package_id):
        """现场终端下载：原始不可变执行包 + 中继侧批次指纹。"""
        p = self.db.query_one(
            "SELECT * FROM packages WHERE package_id=?", (package_id,))
        if p is None:
            return None
        payload = json.loads(p["payload"])
        batches = []
        for b in self.db.query(
                "SELECT batch_id,vehicle,origin,destination,batch_summary,"
                "expected_kinds FROM batches WHERE package_id=? ORDER BY rowid",
                (package_id,)):
            batches.append(dict(b))
        return {"package_id": p["package_id"], "content_hash": p["content_hash"],
                "summary": p["summary"], "payload": payload, "batches": batches}

    def batch_detail(self, batch_id):
        b = self.db.query_one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        if b is None:
            return None
        receipts = [dict(r) for r in self.db.query(
            "SELECT * FROM receipts WHERE batch_id=? ORDER BY seq", (batch_id,))]
        for r in receipts:
            r["kind_label"] = {"departed": "已离场",
                               "not_departed": "未能出发",
                               "arrived": "到达确认"}.get(r["kind"], r["kind"])
        stats = self._batch_stats(b)
        pkg = self.db.query_one(
            "SELECT summary FROM packages WHERE package_id=?", (b["package_id"],))
        return {
            "batch_id": b["batch_id"], "package_id": b["package_id"],
            "package_summary": pkg["summary"] if pkg else "",
            "vehicle": b["vehicle"], "origin": b["origin"],
            "destination": b["destination"],
            "batch_summary": b["batch_summary"],
            "expected_kinds": b["expected_kinds"].split(",") if b["expected_kinds"] else [],
            "payload": json.loads(b["payload"]),
            "next_seq": b["next_seq"], "blocked_seq": b["blocked_seq"],
            "blocked_reason": b["blocked_reason"],
            "blocked_reason_label": TERMINAL_LABEL.get(b["blocked_reason"], ""),
            "blocked_detail": b["blocked_detail"],
            "receipts": receipts, **stats,
        }

    def audit(self, package_id="", batch_id="", limit=200):
        limit = max(1, min(int(limit or 200), 1000))
        sql = "SELECT * FROM audit_log WHERE 1=1"
        params = []
        if package_id:
            sql += " AND package_id=?"
            params.append(package_id)
        if batch_id:
            sql += " AND batch_id=?"
            params.append(batch_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.db.query(sql, params)]
