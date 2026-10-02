#!/usr/bin/env python3
"""离线车队回执中继 —— 核心语义测试（直接调用服务层，零第三方依赖）。

覆盖：
  1. 不可变导入：幂等重放 / 同 id 不同内容拒绝 / 批次摘要指纹
  2. 乱序缺口：#2/#3 缓冲不转发；补 #1 后连续转发
  3. 重复回执：完全相同重放返回原状态、不重复回调
  4. 冲突回执：同 seq 内容不同、receipt_no 复用 -> 拒绝且游标不推进
  5. 批次摘要不一致拒绝
  6. 瞬时失败：retry、游标不动、安全重试，最终恰好一次（主系统受理一次）
  7. 终态拒绝：withdrawn/vehicle_changed/destination_changed 建屏障，后续不可越过
  8. 重启恢复：新服务接管同一 DB；forwarding 重置幂等重放；forwarded 绝不重复回调
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from db import DB
from relay import RelayService
from adapters import MockAdapter, MockMainSystem


def make_package(pid="P1", batches=("B1", "B2")):
    return {"package_id": pid, "summary": "测试令", "issued_at": 1000,
            "batches": [{"batch_id": b, "vehicle": "V-" + b, "origin": "G1",
                         "destination": "S1", "households": ["H1", "H2"],
                         "expected_kinds": ["departed", "arrived"]} for b in batches]}


class RelayCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = DB(os.path.join(self.tmp.name, "relay.db"))
        self.main = MockMainSystem()
        self.svc = RelayService(self.db, MockAdapter(self.main), pump_interval=60)
        self.svc.import_package(make_package())
        b = self.db.query_one("SELECT batch_summary FROM batches WHERE batch_id='B1'")
        self.summary_b1 = b["batch_summary"]
        b2 = self.db.query_one("SELECT batch_summary FROM batches WHERE batch_id='B2'")
        self.summary_b2 = b2["batch_summary"]

    def tearDown(self):
        self.svc.stop_background()
        self.db.close()
        self.tmp.cleanup()

    def receipt(self, seq, no=None, batch="B1", kind="departed", summary=None,
                details=None, occurred=2000):
        return {"batch_id": batch,
                "batch_summary": summary or (self.summary_b1 if batch == "B1"
                                             else self.summary_b2),
                "seq": seq,
                "receipt_no": no or f"RC-{batch}-{seq}",
                "kind": kind, "occurred_at": occurred,
                "details": details if details is not None else {"k": "v"}}

    # ---------------------------------------------------------------- 1 导入
    def test_import_idempotent_and_immutable(self):
        again = self.svc.import_package(make_package())
        self.assertTrue(again["idempotent_replay"])

        changed = make_package()
        changed["summary"] = "被篡改的内容"
        from models import ValidationError
        with self.assertRaises(ValidationError):
            self.svc.import_package(changed)
        # 原包未被覆盖
        pkgs = self.svc.list_packages()
        self.assertEqual(pkgs[0]["summary"], "测试令")

    # ---------------------------------------------------------------- 2 乱序
    def test_out_of_order_gap_buffers_then_flushes(self):
        r2 = self.svc.receive_receipt(self.receipt(2, "RC-B1-2"))
        self.assertEqual(r2["status"], "accepted")
        r3 = self.svc.receive_receipt(self.receipt(3, "RC-B1-3"))
        self.assertEqual(r3["status"], "accepted")

        # 缺口存在：#2 缓冲，主系统一条都没收到，游标仍为 1
        out = self.svc.pump_batch("B1")
        self.assertEqual(out["status"], "gap_or_idle")
        self.assertEqual(out["cursor"], 1)
        self.assertEqual(len(self.main.accepted), 0)
        detail = self.svc.batch_detail("B1")
        self.assertEqual(detail["gaps"], [1])
        self.assertEqual(detail["buffered_behind_cursor"], [2, 3])
        self.assertEqual(detail["derived_status"], "awaiting_gap")

        # 补齐 #1 -> 1,2,3 连续转发
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        out = self.svc.pump_batch("B1")
        self.assertEqual(out["forwarded_now"], [1, 2, 3])
        self.assertEqual(len(self.main.accepted), 3)
        detail = self.svc.batch_detail("B1")
        self.assertEqual(detail["next_seq"], 4)
        self.assertEqual(detail["gaps"], [])
        self.assertEqual(detail["derived_status"], "caught_up")

    def test_gap_inside_sequence(self):
        # #1,#3 到达：#1 转发，#3 缓冲等待 #2
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        self.svc.pump_batch("B1")
        self.svc.receive_receipt(self.receipt(3, "RC-B1-3"))
        out = self.svc.pump_batch("B1")
        self.assertEqual(out["status"], "gap_or_idle")
        self.assertEqual(len(self.main.accepted), 1)
        self.svc.receive_receipt(self.receipt(2, "RC-B1-2"))
        self.svc.pump_batch("B1")
        self.assertEqual(len(self.main.accepted), 3)

    # ---------------------------------------------------------------- 3 重复
    def test_duplicate_replay_is_safe_and_no_second_callback(self):
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        self.svc.pump_batch("B1")
        self.assertEqual(self.main.attempts["RC-B1-1"], 1)

        # 已转发后完全相同的再次提交：replay，不再回调
        r = self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        self.assertEqual(r["status"], "replay")
        self.assertEqual(r["state"], "forwarded")
        self.svc.pump_batch("B1")
        self.assertEqual(self.main.attempts["RC-B1-1"], 1)

        # 缓冲态的重复提交同样 replay
        self.svc.receive_receipt(self.receipt(5, "RC-B1-5"))
        r = self.svc.receive_receipt(self.receipt(5, "RC-B1-5"))
        self.assertEqual(r["status"], "replay")

    def test_bulk_replay_of_all(self):
        self.svc.receive_many([self.receipt(1, "RC-B1-1")])
        r = self.svc.receive_many([self.receipt(1, "RC-B1-1")])
        self.assertEqual(r["counts"], {"accepted": 0, "replay": 1, "conflict": 0, "invalid": 0})

    # ---------------------------------------------------------------- 4 冲突
    def test_same_seq_different_content_rejected_no_cursor(self):
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1", details={"a": 1}))
        bad = self.svc.receive_receipt(self.receipt(1, "RC-B1-1-X", details={"a": 2}))
        self.assertEqual(bad["status"], "conflict")
        self.assertEqual(bad["http_status"], 409)
        # 原回执仍正常转发，游标推进
        self.svc.pump_batch("B1")
        self.assertEqual(len(self.main.accepted), 1)
        self.assertEqual(self.db.query_one(
            "SELECT receipt_no FROM receipts WHERE batch_id='B1' AND seq=1")["receipt_no"],
            "RC-B1-1")

    def test_receipt_no_reuse_rejected(self):
        # 同编号换序号（即使内容指纹相同）也拒绝
        self.svc.receive_receipt(self.receipt(1, "DUP-NO"))
        bad = self.svc.receive_receipt(self.receipt(2, "DUP-NO"))
        self.assertEqual(bad["status"], "conflict")
        self.assertEqual(bad["reason"], "receipt_no_reused")
        # 不同批次复用编号同样拒绝
        bad2 = self.svc.receive_receipt(self.receipt(1, "DUP-NO", batch="B2"))
        self.assertEqual(bad2["status"], "conflict")

    def test_same_no_same_seq_but_diff_kind_rejected(self):
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1", kind="departed"))
        bad = self.svc.receive_receipt(self.receipt(1, "RC-B1-1", kind="arrived"))
        self.assertEqual(bad["status"], "conflict")

    # ---------------------------------------------------------------- 5 摘要
    def test_wrong_batch_summary_rejected(self):
        bad = self.svc.receive_receipt(self.receipt(1, "RC-X", summary="deadbeefdeadbeef"))
        self.assertEqual(bad["status"], "invalid")
        self.assertEqual(bad["reason"], "summary_mismatch")
        out = self.svc.pump_batch("B1")
        self.assertEqual(out["status"], "gap_or_idle")  # 未插入，游标不动
        self.assertEqual(len(self.main.accepted), 0)

    def test_unknown_batch_rejected(self):
        r = self.svc.receive_receipt(self.receipt(1, "RC-Z", batch="NOPE"))
        self.assertEqual(r["status"], "invalid")
        self.assertEqual(r["reason"], "unknown_batch")

    # ---------------------------------------------------------------- 6 瞬时失败
    def test_transient_retry_then_success_exactly_once(self):
        self.main.configure(transient_times={"B1": 2})
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))

        o1 = self.svc.pump_batch("B1")
        self.assertEqual(o1["status"], "retry")
        row = self.db.query_one("SELECT state,attempts FROM receipts WHERE batch_id='B1' AND seq=1")
        self.assertEqual(row["state"], "retry")
        self.assertEqual(row["attempts"], 1)
        self.assertEqual(self.db.query_one(
            "SELECT next_seq FROM batches WHERE batch_id='B1'")["next_seq"], 1)

        # 已缓冲的 #2 不会越过待重试的 #1
        self.svc.receive_receipt(self.receipt(2, "RC-B1-2"))
        o2 = self.svc.pump_batch("B1")
        self.assertEqual(o2["status"], "retry")
        self.assertEqual(len(self.main.accepted), 0)

        # 第三次尝试成功（2 次故障已耗尽）：#1、#2 一次性连续转发
        o3 = self.svc.pump_batch("B1")
        self.assertEqual(o3["status"] != "retry", True)
        self.assertEqual(o3["forwarded_now"], [1, 2])
        # 主系统侧收到过 3 次对 #1 的调用，但只受理 1 次（幂等契约）
        self.assertEqual(self.main.attempts["RC-B1-1"], 3)
        self.assertEqual(len(self.main.accepted), 2)

    def test_retry_all_independent_batches(self):
        self.main.configure(transient_times={"B1": 1})
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1", batch="B1"))
        self.svc.receive_receipt(self.receipt(1, "RC-B2-1", batch="B2"))
        self.svc.pump_all()
        self.assertEqual(self.db.query_one(
            "SELECT state FROM receipts WHERE batch_id='B1' AND seq=1")["state"], "retry")
        self.assertEqual(self.db.query_one(
            "SELECT state FROM receipts WHERE batch_id='B2' AND seq=1")["state"], "forwarded")
        self.svc.pump_all()
        self.assertEqual(len(self.main.accepted), 2)

    # ---------------------------------------------------------------- 7 终态
    def test_terminal_reason_builds_barrier(self):
        for reason in ("withdrawn", "vehicle_changed", "destination_changed"):
            bid = "T-" + reason
            self.svc.import_package(make_package("P-" + reason, (bid,)))
            bs = self.db.query_one(
                "SELECT batch_summary FROM batches WHERE batch_id=?", (bid,))["batch_summary"]
            self.main.configure(terminal={f"{bid}:1": reason})
            self.svc.receive_receipt(self.receipt(1, f"RC-{bid}-1", batch=bid, summary=bs))
            self.svc.receive_receipt(self.receipt(2, f"RC-{bid}-2", batch=bid, summary=bs))
            out = self.svc.pump_batch(bid)
            self.assertEqual(out["status"], "blocked")
            self.assertEqual(out["blocked_seq"], 1)
            self.assertEqual(out["reason"], reason)
            detail = self.svc.batch_detail(bid)
            self.assertEqual(detail["blocked_reason"], reason)
            # #2 已持久化但永远停在缓冲，绝不越过 #1
            self.assertEqual(detail["receipts"][1]["state"], "buffered")
            # 再怎么重试都不会再发出回调
            self.svc.pump_batch(bid)
            self.svc.pump_all()
            self.assertEqual(self.main.attempts.get(f"RC-{bid}-2", 0), 0)

    def test_terminal_by_receipt_no(self):
        self.main.configure(terminal_by_no={"RC-B1-1": "vehicle_changed"})
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        out = self.svc.pump_batch("B1")
        self.assertEqual(out["reason"], "vehicle_changed")
        self.assertEqual(out["blocked_seq"], 1)

    # ---------------------------------------------------------------- 8 重启
    def test_restart_recovery_no_duplicate_callback(self):
        # 转发 #1 成功后模拟重启：新 DB/新服务接管同一文件
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        self.svc.pump_batch("B1")
        self.svc.stop_background()
        self.db.close()

        db2 = DB(self.db.path)
        n = db2.recover_inflight()
        self.assertEqual(n, 0)  # 没有卡在 forwarding 的行
        main2 = MockMainSystem()  # 新进程视角的"主系统"仍是真实远端；这里验证不会重复调用
        svc2 = RelayService(db2, MockAdapter(main2), pump_interval=60)

        # #1 已是 forwarded：重启后 pump 绝不重调
        out = svc2.pump_batch("B1")
        self.assertEqual(out["status"], "gap_or_idle")
        self.assertEqual(len(main2.accepted), 0)

        # #2 在重启前已缓冲，重启后继续处理
        # （重新打开原服务插入后再关 —— 直接在新服务插入模拟持久缓冲）
        svc2.receive_receipt(self.receipt(2, "RC-B1-2"))
        svc2.pump_batch("B1")
        self.assertEqual(len(main2.accepted), 1)
        db2.close()

    def test_restart_with_inflight_row_replays_idempotently(self):
        # 手工把一行置为 forwarding（模拟"回调结果未落盘"崩溃点）
        self.svc.receive_receipt(self.receipt(1, "RC-B1-1"))
        with self.db.lock:
            self.db.conn.execute(
                "UPDATE receipts SET state='forwarding', attempts=1 "
                "WHERE batch_id='B1' AND seq=1")
            self.db.conn.commit()
        self.svc.stop_background()
        self.db.close()

        db2 = DB(self.db.path)
        self.assertEqual(db2.recover_inflight(), 1)
        row = db2.query_one("SELECT state FROM receipts WHERE batch_id='B1' AND seq=1")
        self.assertEqual(row["state"], "retry")
        svc2 = RelayService(db2, MockAdapter(self.main), pump_interval=60)
        svc2.pump_batch("B1")
        self.assertEqual(len(self.main.accepted), 1)
        db2.close()

    def test_kinds_and_audit(self):
        for i, kind in enumerate(("departed", "not_departed", "arrived"), start=1):
            r = self.svc.receive_receipt(self.receipt(i, f"RC-K-{i}", kind=kind))
            self.assertEqual(r["status"], "accepted")
        self.svc.pump_batch("B1")
        events = [e["action"] for e in self.svc.audit(batch_id="B1")]
        self.assertIn("receipt_received", events)
        self.assertIn("forwarded", events)
        self.assertEqual(len(self.main.accepted), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
