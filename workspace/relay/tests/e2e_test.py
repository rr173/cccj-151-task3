#!/usr/bin/env python3
"""端到端测试：真实 HTTP 端口 + 进程重启恢复（零第三方依赖）。

启动 app/server.py 子进程（临时 DB、mock 适配器），覆盖：
  健康检查/页面、导入幂等与不可变拒绝、终端下载、批量上传、
  乱序缓冲、后台自动连续转发、瞬时故障安全重试（主系统只受理一次）、
  终态屏障、冲突 409 不推进游标、审计、进程重启后队列恢复与已成功回调不重复。
"""
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.join(HERE, "..", "app", "server.py")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_up(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.15)
    return False


class E2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = os.path.join(cls.tmp.name, "relay.db")
        cls.port = free_port()
        cls.proc = subprocess.Popen(
            [sys.executable, APP],
            env=dict(os.environ, RELAY_PORT=str(cls.port), RELAY_DB=cls.db,
                     RELAY_ADAPTER="mock", RELAY_PUMP_INTERVAL="1"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not wait_up(cls.port):
            cls.proc.kill()
            raise RuntimeError("relay 未在超时内启动")

    @classmethod
    def tearDownClass(cls):
        cls.proc.send_signal(signal.SIGINT)
        cls.proc.wait(timeout=5)
        cls.tmp.cleanup()

    def call(self, path, method="GET", body=None, expect=None):
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                code = r.status
                payload = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            code = e.code
            payload = json.loads(e.read().decode())
        if expect is not None:
            self.assertEqual(code, expect, f"{method} {path} -> {code}: {payload}")
        return code, payload

    def poll_batch(self, bid, check, timeout=12):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            _, d = self.call(f"/api/batches/{bid}")
            last = d
            if check(d):
                return d
            time.sleep(0.25)
        self.fail(f"批次 {bid} 未在 {timeout}s 内满足条件，最后状态: {json.dumps(last, ensure_ascii=False)[:500]}")

    def test_01_health_and_page(self):
        _, h = self.call("/api/health")
        self.assertEqual(h["adapter"], "mock")
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=5) as r:
            self.assertIn("text/html", r.headers["Content-Type"])
            self.assertIn("离线车队回执中继", r.read().decode())

    def test_02_import_idempotent_immutable(self):
        pkg = {"package_id": "E2E-P1", "summary": "端到端令", "issued_at": 1700000000,
               "batches": [
                   {"batch_id": "E-B1", "vehicle": "V1", "origin": "G1", "destination": "S1",
                    "households": ["H1", "H2"]},
                   {"batch_id": "E-B2", "vehicle": "V2", "origin": "G2", "destination": "S2",
                    "households": ["H3"]},
                   {"batch_id": "E-B3", "vehicle": "V3", "origin": "G3", "destination": "S3",
                    "households": ["H4"]}]}
        _, r1 = self.call("/api/packages", "POST", pkg, expect=200)
        self.assertFalse(r1["idempotent_replay"])
        _, r2 = self.call("/api/packages", "POST", pkg, expect=200)
        self.assertTrue(r2["idempotent_replay"])
        tampered = json.loads(json.dumps(pkg))
        tampered["batches"][0]["vehicle"] = "V9-改派车"
        self.call("/api/packages", "POST", tampered, expect=409)
        _, dl = self.call("/api/packages/E2E-P1/download")
        self.assertEqual(dl["content_hash"], r1["content_hash"])
        self.assertEqual(len(dl["batches"]), 3)
        self.summaries = {b["batch_id"]: b["batch_summary"] for b in dl["batches"]}
        E2E.summaries = self.summaries

    def test_03_out_of_order_then_gap_fill(self):
        s = self.summaries
        # 先传 #2/#3（#1 缺口）
        code, bulk = self.call("/api/receipts/bulk", "POST", {"receipts": [
            {"batch_id": "E-B1", "batch_summary": s["E-B1"], "seq": 2,
             "receipt_no": "E-B1-2", "kind": "departed", "occurred_at": 10, "details": {}},
            {"batch_id": "E-B1", "batch_summary": s["E-B1"], "seq": 3,
             "receipt_no": "E-B1-3", "kind": "arrived", "occurred_at": 20, "details": {}}]})
        self.assertEqual(bulk["counts"]["accepted"], 2)
        time.sleep(1.2)  # 等后台泵一轮
        _, d = self.call("/api/batches/E-B1")
        self.assertEqual(d["next_seq"], 1)
        self.assertEqual(d["gaps"], [1])
        self.assertEqual({r["seq"]: r["state"] for r in d["receipts"]},
                         {2: "buffered", 3: "buffered"})

        # 完全相同的再次批量上传 -> replay
        _, replay = self.call("/api/receipts/bulk", "POST", {"receipts": [
            {"batch_id": "E-B1", "batch_summary": s["E-B1"], "seq": 2,
             "receipt_no": "E-B1-2", "kind": "departed", "occurred_at": 10, "details": {}},
            {"batch_id": "E-B1", "batch_summary": s["E-B1"], "seq": 3,
             "receipt_no": "E-B1-3", "kind": "arrived", "occurred_at": 20, "details": {}}]})
        self.assertEqual(replay["counts"]["replay"], 2)

        # 补 #1 -> 1,2,3 连续转发
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-B1", "batch_summary": s["E-B1"], "seq": 1,
                   "receipt_no": "E-B1-1", "kind": "departed",
                   "occurred_at": 9, "details": {}}, expect=200)
        d = self.poll_batch("E-B1", lambda x: x["next_seq"] == 4
                            and x["states"]["forwarded"] == 3)
        self.assertEqual(d["derived_status"], "caught_up")
        _, mock = self.call("/api/mock/config")
        self.assertEqual(mock["attempts"]["E-B1-1"], 1)  # 成功项只回调一次

    def test_04_conflicts_do_not_advance_cursor(self):
        s = self.summaries
        # 同 seq 不同内容
        c1, r1 = self.call("/api/receipts", "POST",
                           {"batch_id": "E-B2", "batch_summary": s["E-B2"], "seq": 1,
                            "receipt_no": "E-B2-1", "kind": "departed",
                            "occurred_at": 1, "details": {"odo": 10}})
        self.assertEqual(c1, 200)
        c2, r2 = self.call("/api/receipts", "POST",
                           {"batch_id": "E-B2", "batch_summary": s["E-B2"], "seq": 1,
                            "receipt_no": "E-B2-1", "kind": "departed",
                            "occurred_at": 1, "details": {"odo": 999}}, expect=409)
        self.assertEqual(r2["status"], "conflict")  # 逐条拒绝信封
        self.assertIn("序号", r2["detail"])
        # 回执编号复用
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-B2", "batch_summary": s["E-B2"], "seq": 2,
                   "receipt_no": "E-B2-1", "kind": "arrived",
                   "occurred_at": 2, "details": {}}, expect=409)
        # 错误摘要
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-B2", "batch_summary": "0" * 16, "seq": 2,
                   "receipt_no": "E-B2-2", "kind": "arrived",
                   "occurred_at": 2, "details": {}}, expect=422)
        d = self.poll_batch("E-B2", lambda x: x["states"]["forwarded"] == 1)
        self.assertEqual(d["next_seq"], 2)

    def test_05_transient_retry_exactly_once(self):
        s = self.summaries
        self.call("/api/mock/config", "POST",
                  {"transient_times": {"E-B3": 2}}, expect=200)
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-B3", "batch_summary": s["E-B3"], "seq": 1,
                   "receipt_no": "E-B3-1", "kind": "departed",
                   "occurred_at": 1, "details": {}}, expect=200)
        # 首次 pump 应落入 retry（手动重试接口立即触发，不等后台）
        _, rr = self.call("/api/retry", "POST", {"batch_id": "E-B3"})
        self.assertEqual(rr["result"]["status"], "retry")
        _, d = self.call("/api/batches/E-B3")
        self.assertEqual(d["states"]["retry"], 1)
        self.assertIn("可安全重试", d["receipts"][0]["last_error"])
        # 再重试一次耗尽故障 -> 成功；主系统对同一回执编号只受理一次
        self.call("/api/retry", "POST", {"batch_id": "E-B3"})
        self.poll_batch("E-B3", lambda x: x["states"]["forwarded"] == 1)
        _, mock = self.call("/api/mock/config")
        self.assertEqual(mock["attempts"]["E-B3-1"], 3)  # 3 次到达
        self.assertEqual(len([a for a in mock["accepted"]
                              if a["receipt_no"] == "E-B3-1"]), 1)  # 只受理 1 条

    def test_06_terminal_barrier(self):
        pkg = {"package_id": "E2E-P2", "summary": "屏障令",
               "batches": [{"batch_id": "E-T1", "vehicle": "V9", "origin": "G9",
                            "destination": "S9", "households": ["H9"]}]}
        self.call("/api/packages", "POST", pkg, expect=200)
        _, dl = self.call("/api/packages/E2E-P2/download")
        summ = dl["batches"][0]["batch_summary"]
        self.call("/api/mock/config", "POST",
                  {"terminal": {"E-T1:1": "withdrawn"}})
        for seq, no, kind in [(1, "E-T1-1", "departed"),
                              (2, "E-T1-2", "not_departed"),
                              (3, "E-T1-3", "arrived")]:
            self.call("/api/receipts", "POST",
                      {"batch_id": "E-T1", "batch_summary": summ, "seq": seq,
                       "receipt_no": no, "kind": kind,
                       "occurred_at": seq, "details": {}}, expect=200)
        d = self.poll_batch("E-T1", lambda x: x["blocked_seq"] == 1)
        self.assertEqual(d["blocked_reason"], "withdrawn")
        states = {r["seq"]: r["state"] for r in d["receipts"]}
        self.assertEqual(states, {1: "rejected", 2: "buffered", 3: "buffered"})
        # 手动/后台重试都无法越过
        self.call("/api/retry", "POST", {"batch_id": "E-T1"})
        time.sleep(1.2)
        _, d2 = self.call("/api/batches/E-T1")
        self.assertEqual({r["seq"]: r["state"] for r in d2["receipts"]}, states)
        _, mock = self.call("/api/mock/config")
        self.assertNotIn("E-T1-2", mock["attempts"])
        self.assertNotIn("E-T1-3", mock["attempts"])

    def test_07_audit(self):
        _, a = self.call("/api/audit?limit=500")
        actions = {e["action"] for e in a["events"]}
        for required in ("package_imported", "receipt_received", "forwarded",
                         "retry_scheduled", "terminal_rejected"):
            self.assertIn(required, actions)

    def test_08_restart_recovery(self):
        # 再建一个批次：#1 成功转发、#2 缓冲，随后杀进程重启
        pkg = {"package_id": "E2E-P3", "summary": "重启令",
               "batches": [{"batch_id": "E-R1", "vehicle": "V7", "origin": "G7",
                            "destination": "S7", "households": ["H7"]}]}
        self.call("/api/packages", "POST", pkg, expect=200)
        _, dl = self.call("/api/packages/E2E-P3/download")
        summ = dl["batches"][0]["batch_summary"]
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-R1", "batch_summary": summ, "seq": 1,
                   "receipt_no": "E-R1-1", "kind": "departed",
                   "occurred_at": 1, "details": {}}, expect=200)
        self.poll_batch("E-R1", lambda x: x["states"]["forwarded"] == 1)
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-R1", "batch_summary": summ, "seq": 2,
                   "receipt_no": "E-R1-2", "kind": "arrived",
                   "occurred_at": 2, "details": {}}, expect=200)
        time.sleep(1.2)  # 让 #2 保持缓冲（实际它会在无缺口时立即被 kick 转发…）
        # 注：无缺口时 #2 已应转发；这里重启主要验证状态不丢、不重复
        before = (self.call("/api/batches/E-R1")[1])

        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()
        self.proc = subprocess.Popen(
            [sys.executable, APP],
            env=dict(os.environ, RELAY_PORT=str(self.port), RELAY_DB=self.db,
                     RELAY_ADAPTER="mock", RELAY_PUMP_INTERVAL="1"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(wait_up(self.port))

        _, after = self.call("/api/batches/E-R1")
        self.assertEqual(after["next_seq"], before["next_seq"])
        self.assertEqual(after["states"], before["states"])
        # 已 forwarded 的 #1 在重启后不会再次出现在新主系统受理记录里
        _, mock = self.call("/api/mock/config")
        self.assertNotIn("E-R1-1", mock["attempts"])
        # 队列仍可继续：补 #3 正常转发
        self.call("/api/receipts", "POST",
                  {"batch_id": "E-R1", "batch_summary": summ, "seq": 3,
                   "receipt_no": "E-R1-3", "kind": "arrived",
                   "occurred_at": 3, "details": {}}, expect=200)
        # 若重启前 #2 未转发（缺口场景），恢复后也会随 #3 之前先被处理
        d = self.poll_batch("E-R1",
                            lambda x: x["states"].get("forwarded", 0) >= 2 and
                            x["next_seq"] in (3, 4), timeout=8)
        self.assertGreaterEqual(d["states"]["forwarded"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
