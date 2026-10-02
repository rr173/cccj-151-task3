#!/usr/bin/env python3
"""HttpAdapter 协议分类测试（起本地 HTTP 桩主系统，零第三方依赖）。

验证：200 ack / 200 terminal 三类终态 / 408·429·5xx·连接拒绝 -> transient /
Idempotency-Key 头携带 receipt_no。
"""
import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from adapters import HttpAdapter

BEHAVIOR = {}
SEEN_HEADERS = []


class Stub(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        SEEN_HEADERS.append(self.headers.get("Idempotency-Key"))
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        rule = BEHAVIOR.get("rule", "ack")
        if rule == "ack":
            self._json(200, {"status": "ack", "ref": "REF-1"})
        elif rule.startswith("terminal:"):
            reason = rule.split(":", 1)[1]
            self._json(200, {"status": "terminal", "reason": reason, "detail": "d"})
        elif rule.startswith("http:"):
            code = int(rule.split(":", 1)[1])
            self._json(code, {"detail": f"http error {code}"})
        else:
            self._json(200, {})

    def _json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class HttpAdapterCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
        cls.port = cls.srv.server_address[1]
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        BEHAVIOR["rule"] = "ack"
        SEEN_HEADERS.clear()

    def adapter(self):
        return HttpAdapter({"url": f"http://127.0.0.1:{self.port}/cb", "timeout": 3})

    def env(self, no="RC-1"):
        return {"batch_id": "B", "seq": 1, "receipt_no": no, "kind": "departed",
                "batch_summary": "x", "occurred_at": 1, "payload": {}}

    def test_ack_and_idempotency_header(self):
        r = self.adapter().callback(self.env())
        self.assertEqual(r.outcome, "ack")
        self.assertEqual(r.ref, "REF-1")
        self.assertEqual(SEEN_HEADERS[-1], "RC-1")

    def test_terminal_reasons(self):
        for reason in ("withdrawn", "vehicle_changed", "destination_changed"):
            BEHAVIOR["rule"] = "terminal:" + reason
            r = self.adapter().callback(self.env("RC-" + reason))
            self.assertEqual(r.outcome, "terminal")
            self.assertEqual(r.reason, reason)

    def test_terminal_unknown_reason_normalized(self):
        BEHAVIOR["rule"] = "terminal:bogus"
        r = self.adapter().callback(self.env())
        self.assertEqual(r.outcome, "terminal")
        self.assertEqual(r.reason, "withdrawn")

    def test_transient_statuses(self):
        for code in (408, 429, 500, 502, 503):
            BEHAVIOR["rule"] = "http:" + str(code)
            r = self.adapter().callback(self.env())
            self.assertEqual(r.outcome, "transient", code)
            self.assertIn(str(code), r.reason)

    def test_other_4xx_is_conservative_transient(self):
        BEHAVIOR["rule"] = "http:400"
        r = self.adapter().callback(self.env())
        self.assertEqual(r.outcome, "transient")

    def test_connection_refused_is_transient(self):
        a = HttpAdapter({"url": "http://127.0.0.1:1/cb", "timeout": 2})
        r = a.callback(self.env())
        self.assertEqual(r.outcome, "transient")
        self.assertEqual(r.reason, "connect_error")


if __name__ == "__main__":
    unittest.main(verbosity=2)
