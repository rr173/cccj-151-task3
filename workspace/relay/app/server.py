"""离线车队回执中继 —— HTTP 服务（Python 标准库，零第三方依赖）。

线程模型：ThreadingHTTPServer；所有 SQLite 写事务在 DB 单连接锁内串行，
回调等慢网络操作在锁外执行（见 relay.RelayService.pump_batch）。
"""
import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import DB
from models import ValidationError
from relay import RelayService
from adapters import MockMainSystem, load_adapter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "..", "web")
DB_PATH = os.environ.get("RELAY_DB", os.path.join(BASE_DIR, "..", "data", "relay.db"))
PORT = int(os.environ.get("RELAY_PORT", "8090"))

db = DB(DB_PATH)
recovered = db.recover_inflight()
mock_main = MockMainSystem()
adapter = load_adapter({}, mock_main)
service = RelayService(db, adapter, pump_interval=float(os.environ.get("RELAY_PUMP_INTERVAL", "2")))
service.start_background()
if recovered:
    print(f"[relay] 启动恢复：{recovered} 条转发中的回执已重置为待重试（按回执编号幂等）")


class Handler(BaseHTTPRequestHandler):
    server_version = "ConvoyReceiptRelay/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[relay] %s - %s\n" % (self.address_string(), fmt % args))

    # ---------------------------------------------------------- 读写辅助
    def _send_json(self, obj, status=200, extra_headers=None):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, ctype):
        try:
            with open(path, "rb") as f:
                body = f.read()
        except FileNotFoundError:
            self._send_json({"error": "not_found"}, 404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as e:
            raise ValidationError(f"请求体不是合法 JSON: {e}", code="bad_json")

    def _handle(self, fn):
        try:
            fn()
        except ValidationError as e:
            status = 409 if e.code in ("package_id_reused",) else 422
            self._send_json({"ok": False, "error": e.code, "detail": str(e)}, status)
        except Exception as e:
            traceback.print_exc()
            self._send_json({"ok": False, "error": "internal",
                             "detail": f"{type(e).__name__}: {e}"}, 500)

    # ---------------------------------------------------------- 路由
    def do_GET(self):
        self._handle(self._route_get)

    def do_POST(self):
        self._handle(self._route_post)

    def _route_get(self):
        u = urlparse(self.path)
        p = [x for x in u.path.split("/") if x]
        q = {k: v[0] for k, v in parse_qs(u.query).items()}

        if not p:
            return self._send_file(os.path.join(FRONTEND_DIR, "index.html"),
                                   "text/html; charset=utf-8")
        if len(p) == 1 and p[0] in ("index.html", "index.htm"):
            return self._send_file(os.path.join(FRONTEND_DIR, "index.html"),
                                   "text/html; charset=utf-8")
        if len(p) == 1 and p[0] == "app.js":
            return self._send_file(os.path.join(FRONTEND_DIR, "app.js"),
                                   "application/javascript; charset=utf-8")
        if len(p) == 1 and p[0] == "styles.css":
            return self._send_file(os.path.join(FRONTEND_DIR, "styles.css"),
                                   "text/css; charset=utf-8")

        if p == ["api", "health"]:
            return self._send_json({
                "ok": True, "service": "offline-convoy-receipt-relay",
                "adapter": getattr(adapter, "name", "custom"),
                "recovered_on_start": recovered})
        if p == ["api", "packages"]:
            return self._send_json({"packages": service.list_packages()})
        if len(p) == 4 and p[:2] == ["api", "packages"]:
            if p[3] == "download":
                dl = service.package_download(p[2])
                return self._send_json(dl if dl else {"error": "not_found"},
                                       200 if dl else 404)
            if p[3] == "detail":
                pkg = service.package_detail(p[2])
                return self._send_json(pkg if pkg else {"error": "not_found"},
                                       200 if pkg else 404)
            return self._send_json({"error": "not_found"}, 404)
        if len(p) == 3 and p[:2] == ["api", "packages"]:
            pkg = service.package_detail(p[2])
            return self._send_json(pkg if pkg else {"error": "not_found"},
                                   200 if pkg else 404)
        if p == ["api", "batches"]:
            out = []
            for pkg in service.list_packages():
                out.extend(pkg["batches"])
            return self._send_json({"batches": out})
        if len(p) == 3 and p[:2] == ["api", "batches"]:
            detail = service.batch_detail(p[2])
            return self._send_json(detail if detail else {"error": "not_found"},
                                   200 if detail else 404)
        if p == ["api", "audit"]:
            return self._send_json({"events": service.audit(
                package_id=q.get("package_id", ""),
                batch_id=q.get("batch_id", ""),
                limit=q.get("limit", "200"))})
        if p == ["api", "mock", "config"]:
            return self._send_json(mock_main.snapshot())
        return self._send_json({"error": "not_found", "path": u.path}, 404)

    def _route_post(self):
        u = urlparse(self.path)
        p = [x for x in u.path.split("/") if x]
        body = self._read_json()

        if p == ["api", "packages"]:
            res = service.import_package(body)
            return self._send_json(res, 200)
        if p == ["api", "receipts"]:
            res = service.receive_receipt(body)
            return self._send_json(res, res.get("http_status", 200))
        if p == ["api", "receipts", "bulk"]:
            items = body.get("receipts") if isinstance(body, dict) else body
            return self._send_json(service.receive_many(items))
        if p == ["api", "retry"]:
            bid = body.get("batch_id") if isinstance(body, dict) else None
            if bid:
                res = service.pump_batch(bid)
            else:
                res = service.pump_all()
            return self._send_json({"ok": True, "result": res})
        if p == ["api", "mock", "config"]:
            # mock_main 同时被进程内 MockAdapter 与演示回调端点
            # /callbacks/relay-receipt 使用；两种默认部署下配置都有效。
            note = ("当前适配器为 mock（进程内回调）"
                    if getattr(adapter, "name", "") == "mock"
                    else "当前适配器为 http：配置作用于演示回调端点 "
                         "/callbacks/relay-receipt（同进程模拟主系统）")
            return self._send_json({"ok": True, "note": note,
                                    "config": mock_main.configure(
                transient_times=body.get("transient_times"),
                terminal=body.get("terminal"),
                terminal_by_no=body.get("terminal_by_no"),
                latency=body.get("latency"),
                reset=bool(body.get("reset")))})
        if p == ["callbacks", "relay-receipt"]:
            # 演示用：进程内"主系统"HTTP 回调端点（HttpAdapter 的默认目标）。
            r = mock_main.receive(body)
            status = 200
            if r.get("status") == "transient":
                status = r.get("http_status", 503)
            return self._send_json(r, status)
        return self._send_json({"error": "not_found", "path": u.path}, 404)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[relay] 离线车队回执中继监听 http://0.0.0.0:{PORT} "
          f"(db={DB_PATH}, adapter={getattr(adapter, 'name', 'custom')})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        service.stop_background()
        server.server_close()


if __name__ == "__main__":
    main()
