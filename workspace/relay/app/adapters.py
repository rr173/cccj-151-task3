"""主系统回调适配器（可替换）。

适配器契约：
  * callback() 向撤离编排系统转发一条已连续放行的回执；
  * 必须按 receipt_no 幂等：中继在"超时但主系统其实已受理"等场景下会用同一
    receipt_no 安全重试，主系统/适配器必须返回首次结果而不是重复生效；
  * 返回三类结果：
      ack       —— 主系统受理，回执完成（恰好一次）；
      transient —— 超时/5xx/网络中断/未知错误，可安全重试，游标不动；
      terminal  —— 批次已撤回/车辆或目的地已变化，终态拒绝，建立屏障，
                    该批次后续序号永远不得越过。

内置实现：
  * MockAdapter   进程内模拟主系统（演示/测试，行为可通过管理 API 配置）；
  * HttpAdapter   真实对接：POST JSON 到主系统 URL，带 Idempotency-Key 头；
  * 自定义        RELAY_ADAPTER=module.path:factory_name，
                  factory(config:dict) 返回具备 callback(env)->dict 的对象。
"""
import importlib
import json
import os
import socket
import threading
import urllib.error
import urllib.request
import uuid

from db import TERMINAL_REASONS


class CallbackResult:
    __slots__ = ("outcome", "reason", "detail", "ref")

    def __init__(self, outcome, reason="", detail="", ref=""):
        assert outcome in ("ack", "transient", "terminal")
        if outcome == "terminal" and reason not in TERMINAL_REASONS:
            reason = "withdrawn"
        self.outcome = outcome
        self.reason = reason
        self.detail = detail
        self.ref = ref

    def as_dict(self):
        return {"outcome": self.outcome, "reason": self.reason,
                "detail": self.detail, "ref": self.ref}


# ---------------------------------------------------------------- 模拟主系统
class MockMainSystem:
    """进程内模拟的"撤离编排主系统"。

    行为配置（管理 API /mock/config 可改，仅演示测试用）：
      transient_times: {batch_id: n}  该批次前 n 次回调注入超时/5xx，之后成功；
      terminal:        {"batch_id:seq": "withdrawn|vehicle_changed|destination_changed"}
      terminal_by_no:  {receipt_no: reason}
      latency:         每次调用延迟秒数（模拟弱网）

    受理记录 accepted: {(batch_id, receipt_no): ref} —— 同一回执编号重放返回同一结果，
    attempts 记录每个回执编号的实际到达次数，恰好一次语义可直接断言
    （重放不会产生第二条受理）。
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.transient_times = {}
        self.terminal = {}
        self.terminal_by_no = {}
        self.latency = 0.0
        self.accepted = {}          # (batch_id, receipt_no) -> main_ref
        self.attempts = {}          # receipt_no -> 到达主系统的调用次数

    def configure(self, transient_times=None, terminal=None,
                  terminal_by_no=None, latency=None, reset=False):
        with self.lock:
            if reset:
                self.transient_times.clear()
                self.terminal.clear()
                self.terminal_by_no.clear()
                self.accepted.clear()
                self.attempts.clear()
                self.latency = 0.0
            if transient_times is not None:
                self.transient_times = {str(k): int(v) for k, v in transient_times.items()}
            if terminal is not None:
                self.terminal = {str(k): str(v) for k, v in terminal.items()}
            if terminal_by_no is not None:
                self.terminal_by_no = {str(k): str(v) for k, v in terminal_by_no.items()}
            if latency is not None:
                self.latency = float(latency)
            return self.snapshot()

    def snapshot(self):
        with self.lock:
            return {
                "transient_times": dict(self.transient_times),
                "terminal": dict(self.terminal),
                "terminal_by_no": dict(self.terminal_by_no),
                "latency": self.latency,
                "accepted": [{"batch_id": k[0], "receipt_no": k[1], "ref": v}
                             for k, v in sorted(self.accepted.items())],
                "attempts": dict(self.attempts),
                "accepted_count": len(self.accepted),
            }

    def receive(self, env):
        """主系统侧接收逻辑；同时被 MockAdapter 与演示用 HTTP 回调端点调用。"""
        with self.lock:
            if self.latency:
                time_sleep(self.latency)
            no = env["receipt_no"]
            bid = env["batch_id"]
            self.attempts[no] = self.attempts.get(no, 0) + 1
            key = (bid, no)

            # 幂等：已受理的回执编号重放，返回首次受理引用，绝不二次生效
            if key in self.accepted:
                return {"status": "ack", "ref": self.accepted[key], "replay": True}

            reason = self.terminal_by_no.get(no) or self.terminal.get(
                f"{bid}:{env['seq']}")
            if reason:
                if reason not in TERMINAL_REASONS:
                    reason = "withdrawn"
                return {"status": "terminal", "reason": reason,
                        "detail": "模拟主系统：该批次已不可用于此回执"}

            left = self.transient_times.get(bid, 0)
            if left > 0:
                self.transient_times[bid] = left - 1
                # 在两种瞬时故障间轮换，便于页面展示不同失败原因
                which = left % 2
                return {"status": "transient",
                        "reason": "timeout" if which else "http_503",
                        "detail": "模拟主系统：注入的临时故障（超时/暂不可用），可安全重试",
                        "http_status": 408 if which else 503}

            ref = "MAIN-" + uuid.uuid4().hex[:12]
            self.accepted[key] = ref
            return {"status": "ack", "ref": ref}


def time_sleep(sec):
    import time
    time.sleep(sec)


class MockAdapter:
    """进程内适配器：直接调用同进程 MockMainSystem，无网络。"""
    name = "mock"

    def __init__(self, main: MockMainSystem, config=None):
        self.main = main

    def callback(self, env):
        r = self.main.receive(env)
        status = r.get("status")
        if status == "ack":
            return CallbackResult("ack", ref=r.get("ref", ""))
        if status == "terminal":
            return CallbackResult("terminal", reason=r.get("reason", "withdrawn"),
                                  detail=r.get("detail", ""))
        return CallbackResult("transient", reason=r.get("reason", "transient"),
                              detail=r.get("detail", ""))


# ---------------------------------------------------------------- HTTP 适配器
class HttpAdapter:
    """对接真实撤离编排系统的 HTTP 回调。

    请求：POST {url}，JSON 信封，Idempotency-Key: <receipt_no>。
    判定：
      网络错误/超时/408/429/5xx        -> transient（安全重试）；
      2xx 且 body.status=ack           -> ack；
      2xx 且 body.status=terminal      -> terminal（body.reason 指定三类终态之一）；
      其他 4xx                         -> transient（保守重试，失败原因在页面可见），
                                          只有显式终态原因才建立屏障。
    """
    name = "http"

    def __init__(self, config=None):
        cfg = config or {}
        self.url = cfg.get("url") or os.environ.get(
            "RELAY_MAIN_URL", "http://127.0.0.1:8090/callbacks/relay-receipt")
        self.timeout = float(cfg.get("timeout") or os.environ.get("RELAY_HTTP_TIMEOUT", "5"))

    def callback(self, env):
        body = json.dumps(env, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, method="POST",
            headers={"Content-Type": "application/json; charset=utf-8",
                     "Idempotency-Key": str(env.get("receipt_no", ""))})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                status_code = resp.status
        except urllib.error.HTTPError as e:
            status_code = e.code
            try:
                raw = e.read().decode("utf-8")
            except Exception:
                raw = ""
        except (socket.timeout, TimeoutError):
            return CallbackResult("transient", "timeout", f"回调超时（>{self.timeout}s），可安全重试")
        except urllib.error.URLError as e:
            return CallbackResult("transient", "connect_error", f"无法连接主系统：{e.reason}")
        except Exception as e:  # 任何未知网络异常都按可重试处理
            return CallbackResult("transient", "network_error", str(e))

        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            data = {}

        if status_code == 200 or (200 <= status_code < 300):
            st = data.get("status", "ack")
            if st == "terminal":
                reason = data.get("reason", "withdrawn")
                if reason not in TERMINAL_REASONS:
                    reason = "withdrawn"
                return CallbackResult("terminal", reason,
                                      detail=data.get("detail", ""), ref=data.get("ref", ""))
            return CallbackResult("ack", ref=data.get("ref", ""))

        if status_code in (408, 429) or status_code >= 500:
            return CallbackResult("transient", f"http_{status_code}",
                                  f"主系统返回 {status_code}，稍后安全重试")
        # 其余 4xx：保守按瞬时处理并暴露原因（不把未知业务错误误判成终态屏障）
        return CallbackResult("transient", f"http_{status_code}",
                              data.get("detail") or f"主系统返回 {status_code}：{raw[:200]}")


# ---------------------------------------------------------------- 装载
def load_adapter(config, mock_main):
    kind = (config.get("adapter") or os.environ.get("RELAY_ADAPTER", "mock")).strip()
    if kind == "mock":
        return MockAdapter(mock_main, config)
    if kind == "http":
        return HttpAdapter(config)
    # 自定义："module.path:factory" —— factory(config)->adapter 对象
    if ":" in kind:
        mod_path, factory = kind.rsplit(":", 1)
        mod = importlib.import_module(mod_path)
        return getattr(mod, factory)(config)
    raise ValueError(f"未知适配器类型: {kind}")
