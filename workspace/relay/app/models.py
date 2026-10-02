"""规范化编码与内容指纹。

"完全相同"的判定基于规范化 JSON（键排序、无多余空白），
因此导入方/终端重排 JSON 键序不会破坏幂等；但任何实质字段变化都会得到不同指纹。
"""
import hashlib
import json


def canonical(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def receipt_fingerprint(kind, occurred_at, details):
    """回执内容指纹（不含 batch_id/seq/receipt_no/batch_summary 等路由/校验字段）。

    同一 (batch_id, seq) 或同一 receipt_no 再次提交时，业务内容必须完全一致。
    """
    basis = {"kind": kind, "occurred_at": occurred_at, "details": details or {}}
    return sha256_hex(canonical(basis))


def batch_summary_of(batch_obj):
    """批次摘要：车辆 + 出发地 + 目的地 + 搭载名单。

    现场终端回执必须原样携带该摘要；主系统撤回/改车/改目的地后，
    终端若仍上传旧批次视图，会在回调处得到终态拒绝。
    """
    households = batch_obj.get("households") or batch_obj.get("members") or []
    if households and isinstance(households[0], dict):
        households = sorted(str(h.get("ref") or h.get("id") or h) for h in households)
    else:
        households = sorted(str(h) for h in households)
    basis = {
        "vehicle": str(batch_obj.get("vehicle") or batch_obj.get("vehicle_code") or ""),
        "origin": str(batch_obj.get("origin") or batch_obj.get("grid") or ""),
        "destination": str(batch_obj.get("destination") or batch_obj.get("shelter") or ""),
        "households": households,
    }
    return sha256_hex(canonical(basis))[:16]


class ValidationError(Exception):
    def __init__(self, message, code="invalid"):
        super().__init__(message)
        self.code = code
