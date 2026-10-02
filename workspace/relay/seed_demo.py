#!/usr/bin/env python3
"""演示播种：向运行中的中继导入示例执行包、注入故障、上传若干回执。

用法：
  python3 seed_demo.py                 # 默认 http://127.0.0.1:8090
  python3 seed_demo.py --url URL --transient-batch B-N2-01 --transient-n 2

播种后可在页面观察：
  B-N2-01：#2/#3 已缓冲等待缺口（不转发）——在终端页"补传序号 1"即可连续转发；
  B-N2-02：#1/#2 正常转发成功；
  指定批次：前 n 次回调超时/5xx（retry），后台自动/手动安全重试后成功，
           模拟主系统受理明细证明重试没有重复生效。
"""
import argparse
import json
import os
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


def call(url, method="GET", body=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8090")
    ap.add_argument("--transient-batch", default="B-N2-03")
    ap.add_argument("--transient-n", type=int, default=2)
    args = ap.parse_args()
    base = args.url.rstrip("/")

    with open(os.path.join(HERE, "examples", "sample_package.json"), encoding="utf-8") as f:
        pkg = json.load(f)
    st, r = call(base + "/api/packages", "POST", pkg)
    print("导入执行包:", st, "幂等重放" if r.get("idempotent_replay") else "首次导入")

    cfg = {}
    if args.transient_n > 0:
        cfg["transient_times"] = {args.transient_batch: args.transient_n}
    st, r = call(base + "/api/mock/config", "POST", cfg)
    print("故障注入:", st, cfg)

    summaries = {b["batch_id"]: None for b in pkg["batches"]}
    st, dl = call(base + f"/api/packages/{pkg['package_id']}/download")
    for b in dl["batches"]:
        summaries[b["batch_id"]] = b["batch_summary"]

    now = int(time.time())
    # B-N2-01：乱序上传 #2 #3（#1 缺口 -> 持久缓冲不转发）
    receipts = [
        {"batch_id": "B-N2-01", "batch_summary": summaries["B-N2-01"], "seq": 2,
         "receipt_no": "RC-DEMO-N201-2", "kind": "departed", "occurred_at": now,
         "details": {"odo_km": 0.4, "note": "乱序先到"}},
        {"batch_id": "B-N2-01", "batch_summary": summaries["B-N2-01"], "seq": 3,
         "receipt_no": "RC-DEMO-N201-3", "kind": "departed", "occurred_at": now + 1,
         "details": {"odo_km": 0.6, "note": "乱序先到"}},
        # B-N2-02：顺序两条，立即转发
        {"batch_id": "B-N2-02", "batch_summary": summaries["B-N2-02"], "seq": 1,
         "receipt_no": "RC-DEMO-N202-1", "kind": "departed", "occurred_at": now,
         "details": {"odo_km": 0.0}},
        {"batch_id": "B-N2-02", "batch_summary": summaries["B-N2-02"], "seq": 2,
         "receipt_no": "RC-DEMO-N202-2", "kind": "arrived", "occurred_at": now + 1800,
         "details": {"elapsed_min": 30, "souls": 6}},
        # 目标故障批次：#1 触发 transient，retry 等待后台/手动重试
        {"batch_id": args.transient_batch,
         "batch_summary": summaries[args.transient_batch], "seq": 1,
         "receipt_no": "RC-DEMO-FAULT-1", "kind": "departed", "occurred_at": now,
         "details": {"note": "前两次回调将超时/5xx"}},
    ]
    st, r = call(base + "/api/receipts/bulk", "POST", {"receipts": receipts})
    print("批量上传:", st, r["counts"])
    for item in r["results"]:
        print("  -", item["status"], item["batch_id"], "#" + str(item["seq"]),
              item.get("detail", ""))
    print("\n下一步：打开", base, "→ 队列页观察缺口/重试；终端页给 B-N2-01 补传 #1。")


if __name__ == "__main__":
    sys.exit(main())
