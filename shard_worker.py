#!/usr/bin/env python3
"""shard_worker.py — 云端分片回补 worker (GitHub Actions 用)
按 shard_id 把 5575 只分成 N 片, 每次运行回补一片的一段, parquet 提交到本仓库。
用法: shard_worker.py <shard_id> <total_shards> [batch]
断点: shard_<id>_done.txt
"""
import json, os, sys, time, datetime, urllib.request

WH = "wh"
DONE = f"shard_{sys.argv[1]}_done.txt"
BATCH = int(sys.argv[3]) if len(sys.argv) > 3 else 120
UA_S = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}
UA_T = {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com"}

DATASETS = {"daily": ["symbol", "date", "open", "close", "high", "low", "volume"]}

def get(url, headers, timeout=12):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")

def sina_daily(code, datalen=1023 * 5):
    url = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={code}&scale=240&ma=no&datalen={datalen}")
    d = json.loads(get(url, UA_S))
    return [[r["day"], r["open"], r["close"], r["high"], r["low"], r["volume"]] for r in d]

def tencent_daily(code, end, count=800):
    url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,{end},{count},qfq")
    d = json.loads(get(url, UA_T))
    k = d["data"][code]
    key = next((x for x in k if "qfqday" in x or x == "day"), None)
    return k.get(key, []) if key else []

def fetch_all(code):
    all_rows = {}
    try:
        for r in sina_daily(code):
            all_rows[r[0]] = r
    except Exception:
        pass
    earliest = min(all_rows) if all_rows else "2026-10-15"
    if earliest > "1999-01-01":
        end, fail = earliest, 0
        while end > "1991-01-01" and fail < 2:
            try:
                rows = tencent_daily(code, end)
            except Exception:
                fail += 1; time.sleep(2); continue
            if not rows: break
            new = [r for r in rows if r[0] not in all_rows]
            if not new: break
            for r in rows: all_rows[r[0]] = r
            end = rows[0][0]; time.sleep(0.4)
    return all_rows

def write_partition(ds, year, rows):
    import pyarrow.parquet as pq, pyarrow as pa, pathlib
    cols = DATASETS[ds]
    pathlib.Path(f"{WH}/{ds}").mkdir(parents=True, exist_ok=True)
    path = f"{WH}/{ds}/{year}.parquet"
    if os.path.exists(path):
        t = pq.read_table(path)
        names = t.column_names
        order = [names.index(c) for c in cols]
        old_keys = set(); old_rows = []
        for row in zip(*[t.column(n).to_pylist() for n in names]):
            fixed = tuple(row[i] for i in order)
            old_rows.append(fixed); old_keys.add((fixed[0], fixed[1]))
        rows = old_rows + [r for r in rows if (r[0], r[1]) not in old_keys]
    rows.sort(key=lambda r: (r[0], r[1]))
    pq.write_table(pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)}),
                   path, compression="zstd")

def main():
    shard, total = int(sys.argv[1]), int(sys.argv[2])
    syms = json.load(open("symbols.json"))
    codes = sorted(r["symbol"] for r in syms)
    codes = codes[shard::total]
    done = set(open(DONE).read().split()) if os.path.exists(DONE) else set()
    todo = [c for c in codes if c not in done][:BATCH]
    print(f"shard {shard}/{total}: {len(todo)}/{len(codes)} 本轮待补", flush=True)
    t0 = time.time()
    for i, code in enumerate(todo):
        rows = fetch_all(code)
        if rows:
            by_year = {}
            for d, r in rows.items():
                by_year.setdefault(d[:4], []).append(
                    [code, d, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5] or 0)])
            for yr, rr in by_year.items():
                try: write_partition("daily", yr, rr)
                except Exception as e: print(f"  {code} y{yr} FAIL {e}", flush=True)
        open(DONE, "a").write(code + "\n")
        if (i + 1) % 10 == 0:
            rate = (i + 1) / (time.time() - t0) * 60
            print(f"  {i+1}/{len(todo)} {code} {len(rows)}日 {rate:.0f}/min", flush=True)
        time.sleep(0.5)
    print("batch done", flush=True)

if __name__ == "__main__":
    main()
