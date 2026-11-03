"""Collect AntiLag decision snapshots and train a process-specific shadow regressor."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import tempfile
import time
import urllib.parse
import urllib.request

SCHEMA = Path(__file__).resolve().parents[1] / "adapter/outboundgroup/antilag_features.txt"
FORMAT = "antilag-stall-seconds-per-active-minute-v1"


def collect(args):
    # Keep controller credentials on loopback, bypass proxies and refuse redirects.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args):
            return None

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    url = f"http://127.0.0.1:{args.port}/group/{urllib.parse.quote(args.group, safe='')}/decisions"
    headers = {"Authorization": "Bearer " + os.environ.get("MIHOMO_SECRET", "")}
    args.directory.mkdir(exist_ok=True, parents=True)
    seen = set()
    while True:
        try:
            with opener.open(urllib.request.Request(url, headers=headers), timeout=8) as response:
                rows = json.load(response)
        except OSError as exc:
            if not args.watch:
                raise
            print(f"Controller unavailable: {exc}", flush=True)
            time.sleep(30)
            continue
        rows = [r for r in rows if r.get("process", "").casefold() == args.process.casefold()]
        if args.watch:
            finished = [r for r in rows if r.get("endedAt")]
            rows = [r for r in finished if (r["binding"], r["startedAt"]) not in seen]
            seen = {(r["binding"], r["startedAt"]) for r in finished}
        if rows:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f%z")
            path = args.directory / f"antilag-{stamp}.json"
            with path.open("x", encoding="utf-8") as out:
                json.dump(rows, out, ensure_ascii=False)
            print(f"Saved {len(rows)} decisions to {path}", flush=True)
        if not args.watch:
            return
        time.sleep(30)


def samples(args):
    unique = {}
    paths = [file for path in args.snapshots for file in
             (sorted(path.glob("antilag-*.json")) if path.is_dir() else [path])]
    for path in paths:
        with path.open(encoding="utf-8-sig") as src:
            rows = json.load(src)
        if not isinstance(rows, list):
            raise ValueError(f"{path} is not a decision snapshot")
        for r in rows:
            if r.get("process", "").casefold() != args.process.casefold():
                continue
            if not r.get("endedAt") or not r.get("selected") or r["status"] in ("active", "interrupted"):
                continue
            active, excess = r["activeSeconds"], r["excessSeconds"]
            if not (math.isfinite(active) and active >= 20 and 0 <= excess <= active):
                continue
            chosen = next((c for c in r["candidates"] if c["node"] == r["selected"]), None)
            if chosen is None:
                continue
            start = datetime.fromisoformat(r["startedAt"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(r["endedAt"].replace("Z", "+00:00"))
            if end < start:
                continue
            key = r["binding"], start
            unique[key] = (r, chosen, start, end, 60 * excess / active, min(active, 300))
    return sorted(unique.values(), key=lambda s: s[2])


def split(samples):
    # Keep path generations of one binding together. The training outcome must
    # already have been observable when the first held-out binding was opened.
    starts = {}
    for r, _, start, *_ in samples:
        starts.setdefault(r["binding"], start)
    bindings = sorted(starts, key=starts.get)
    if len(bindings) < 100:
        raise ValueError(f"Only {len(bindings)} eligible bindings; collect at least 100.")
    holdout = set(bindings[int(0.8 * len(bindings)):])
    cutoff = min(starts[k] for k in holdout)
    validation = [s for s in samples if s[0]["binding"] in holdout]
    train = [s for s in samples if s[0]["binding"] not in holdout and s[3] < cutoff]
    if len({s[0]["binding"] for s in train}) < 80:
        raise ValueError("Fewer than 80 training bindings end before the validation period.")
    nodes = {}
    for r, c, *_ in train:
        region = r.get("destinationRegion")
        if region and region not in ("unknown", "global"):
            nodes.setdefault(region, set()).add(c["node"])
    if not any(len(v) >= 2 for v in nodes.values()):
        raise ValueError("Need observations on multiple exits in at least one identified region.")
    if len({s[4] for s in train}) < 2:
        raise ValueError("Training labels are constant.")
    return train, validation


def category_values(row, candidate):
    return {"node": candidate["node"], "destinationRegion": row.get("destinationRegion", ""),
            "exitCountry": candidate.get("exitCountry", "")}


def vector(row, candidate, paths, categories):
    values = []
    for path in paths:
        v = candidate
        for key in path.split("."):
            v = v.get(key) if isinstance(v, dict) else None
        values.append(float(v) if isinstance(v, (int, float)) else -1.0)
    observed = category_values(row, candidate)
    values.extend(float(observed[c["field"]] == c["value"]) for c in categories)
    return values


def train(args):
    data, validation = split(samples(args))
    import lightgbm as lgb
    import numpy as np

    counts = {field: Counter() for field in ("destinationRegion", "exitCountry", "node")}
    for row, candidate, *_ in data:
        for key, value in category_values(row, candidate).items():
            if value:
                counts[key][value] += 1
    cats = [{"field": field, "value": val} for field, cnt in counts.items()
            for val, _ in cnt.most_common(32)]
    paths = SCHEMA.read_text().split()
    names = [p.replace(".", "_") for p in paths] + [f"cat_{i}" for i in range(len(cats))]

    def arrays(rows):
        return (np.asarray([vector(s[0], s[1], paths, cats) for s in rows]),
                np.asarray([s[4] for s in rows]), np.asarray([s[5] for s in rows]))

    x, y, w = arrays(data)
    test_x, test_y, test_w = arrays(validation)
    booster = lgb.train(dict(objective="regression", learning_rate=0.05, num_leaves=7,
                             min_data_in_leaf=10, verbosity=-1, num_threads=1, seed=17),
                        lgb.Dataset(x, label=y, weight=w, feature_name=names), num_boost_round=40)
    pred = np.clip(booster.predict(test_x), 0, 60)
    model_mae = np.average(np.abs(pred - test_y), weights=test_w)
    # For MAE the best constant is the weighted median of training labels.
    order = np.argsort(y)
    baseline = y[order][np.searchsorted(np.cumsum(w[order]), w.sum() / 2)]
    baseline_mae = np.average(np.abs(baseline - test_y), weights=test_w)
    report = dict(trainRows=len(data), validationRows=len(validation),
                  weightedMAE=float(model_mae), constantMAE=float(baseline_mae))
    spec = dict(format=FORMAT, process=args.process, categories=cats, validation=report,
                booster=booster.model_to_string())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=args.output.parent, delete=False, mode="w", encoding="utf-8") as out:
        tmp = Path(out.name)
    try:
        tmp.write_text(json.dumps(spec, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, args.output)
    finally:
        tmp.unlink(missing_ok=True)
    print(json.dumps(report), args.output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    collector = sub.add_parser("collect")
    collector.add_argument("directory", type=Path)
    collector.add_argument("--process", default="Darktide.exe")
    collector.add_argument("--port", type=int, default=9090)
    collector.add_argument("--group", default="AntiLag")
    collector.add_argument("--watch", action="store_true", help="save newly completed decisions every 30 seconds")
    trainer = sub.add_parser("train")
    trainer.add_argument("snapshots", type=Path, nargs="+")
    trainer.add_argument("--process", default="Darktide.exe")
    trainer.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        (collect if args.cmd == "collect" else train)(args)
    except (ValueError, OSError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
