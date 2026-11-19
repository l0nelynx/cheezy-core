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
import urllib.error
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
    url = f"http://127.0.0.1:{args.port}/group/{urllib.parse.quote(args.group, safe='')}/outcomes"
    headers = {"Authorization": "Bearer " + os.environ.get("MIHOMO_SECRET", "")}
    args.directory.mkdir(exist_ok=True, parents=True)
    seen = set()
    while True:
        try:
            try:
                response = opener.open(urllib.request.Request(url, headers=headers), timeout=8)
            except urllib.error.HTTPError as exc:
                if exc.code != 404:
                    raise
                exc.close()
                # Older cores expose only the recent decisions.
                recent = url.rsplit("/", 1)[0] + "/decisions"
                response = opener.open(urllib.request.Request(recent, headers=headers), timeout=8)
            with response:
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
        if args.progress:
            show_progress(args)
        if not args.watch:
            return
        time.sleep(30)


def samples(snapshots, process):
    unique = {}
    paths = [file for path in snapshots for file in
             (sorted(path.glob("antilag-*.json")) if path.is_dir() else [path])]
    for path in paths:
        with path.open(encoding="utf-8-sig") as src:
            rows = json.load(src)
        if not isinstance(rows, list):
            raise ValueError(f"{path} is not a decision snapshot")
        for r in rows:
            if r.get("process", "").casefold() != process.casefold():
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


def evaluate_split(samples):
    # Keep path generations of one binding together. The training outcome must
    # already have been observable when the first held-out binding was opened.
    starts = {}
    for r, _, start, *_ in samples:
        starts.setdefault(r["binding"], start)
    bindings = sorted(starts, key=starts.get)
    blockers = []
    if len(bindings) < 100:
        blockers.append(f"Eligible bindings: {len(bindings)}/100.")
    holdout = set(bindings[int(0.8 * len(bindings)):])
    cutoff = min((starts[k] for k in holdout), default=None)
    validation = [s for s in samples if s[0]["binding"] in holdout]
    train = [s for s in samples if s[0]["binding"] not in holdout and s[3] < cutoff]
    train_count = len({s[0]["binding"] for s in train})
    if train_count < 80:
        blockers.append(f"Training bindings ending before validation: {train_count}/80.")
    nodes = {}
    for r, c, *_ in train:
        region = r.get("destinationRegion")
        if region and region not in ("unknown", "global"):
            nodes.setdefault(region, set()).add(c["node"])
    if not any(len(v) >= 2 for v in nodes.values()):
        blockers.append("Need observations on multiple exits in at least one identified region.")
    if len({s[4] for s in train}) < 2:
        blockers.append("Need at least two distinct observed stall rates in training.")
    return train, validation, blockers


def show_progress(args):
    observations = samples([args.directory], args.process)
    train_rows, validation, blockers = evaluate_split(observations)
    bindings = {s[0]["binding"] for s in observations}
    train_bindings = {s[0]["binding"] for s in train_rows}
    validation_bindings = {s[0]["binding"] for s in validation}
    print(f"{datetime.now().astimezone():%Y-%m-%d %H:%M:%S %z} | {args.group} | {args.process}")
    print(f"Eligible bindings: {len(bindings)}/100 | remaining: {max(0, 100-len(bindings))} | "
          f"train: {len(train_bindings)}/80 | holdout: {len(validation_bindings)}")
    pairs = {}
    for row, candidate, *_ in observations:
        key = row.get("destinationRegion") or "unknown", candidate["node"]
        pairs.setdefault(key, set()).add(row["binding"])
    for (region, node), ids in sorted(pairs.items()):
        print(f"  {region:<16} {node.strip()}   {len(ids)}")
    print("Training gates: " + (" ".join(blockers) if blockers else "ready"), flush=True)


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
    data, validation, blockers = evaluate_split(samples(args.snapshots, args.process))
    if blockers:
        raise ValueError(" ".join(blockers))
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
    collector.add_argument("--progress", action="store_true", help="report cumulative training readiness from saved snapshots")
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
