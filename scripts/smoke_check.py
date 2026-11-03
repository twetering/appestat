#!/usr/bin/env python3
"""
Smoke check: render every page and call every parameterless GET endpoint against the
real database, and print the headline numbers. Used before/after refactors to make
sure nothing that used to work is broken.

Usage:
    venv/bin/python scripts/smoke_check.py                # print report
    venv/bin/python scripts/smoke_check.py --save base    # store as data/cache/smoke_base.json
    venv/bin/python scripts/smoke_check.py --compare base # diff against a stored run
"""

import argparse
import contextlib
import io
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def collect():
    with contextlib.redirect_stdout(io.StringIO()):  # migrations print on import
        import app as appmod
    client = appmod.app.test_client()
    year = str(datetime.now().year)

    statuses = {}
    rules = sorted(appmod.app.url_map.iter_rules(), key=lambda r: r.rule)
    for rule in rules:
        if "GET" not in rule.methods or rule.arguments or rule.rule.startswith("/static"):
            continue
        if rule.rule.startswith("/api/ah/"):
            continue  # talks to Albert Heijn
        statuses[rule.rule] = client.get(rule.rule).status_code

    first_invoice = client.get("/api/invoices").get_json()[0]["id"]
    for path in (f"/invoice/{first_invoice}", f"/api/invoices/{first_invoice}",
                 f"/api/analysis?year={year}", f"/api/budget/dashboard?year={year}"):
        statuses[path] = client.get(path).status_code

    analysis = client.get(f"/api/analysis?year={year}").get_json()
    summary = analysis.get("summary", {})
    metrics = {
        "invoices_total": len(client.get("/api/invoices").get_json()),
        f"spent_{year}": summary.get("total_spent"),
        f"invoices_{year}": summary.get("total_invoices"),
        f"savings_{year}": summary.get("total_savings"),
        "categories": len(client.get("/api/categories").get_json()),
        "catalog_products": len(client.get("/api/catalog?limit=5000").get_json()),
    }
    return {"statuses": statuses, "metrics": metrics}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--save")
    parser.add_argument("--compare")
    args = parser.parse_args()

    result = collect()
    failing = {p: s for p, s in result["statuses"].items() if s >= 400}
    print(f"{len(result['statuses'])} routes checked, {len(failing)} failing")
    for path, status in failing.items():
        print(f"  ✗ {status} {path}")
    for k, v in result["metrics"].items():
        print(f"  {k}: {v}")

    cache = ROOT / "data" / "cache"
    if args.save:
        cache.mkdir(parents=True, exist_ok=True)
        (cache / f"smoke_{args.save}.json").write_text(json.dumps(result, indent=2))
    if args.compare:
        base = json.loads((cache / f"smoke_{args.compare}.json").read_text())
        for path, status in base["statuses"].items():
            now = result["statuses"].get(path)
            if now != status:
                print(f"  ≠ {path}: {status} -> {now}")
        for k, v in base["metrics"].items():
            if result["metrics"].get(k) != v:
                print(f"  ≠ {k}: {v} -> {result['metrics'].get(k)}")
    sys.exit(1 if failing else 0)


if __name__ == "__main__":
    main()
