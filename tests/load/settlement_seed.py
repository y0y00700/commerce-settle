#!/usr/bin/env python3
"""Seed payable settlement items and measure the daily settlement batch on the local Compose stack.

Rows are generated inside PostgreSQL with generate_series so large profiles do not stream over the
network. IDs are md5 values of the run ID, so a run can be inspected and removed without a marker
column. Only the local Compose project is supported; results go to tests/load/work/settlement-<run>/.
"""

import argparse
import datetime as dt
import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORK = ROOT / "tests" / "load" / "work"
CREDENTIAL = WORK / "settlement-perf.credential"
OVERLAY = ROOT / "tests" / "load" / "compose.settlement-perf.yml"
KST = dt.timezone(dt.timedelta(hours=9))
TRIGGER_PATH = "/internal/perf/daily-settlement-runs"

PROFILES = {
    "small": {"sellers": 1_000, "items": 10_000, "skew": False},
    "medium": {"sellers": 5_000, "items": 100_000, "skew": True},
    "large": {"sellers": 20_000, "items": 270_000, "skew": True, "max_share": 0.10},
}
TOP_SELLER_RATIO = 0.01
TOP_ITEM_SHARE = 0.70
DEFAULT_PRODUCTS = 5
LARGEST_SELLER_PRODUCTS = 50
PHASES = ("existing", "products", "context", "reserve")
RUN_ID = re.compile(r"[A-Za-z0-9_-]{1,40}")


# ---------------------------------------------------------------------------------------------
# Pure helpers (unit-tested)
# ---------------------------------------------------------------------------------------------

def spread(total, count):
    """Split total into count non-increasing integers that differ by at most one."""
    if count <= 0:
        return []
    base, rest = divmod(total, count)
    return [base + 1 if i < rest else base for i in range(count)]


def item_counts(sellers, items, skew, max_share=None):
    """Items per seller. Skew gives the top 1% of sellers 70% of items; max_share pins the largest seller."""
    if sellers <= 0 or items < sellers:
        raise ValueError("Every seller needs at least one item")
    if not skew:
        return spread(items, sellers)
    top = max(1, round(sellers * TOP_SELLER_RATIO))
    top_items = round(items * TOP_ITEM_SHARE)
    tail = items - top_items
    if tail < sellers - top:
        raise ValueError("Too few items for the tail sellers")
    if max_share is None or top == 1:
        head = spread(top_items, top)
    else:
        largest = round(items * max_share)
        if largest > top_items or top_items - largest < top - 1:
            raise ValueError("max_share does not fit inside the top-seller share")
        head = [largest] + spread(top_items - largest, top - 1)
    return head + spread(tail, sellers - top)


def product_counts(counts, fixed=None):
    """Products per seller. Default: 5 each and 50 for the largest seller, never more than its items."""
    if fixed is not None:
        return [min(c, fixed) for c in counts]
    largest = max(range(len(counts)), key=lambda i: counts[i])
    return [min(c, LARGEST_SELLER_PRODUCTS if i == largest else DEFAULT_PRODUCTS)
            for i, c in enumerate(counts)]


def cutoff_of(batch_date):
    """Same rule as SettlementBatch.cutoffOf: 10:00 Asia/Seoul of batchDate."""
    return dt.datetime.combine(batch_date, dt.time(10, 0), KST)


_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)')
_LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_prometheus(text, prefix):
    """Map (name, frozenset(labels)) -> value for samples whose name starts with prefix."""
    samples = {}
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        match = _SAMPLE.match(line)
        if not match:
            continue
        labels = frozenset(_LABEL.findall(match.group(2) or ""))
        samples[(match.group(1), labels)] = float(match.group(3))
    return samples


def _labels_without(labels, key):
    return frozenset((k, v) for k, v in labels if k != key)


def histogram_delta(before, after, base, select):
    """count/sum/bucket deltas of one Prometheus histogram series, picked by a label predicate."""
    def pick(samples, suffix):
        return {labels: value for (name, labels), value in samples.items()
                if name == base + suffix and select(dict(labels))}
    result = {}
    for suffix in ("_count", "_sum"):
        now, old = pick(after, suffix), pick(before, suffix)
        result[suffix[1:]] = sum(now.values()) - sum(old.values())
    buckets = {}
    now, old = pick(after, "_bucket"), pick(before, "_bucket")
    for labels, value in now.items():
        le = dict(labels)["le"]
        delta = value - old.get(labels, 0.0)
        key = float("inf") if le == "+Inf" else float(le)
        buckets[key] = buckets.get(key, 0.0) + delta
    result["buckets"] = sorted(buckets.items())
    return result


def quantile(buckets, q):
    """Upper bound of the cumulative bucket that holds quantile q, or None without samples."""
    if not buckets or buckets[-1][1] <= 0:
        return None
    target = q * buckets[-1][1]
    for upper, cumulative in buckets:
        if cumulative >= target:
            return upper
    return buckets[-1][0]


def summarize(delta, unit=1.0):
    count = delta["count"]
    return {
        "count": int(count),
        "sum": round(delta["sum"] * unit, 3),
        "mean": round(delta["sum"] / count * unit, 3) if count else None,
        **{f"p{int(q * 100)}": (None if (v := quantile(delta["buckets"], q)) is None
                                else (v if v == float("inf") else round(v * unit, 3)))
           for q in (0.5, 0.95, 0.99)},
    }


# ---------------------------------------------------------------------------------------------
# Compose access
# ---------------------------------------------------------------------------------------------

def compose(*args, input_text=None, timeout=600):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, input=input_text, text=True,
                          capture_output=True, timeout=timeout, check=True)


def sql(database, statement, timeout=600):
    try:
        return compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", database, "-At",
                       "-v", "ON_ERROR_STOP=1", input_text=statement, timeout=timeout).stdout.strip()
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"psql failed on {database}: {error.stderr.strip()[-2000:]}") from None


def settlement_http(method, url, body=None, credential=None):
    """Call Settlement inside its container. The credential is passed on stdin, never as an argument."""
    command = ["exec", "-T", "settlement", "curl", "-sS", "-o", "-", "-w", "\n%{http_code}"]
    stdin = None
    if credential is not None:
        command += ["-H", "@-"]
        stdin = f"Authorization: Bearer {credential}\n"
    if body is not None:
        command += ["-H", "Content-Type: application/json", "--data-binary", json.dumps(body)]
    command += ["-X", method, url]
    out = compose(*command, input_text=stdin, timeout=60).stdout
    text, _, status = out.rpartition("\n")
    return int(status), text


def scrape():
    status, text = settlement_http("GET", "http://localhost:9091/actuator/prometheus")
    if status != 200:
        raise RuntimeError(f"Prometheus scrape failed with HTTP {status}")
    return parse_prometheus(text, "settlement_batch_seller")


def credential():
    if not CREDENTIAL.exists():
        WORK.mkdir(parents=True, exist_ok=True)
        fd = os.open(CREDENTIAL, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(secrets.token_urlsafe(48))
    return CREDENTIAL.read_text().strip()


def ids(prefix, kind, *parts):
    joined = " || ':' || ".join(parts)
    return f"md5('{prefix}:{kind}:' || {joined})::uuid"


def int_array(values):
    return "'{" + ",".join(str(v) for v in values) + "}'::int[]"


def run_dir(run_id):
    return WORK / f"settlement-{run_id}"


def write_json(path, value):
    """Strict JSON: +Inf quantiles become the string "+Inf" instead of a non-standard Infinity."""
    def strict(v):
        if isinstance(v, float) and v == float("inf"):
            return "+Inf"
        if isinstance(v, dict):
            return {k: strict(x) for k, x in v.items()}
        if isinstance(v, list):
            return [strict(x) for x in v]
        return v
    path.write_text(json.dumps(strict(value), indent=2, allow_nan=False) + "\n")


def load_seed(run_id):
    if not RUN_ID.fullmatch(run_id):
        raise SystemExit("run-id must match [A-Za-z0-9_-]{1,40}")
    path = run_dir(run_id) / "seed.json"
    if not path.exists():
        raise SystemExit(f"No seed for run {run_id}: {path}")
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------------------

def seller_rows(prefix, counts, products):
    return (f"SELECT n, c, p, {ids(prefix, 's', 'n')} AS seller_id "
            f"FROM unnest({int_array(counts)}, {int_array(products)}) WITH ORDINALITY AS t(c, p, n)")


def seller_ids_sql(seed):
    return (f"SELECT {ids(seed['prefix'], 's', 'n')} FROM generate_series(1, {seed['sellers']}) n")


def cmd_up(args):
    env_line = f"PERF_OPERATOR_CREDENTIAL={credential()}"
    print("Run in a separate tmux session (build can take minutes):")
    print(f"  {env_line.split('=')[0]}=$(cat {CREDENTIAL.relative_to(ROOT)}) "
          f"docker compose -f compose.yml -f {OVERLAY.relative_to(ROOT)} up -d --build --wait "
          "--wait-timeout 300")


def cmd_seed(args):
    profile = dict(PROFILES[args.profile]) if args.profile else {"skew": False}
    if args.sellers:
        profile["sellers"] = args.sellers
    if args.items:
        profile["items"] = args.items
    if "sellers" not in profile or "items" not in profile:
        raise SystemExit("Choose --profile or both --sellers and --items")
    counts = item_counts(profile["sellers"], profile["items"], profile["skew"], profile.get("max_share"))
    products = product_counts(counts, args.products_per_seller)
    run_id = args.run_id or dt.datetime.now(KST).strftime("%Y%m%d%H%M%S")
    if not RUN_ID.fullmatch(run_id):
        raise SystemExit("run-id must match [A-Za-z0-9_-]{1,40}")
    prefix = f"perf:{run_id}"
    batch_date = dt.date.fromisoformat(args.batch_date)
    cutoff = cutoff_of(batch_date)
    if cutoff > dt.datetime.now(KST):
        raise SystemExit("batch-date cutoff must already have passed")
    out = run_dir(run_id)
    if out.exists():
        raise SystemExit(f"Run directory already exists: {out}")

    seed = {
        "runId": run_id, "prefix": prefix, "profile": args.profile, "batchDate": batch_date.isoformat(),
        "cutoffAt": cutoff.isoformat(), "sellers": len(counts), "items": sum(counts),
        "skew": profile["skew"], "maxShare": profile.get("max_share"),
        "largestSellerItems": max(counts), "products": sum(products),
        "largestSellerProducts": products[counts.index(max(counts))],
        "productsPerSellerOverride": args.products_per_seller,
        "state": "SEEDING", "createdAt": dt.datetime.now(KST).isoformat(),
        "gitSha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
                                 capture_output=True).stdout.strip(),
    }
    # Written first so `clean` can remove a partially seeded run.
    out.mkdir(parents=True)
    write_json(out / "seed.json", seed)
    rows = seller_rows(prefix, counts, products)
    started = time.monotonic()
    sql("commerce", f"""
        BEGIN;
        INSERT INTO commerce.seller_accounts (id, email_normalized, password_hash, display_name, status,
            auth_version, payout_account_ref, created_at, updated_at, version)
        SELECT seller_id, 'perf-{run_id}-' || n || '@seed.local', 'perf-seed-no-login', 'perf seller ' || n,
            'ACTIVE', 0, 'perf-acct-{run_id}-' || n, now(), now(), 0
        FROM ({rows}) s;
        INSERT INTO commerce.products (id, seller_id, name, unit_price, status, created_at, updated_at, version)
        SELECT {ids(prefix, 'p', 'n', 'k')}, seller_id, 'perf-{run_id}-' || n || '-' || k, 10000, 'STOPPED',
            now(), now(), 0
        FROM ({rows}) s, generate_series(1, s.p) k;
        COMMIT;""")
    sql("settlement", f"""
        BEGIN;
        INSERT INTO settlement.settlement_items (id, payment_id, order_id, product_id, seller_id,
            eligible_received_at, ready_at, total_amount, platform_coupon_amount, settlement_base_amount,
            external_amount, mileage_amount, fee_amount, net_amount, status, purchase_confirmed_at,
            order_version, payout_id, created_at, updated_at, version)
        SELECT {ids(prefix, 'i', 'n', 'j')}, {ids(prefix, 'pay', 'n', 'j')}, {ids(prefix, 'ord', 'n', 'j')},
            {ids(prefix, 'p', 'n', '(1 + (j - 1) % s.p)')}, s.seller_id,
            x.r - interval '1 day', x.r, x.a, 0, x.a, x.a, 0, x.a * 15 / 100, x.a - x.a * 15 / 100,
            'PAYABLE', x.r, 0, NULL, x.r - interval '8 days', x.r, 0
        FROM ({rows}) s, generate_series(1, s.c) j,
            LATERAL (SELECT timestamptz '{cutoff.isoformat()}'
                         - make_interval(secs => 1 + ((s.n * 7919 + j::bigint * 104729) % 86000)) AS r,
                     (1000 + ((s.n * 31 + j::bigint * 17) % 991) * 100)::bigint AS a) x;
        COMMIT;
        VACUUM ANALYZE settlement.settlement_items;""", timeout=1800)
    elapsed = time.monotonic() - started

    seed.update(state="SEEDED", seedSeconds=round(elapsed, 1))
    write_json(out / "seed.json", seed)
    print(json.dumps(seed, indent=2))


def job_status(batch_date):
    row = sql("settlement", f"""
        SELECT e.status, e.exit_code, e.start_time, e.end_time
        FROM settlement.batch_job_execution e
        JOIN settlement.batch_job_execution_params p ON p.job_execution_id = e.job_execution_id
        WHERE p.parameter_name = 'batchDate' AND p.parameter_value = '{batch_date}'
        ORDER BY e.job_execution_id DESC LIMIT 1""")
    return row.split("|") if row else None


def cmd_run(args):
    seed = load_seed(args.run_id)
    if seed.get("state", "SEEDED") != "SEEDED":
        raise SystemExit("Seeding did not finish; clean this run and seed again")
    batch_date = seed["batchDate"]
    if job_status(batch_date):
        raise SystemExit(f"A daily batch already exists for {batch_date}; seed a new batch date")
    foreign = sql("settlement", f"""
        SELECT count(*) FROM settlement.settlement_items
        WHERE status = 'PAYABLE' AND payout_id IS NULL AND ready_at < timestamptz '{seed['cutoffAt']}'
          AND seller_id NOT IN ({seller_ids_sql(seed)})""")
    if int(foreign):
        raise SystemExit(f"{foreign} payable items outside this run would join the batch; stop")
    before = scrape()
    status, body = settlement_http("POST", f"http://localhost:8080{TRIGGER_PATH}",
                                   {"batchDate": batch_date}, credential())
    if status != 202:
        raise SystemExit(f"Trigger failed with HTTP {status}: {body[:300]}")
    started = time.monotonic()
    while True:
        state = job_status(batch_date)
        # 202 is only an acceptance; demo mode or a held executor lock leave no execution.
        if state is None and time.monotonic() - started > 30:
            raise SystemExit("No job execution appeared within 30s; check settlement logs")
        if state and state[0] in ("COMPLETED", "FAILED", "STOPPED", "ABANDONED"):
            break
        if time.monotonic() - started > args.timeout:
            raise SystemExit(f"Batch did not finish within {args.timeout}s (last: {state})")
        time.sleep(1)
    after = scrape()
    phases = {}
    for phase in PHASES:
        delta = histogram_delta(before, after, "settlement_batch_seller_phase_seconds",
                                lambda labels, p=phase: labels.get("phase") == p)
        phases[phase] = summarize(delta, unit=1000.0)
    per_seller = {}
    for name in ("products", "items"):
        delta = histogram_delta(before, after, f"settlement_batch_seller_{name}", lambda labels: True)
        per_seller[name] = summarize(delta)
    step = sql("settlement", f"""
        SELECT s.read_count, s.write_count, s.write_skip_count,
               extract(epoch FROM s.end_time - s.start_time)
        FROM settlement.batch_step_execution s
        JOIN settlement.batch_job_execution_params p ON p.job_execution_id = s.job_execution_id
        WHERE p.parameter_name = 'batchDate' AND p.parameter_value = '{batch_date}'
        ORDER BY s.step_execution_id DESC LIMIT 1""").split("|")
    job_seconds = float(sql("settlement", f"""
        SELECT extract(epoch FROM e.end_time - e.start_time) FROM settlement.batch_job_execution e
        JOIN settlement.batch_job_execution_params p ON p.job_execution_id = e.job_execution_id
        WHERE p.parameter_name = 'batchDate' AND p.parameter_value = '{batch_date}'
        ORDER BY e.job_execution_id DESC LIMIT 1"""))
    phase_ms = sum(p["sum"] for p in phases.values())
    result = {
        "runId": seed["runId"], "batchDate": batch_date, "status": state[0], "exitCode": state[1],
        "jobSeconds": round(job_seconds, 3),
        "sellersRead": int(step[0]), "sellersWritten": int(step[1]), "sellersSkipped": int(step[2]),
        # Phase counts exceed sellersRead when sellers are skipped: Spring Batch re-runs the writer
        # for a skipped chunk while scanning it, so each skip records its phases twice.
        "phasesMs": phases,
        # reader paging, chunk bookkeeping and Spring Batch metadata writes
        "outsidePhasesMs": round(job_seconds * 1000 - phase_ms, 1),
        "perSeller": per_seller,
        "conditions": conditions(),
    }
    write_json(run_dir(seed["runId"]) / "run.json", result)
    print(json.dumps(result, indent=2))
    if state[0] != "COMPLETED":
        sys.exit(1)


def conditions():
    env = json.loads(compose("exec", "-T", "settlement", "sh", "-c",
                             "printf '{\"javaToolOptions\":\"%s\",\"dbPool\":\"%s\",\"sampling\":\"%s\","
                             "\"payoutWorker\":\"%s\"}' \"$JAVA_TOOL_OPTIONS\" \"$SETTLEMENT_DB_POOL_SIZE\" "
                             "\"$MANAGEMENT_TRACING_SAMPLING_PROBABILITY\" \"$SETTLEMENT_PAYOUT_WORKER_ENABLED\"",
                             timeout=30).stdout)
    inspect = subprocess.run(["docker", "inspect", "-f", "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}",
                              compose("ps", "-q", "settlement").stdout.strip()],
                             text=True, capture_output=True).stdout.split()
    pool = parse_prometheus(settlement_http("GET", "http://localhost:9091/actuator/prometheus")[1],
                            "hikaricp_connections_max")
    env["dbPool"] = int(max(pool.values())) if pool else None
    env["memoryLimitBytes"] = int(inspect[0]) if inspect else None
    env["nanoCpus"] = int(inspect[1]) if len(inspect) > 1 else None
    return env


def cmd_report(args):
    seed = load_seed(args.run_id)
    sellers = seller_ids_sql(seed)
    checks = {
        "itemsNotReserved": f"""SELECT count(*) FROM settlement.settlement_items
            WHERE seller_id IN ({sellers})
              AND (status NOT IN ('PAYOUT_RESERVED', 'PAID') OR payout_id IS NULL)""",
        "itemCountDifference": f"""SELECT count(*) - {seed['items']} FROM settlement.settlement_items
            WHERE seller_id IN ({sellers})""",
        "reservationMismatch": f"""SELECT count(*) FROM settlement.settlement_items i
            WHERE i.seller_id IN ({sellers}) AND (SELECT count(*) FROM settlement.settlement_reservations r
                WHERE r.item_id = i.id AND r.payout_id = i.payout_id AND r.amount = i.net_amount) <> 1""",
        "batchAmountMismatch": f"""SELECT count(*) FROM settlement.settlement_batches b
            JOIN settlement.payouts p ON p.batch_id = b.id
            WHERE b.seller_id IN ({sellers}) AND b.net_amount <> p.amount""",
        "itemsWithForeignPayout": f"""SELECT count(*) FROM settlement.settlement_items i
            LEFT JOIN settlement.payouts p ON p.id = i.payout_id
            WHERE i.seller_id IN ({sellers}) AND (p.id IS NULL OR p.seller_id <> i.seller_id)""",
        "payoutAmountMismatch": f"""SELECT count(*) FROM settlement.payouts p
            WHERE p.seller_id IN ({sellers}) AND p.amount <> (SELECT coalesce(sum(net_amount), 0)
                FROM settlement.settlement_items i WHERE i.payout_id = p.id)""",
        "sellersWithoutOneBatch": f"""SELECT count(*) FROM ({sellers}) s(id)
            WHERE (SELECT count(*) FROM settlement.settlement_batches b
                   WHERE b.seller_id = s.id AND b.batch_date = date '{seed['batchDate']}') <> 1""",
        "payoutsWithoutOneJob": f"""SELECT count(*) FROM settlement.payouts p
            WHERE p.seller_id IN ({sellers})
              AND (SELECT count(*) FROM settlement.payout_jobs j WHERE j.payout_id = p.id) <> 1""",
        "netAmountDifference": f"""SELECT (SELECT coalesce(sum(net_amount), 0) FROM settlement.settlement_items
                WHERE seller_id IN ({sellers}))
              - (SELECT coalesce(sum(amount), 0) FROM settlement.payouts WHERE seller_id IN ({sellers}))""",
    }
    result = {name: int(sql("settlement", query)) for name, query in checks.items()}
    result["passed"] = all(value == 0 for value in result.values())
    write_json(run_dir(seed["runId"]) / "report.json", result)
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        sys.exit(1)


def cmd_clean(args):
    seed = load_seed(args.run_id)
    sellers = seller_ids_sql(seed)
    sql("settlement", f"""
        BEGIN;
        DELETE FROM settlement.outbox WHERE aggregate_id IN
            (SELECT id FROM settlement.settlement_batches WHERE seller_id IN ({sellers}));
        DELETE FROM settlement.settlement_reservations WHERE item_id IN
            (SELECT id FROM settlement.settlement_items WHERE seller_id IN ({sellers}));
        DELETE FROM settlement.settlement_entries WHERE item_id IN
            (SELECT id FROM settlement.settlement_items WHERE seller_id IN ({sellers}));
        DELETE FROM settlement.payout_job_replays WHERE job_id IN
            (SELECT j.id FROM settlement.payout_jobs j JOIN settlement.payouts p ON p.id = j.payout_id
             WHERE p.seller_id IN ({sellers}));
        DELETE FROM settlement.payout_jobs WHERE payout_id IN
            (SELECT id FROM settlement.payouts WHERE seller_id IN ({sellers}));
        DELETE FROM settlement.payouts WHERE seller_id IN ({sellers});
        DELETE FROM settlement.settlement_batches WHERE seller_id IN ({sellers});
        DELETE FROM settlement.settlement_items WHERE seller_id IN ({sellers});
        COMMIT;""", timeout=1800)
    if args.commerce:
        sql("commerce", f"""
            BEGIN;
            DELETE FROM commerce.products WHERE seller_id IN ({sellers});
            DELETE FROM commerce.seller_accounts WHERE id IN ({sellers});
            COMMIT;""")
    # Spring Batch metadata is kept as run history.
    print(f"Removed settlement rows of run {seed['runId']}"
          + (" and its commerce sellers/products" if args.commerce else ""))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up", help="print the Compose command with the perf overlay").set_defaults(func=cmd_up)
    seed = sub.add_parser("seed", help="create sellers, products and payable items")
    seed.add_argument("--profile", choices=sorted(PROFILES))
    seed.add_argument("--sellers", type=int)
    seed.add_argument("--items", type=int)
    seed.add_argument("--products-per-seller", type=int)
    seed.add_argument("--batch-date", required=True, help="YYYY-MM-DD whose 10:00 KST cutoff has passed")
    seed.add_argument("--run-id")
    seed.set_defaults(func=cmd_seed)
    run = sub.add_parser("run", help="start the daily batch for a seeded run and wait")
    run.add_argument("--run-id", required=True)
    run.add_argument("--timeout", type=int, default=3600)
    run.set_defaults(func=cmd_run)
    report = sub.add_parser("report", help="check amounts and assignments of a finished run")
    report.add_argument("--run-id", required=True)
    report.set_defaults(func=cmd_report)
    clean = sub.add_parser("clean", help="delete settlement rows of a run")
    clean.add_argument("--run-id", required=True)
    clean.add_argument("--commerce", action="store_true", help="also delete seeded sellers and products")
    clean.set_defaults(func=cmd_clean)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
