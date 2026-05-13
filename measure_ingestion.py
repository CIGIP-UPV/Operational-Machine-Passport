#!/usr/bin/env python3
"""
measure_ingestion.py
====================

Mide la latencia de ingestión y el throughput del Operational Machine
Passport consultando Prometheus, y rellena automáticamente la Table 6
del artículo (latencia y throughput por protocolo).

Cómo usarlo
-----------
1. Asegúrate de que la plataforma está corriendo y de que Prometheus
   acumula al menos `--window * --replications` minutos de datos:

       cd Operational-Machine-Passport
       docker compose up -d
       # espera al menos 60 min si vas a hacer 5 ventanas de 10 min

2. Ejecuta el script (no requiere pip install, sólo Python 3.8+):

       python3 measure_ingestion.py
       python3 measure_ingestion.py --window 10m --replications 5
       python3 measure_ingestion.py --prom-url http://localhost:9090 \\
                                    --window 10m --replications 5 \\
                                    --out-csv ingestion.csv \\
                                    --out-tex ingestion_filled.tex

Salida
------
- Tabla en consola (mean ± std a través de las réplicas).
- CSV crudo con un registro por (réplica, protocolo).
- Fichero .tex con los valores [MEASURE] sustituidos, listo para
  reemplazar el bloque de Table 6 en el manuscrito.

Métricas consultadas (todas ya las expone tu plataforma):
- asset_exporter_asset_scrape_duration_seconds  (gauge, por asset)
- asset_exporter_asset_scraped_nodes_total      (counter, por asset)
- asset_exporter_asset_scrape_success           (gauge 0/1)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
import time
from typing import Optional
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


# ----------------------------- utilidades --------------------------------- #

WINDOW_RE = re.compile(r"^(\d+)(s|m|h)$")


def parse_window(s: str) -> tuple[int, str]:
    m = WINDOW_RE.match(s)
    if not m:
        raise ValueError(f"--window debe tener formato Ns / Nm / Nh, no '{s}'")
    n, u = int(m.group(1)), m.group(2)
    seconds = n * {"s": 1, "m": 60, "h": 3600}[u]
    return seconds, s  # devolvemos también el literal para usarlo en PromQL


def prom_query(prom_url: str, query: str, t: Optional[int] = None) -> list[dict]:
    """Lanza una instant query contra Prometheus. Devuelve result list."""
    params = {"query": query}
    if t is not None:
        params["time"] = t
    url = f"{prom_url.rstrip('/')}/api/v1/query?{urlencode(params)}"
    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=15) as r:
            payload = json.loads(r.read().decode("utf-8"))
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"no se pudo consultar Prometheus en {url!r}: {e}") from e
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus devolvió error: {payload}")
    return payload["data"]["result"]


def scalar(result: list[dict]) -> float:
    if not result:
        return float("nan")
    return float(result[0]["value"][1])


# ----------------------------- experimento -------------------------------- #

def run_replication(prom_url: str, win_literal: str, win_seconds: int,
                    rep_idx: int, eval_time: int,
                    mqtt_pattern: str = ".*mqtt.*",
                    scrape_interval_s: float = 5.0) -> list[dict]:
    """Calcula las métricas de Table 6 para una ventana terminada en eval_time.

    El protocolo se distingue por asset_id: cualquier asset_id que case con
    `mqtt_pattern` (regex RE2) se considera MQTT; el resto, OPC UA.

    Throughput se calcula contando las series de señales semánticas
    publicadas por el exporter (asset_*_value: signal, sensor, status,
    production, energy, maintenance, alarm) por protocolo, dividido por
    el intervalo de scrape de Prometheus (`scrape_interval_s`, default 5 s).
    Cada serie corresponde a un dato actualizado por ciclo de scrape.
    """

    # Para latencia: filtro en asset_id sobre la métrica scrape_duration.
    # Para throughput: filtro en asset_id sobre las gauges de señal,
    # contando el número de series distintas que cumplen el patrón.
    latency_sel = {
        "opcua": f'{{asset_id!~"{mqtt_pattern}"}}',
        "mqtt":  f'{{asset_id=~"{mqtt_pattern}"}}',
    }
    signal_sel = {
        "opcua": f'{{__name__=~"asset_.+_value", asset_id!~"{mqtt_pattern}"}}',
        "mqtt":  f'{{__name__=~"asset_.+_value", asset_id=~"{mqtt_pattern}"}}',
    }

    rows: list[dict] = []
    for proto in ("opcua", "mqtt"):
        lsel = latency_sel[proto]
        ssel = signal_sel[proto]
        queries = {
            "mean_s":   f"avg by () (avg_over_time(asset_exporter_asset_scrape_duration_seconds{lsel}[{win_literal}]))",
            "p95_s":    f"avg by () (quantile_over_time(0.95, asset_exporter_asset_scrape_duration_seconds{lsel}[{win_literal}]))",
            "p99_s":    f"avg by () (quantile_over_time(0.99, asset_exporter_asset_scrape_duration_seconds{lsel}[{win_literal}]))",
            "tput_sps": f"count({ssel}) / {scrape_interval_s}",
        }
        row = {
            "replication": rep_idx,
            "protocol": proto,
            "eval_time_unix": eval_time,
        }
        for key, q in queries.items():
            row[key] = scalar(prom_query(prom_url, q, eval_time))
        rows.append(row)

    # success rate (sobre cualquier asset)
    succ_q = (
        f"avg by () (avg_over_time(asset_exporter_asset_scrape_success[{win_literal}])) * 100"
    )
    rows.append({
        "replication": rep_idx,
        "protocol": "both",
        "eval_time_unix": eval_time,
        "success_pct": scalar(prom_query(prom_url, succ_q, eval_time)),
    })
    return rows


# ----------------------------- agregación --------------------------------- #

def aggregate(rows: list[dict]) -> dict:
    """Calcula mean y std de cada métrica a través de las réplicas."""
    agg: dict = {}
    for proto in ("opcua", "mqtt"):
        proto_rows = [r for r in rows if r["protocol"] == proto]
        for metric in ("mean_s", "p95_s", "p99_s", "tput_sps"):
            values = [r[metric] for r in proto_rows
                      if metric in r and not math.isnan(r[metric])]
            if values:
                mu = statistics.fmean(values)
                sd = statistics.stdev(values) if len(values) > 1 else 0.0
            else:
                mu, sd = float("nan"), float("nan")
            agg[(proto, metric)] = (mu, sd)
    # success
    succ_values = [r["success_pct"] for r in rows
                   if r["protocol"] == "both" and "success_pct" in r
                   and not math.isnan(r["success_pct"])]
    if succ_values:
        agg[("both", "success_pct")] = (statistics.fmean(succ_values),
                                        statistics.stdev(succ_values) if len(succ_values) > 1 else 0.0)
    else:
        agg[("both", "success_pct")] = (float("nan"), float("nan"))
    return agg


# ----------------------------- formateo ----------------------------------- #

def fmt(mu: float, sd: float, digits: int = 3) -> str:
    if math.isnan(mu):
        return "n/a"
    if sd == 0 or math.isnan(sd):
        return f"{mu:.{digits}f}"
    return f"{mu:.{digits}f} $\\pm$ {sd:.{digits}f}"


def print_console(agg: dict) -> None:
    print()
    print(f"{'Metric':<28} {'Protocol':<8} {'Mean':<20} {'p95':<20} {'p99':<20}")
    print("-" * 96)
    for proto in ("opcua", "mqtt"):
        mu_m, sd_m = agg[(proto, "mean_s")]
        mu_95, sd_95 = agg[(proto, "p95_s")]
        mu_99, sd_99 = agg[(proto, "p99_s")]
        print(f"{'Scrape duration (s)':<28} {proto:<8} "
              f"{fmt(mu_m, sd_m):<20} {fmt(mu_95, sd_95):<20} {fmt(mu_99, sd_99):<20}")
    for proto in ("opcua", "mqtt"):
        mu, sd = agg[(proto, "tput_sps")]
        print(f"{'Throughput (samples/s)':<28} {proto:<8} {fmt(mu, sd, 2):<20} {'---':<20} {'---':<20}")
    mu, sd = agg[("both", "success_pct")]
    print(f"{'Scrape success rate (%)':<28} {'both':<8} {fmt(mu, sd, 2):<20} {'---':<20} {'---':<20}")
    print()


def write_csv(rows: list[dict], path: str) -> None:
    fieldnames = ["replication", "protocol", "eval_time_unix",
                  "mean_s", "p95_s", "p99_s", "tput_sps", "success_pct"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def write_tex(agg: dict, path: str) -> None:
    def row(metric, proto, dur=False, digits=3):
        if metric == "tput":
            mu, sd = agg[(proto, "tput_sps")]
            return f"{fmt(mu, sd, 2):<22}"
        elif metric == "success":
            mu, sd = agg[("both", "success_pct")]
            return f"{fmt(mu, sd, 2):<22}"
        else:
            mu, sd = agg[(proto, metric)]
            return f"{fmt(mu, sd, digits):<22}"

    with open(path, "w") as f:
        f.write("% Generated by measure_ingestion.py\n")
        f.write("% Drop-in replacement for Table 6 in OMP_new_tables.tex\n\n")
        f.write("\\begin{table}[t]\n")
        f.write("\\caption{Ingestion latency and throughput (Prometheus over the measurement window).}\n")
        f.write("\\label{tab:ingestion}\n")
        f.write("\\centering\n")
        f.write("\\footnotesize\n")
        f.write("\\begin{tabular}{p{3.3cm} c c c c}\n")
        f.write("\\toprule\n")
        f.write("\\textbf{Metric} & \\textbf{Protocol} & \\textbf{Mean} & \\textbf{p95} & \\textbf{p99} \\\\\n")
        f.write("\\midrule\n")
        for proto in ("opcua", "mqtt"):
            label = "OPC UA" if proto == "opcua" else "MQTT"
            f.write(f"Scrape duration / asset (s) & {label} & "
                    f"{row('mean_s', proto)} & "
                    f"{row('p95_s', proto)} & "
                    f"{row('p99_s', proto)} \\\\\n")
        for proto in ("opcua", "mqtt"):
            label = "OPC UA" if proto == "opcua" else "MQTT"
            f.write(f"Ingested samples (samples/s) & {label} & {row('tput', proto)} & --- & --- \\\\\n")
        f.write(f"Scrape success rate (\\%) & Both & {row('success', 'both')} & --- & --- \\\\\n")
        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")
        f.write("\\end{table}\n")


# ----------------------------- entrypoint --------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--prom-url", default="http://localhost:9090",
                    help="Prometheus base URL (default: http://localhost:9090)")
    ap.add_argument("--window", default="10m",
                    help="Tamaño de cada ventana de réplica (e.g. 5m, 10m, 1h)")
    ap.add_argument("--replications", type=int, default=5,
                    help="Número de ventanas consecutivas a evaluar (default: 5)")
    ap.add_argument("--out-csv", default="ingestion_results.csv",
                    help="Fichero CSV de salida")
    ap.add_argument("--out-tex", default="ingestion_table_filled.tex",
                    help="Snippet LaTeX con valores rellenados")
    ap.add_argument("--mqtt-pattern", default=".*mqtt.*",
                    help="Regex RE2 que identifica asset_ids del protocolo MQTT; "
                         "el resto se considera OPC UA (default: .*mqtt.*)")
    ap.add_argument("--scrape-interval", type=float, default=5.0,
                    help="Intervalo de scrape de Prometheus en segundos, usado "
                         "para convertir scraped_nodes (gauge) en throughput "
                         "(default: 5.0)")
    args = ap.parse_args()

    win_s, win_literal = parse_window(args.window)
    now = int(time.time())

    # ¿Hay datos suficientes hacia atrás?
    needed = win_s * args.replications
    print(f"[i] Evaluating last {args.replications} windows of {args.window} "
          f"(= {needed//60} min) ending at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))}.")
    print(f"[i] Prometheus URL: {args.prom_url}")

    # Smoke test: ¿hay alguna métrica de exporter?
    smoke = prom_query(args.prom_url, "count(asset_exporter_asset_scrape_success)", now)
    if not smoke or scalar(smoke) == 0:
        print("[!] No 'asset_exporter_asset_scrape_success' samples found. "
              "¿Está corriendo el exporter? ¿Prometheus lo está scrapeando?",
              file=sys.stderr)
        return 2

    # Descubrir qué asset_ids hay y clasificarlos por protocolo
    import re as _re
    ids_result = prom_query(args.prom_url,
                            "group by (asset_id) (asset_exporter_asset_scrape_success)",
                            now)
    all_ids = sorted(r["metric"].get("asset_id", "") for r in ids_result)
    rx = _re.compile(args.mqtt_pattern)
    mqtt_ids  = [a for a in all_ids if rx.fullmatch(a)]
    opcua_ids = [a for a in all_ids if not rx.fullmatch(a)]
    print(f"[i] OPC UA assets ({len(opcua_ids)}): {', '.join(opcua_ids) or '(none)'}")
    print(f"[i] MQTT  assets ({len(mqtt_ids)}): {', '.join(mqtt_ids) or '(none)'}")
    if not opcua_ids and not mqtt_ids:
        print("[!] No asset_ids found at all. Aborting.", file=sys.stderr)
        return 2
    if not opcua_ids:
        print("[!] Warning: no OPC UA asset_ids matched. "
              "Comprueba --mqtt-pattern.", file=sys.stderr)
    if not mqtt_ids:
        print("[!] Warning: no MQTT asset_ids matched. "
              "Comprueba --mqtt-pattern.", file=sys.stderr)

    all_rows: list[dict] = []
    for i in range(args.replications):
        # ventanas no solapadas, en orden cronológico
        offset = (args.replications - 1 - i) * win_s
        t = now - offset
        print(f"[i] Replication {i+1}/{args.replications} "
              f"(end={time.strftime('%H:%M:%S', time.localtime(t))})…")
        try:
            all_rows.extend(run_replication(args.prom_url, win_literal, win_s,
                                            i + 1, t, args.mqtt_pattern,
                                            args.scrape_interval))
        except Exception as e:
            print(f"[!] Replication {i+1} failed: {e}", file=sys.stderr)

    write_csv(all_rows, args.out_csv)
    print(f"[ok] CSV escrito en {args.out_csv}")

    agg = aggregate(all_rows)
    print_console(agg)

    write_tex(agg, args.out_tex)
    print(f"[ok] LaTeX (Table 6 rellena) escrito en {args.out_tex}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
