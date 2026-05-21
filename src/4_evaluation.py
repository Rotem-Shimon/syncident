"""
Phase 5 — Evaluation: compares grouping methods using exported incident artifacts.

Persistence uses the Split-Artifact Export Pattern so large benchmarks do not require
loading one combined JSON blob into the frontend:
  - incidents_summary.json: lightweight per-incident metadata index (no raw payloads)
  - incident_events_mapping.json: incident_id -> full raw event payload arrays

FR10 (incident summaries): upstream steps already emit each incident’s time range, event
count, and involved components (CSV). This module does not re-derive those fields per row;
it *aggregates* them so you can defend whether the **primary** method (AlertFusion) reshapes
noise differently than the **reference** method (baseline)—i.e. whether summaries are
tighter, broader, or more component-coherent in aggregate.

FR13 (Noise Reduction Ratio): the project’s headline quantitative goal is compressing the
event stream into fewer operator-facing units without hiding distinct failures. NRR pairs
directly with that narrative: it is monotone in incident count relative to a fixed
N_raw_logs—so you can quote one number per method and argue trade-offs against baseline
(FR14) under the same event denominator.

Qualitative contrast (why avg duration & avg components): NRR alone does not say *how*
incidents differ structurally. Mean duration reflects whether a method tends to fuse events
into **longer episodic spans** (calendar buckets vs correlation-driven chains). Mean
components-per-incident proxies how **wide** each summary is on the shared-component axis
from the normalized schema—useful for arguing over-merge risk vs noise collapse in defense.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import mmap
import sys
from pathlib import Path
from typing import Any

import pandas as pd

# Split-artifact filenames (lightweight index vs. heavy per-incident event payloads).
INCIDENTS_SUMMARY_FILENAME = "incidents_summary.json"
INCIDENT_EVENTS_MAPPING_FILENAME = "incident_events_mapping.json"
METHOD_ALERT_FUSION = "alert_fusion"
METHOD_BASELINE = "baseline"
_ARTIFACT_VERSION = 1


def noise_reduction_ratio(n_incidents: int, n_raw_logs: int) -> float:
    """
    FR13 KPI: NRR = 1 - N_incidents / N_raw_logs.

    Interpretation for defense: when N_incidents << N_raw_logs, NRR approaches 1—more of the
    stream is absorbed into fewer incidents. Holding N_raw_logs fixed (same normalized
    events) makes this a clean paired comparison between baseline and AlertFusion; it is the
    single scalar reviewers expect when you claim noise reduction without dataset drift.
    """
    if n_raw_logs <= 0:
        raise ValueError("n_raw_logs must be positive for NRR.")
    return 1.0 - (n_incidents / n_raw_logs)


def incident_duration_seconds(df: pd.DataFrame) -> pd.Series:
    """
    Uses FR10’s reported start/end bounds per incident. Averaging spans exposes whether a
    method produces **shorter-lived** incidents (many small windows) vs **longer fused**
    episodes—important because aggressive compression can inflate duration if unrelated
    events sit in the same cluster.
    """
    start = pd.to_datetime(df["start_time"])
    end = pd.to_datetime(df["end_time"])
    return (end - start).dt.total_seconds()


def components_per_incident(df: pd.DataFrame) -> pd.Series:
    """Count distinct block ids listed in involved_components (semicolon-separated)."""
    col = df["involved_components"].fillna("").astype(str)

    def count_cell(s: str) -> int:
        parts = [p for p in s.split(";") if p.strip()]
        return len(parts)

    return col.map(count_cell)


def summarize_method(
    df: pd.DataFrame,
    n_raw_logs: int,
    method_name: str,
) -> dict[str, Any]:
    """Package FR13 + FR10-derived shape statistics for one grouping output (JSON + tables)."""
    n_inc = len(df)
    durs = incident_duration_seconds(df)
    comps = components_per_incident(df)
    avg_dur = float(durs.mean()) if not durs.empty else 0.0
    avg_comps = float(comps.mean()) if not comps.empty else 0.0
    if pd.isna(avg_dur):
        avg_dur = 0.0
    if pd.isna(avg_comps):
        avg_comps = 0.0
    return {
        "method": method_name,
        "n_incidents": n_inc,
        "nrr": round(noise_reduction_ratio(n_inc, n_raw_logs), 6),
        "avg_duration_seconds": round(avg_dur, 4),
        "avg_components_per_incident": round(avg_comps, 4),
    }


def _safe_component_set(raw: Any) -> set[str]:
    if raw is None:
        return set()
    if isinstance(raw, (set, tuple)):
        return {str(x).strip() for x in raw if str(x).strip()}
    if isinstance(raw, list):
        return {str(x).strip() for x in raw if str(x).strip()}
    text = str(raw).strip()
    if not text:
        return set()
    if ";" in text:
        parts = text.split(";")
    elif "," in text:
        parts = text.split(",")
    else:
        parts = [text]
    return {p.strip() for p in parts if p.strip()}


def load_ground_truth(path: Path) -> tuple[pd.DataFrame, int]:
    """
    Load fault windows from JSON and normalize to timestamps.

    Expected primary format: parallel arrays with at least:
    - timestamp (epoch seconds)
    - duration (seconds)
    Optional component hints:
    - service (list/string)
    - cmdb_id (list/string)
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    skipped = 0

    if isinstance(payload, dict) and isinstance(payload.get("timestamp"), list):
        ts_list = payload.get("timestamp", [])
        dur_list = payload.get("duration", [])
        svc_list = payload.get("service", [])
        typ_list = payload.get("failure_type", [])
        cmdb_list = payload.get("cmdb_id", [])
        n = max(len(ts_list), len(dur_list), len(svc_list), len(typ_list), len(cmdb_list))

        for i in range(n):
            ts_raw = ts_list[i] if i < len(ts_list) else None
            dur_raw = dur_list[i] if i < len(dur_list) else None
            svc_raw = svc_list[i] if i < len(svc_list) else None
            typ_raw = typ_list[i] if i < len(typ_list) else None
            cmdb_raw = cmdb_list[i] if i < len(cmdb_list) else None

            ts = pd.to_datetime(ts_raw, unit="s", errors="coerce")
            dur_sec = pd.to_numeric(dur_raw, errors="coerce")
            if pd.isna(ts) or pd.isna(dur_sec) or float(dur_sec) < 0:
                skipped += 1
                continue

            services = _safe_component_set(svc_raw) | _safe_component_set(cmdb_raw)
            rows.append(
                {
                    "fault_id": i + 1,
                    "fault_start": ts,
                    "fault_end": ts + pd.to_timedelta(float(dur_sec), unit="s"),
                    "service_components": services,
                    "failure_type": "" if typ_raw is None else str(typ_raw),
                }
            )
    elif isinstance(payload, list):
        for i, rec in enumerate(payload, start=1):
            if not isinstance(rec, dict):
                skipped += 1
                continue
            ts = pd.to_datetime(rec.get("timestamp"), unit="s", errors="coerce")
            dur_sec = pd.to_numeric(rec.get("duration"), errors="coerce")
            if pd.isna(ts) or pd.isna(dur_sec) or float(dur_sec) < 0:
                skipped += 1
                continue
            services = _safe_component_set(rec.get("service")) | _safe_component_set(rec.get("cmdb_id"))
            rows.append(
                {
                    "fault_id": i,
                    "fault_start": ts,
                    "fault_end": ts + pd.to_timedelta(float(dur_sec), unit="s"),
                    "service_components": services,
                    "failure_type": str(rec.get("failure_type", "")),
                }
            )
    else:
        raise ValueError("Unsupported ground truth JSON structure.")

    gt_df = pd.DataFrame(rows, columns=["fault_id", "fault_start", "fault_end", "service_components", "failure_type"])
    return gt_df, skipped


def compute_alignment_metrics(
    incidents_df: pd.DataFrame,
    ground_truth_df: pd.DataFrame,
) -> dict[str, Any]:
    """
    Deterministic overlap metrics for FR15.

    A temporal match exists if:
        incident_start <= fault_end AND incident_end >= fault_start

    We report:
    - recall_temporal: fraction of faults captured by at least one incident
    - precision_temporal: fraction of incidents matching at least one fault
    - recall_component_aware / precision_component_aware (when service hints exist)
    """
    if incidents_df.empty or ground_truth_df.empty:
        return {
            "n_faults": int(len(ground_truth_df)),
            "n_incidents": int(len(incidents_df)),
            "recall_temporal": 0.0,
            "precision_temporal": 0.0,
            "recall_component_aware": 0.0,
            "precision_component_aware": 0.0,
            "faults_captured_temporal": 0,
            "incidents_aligned_temporal": 0,
            "faults_captured_component_aware": 0,
            "incidents_aligned_component_aware": 0,
        }

    inc = incidents_df.copy()
    inc["start_time"] = pd.to_datetime(inc["start_time"])
    inc["end_time"] = pd.to_datetime(inc["end_time"])
    inc["component_set"] = inc["involved_components"].map(_safe_component_set)

    gt = ground_truth_df.copy()
    gt["fault_start"] = pd.to_datetime(gt["fault_start"])
    gt["fault_end"] = pd.to_datetime(gt["fault_end"])
    gt["service_components"] = gt["service_components"].map(_safe_component_set)

    fault_hit_temporal = [False] * len(gt)
    incident_hit_temporal = [False] * len(inc)
    fault_hit_component = [False] * len(gt)
    incident_hit_component = [False] * len(inc)

    # Deterministic nested iteration: incidents in input order, faults by fault_id order.
    gt = gt.sort_values("fault_id").reset_index(drop=True)
    inc = inc.reset_index(drop=True)

    for i_idx, irow in inc.iterrows():
        i_start = irow["start_time"]
        i_end = irow["end_time"]
        i_comp = irow["component_set"]
        for f_idx, frow in gt.iterrows():
            overlap = i_start <= frow["fault_end"] and i_end >= frow["fault_start"]
            if not overlap:
                continue

            incident_hit_temporal[i_idx] = True
            fault_hit_temporal[f_idx] = True

            # Component-aware variant only when fault side provides service hints.
            svc = frow["service_components"]
            if svc and i_comp.intersection(svc):
                incident_hit_component[i_idx] = True
                fault_hit_component[f_idx] = True

    n_faults = len(gt)
    n_inc = len(inc)

    faults_captured_temporal = sum(fault_hit_temporal)
    incidents_aligned_temporal = sum(incident_hit_temporal)
    faults_captured_component = sum(fault_hit_component)
    incidents_aligned_component = sum(incident_hit_component)

    return {
        "n_faults": n_faults,
        "n_incidents": n_inc,
        "recall_temporal": round(faults_captured_temporal / n_faults, 6) if n_faults else 0.0,
        "precision_temporal": round(incidents_aligned_temporal / n_inc, 6) if n_inc else 0.0,
        "recall_component_aware": round(faults_captured_component / n_faults, 6) if n_faults else 0.0,
        "precision_component_aware": round(incidents_aligned_component / n_inc, 6) if n_inc else 0.0,
        "faults_captured_temporal": faults_captured_temporal,
        "incidents_aligned_temporal": incidents_aligned_temporal,
        "faults_captured_component_aware": faults_captured_component,
        "incidents_aligned_component_aware": incidents_aligned_component,
    }


def _involved_components_list(raw: Any) -> list[str]:
    """Parse FR10 semicolon-separated involved_components into a sorted unique list."""
    return sorted(_safe_component_set(raw))


def _per_incident_top_metrics(row: pd.Series) -> dict[str, Any]:
    """Lightweight per-incident derived metrics for the summary index (no raw payloads)."""
    start = pd.to_datetime(row["start_time"])
    end = pd.to_datetime(row["end_time"])
    duration_sec = float((end - start).total_seconds())
    if duration_sec < 0:
        duration_sec = 0.0
    components = _involved_components_list(row.get("involved_components"))
    return {
        "duration_seconds": round(duration_sec, 4),
        "n_components": len(components),
    }


def _incident_summary_row(row: pd.Series) -> dict[str, Any]:
    """One lightweight incident record for incidents_summary.json."""
    record: dict[str, Any] = {
        "incident_id": str(int(row["incident_id"])),
        "start_time": str(row["start_time"]),
        "end_time": str(row["end_time"]),
        "event_count": int(row["event_count"]),
        "involved_components": _involved_components_list(row.get("involved_components")),
        "top_metrics": _per_incident_top_metrics(row),
    }
    if "dominant_template" in row.index and pd.notna(row["dominant_template"]):
        record["dominant_template"] = str(row["dominant_template"])
    return record


def build_incidents_summary(
    baseline_incidents_df: pd.DataFrame,
    smart_incidents_df: pd.DataFrame,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    """
    Lightweight index artifact: high-level metadata per incident plus run-level top metrics.
    Excludes raw log payloads so the frontend can load incident lists without O(events) memory.
    """
    payload: dict[str, Any] = {
        "artifact_version": _ARTIFACT_VERSION,
        "n_raw_logs": int(metrics["n_raw_logs"]),
        "nrr_formula": str(metrics.get("nrr_formula", "1 - n_incidents / n_raw_logs")),
        "methods": {
            "baseline": {
                "top_metrics": dict(metrics["baseline"]),
                "incidents": [
                    _incident_summary_row(row)
                    for _, row in baseline_incidents_df.sort_values(
                        ["start_time", "incident_id"], kind="mergesort"
                    ).iterrows()
                ],
            },
            "alert_fusion": {
                "top_metrics": dict(metrics["alert_fusion"]),
                "incidents": [
                    _incident_summary_row(row)
                    for _, row in smart_incidents_df.sort_values(
                        ["start_time", "incident_id"], kind="mergesort"
                    ).iterrows()
                ],
            },
        },
    }
    if "alignment_metrics" in metrics:
        payload["alignment_metrics"] = metrics["alignment_metrics"]
    return payload


def _event_payload(row: pd.Series) -> dict[str, str]:
    """Single normalized log event for incident_events_mapping.json."""
    return {
        "timestamp": str(row["timestamp"]),
        "component": str(row["component"]),
        "raw_message": str(row["raw_message"]),
    }


def build_incident_events_mapping(
    normalized_df: pd.DataFrame,
    baseline_event_ids: list[int],
    smart_event_ids: list[int],
) -> dict[str, Any]:
    """
    Heavy lookup artifact: incident_id (string key) -> ordered list of raw event payloads.
    Physically separated from incidents_summary.json so benchmarks stay UI-safe.
    """
    n_events = len(normalized_df)
    if len(baseline_event_ids) != n_events:
        raise ValueError(
            f"baseline assignment length {len(baseline_event_ids)} != event count {n_events}",
        )
    if len(smart_event_ids) != n_events:
        raise ValueError(
            f"alert_fusion assignment length {len(smart_event_ids)} != event count {n_events}",
        )

    baseline_map: dict[str, list[dict[str, str]]] = {}
    smart_map: dict[str, list[dict[str, str]]] = {}

    for idx in range(n_events):
        row = normalized_df.iloc[idx]
        event = _event_payload(row)
        b_id = str(int(baseline_event_ids[idx]))
        s_id = str(int(smart_event_ids[idx]))
        baseline_map.setdefault(b_id, []).append(event)
        smart_map.setdefault(s_id, []).append(event)

    return {
        "artifact_version": _ARTIFACT_VERSION,
        "methods": {
            "baseline": baseline_map,
            "alert_fusion": smart_map,
        },
    }


def _load_pipeline_module(alias: str, filename: str) -> Any:
    """Lazy-load sibling pipeline modules without package-relative imports."""
    src_dir = Path(__file__).resolve().parent
    module_path = src_dir / filename
    spec = importlib.util.spec_from_file_location(alias, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


def _prepare_normalized_events_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Match FR8 chronological ordering so row indices align with assignment vectors."""
    df = raw_df.copy()
    df["_ts"] = pd.to_datetime(df["_ts"] if "_ts" in df.columns else df["timestamp"])
    if "_ord" not in df.columns:
        df["_ord"] = range(len(df))
    df = df.sort_values(["_ts", "_ord"], kind="mergesort").reset_index(drop=True)
    df["raw_message"] = df["raw_message"].fillna("").astype(str)
    return df


def resolve_event_assignments(
    normalized_df: pd.DataFrame,
    *,
    baseline_event_ids: list[int] | None = None,
    smart_event_ids: list[int] | None = None,
) -> tuple[list[int], list[int], pd.DataFrame | None]:
    """
    Return (baseline_ids, smart_ids, smart_incidents_override) aligned 1:1 with rows.

    When smart_event_ids is recomputed, smart_incidents_override is a fresh FR10 table
    derived from the same fusion pass so incidents_summary.json stays synchronized with
    incident_events_mapping.json. Returns None for the override when the caller supplied
    precomputed AlertFusion assignments (pipeline runs).
    """
    ordered = _prepare_normalized_events_df(normalized_df)
    n = len(ordered)

    if baseline_event_ids is None:
        baseline_mod = _load_pipeline_module("eval_baseline", "2_baseline.py")
        baseline_event_ids = baseline_mod.event_to_baseline_incident_ids(ordered).tolist()
    elif len(baseline_event_ids) != n:
        raise ValueError("baseline_event_ids length does not match normalized event count")

    smart_incidents_override: pd.DataFrame | None = None
    if smart_event_ids is None:
        clustering_mod = _load_pipeline_module("eval_clustering", "3_clustering.py")
        incidents, smart_event_ids = clustering_mod.run_alert_fusion(ordered)
        smart_incidents_override = clustering_mod.incidents_to_dataframe(incidents)
    elif len(smart_event_ids) != n:
        raise ValueError("smart_event_ids length does not match normalized event count")

    return baseline_event_ids, smart_event_ids, smart_incidents_override


def load_incidents_summary(run_dir: Path) -> dict[str, Any]:
    """Load the lightweight incidents_summary.json index for UI initialization."""
    path = run_dir / INCIDENTS_SUMMARY_FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"Missing split-artifact index: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def metrics_from_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """
    Rebuild the metrics dict shape expected by dashboard widgets from the summary index.

    Re-derives method-level aggregates from the indexed incident rows so KPI cards stay
    synchronized with incidents_summary.json even when stored top_metrics are stale.
    """
    n_raw_logs = int(summary["n_raw_logs"])
    baseline_incidents = summary["methods"][METHOD_BASELINE]["incidents"]
    alert_incidents = summary["methods"][METHOD_ALERT_FUSION]["incidents"]
    baseline_df = incidents_summary_to_dataframe(baseline_incidents)
    alert_df = incidents_summary_to_dataframe(alert_incidents)

    metrics: dict[str, Any] = {
        "n_raw_logs": n_raw_logs,
        "nrr_formula": str(summary.get("nrr_formula", "1 - n_incidents / n_raw_logs")),
        "baseline": summarize_method(baseline_df, n_raw_logs, "baseline_fixed_window"),
        "alert_fusion": summarize_method(alert_df, n_raw_logs, "alert_fusion"),
    }
    if "alignment_metrics" in summary:
        metrics["alignment_metrics"] = summary["alignment_metrics"]
    return metrics


def incidents_summary_to_dataframe(incidents: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert summary incident records into a compact FR10-style table for charts/tables."""
    if not incidents:
        return pd.DataFrame(
            columns=[
                "incident_id",
                "start_time",
                "end_time",
                "event_count",
                "involved_components",
                "dominant_template",
            ],
        )
    rows: list[dict[str, Any]] = []
    for inc in incidents:
        comps = inc.get("involved_components", [])
        if isinstance(comps, list):
            comp_cell = ";".join(str(c) for c in comps)
        else:
            comp_cell = str(comps)
        rows.append(
            {
                "incident_id": int(inc["incident_id"]),
                "start_time": str(inc["start_time"]),
                "end_time": str(inc["end_time"]),
                "event_count": int(inc["event_count"]),
                "involved_components": comp_cell,
                "dominant_template": str(inc.get("dominant_template", "")),
            },
        )
    return pd.DataFrame(rows)


def load_ui_context(run_dir: Path) -> dict[str, Any]:
    """
    Frontend initialization bundle: metrics + lightweight incident tables only.
    Never loads incident_events_mapping.json — callers must lazy-load event slices.
    """
    summary = load_incidents_summary(run_dir)
    return {
        "run_dir": run_dir,
        "summary": summary,
        "metrics": metrics_from_summary(summary),
        "smart_df": incidents_summary_to_dataframe(
            summary["methods"][METHOD_ALERT_FUSION]["incidents"],
        ),
        "baseline_df": incidents_summary_to_dataframe(
            summary["methods"][METHOD_BASELINE]["incidents"],
        ),
        "mapping_path": run_dir / INCIDENT_EVENTS_MAPPING_FILENAME,
    }


def load_incident_events_slice(
    mapping_path: Path,
    incident_id: str,
    method: str = METHOD_ALERT_FUSION,
) -> list[dict[str, str]]:
    """
    Lazy-load one incident's event payloads from incident_events_mapping.json.

    Uses mmap + JSONDecoder.raw_decode to parse only the array for the requested
    incident_id inside the chosen method partition, avoiding a full-file object tree.
    """
    if not mapping_path.is_file():
        return []

    needle = f'"{incident_id}":['
    method_anchor = f'"{method}":{{'

    with mapping_path.open("r", encoding="utf-8") as handle:
        with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            blob = mm[:]
    text = blob.decode("utf-8")

    method_pos = text.find(method_anchor)
    if method_pos < 0:
        return []

    key_pos = text.find(needle, method_pos + len(method_anchor))
    if key_pos < 0:
        return []

    arr_start = key_pos + len(needle) - 1
    decoder = json.JSONDecoder()
    try:
        events, _end = decoder.raw_decode(text, arr_start)
    except json.JSONDecodeError:
        return []

    if not isinstance(events, list):
        return []
    return [evt for evt in events if isinstance(evt, dict)]


def write_split_artifacts(
    run_dir: Path,
    incidents_summary: dict[str, Any],
    incident_events_mapping: dict[str, Any],
) -> tuple[Path, Path]:
    """Persist the two isolated JSON artifacts under run_dir."""
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = run_dir / INCIDENTS_SUMMARY_FILENAME
    mapping_path = run_dir / INCIDENT_EVENTS_MAPPING_FILENAME

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(incidents_summary, f, indent=2, ensure_ascii=False)
        f.write("\n")

    with mapping_path.open("w", encoding="utf-8") as f:
        json.dump(incident_events_mapping, f, separators=(",", ":"), ensure_ascii=False)
        f.write("\n")

    return summary_path, mapping_path


def export_split_artifacts(
    run_dir: Path,
    normalized_df: pd.DataFrame,
    baseline_incidents_df: pd.DataFrame,
    smart_incidents_df: pd.DataFrame,
    metrics: dict[str, Any],
    *,
    baseline_event_ids: list[int] | None = None,
    smart_event_ids: list[int] | None = None,
) -> tuple[Path, Path]:
    """
    Split-Artifact Export Pattern: write lightweight summary index and heavy event mapping
    as two isolated JSON files so evaluation/FR14 metrics stay synchronized while the
    frontend never loads a single combined blob.
    """
    ordered = _prepare_normalized_events_df(normalized_df)
    base_ids, smart_ids, smart_override = resolve_event_assignments(
        ordered,
        baseline_event_ids=baseline_event_ids,
        smart_event_ids=smart_event_ids,
    )
    smart_for_summary = smart_override if smart_override is not None else smart_incidents_df
    summary = build_incidents_summary(baseline_incidents_df, smart_for_summary, metrics)
    mapping = build_incident_events_mapping(ordered, base_ids, smart_ids)
    return write_split_artifacts(run_dir, summary, mapping)


def build_metrics(
    baseline_path: Path,
    smart_path: Path,
    raw_events_path: Path,
    ground_truth_path: Path | None = None,
) -> dict[str, Any]:
    """
    Align both incident tables to the same N_raw_logs (normalized event count) so NRR and
    side-by-side means are **comparable by construction**—central to FR14-style baseline
    vs primary claims.
    """
    baseline = pd.read_csv(baseline_path)
    smart = pd.read_csv(smart_path)
    raw_df = pd.read_csv(raw_events_path)
    return build_metrics_from_frames(baseline, smart, raw_df, ground_truth_path)


def build_metrics_from_frames(
    baseline_df: pd.DataFrame,
    smart_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    ground_truth_path: Path | None = None,
) -> dict[str, Any]:
    """In-memory variant of build_metrics for pipeline runs that already hold DataFrames."""
    n_raw_logs = len(raw_df)

    metrics: dict[str, Any] = {
        "n_raw_logs": n_raw_logs,
        "nrr_formula": "1 - n_incidents / n_raw_logs",
        "baseline": summarize_method(baseline_df, n_raw_logs, "baseline_fixed_window"),
        "alert_fusion": summarize_method(smart_df, n_raw_logs, "alert_fusion"),
    }
    if ground_truth_path is not None:
        gt_df, skipped = load_ground_truth(ground_truth_path)
        metrics["alignment_metrics"] = {
            "ground_truth_source": str(ground_truth_path),
            "ground_truth_records_loaded": int(len(gt_df)),
            "ground_truth_records_skipped": int(skipped),
            "baseline": compute_alignment_metrics(baseline_df, gt_df),
            "alert_fusion": compute_alignment_metrics(smart_df, gt_df),
        }
    return metrics


def print_comparison_table(metrics: dict[str, Any]) -> None:
    """Human-readable FR14 table: NRR first (headline), then qualitative span/breadth cues."""
    b = metrics["baseline"]
    a = metrics["alert_fusion"]
    n = metrics["n_raw_logs"]

    labels = [
        ("N_raw_logs (events)", f"{n}", f"{n}"),
        ("N_incidents", str(b["n_incidents"]), str(a["n_incidents"])),
        ("NRR = 1 - N_inc / N_raw", f"{b['nrr']:.6f}", f"{a['nrr']:.6f}"),
        ("Avg duration (s)", f"{b['avg_duration_seconds']:.4f}", f"{a['avg_duration_seconds']:.4f}"),
        ("Avg components / incident", f"{b['avg_components_per_incident']:.4f}", f"{a['avg_components_per_incident']:.4f}"),
    ]

    w_label = max(len(r[0]) for r in labels)
    w_b = max(len(r[1]) for r in labels)
    w_a = max(len(r[2]) for r in labels)

    sep = "-" * (w_label + w_b + w_a + 7)
    header = f"| {'Metric':<{w_label}} | {'Baseline':>{w_b}} | {'AlertFusion':>{w_a}} |"

    print(sep)
    print(header)
    print(sep)
    for lab, vb, va in labels:
        print(f"| {lab:<{w_label}} | {vb:>{w_b}} | {va:>{w_a}} |")
    print(sep)
    print(f"Formula stored in metrics: {metrics['nrr_formula']}")


def main() -> None:
    """CLI: load FR10 exports, write metrics.json + split JSON artifacts, print comparison."""
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Evaluate baseline vs AlertFusion (FR13 NRR + FR10-summary aggregates).",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=root / "data" / "baseline_incidents.csv",
    )
    parser.add_argument(
        "--smart",
        type=Path,
        default=root / "data" / "smart_incidents.csv",
    )
    parser.add_argument(
        "--raw-events",
        type=Path,
        default=root / "data" / "normalized_events.csv",
        help="CSV of normalized events; row count defines N_raw_logs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "data" / "metrics.json",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help=(
            "Directory for split JSON artifacts "
            f"({INCIDENTS_SUMMARY_FILENAME}, {INCIDENT_EVENTS_MAPPING_FILENAME}); "
            "defaults to the metrics output parent directory."
        ),
    )
    parser.add_argument(
        "--ground-truth",
        type=Path,
        default=None,
        help="Optional path to groundtruth.json for FR15 alignment metrics.",
    )
    args = parser.parse_args()

    for p, name in [
        (args.baseline, "baseline_incidents.csv"),
        (args.smart, "smart_incidents.csv"),
        (args.raw_events, "normalized_events.csv"),
    ]:
        if not p.is_file():
            print(f"Missing file ({name}): {p}", file=sys.stderr)
            sys.exit(1)
    if args.ground_truth is not None and not args.ground_truth.is_file():
        print(f"Missing file (groundtruth.json): {args.ground_truth}", file=sys.stderr)
        sys.exit(1)

    baseline_df = pd.read_csv(args.baseline)
    smart_df = pd.read_csv(args.smart)
    raw_df = pd.read_csv(args.raw_events)
    metrics = build_metrics_from_frames(baseline_df, smart_df, raw_df, args.ground_truth)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
        f.write("\n")

    run_dir = args.run_dir if args.run_dir is not None else args.output.parent
    summary_path, mapping_path = export_split_artifacts(
        run_dir,
        raw_df,
        baseline_df,
        smart_df,
        metrics,
    )

    print(f"Wrote {args.output}")
    print(f"Wrote {summary_path}")
    print(f"Wrote {mapping_path}\n")
    print_comparison_table(metrics)


if __name__ == "__main__":
    main()
