"""
syncident.py — single, standalone, self-contained Syncident system.

One in-memory pipeline:

    Ingest / Normalize  ->  Baseline Grouping  ->  AlertFusion Grouping  ->  Evaluation

Targeted data sources (5)
-------------------------
    * LogHub text  : HDFS, Spark.
    * AIOpsArena CSV : Pod_Failure, Network_Delay, CPU_Stress.

Execution modes
---------------
    streamlit run syncident.py
        -> dashboard: pick a local .log/.txt/.csv stream, auto-discover its ground-truth
           JSON, run the pipeline, and render KPIs, timeline, and case studies.
    python syncident.py --input <log_or_csv> [--mode text|csv] [--ground-truth <gt.json>]
        -> prints the baseline vs AlertFusion comparison matrix to the terminal.

Deterministic core algorithms (mathematically untouched)
--------------------------------------------------------
    * Baseline       : consecutive 300 s fixed windows anchored to the first timestamp.
    * AlertFusion    : single-pass incremental clustering, 600 s active sliding window.
    * Adaptive decay : lambda = ln(2) / median(inter-arrival delta) in the active window,
                       static fallback only when the window has < 2 events or a 0 delta.
    * Anti-Flapping  : merge closed incidents when gap <= 1800 s AND dominant-component
                       AND dominant-template signatures are identical.
"""

from __future__ import annotations

import argparse
import bisect
import json
import re
import sys
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

# ===========================================================================
# Configuration (static, deterministic — no tuning, no schema discovery)
# ===========================================================================

# --- Ingestion: LogHub family (text mode: HDFS / Spark) --------------------
# HDFS: strict template capturing timestamp, message, and the blk_ component.
HDFS_REGEX_TEMPLATE = (
    r"^(?P<timestamp>\d{6}\s+\d{6})\s+\S+\s+\S+\s+\S+:\s+"
    r"(?P<raw_message>.*?(?P<component>blk_-?\d+).*)$"
)
# Spark fallback: when the HDFS template misses, isolate the leading
# "YY/MM/DD HH:MM:SS" timestamp so the line is parsed instead of dropped.
GENERIC_TS_RE = re.compile(r"^\s*(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})")
# Leading log levels are skipped so the component maps to the real module
# (e.g. Spark "SparkContext") rather than the level word.
LOG_LEVELS = frozenset({"TRACE", "DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"})

# --- Ingestion: CSV mode (AIOpsArena Pod_Failure / Network_Delay / CPU_Stress)
ARENA_TIMESTAMP_COL = "timestamp"
ARENA_COMPONENT_COL = "cmdb_id"
ARENA_MESSAGE_COL = "message"

# Timestamp formats for the 5 targeted sources; epoch + generic parse cover the rest.
COMMON_TS_FORMATS = (
    "%y%m%d %H%M%S",      # HDFS
    "%y/%m/%d %H:%M:%S",  # Spark
    "%Y-%m-%d %H:%M:%S",  # AIOpsArena CSV
    "%Y-%m-%dT%H:%M:%S",  # AIOpsArena CSV (ISO)
)

MAX_PARSE_LINE_CHARS = 200_000
MAX_RAW_MESSAGE_CHARS = 12_000
MAX_SKIPPED_RECORDS = 20_000
CSV_CHUNK_SIZE = 100_000

# --- Baseline grouping ------------------------------------------------------
BASELINE_WINDOW_SEC = 300

# --- AlertFusion grouping ---------------------------------------------------
ACTIVE_WINDOW_SEC = 600
# Lowered from 0.7 to 0.5: with the simplified word-token Jaccard text signal and
# the strict signal weights, a 0.7 cutoff isolated nearly every LogHub line into its
# own cluster (~1939 incidents / 2000 rows). 0.5 restores proper multi-signal merging.
ASSIGN_THRESHOLD = 0.5
FLAPPING_WINDOW_SEC = 1800

# Adaptive time decay: lambda = ln(2) / median(delta). Static fallback below.
TIME_DECAY_LAMBDA = 0.003
_LN2 = 0.6931471805599453  # math.log(2), inlined to keep the import surface minimal.

# Similarity signal weights (sum to 1.0).
W_TIME = 0.35
W_COMPONENT = 0.30
W_TEXT = 0.35

MAX_TEMPLATE_CHARS = 256

# Volatile-token masks so structurally identical log lines collapse to one template.
UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
)
HEX_ADDRESS_PATTERN = re.compile(r"\b0x[0-9a-fA-F]+\b")
HEX_ID_PATTERN = re.compile(r"\b[0-9a-fA-F]{12,}\b")

# --- Dashboard configuration (local file selector mode) --------------------
# The Streamlit dashboard scans the project root for .log/.csv files and runs the
# in-memory pipeline on the selected one. Ingestion mode is derived from the
# extension (.csv -> "csv", otherwise "text"). Ground truth is auto-discovered.
DASHBOARD_SCAN_DIR = "."
DASHBOARD_FILE_GLOBS = ("*.log", "*.txt", "*.csv")
DEFAULT_GT_FILENAME = "gt.json"  # generic fallback ground-truth file in the scan dir.

# ===========================================================================
# Data structures
# ===========================================================================
@dataclass
class NormalizationResult:
    """Output of an ingestion pass: clean events plus skip accounting."""

    normalized_df: pd.DataFrame
    skipped_records: list[dict[str, Any]]
    skipped_overflow_count: int = 0

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_records) + int(self.skipped_overflow_count)


@dataclass(frozen=True)
class FusionParams:
    """AlertFusion correlation knobs (static defaults; no auto-tuning)."""

    w_time: float
    w_component: float
    w_text: float
    assign_threshold: float


@dataclass
class Incident:
    """Mutable cluster state for incremental AlertFusion."""

    incident_id: int
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    component_counts: Counter[str] = field(default_factory=Counter)
    message_counts: Counter[str] = field(default_factory=Counter)
    event_count: int = 0
    last_message: str = ""
    last_template: str = ""

    def dominant_component(self) -> str:
        if not self.component_counts:
            return ""
        return self.component_counts.most_common(1)[0][0]

    def dominant_message(self) -> str:
        if not self.message_counts:
            return ""
        return self.message_counts.most_common(1)[0][0]

    def add_event(self, ts: pd.Timestamp, component: str, raw_message: str) -> None:
        self.end_time = ts
        normalized = normalized_template(raw_message)
        self.component_counts[component] += 1
        self.message_counts[normalized] += 1
        self.event_count += 1
        self.last_message = raw_message
        self.last_template = normalized


@dataclass
class AnalysisResult:
    """Pure in-memory analysis bundle consumed by the CLI and the UI."""

    events_df: pd.DataFrame
    baseline_incidents: pd.DataFrame
    alert_incidents: pd.DataFrame
    metrics: dict[str, Any]
    ground_truth_df: pd.DataFrame | None
    skipped_count: int


# ===========================================================================
# Stage 1 — Ingestion / Normalization (two static pathways)
# ===========================================================================
def _append_skipped(
    skipped: list[dict[str, Any]],
    record: dict[str, Any],
    overflow_count: int,
) -> int:
    if len(skipped) < MAX_SKIPPED_RECORDS:
        skipped.append(record)
        return overflow_count
    return overflow_count + 1


def _trim_message(text: str) -> str:
    if len(text) <= MAX_RAW_MESSAGE_CHARS:
        return text
    return text[:MAX_RAW_MESSAGE_CHARS]


def _parse_timestamp_scalar(raw_ts: Any) -> datetime | None:
    if raw_ts is None:
        return None
    ts_text = str(raw_ts).strip()
    if not ts_text:
        return None
    for fmt in COMMON_TS_FORMATS:
        try:
            return datetime.strptime(ts_text, fmt)
        except ValueError:
            pass
    parsed = pd.to_datetime(ts_text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _parse_timestamp_series(raw: pd.Series) -> pd.Series:
    """Vectorized: 10-digit epoch seconds -> explicit formats -> generic parse -> NaT."""
    text = raw.astype("string").fillna("").str.strip()
    parsed = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")

    epoch_mask = text.str.fullmatch(r"\d{10}(?:\.\d+)?").fillna(False)
    if epoch_mask.any():
        parsed.loc[epoch_mask] = pd.to_datetime(
            pd.to_numeric(text[epoch_mask], errors="coerce"), unit="s", errors="coerce"
        )
    for fmt in COMMON_TS_FORMATS:
        missing = parsed.isna() & text.ne("")
        if missing.any():
            parsed.loc[missing] = pd.to_datetime(text[missing], format=fmt, errors="coerce")
    missing = parsed.isna() & text.ne("")
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(text[missing], errors="coerce")
    return parsed


def _finalize_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["timestamp", "component", "raw_message"])
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["component"] = df["component"].astype(str)
    df["raw_message"] = df["raw_message"].astype(str)
    return df


def _extract_loghub_component(remainder: str) -> str:
    """Spark component: the module token after an optional leading log level."""
    tokens = remainder.split()
    if not tokens:
        return ""
    if tokens[0].rstrip(":").upper() in LOG_LEVELS and len(tokens) > 1:
        tokens = tokens[1:]
    return tokens[0].rstrip(":")


def _parse_loghub_line(line: str) -> tuple[datetime | None, str, str] | None:
    """Spark fallback for lines the HDFS template rejects: timestamp + module + message."""
    m_ts = GENERIC_TS_RE.match(line)
    if not m_ts:
        return None
    ts = _parse_timestamp_scalar(m_ts.group(1))
    remainder = line[m_ts.end():].strip()
    return ts, _extract_loghub_component(remainder), (remainder or line)


def normalize_text(lines: Iterable[str], regex_pattern: str = HDFS_REGEX_TEMPLATE) -> NormalizationResult:
    """LogHub text ingestion: strict HDFS template first, Spark timestamp fallback on a miss."""
    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    overflow = 0
    pattern = re.compile(regex_pattern)

    for line_no, raw in enumerate(lines, start=1):
        line = raw.rstrip("\n\r")
        if not line.strip():
            continue
        if len(line) > MAX_PARSE_LINE_CHARS:
            overflow = _append_skipped(
                skipped, {"line_no": line_no, "reason": "line_too_long", "raw": line[:250]}, overflow
            )
            continue

        match = pattern.search(line)
        if match:
            groups = match.groupdict()
            ts = _parse_timestamp_scalar(groups.get("timestamp"))
            comp = (groups.get("component") or "").strip()
            msg = (groups.get("raw_message") or "").strip() or line
        else:
            fallback = _parse_loghub_line(line)
            if fallback is None:
                overflow = _append_skipped(
                    skipped, {"line_no": line_no, "reason": "no_timestamp", "raw": line[:250]}, overflow
                )
                continue
            ts, comp, msg = fallback

        msg = _trim_message(msg)

        reasons: list[str] = []
        if ts is None:
            reasons.append("invalid_timestamp")
        if not comp:
            reasons.append("missing_component")
        if not msg:
            reasons.append("missing_raw_message")
        if reasons:
            overflow = _append_skipped(
                skipped, {"line_no": line_no, "reason": ",".join(reasons), "raw": line[:250]}, overflow
            )
            continue

        rows.append({"timestamp": ts, "component": comp, "raw_message": msg})

    return NormalizationResult(
        normalized_df=_finalize_rows(rows),
        skipped_records=skipped,
        skipped_overflow_count=overflow,
    )


def _normalize_arena_chunk(df: pd.DataFrame, row_index_offset: int) -> NormalizationResult:
    if df.empty:
        empty = pd.DataFrame(columns=["timestamp", "component", "raw_message"])
        return NormalizationResult(normalized_df=empty, skipped_records=[], skipped_overflow_count=0)

    def _col(name: str) -> pd.Series:
        if name in df.columns:
            return df[name]
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="string")

    ts_text = _col(ARENA_TIMESTAMP_COL).astype("string").fillna("").str.strip()
    parsed_ts = _parse_timestamp_series(ts_text)
    component = _col(ARENA_COMPONENT_COL).astype("string").fillna("").str.strip()
    raw_message = (
        _col(ARENA_MESSAGE_COL).astype("string").fillna("").str.strip().str.slice(0, MAX_RAW_MESSAGE_CHARS)
    )

    ts_valid = parsed_ts.notna()
    component_valid = component.ne("")
    message_valid = raw_message.ne("")
    valid_mask = ts_valid & component_valid & message_valid

    normalized_df = pd.DataFrame(
        {
            "timestamp": parsed_ts[valid_mask].dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "component": component[valid_mask].astype(str),
            "raw_message": raw_message[valid_mask].astype(str),
        }
    )

    reason_series = pd.Series(
        np.where(~ts_valid, "invalid_timestamp,", "")
        + np.where(~component_valid, "missing_component,", "")
        + np.where(~message_valid, "missing_raw_message,", ""),
        index=df.index,
    ).str.rstrip(",")
    preview_series = (
        ts_text.str.slice(0, 60) + " | " + component.str.slice(0, 60) + " | " + raw_message.str.slice(0, 120)
    ).str.slice(0, 250)

    skipped: list[dict[str, Any]] = []
    overflow = 0
    for idx in df.index[~valid_mask].tolist():
        if isinstance(idx, (int, np.integer)):
            global_idx: int | str = int(idx) + row_index_offset
        else:
            global_idx = str(idx)
        overflow = _append_skipped(
            skipped,
            {"index": global_idx, "reason": reason_series.loc[idx], "record_preview": preview_series.loc[idx]},
            overflow,
        )

    return NormalizationResult(
        normalized_df=normalized_df, skipped_records=skipped, skipped_overflow_count=overflow
    )


def normalize_arena_csv(csv_source: Any, chunksize: int = CSV_CHUNK_SIZE) -> NormalizationResult:
    """CSV-mode ingestion for AIOpsArena datasets using the static column mapping."""
    normalized_chunks: list[pd.DataFrame] = []
    skipped: list[dict[str, Any]] = []
    overflow = 0
    row_offset = 0

    try:
        for chunk in pd.read_csv(csv_source, chunksize=chunksize):
            chunk_result = _normalize_arena_chunk(chunk, row_index_offset=row_offset)
            if not chunk_result.normalized_df.empty:
                normalized_chunks.append(chunk_result.normalized_df)
            for rec in chunk_result.skipped_records:
                overflow = _append_skipped(skipped, rec, overflow)
            overflow += int(chunk_result.skipped_overflow_count)
            row_offset += len(chunk)
    except EmptyDataError:
        pass

    if normalized_chunks:
        normalized_df = pd.concat(normalized_chunks, ignore_index=True)
    else:
        normalized_df = pd.DataFrame(columns=["timestamp", "component", "raw_message"])

    return NormalizationResult(
        normalized_df=normalized_df, skipped_records=skipped, skipped_overflow_count=overflow
    )


def detect_family(path: Path) -> str:
    """
    Route a file into one of two logical ingestion families by extension:
        "text" -> LogHub family   (.log, .txt): unstructured system text streams.
        "csv"  -> AIOpsArena family (.csv):     structured microservice telemetry grids.
    """
    return "csv" if Path(path).suffix.lower() == ".csv" else "text"


def ingest_events(path: Path, mode: str | None = None) -> NormalizationResult:
    """
    Ingestion dispatcher across the two logical families.

    mode="text" : LogHub family — HDFS template with a generic LogHub fallback scanner.
    mode="csv"  : AIOpsArena family — structured CSV via the hardcoded column mapping.
    mode=None   : routed automatically via detect_family().
    """
    path = Path(path)
    if mode is None:
        mode = detect_family(path)
    if mode == "csv":
        return normalize_arena_csv(path)
    if mode == "text":
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return normalize_text(handle, HDFS_REGEX_TEMPLATE)
    raise ValueError(f"Unsupported ingestion mode: {mode!r} (use 'text' or 'csv').")


# ===========================================================================
# Shared chronological preparation
# ===========================================================================
def prepare_events_frame(normalized_df: pd.DataFrame) -> pd.DataFrame:
    """Add working columns (_ts, _ord) and enforce stable chronological ordering."""
    required = {"timestamp", "component", "raw_message"}
    missing = required - set(normalized_df.columns)
    if missing:
        raise ValueError(f"Normalized data missing columns: {sorted(missing)}")

    df = normalized_df.copy()
    df["_ts"] = pd.to_datetime(df["timestamp"])
    df["_ord"] = range(len(df))
    df = df.sort_values(["_ts", "_ord"], kind="mergesort").reset_index(drop=True)
    df["raw_message"] = df["raw_message"].fillna("").astype(str)
    return df


# ===========================================================================
# Stage 2 — Baseline grouping: consecutive 300 s fixed windows
# ===========================================================================
def assign_window_index(
    timestamps: pd.Series, t_min: pd.Timestamp, baseline_window_sec: int = BASELINE_WINDOW_SEC
) -> pd.Series:
    delta_sec = (timestamps - t_min).dt.total_seconds()
    return (delta_sec // baseline_window_sec).astype("int64")


def event_to_baseline_incident_ids(
    df: pd.DataFrame, baseline_window_sec: int = BASELINE_WINDOW_SEC
) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="int64")
    if "_ts" not in df.columns:
        raise ValueError("DataFrame must include _ts (call prepare_events_frame first).")
    t_min = df["_ts"].min()
    win = assign_window_index(df["_ts"], t_min, baseline_window_sec)
    ranked_windows = sorted(win.unique())
    id_map = {w: i + 1 for i, w in enumerate(ranked_windows)}
    return win.map(id_map).astype("int64")


def group_baseline_incidents(
    df: pd.DataFrame, baseline_window_sec: int = BASELINE_WINDOW_SEC
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=["incident_id", "start_time", "end_time", "event_count", "involved_components"]
        )

    t_min = df["_ts"].min()
    df = df.copy()
    df["_window"] = assign_window_index(df["_ts"], t_min, baseline_window_sec)

    def components_cell(series: pd.Series) -> str:
        uniq = sorted(series.dropna().unique())
        return ";".join(str(c) for c in uniq)

    grouped = (
        df.groupby("_window", sort=True)
        .agg(
            start_time=("_ts", "min"),
            end_time=("_ts", "max"),
            event_count=("_ts", "size"),
            involved_components=("component", components_cell),
        )
        .reset_index(drop=True)
    )

    grouped.insert(0, "incident_id", range(1, len(grouped) + 1))
    grouped["start_time"] = grouped["start_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    grouped["end_time"] = grouped["end_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return grouped[["incident_id", "start_time", "end_time", "event_count", "involved_components"]]


# ===========================================================================
# Stage 3 — AlertFusion similarity signals
# ===========================================================================
def normalize_message_template(msg: str) -> str:
    """Mask volatile tokens so structurally identical lines collapse to one template."""
    s = UUID_PATTERN.sub("<UUID>", msg)
    s = HEX_ADDRESS_PATTERN.sub("<HEX>", s)
    s = HEX_ID_PATTERN.sub("<HEXID>", s)
    s = re.sub(r"blk_-?[0-9]+", "<BLK>", s)
    s = re.sub(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<IP>", s)
    s = re.sub(r"/[\w./-]+", "<PATH>", s)
    s = re.sub(r"\b\d+\b", "<N>", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalized_template(msg: str) -> str:
    """Masked template capped to a bounded length (no caching, no n-grams)."""
    return normalize_message_template(msg)[:MAX_TEMPLATE_CHARS]


def template_similarity(ta: str, tb: str) -> float:
    """Simplified text signal: word-token Jaccard overlap in [0, 1]."""
    if ta == tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    tokens_a = set(ta.split())
    tokens_b = set(tb.split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return float(len(intersection) / len(union))


def time_proximity_score(
    event_ts: pd.Timestamp, incident_end: pd.Timestamp, lam: float = TIME_DECAY_LAMBDA
) -> float:
    """Exponential temporal affinity: exp(-lam * delta_seconds), 1.0 for overlap/future."""
    delta = (event_ts - incident_end).total_seconds()
    if delta <= 0:
        return 1.0
    return float(np.exp(-lam * delta))


def component_score(event_component: str, component_counts: Counter[str]) -> float:
    """Pure Jaccard; exactly 0.0 when the component is new to the incident."""
    if not component_counts:
        return 0.0
    # Single-element Jaccard: a new component adds 1 to the union (score 0.0);
    # an existing one is already in the set, so the union stays |component_counts|.
    if event_component not in component_counts:
        return 0.0
    return 1.0 / len(component_counts)


def similarity_score(
    event_ts: pd.Timestamp,
    event_component: str,
    event_template: str,
    inc: Incident,
    params: FusionParams,
    lam: float = TIME_DECAY_LAMBDA,
) -> float:
    s_t = time_proximity_score(event_ts, inc.end_time, lam)
    s_c = component_score(event_component, inc.component_counts)
    s_m = template_similarity(event_template, inc.last_template)
    return params.w_time * s_t + params.w_component * s_c + params.w_text * s_m


def is_active(
    event_ts: pd.Timestamp, inc: Incident, active_window_sec: int = ACTIVE_WINDOW_SEC
) -> bool:
    gap_sec = (event_ts - inc.end_time).total_seconds()
    return gap_sec <= active_window_sec


def default_fusion_params() -> FusionParams:
    return FusionParams(
        w_time=W_TIME, w_component=W_COMPONENT, w_text=W_TEXT, assign_threshold=ASSIGN_THRESHOLD
    )


def involved_components_cell(ctr: Counter[str]) -> str:
    return ";".join(str(c) for c in sorted(ctr.keys()))


def _as_ts(val: object) -> pd.Timestamp:
    if isinstance(val, pd.Timestamp):
        return val
    return pd.Timestamp(val)


# ===========================================================================
# Stage 3 — Anti-Flapping merge pass (mandatory post-processing)
# ===========================================================================
def merge_incidents(
    incidents: list[Incident], flapping_window_sec: int = FLAPPING_WINDOW_SEC
) -> tuple[list[Incident], dict[int, int]]:
    """
    Merge adjacent closed incidents when ALL hold:
        1. gap = next.start_time - current.end_time <= flapping_window_sec
        2. identical dominant component
        3. identical dominant message template
    Returns the merged list (ordered by start_time) and an old->final id map.
    """
    if not incidents:
        return [], {}

    ordered = sorted(incidents, key=lambda x: (x.start_time, x.incident_id))
    merged: list[Incident] = []
    old_to_root: dict[int, int] = {}

    current = ordered[0]
    old_to_root[current.incident_id] = current.incident_id

    for nxt in ordered[1:]:
        old_to_root[nxt.incident_id] = nxt.incident_id
        gap_sec = (nxt.start_time - current.end_time).total_seconds()

        cur_dom_comp = current.dominant_component()
        nxt_dom_comp = nxt.dominant_component()
        same_component = bool(cur_dom_comp) and cur_dom_comp == nxt_dom_comp

        cur_dom_tmpl = current.dominant_message()
        nxt_dom_tmpl = nxt.dominant_message()
        same_template = bool(cur_dom_tmpl) and cur_dom_tmpl == nxt_dom_tmpl

        if gap_sec <= flapping_window_sec and same_component and same_template:
            current.end_time = max(current.end_time, nxt.end_time)
            current.event_count += nxt.event_count
            current.component_counts.update(nxt.component_counts)
            current.message_counts.update(nxt.message_counts)
            current.last_message = nxt.last_message
            current.last_template = nxt.last_template
            old_to_root[nxt.incident_id] = current.incident_id
        else:
            merged.append(current)
            current = nxt

    merged.append(current)

    root_to_final: dict[int, int] = {}
    for i, inc in enumerate(merged, start=1):
        root_to_final[inc.incident_id] = i
        inc.incident_id = i

    old_to_final = {old_id: root_to_final[root_id] for old_id, root_id in old_to_root.items()}
    return merged, old_to_final


# ===========================================================================
# Stage 3 — AlertFusion single-pass incremental clustering
# ===========================================================================
def run_alert_fusion(
    df: pd.DataFrame,
    params: FusionParams | None = None,
    active_window_sec: int = ACTIVE_WINDOW_SEC,
    flapping_window_sec: int = FLAPPING_WINDOW_SEC,
) -> tuple[list[Incident], list[int]]:
    """
    Single-pass incremental correlation with an active sliding window, adaptive lambda
    time decay, and a mandatory anti-flapping merge pass.

    Returns (merged_incidents, per_event_final_incident_ids) aligned to df row order.
    """
    if params is None:
        params = default_fusion_params()

    incidents: list[Incident] = []
    active_pool: list[Incident] = []
    event_incident_ids: list[int] = []
    next_id = 1

    window_ts: deque[pd.Timestamp] = deque()
    # Incrementally maintained inter-arrival deltas for the active window.
    # `window_deltas` mirrors the consecutive gaps in arrival order so we can drop the
    # leading gap when the oldest timestamp expires; `sorted_deltas` is the same multiset
    # kept ordered (via bisect) so the median is an O(1) midpoint lookup each event,
    # avoiding the O(W log W) re-sort that ran on every incoming log line.
    window_deltas: deque[float] = deque()
    sorted_deltas: list[float] = []

    ts_arr = df["_ts"].to_numpy()
    comp_arr = df["component"].astype(str).to_numpy()
    msg_arr = df["raw_message"].astype(str).to_numpy()

    for i in range(len(df)):
        ts = _as_ts(ts_arr[i])
        comp = comp_arr[i]
        msg = msg_arr[i]
        msg_template = normalized_template(msg)

        cutoff = ts - pd.Timedelta(seconds=active_window_sec)
        while window_ts and window_ts[0] < cutoff:
            window_ts.popleft()
            if window_deltas:
                stale = window_deltas.popleft()
                del sorted_deltas[bisect.bisect_left(sorted_deltas, stale)]

        if window_ts:
            new_delta = (ts - window_ts[-1]).total_seconds()
            window_deltas.append(new_delta)
            bisect.insort(sorted_deltas, new_delta)
        window_ts.append(ts)

        adaptive_lam = TIME_DECAY_LAMBDA
        n_d = len(sorted_deltas)
        if n_d >= 1:
            median_delta = (
                sorted_deltas[n_d // 2]
                if n_d % 2 == 1
                else (sorted_deltas[n_d // 2 - 1] + sorted_deltas[n_d // 2]) / 2.0
            )
            if median_delta > 0.0:
                adaptive_lam = _LN2 / median_delta

        active_pool = [inc for inc in active_pool if is_active(ts, inc, active_window_sec)]
        active = active_pool
        best: Incident | None = None
        best_score = -1.0
        for inc in active:
            sc = similarity_score(ts, comp, msg_template, inc, params, lam=adaptive_lam)
            if sc > best_score:
                best_score = sc
                best = inc
            elif sc == best_score and best is not None and inc.incident_id < best.incident_id:
                best = inc

        if best is not None and best_score >= params.assign_threshold:
            best.add_event(ts, comp, msg)
            event_incident_ids.append(best.incident_id)
        else:
            inc = Incident(
                incident_id=next_id,
                start_time=ts,
                end_time=ts,
                component_counts=Counter({comp: 1}),
                message_counts=Counter({msg_template: 1}),
                event_count=1,
                last_message=msg,
                last_template=msg_template,
            )
            incidents.append(inc)
            active_pool.append(inc)
            event_incident_ids.append(inc.incident_id)
            next_id += 1

    merged_incidents, id_map = merge_incidents(incidents, flapping_window_sec)
    final_event_ids = [id_map.get(i, i) for i in event_incident_ids]
    return merged_incidents, final_event_ids


def incidents_to_dataframe(incidents: list[Incident]) -> pd.DataFrame:
    rows = []
    for inc in sorted(incidents, key=lambda x: x.start_time):
        rows.append(
            {
                "incident_id": inc.incident_id,
                "start_time": inc.start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_time": inc.end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "event_count": inc.event_count,
                "involved_components": involved_components_cell(inc.component_counts),
                "dominant_template": inc.dominant_message(),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "incident_id",
            "start_time",
            "end_time",
            "event_count",
            "involved_components",
            "dominant_template",
        ],
    )


# ===========================================================================
# Stage 4 — Evaluation metrics (retained verbatim from the evaluation module)
# ===========================================================================
def noise_reduction_ratio(n_incidents: int, n_raw_logs: int) -> float:
    """NRR = 1 - N_incidents / N_raw_logs."""
    if n_raw_logs <= 0:
        raise ValueError("n_raw_logs must be positive for NRR.")
    return 1.0 - (n_incidents / n_raw_logs)


def incident_duration_seconds(df: pd.DataFrame) -> pd.Series:
    start = pd.to_datetime(df["start_time"])
    end = pd.to_datetime(df["end_time"])
    return (end - start).dt.total_seconds()


def components_per_incident(df: pd.DataFrame) -> pd.Series:
    col = df["involved_components"].fillna("").astype(str)

    def count_cell(s: str) -> int:
        parts = [p for p in s.split(";") if p.strip()]
        return len(parts)

    return col.map(count_cell)


def summarize_method(df: pd.DataFrame, n_raw_logs: int, method_name: str) -> dict[str, Any]:
    n_inc = len(df)
    durs = incident_duration_seconds(df) if n_inc else pd.Series(dtype="float64")
    comps = components_per_incident(df) if n_inc else pd.Series(dtype="float64")
    avg_dur = float(durs.mean()) if not durs.empty else 0.0
    avg_comps = float(comps.mean()) if not comps.empty else 0.0
    if pd.isna(avg_dur):
        avg_dur = 0.0
    if pd.isna(avg_comps):
        avg_comps = 0.0
    return {
        "method": method_name,
        "n_incidents": n_inc,
        "nrr": round(noise_reduction_ratio(n_inc, n_raw_logs), 6) if n_raw_logs > 0 else 0.0,
        "avg_duration_seconds": round(avg_dur, 4),
        "avg_components_per_incident": round(avg_comps, 4),
    }


def _safe_component_set(raw: Any) -> set[str]:
    if raw is None:
        return set()
    if isinstance(raw, (set, tuple, list)):
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
    """Load fault windows from JSON (parallel arrays or list of records)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
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

    gt_df = pd.DataFrame(
        rows, columns=["fault_id", "fault_start", "fault_end", "service_components", "failure_type"]
    )
    return gt_df, skipped


def compute_alignment_metrics(
    incidents_df: pd.DataFrame, ground_truth_df: pd.DataFrame
) -> dict[str, Any]:
    """Deterministic temporal + component-aware precision/recall vs ground-truth faults."""
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


def build_metrics(
    events_df: pd.DataFrame,
    baseline_incidents: pd.DataFrame,
    alert_incidents: pd.DataFrame,
    ground_truth_df: pd.DataFrame | None = None,
    gt_source: str | None = None,
    gt_skipped: int = 0,
) -> dict[str, Any]:
    """Assemble the baseline vs AlertFusion comparison metrics, adding fault alignment when GT is present."""
    n_raw_logs = len(events_df)
    metrics: dict[str, Any] = {
        "n_raw_logs": n_raw_logs,
        "nrr_formula": "1 - n_incidents / n_raw_logs",
        "baseline": summarize_method(baseline_incidents, n_raw_logs, "baseline_fixed_window"),
        "alert_fusion": summarize_method(alert_incidents, n_raw_logs, "alert_fusion"),
    }
    if ground_truth_df is not None and not ground_truth_df.empty:
        metrics["alignment_metrics"] = {
            "ground_truth_source": str(gt_source) if gt_source is not None else "",
            "ground_truth_records_loaded": int(len(ground_truth_df)),
            "ground_truth_records_skipped": int(gt_skipped),
            "baseline": compute_alignment_metrics(baseline_incidents, ground_truth_df),
            "alert_fusion": compute_alignment_metrics(alert_incidents, ground_truth_df),
        }
    return metrics


# ===========================================================================
# Linear orchestration: Ingest -> Baseline -> AlertFusion -> Evaluate
# ===========================================================================
def analyze(
    source: Path | str | pd.DataFrame,
    mode: str | None = None,
    ground_truth_path: Path | str | None = None,
    assign_threshold: float = ASSIGN_THRESHOLD,
    active_window_sec: int = ACTIVE_WINDOW_SEC,
    baseline_window_sec: int = BASELINE_WINDOW_SEC,
    flapping_window_sec: int = FLAPPING_WINDOW_SEC,
) -> AnalysisResult:
    """
    Run the entire pipeline in memory and return summaries + metrics.

    `source` is a log file path (text/csv pathways) or a normalized DataFrame.
    The four tuning parameters default to the module constants but can be overridden
    (e.g. from the CLI) and are threaded explicitly into the groupers — no globals.
    When `ground_truth_path` is provided, temporal and component-aware
    precision/recall are computed automatically.
    """
    skipped_count = 0
    if isinstance(source, pd.DataFrame):
        normalized_df = source[["timestamp", "component", "raw_message"]].copy()
    else:
        result = ingest_events(Path(source), mode=mode)
        normalized_df = result.normalized_df
        skipped_count = result.skipped_count

    events = prepare_events_frame(normalized_df)

    baseline_incidents = group_baseline_incidents(events, baseline_window_sec)
    baseline_event_ids = event_to_baseline_incident_ids(events, baseline_window_sec)

    params = FusionParams(
        w_time=W_TIME, w_component=W_COMPONENT, w_text=W_TEXT, assign_threshold=assign_threshold
    )
    incidents, alert_event_ids = run_alert_fusion(
        events, params, active_window_sec=active_window_sec, flapping_window_sec=flapping_window_sec
    )
    alert_incidents = incidents_to_dataframe(incidents)

    # Attach per-event assignments aligned to the (already sorted) events frame.
    if len(events):
        events["baseline_incident_id"] = baseline_event_ids.to_numpy()
        events["alert_incident_id"] = np.asarray(alert_event_ids, dtype="int64")
    else:
        events["baseline_incident_id"] = pd.Series(dtype="int64")
        events["alert_incident_id"] = pd.Series(dtype="int64")

    ground_truth_df: pd.DataFrame | None = None
    gt_skipped = 0
    if ground_truth_path is not None and str(ground_truth_path).strip():
        ground_truth_df, gt_skipped = load_ground_truth(Path(ground_truth_path))

    metrics = build_metrics(
        events,
        baseline_incidents,
        alert_incidents,
        ground_truth_df=ground_truth_df,
        gt_source=str(ground_truth_path) if ground_truth_path is not None else None,
        gt_skipped=gt_skipped,
    )
    # Record the parameters this run actually used so the UI reflects them dynamically.
    metrics["parameters"] = {
        "assign_threshold": assign_threshold,
        "active_window_sec": active_window_sec,
        "baseline_window_sec": baseline_window_sec,
        "flapping_window_sec": flapping_window_sec,
    }

    return AnalysisResult(
        events_df=events,
        baseline_incidents=baseline_incidents,
        alert_incidents=alert_incidents,
        metrics=metrics,
        ground_truth_df=ground_truth_df,
        skipped_count=skipped_count,
    )


# ===========================================================================
# CLI mode — print the baseline vs AlertFusion comparison matrix
# ===========================================================================
def print_comparison_table(metrics: dict[str, Any]) -> None:
    b = metrics["baseline"]
    a = metrics["alert_fusion"]
    n = metrics["n_raw_logs"]

    labels = [
        ("N_raw_logs (events)", f"{n}", f"{n}"),
        ("N_incidents", str(b["n_incidents"]), str(a["n_incidents"])),
        ("NRR = 1 - N_inc / N_raw", f"{b['nrr']:.6f}", f"{a['nrr']:.6f}"),
        ("Avg duration (s)", f"{b['avg_duration_seconds']:.4f}", f"{a['avg_duration_seconds']:.4f}"),
        (
            "Avg components / incident",
            f"{b['avg_components_per_incident']:.4f}",
            f"{a['avg_components_per_incident']:.4f}",
        ),
    ]

    w_label = max(len(r[0]) for r in labels)
    w_b = max(len(r[1]) for r in labels + [("", "Baseline", "")])
    w_a = max(len(r[2]) for r in labels + [("", "", "AlertFusion")])

    sep = "-" * (w_label + w_b + w_a + 10)
    print(sep)
    print(f"| {'Metric':<{w_label}} | {'Baseline':>{w_b}} | {'AlertFusion':>{w_a}} |")
    print(sep)
    for lab, vb, va in labels:
        print(f"| {lab:<{w_label}} | {vb:>{w_b}} | {va:>{w_a}} |")
    print(sep)

    if "alignment_metrics" in metrics:
        al = metrics["alignment_metrics"]
        ab = al["baseline"]
        aa = al["alert_fusion"]
        align_labels = [
            ("Faults loaded", str(al["ground_truth_records_loaded"]), str(al["ground_truth_records_loaded"])),
            ("Precision (temporal)", f"{ab['precision_temporal']:.4f}", f"{aa['precision_temporal']:.4f}"),
            ("Recall (temporal)", f"{ab['recall_temporal']:.4f}", f"{aa['recall_temporal']:.4f}"),
            (
                "Precision (component)",
                f"{ab['precision_component_aware']:.4f}",
                f"{aa['precision_component_aware']:.4f}",
            ),
            (
                "Recall (component)",
                f"{ab['recall_component_aware']:.4f}",
                f"{aa['recall_component_aware']:.4f}",
            ),
        ]
        print("\nFault alignment (vs ground truth):")
        print(sep)
        print(f"| {'Metric':<{w_label}} | {'Baseline':>{w_b}} | {'AlertFusion':>{w_a}} |")
        print(sep)
        for lab, vb, va in align_labels:
            print(f"| {lab:<{w_label}} | {vb:>{w_b}} | {va:>{w_a}} |")
        print(sep)

    print(f"Formula: {metrics['nrr_formula']}")


def run_cli() -> None:
    parser = argparse.ArgumentParser(
        description="Syncident CLI: ingest -> baseline -> AlertFusion -> comparison matrix.",
    )
    parser.add_argument("--input", type=Path, required=True, help="LogHub text log or AIOpsArena CSV.")
    parser.add_argument(
        "--mode",
        choices=["text", "csv"],
        default=None,
        help="Ingestion pathway (default: inferred from file suffix).",
    )
    parser.add_argument(
        "--ground-truth",
        type=Path,
        default=None,
        help="Optional ground-truth JSON for temporal + component-aware precision/recall.",
    )
    # Tuning parameters — default to the module constants when omitted.
    parser.add_argument("--assign-threshold", type=float, default=ASSIGN_THRESHOLD)
    parser.add_argument("--active-window-sec", type=int, default=ACTIVE_WINDOW_SEC)
    parser.add_argument("--baseline-window-sec", type=int, default=BASELINE_WINDOW_SEC)
    parser.add_argument("--flapping-window-sec", type=int, default=FLAPPING_WINDOW_SEC)
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    if args.ground_truth is not None and not args.ground_truth.is_file():
        print(f"Ground-truth not found: {args.ground_truth}", file=sys.stderr)
        sys.exit(1)

    result = analyze(
        args.input,
        mode=args.mode,
        ground_truth_path=args.ground_truth,
        assign_threshold=args.assign_threshold,
        active_window_sec=args.active_window_sec,
        baseline_window_sec=args.baseline_window_sec,
        flapping_window_sec=args.flapping_window_sec,
    )

    print(
        f"Normalized events: {len(result.events_df)} (skipped {result.skipped_count}).\n"
        f"Parameters: BASELINE_WINDOW_SEC={args.baseline_window_sec}, "
        f"ACTIVE_WINDOW_SEC={args.active_window_sec}, ASSIGN_THRESHOLD={args.assign_threshold}, "
        f"FLAPPING_WINDOW_SEC={args.flapping_window_sec}.\n"
    )
    print_comparison_table(result.metrics)


# ===========================================================================
# Enterprise dashboard — local stream selector + automated visual report
# ===========================================================================
def _running_under_streamlit() -> bool:
    """True only when an actual Streamlit runtime is active (no CLI warning side-effects)."""
    try:
        from streamlit.runtime import exists

        return bool(exists())
    except Exception:
        return False


def _incident_alignment_flags(
    alert_incidents: pd.DataFrame, gt_df: pd.DataFrame | None
) -> pd.DataFrame:
    """
    Per-incident alignment against ground-truth faults (deterministic).

    Adds boolean columns aligned_temporal / aligned_component so the case-study
    selectors can pick verified hits and misses without any user interaction.
    """
    out = alert_incidents.copy()
    if out.empty:
        out["aligned_temporal"] = pd.Series(dtype="bool")
        out["aligned_component"] = pd.Series(dtype="bool")
        return out

    starts = pd.to_datetime(out["start_time"])
    ends = pd.to_datetime(out["end_time"])
    comp_sets = out["involved_components"].map(_safe_component_set)

    temporal = [False] * len(out)
    component = [False] * len(out)

    if gt_df is not None and not gt_df.empty:
        gt = gt_df.copy()
        gt["fault_start"] = pd.to_datetime(gt["fault_start"])
        gt["fault_end"] = pd.to_datetime(gt["fault_end"])
        gt["service_components"] = gt["service_components"].map(_safe_component_set)
        for i in range(len(out)):
            i_start = starts.iloc[i]
            i_end = ends.iloc[i]
            i_comp = comp_sets.iloc[i]
            for _, frow in gt.iterrows():
                if i_start <= frow["fault_end"] and i_end >= frow["fault_start"]:
                    temporal[i] = True
                    svc = frow["service_components"]
                    if svc and i_comp.intersection(svc):
                        component[i] = True

    out["aligned_temporal"] = temporal
    out["aligned_component"] = component
    return out


def _pick_success_incident(flagged: pd.DataFrame) -> int | None:
    """Largest verified hit: component-aware (or temporal) match with the most events."""
    if flagged.empty:
        return None
    aligned = flagged[flagged["aligned_component"]]
    if aligned.empty:
        aligned = flagged[flagged["aligned_temporal"]]
    if aligned.empty:
        return None
    aligned = aligned.sort_values(["event_count", "incident_id"], ascending=[False, True])
    return int(aligned.iloc[0]["incident_id"])


def _pick_error_incident(flagged: pd.DataFrame, exclude_id: int | None) -> int | None:
    """
    Representative model limitation: prefer a singleton incident (isolated/split
    cluster caused by text similarity falling below the assignment threshold), then a
    temporally-unaligned incident (false positive). Never reuse the success incident.
    """
    if flagged.empty:
        return None
    pool = flagged[flagged["incident_id"] != exclude_id] if exclude_id is not None else flagged

    singletons = pool[pool["event_count"] == 1]
    if not singletons.empty:
        in_window = singletons[singletons["aligned_temporal"]]
        chosen = in_window if not in_window.empty else singletons
        return int(chosen.sort_values("incident_id").iloc[0]["incident_id"])

    unaligned = pool[~pool["aligned_temporal"]]
    if not unaligned.empty:
        return int(unaligned.sort_values(["event_count", "incident_id"], ascending=[False, True]).iloc[0]["incident_id"])

    if not pool.empty:
        return int(pool.sort_values("incident_id").iloc[0]["incident_id"])
    return None


def _scan_local_files(scan_dir: str = DASHBOARD_SCAN_DIR) -> list[str]:
    """Discover analyzable .log/.txt/.csv files in the scan directory (sorted, de-duplicated)."""
    base = Path(scan_dir)
    found: set[str] = set()
    for pattern in DASHBOARD_FILE_GLOBS:
        for p in base.glob(pattern):
            if p.is_file():
                found.add(p.name)
    return sorted(found)


def _discover_ground_truth(log_path: Path) -> Path | None:
    """
    Auto-locate a ground-truth JSON for a selected file without any user input.

    Tries, in order: "<stem>_gt.json", "<stem>.gt.json", then the generic
    DEFAULT_GT_FILENAME — all relative to the selected file's directory.
    """
    directory = log_path.parent if str(log_path.parent) else Path(".")
    candidates = [
        directory / f"{log_path.stem}_gt.json",
        directory / f"{log_path.stem}.gt.json",
        directory / DEFAULT_GT_FILENAME,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def render_dashboard() -> None:
    """Enterprise incident-noise-reduction dashboard with an automatic local stream selector."""
    import plotly.express as px
    import streamlit as st

    st.set_page_config(page_title="Syncident — Enterprise Platform", layout="wide")

    @st.cache_data(show_spinner="Running Syncident pipeline…")
    def _run(log_path: str, gt_path: str | None, mode: str) -> AnalysisResult:
        gt_arg = gt_path if gt_path and Path(gt_path).is_file() else None
        return analyze(log_path, mode=mode, ground_truth_path=gt_arg)

    st.title("Syncident — Enterprise Incident Noise Reduction Platform")

    # --- Sidebar: automatic local stream selector -------------------------
    found_files = _scan_local_files()
    with st.sidebar:
        st.header("Data source")
        if not found_files:
            st.warning(
                f"No `.log`, `.txt`, or `.csv` streams found in "
                f"`{Path(DASHBOARD_SCAN_DIR).resolve()}`. Add a dataset and reload."
            )
            st.stop()
        selected_file = st.selectbox("Select Target Stream", found_files)

    selected_path = Path(selected_file)
    # Route the stream into a logical family by extension.
    mode = detect_family(selected_path)
    family = "LogHub (text)" if mode == "text" else "AIOpsArena (structured)"
    gt_path = _discover_ground_truth(selected_path)

    if not selected_path.is_file():
        st.error(f"Selected stream no longer exists: `{selected_file}`")
        st.stop()

    try:
        result = _run(str(selected_path), str(gt_path) if gt_path else None, mode)
    except Exception as exc:  # keep the platform responsive on a malformed dataset
        st.error(f"Pipeline failed for `{selected_file}`: {type(exc).__name__}: {exc}")
        st.stop()

    metrics = result.metrics

    # Sidebar config: read the parameters this run actually used (not the static globals).
    params = metrics["parameters"]
    with st.sidebar:
        st.divider()
        st.markdown(
            f"- **Family:** `{family}`\n"
            f"- **Ingestion mode:** `{mode}`\n"
            f"- **Ground truth:** `{gt_path.name if gt_path else 'Unavailable'}`\n"
            f"- **Assign threshold:** `{params['assign_threshold']}`\n"
            f"- **Active window:** `{params['active_window_sec']}s`\n"
            f"- **Baseline window:** `{params['baseline_window_sec']}s`\n"
            f"- **Flapping window:** `{params['flapping_window_sec']}s`"
        )

    # All views read straight from the in-memory result — no disk round-trips.
    incidents_summary = result.alert_incidents
    has_gt = result.ground_truth_df is not None and not result.ground_truth_df.empty

    st.caption(
        f"Analyzing **{selected_file}** — {family} family — "
        f"{metrics['n_raw_logs']:,} normalized events."
    )

    # --- 1. Executive KPI section -----------------------------------------
    st.header("Executive summary")
    b = metrics["baseline"]
    a = metrics["alert_fusion"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Raw logs", f"{metrics['n_raw_logs']:,}")
    c2.metric("Baseline incidents", f"{b['n_incidents']:,}")
    c3.metric("AlertFusion incidents", f"{a['n_incidents']:,}")
    c4.metric(
        "Noise Reduction Ratio",
        f"{a['nrr']:.4f}",
        delta=f"{a['nrr'] - b['nrr']:+.4f} vs baseline",
    )

    st.subheader("Fault alignment (AlertFusion vs ground truth)")
    if has_gt:
        align = metrics["alignment_metrics"]["alert_fusion"]
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("Precision (temporal)", f"{align['precision_temporal']:.3f}")
        p2.metric("Recall (temporal)", f"{align['recall_temporal']:.3f}")
        p3.metric("Precision (component)", f"{align['precision_component_aware']:.3f}")
        p4.metric("Recall (component)", f"{align['recall_component_aware']:.3f}")
    else:
        st.info(
            f"No matching ground-truth JSON found for `{selected_file}` — "
            "alignment metrics and timeline fault bands are **Unavailable**."
        )

    # --- 2. Comparative summary table -------------------------------------
    st.subheader("Baseline vs AlertFusion")
    comparison = pd.DataFrame(
        {
            "Metric": ["Incidents", "NRR", "Avg duration (s)", "Avg components / incident"],
            "Baseline": [
                b["n_incidents"],
                b["nrr"],
                b["avg_duration_seconds"],
                b["avg_components_per_incident"],
            ],
            "AlertFusion": [
                a["n_incidents"],
                a["nrr"],
                a["avg_duration_seconds"],
                a["avg_components_per_incident"],
            ],
        }
    )
    st.dataframe(comparison, width="stretch", hide_index=True)
    if result.skipped_count:
        st.caption(f"Skipped {result.skipped_count:,} malformed input rows during ingestion.")

    st.divider()

    # --- 3. Timeline section (rendered from the lightweight summary) ------
    st.header("Incident timeline")
    if incidents_summary.empty:
        st.info("No incidents to plot.")
    else:
        plot_df = incidents_summary.copy()
        plot_df["time"] = pd.to_datetime(plot_df["start_time"])
        plot_df["top_component"] = (
            plot_df["involved_components"].fillna("").astype(str).map(
                lambda s: s.split(";")[0] if s else "—"
            )
        )
        fig = px.scatter(
            plot_df,
            x="time",
            y="incident_id",
            size="event_count",
            color="top_component",
            title="AlertFusion incidents over time (marker size = event volume)",
            labels={"time": "Time", "incident_id": "AlertFusion incident ID"},
        )
        fig.update_traces(marker=dict(opacity=0.6))
        fig.update_layout(showlegend=False)
        if has_gt:
            for _, fault in result.ground_truth_df.iterrows():
                fig.add_vrect(
                    x0=fault["fault_start"],
                    x1=fault["fault_end"],
                    fillcolor="red",
                    opacity=0.12,
                    line_width=0,
                )
            st.caption("Red bands mark ground-truth fault windows (hits fall inside, misses outside).")
        st.plotly_chart(fig, width="stretch")

    st.divider()

    # --- 4. Incident explorer (direct in-memory filter) -------------------
    st.header("Incident explorer")
    if incidents_summary.empty:
        st.info("No incidents available to inspect.")
    else:
        options = incidents_summary["incident_id"].astype(int).tolist()
        selected_incident = st.selectbox("Inspect an incident", options)
        srow = incidents_summary[incidents_summary["incident_id"] == selected_incident].iloc[0]
        st.caption(
            f"Window: {srow['start_time']} → {srow['end_time']}  |  "
            f"{int(srow['event_count'])} events  |  components: {srow['involved_components']}"
        )
        slice_df = result.events_df[result.events_df["alert_incident_id"] == int(selected_incident)]
        st.dataframe(slice_df, width="stretch", hide_index=True)

    st.divider()

    # --- 5. Algorithmic case studies (in-memory alignment flags) ----------
    st.header("Algorithmic case studies")
    flagged = _incident_alignment_flags(result.alert_incidents, result.ground_truth_df)
    success_id = _pick_success_incident(flagged)
    error_id = _pick_error_incident(flagged, exclude_id=success_id)

    cs1, cs2 = st.columns(2)

    with cs1:
        st.subheader("Case Study 1 — Perfect alignment")
        if success_id is None:
            st.info("No verified hit available for this dataset/ground truth.")
        else:
            row = flagged[flagged["incident_id"] == success_id].iloc[0]
            st.markdown(
                f"**Incident `{success_id}`** grouped a burst of "
                f"**{int(row['event_count'])}** cascading events that falls inside a fault "
                f"window (temporal{' + component-aware' if row['aligned_component'] else ''} match)."
            )
            st.caption(f"Window: {row['start_time']} → {row['end_time']}  |  components: {row['involved_components']}")
            st.dataframe(
                result.events_df[result.events_df["alert_incident_id"] == int(success_id)],
                width="stretch",
                hide_index=True,
            )

    with cs2:
        st.subheader("Case Study 2 — Model limitation")
        if error_id is None:
            st.info("No representative error case available for this dataset/ground truth.")
        else:
            row = flagged[flagged["incident_id"] == error_id].iloc[0]
            if int(row["event_count"]) == 1:
                reason = (
                    "isolated singleton — text similarity dropped below the "
                    f"{ASSIGN_THRESHOLD} threshold, so this event was split into its own cluster"
                )
            elif not bool(row["aligned_temporal"]):
                reason = "false positive — incident does not overlap any ground-truth fault window"
            else:
                reason = "boundary case for manual trace inspection"
            st.markdown(f"**Incident `{error_id}`** illustrates a {reason}.")
            st.caption(f"Window: {row['start_time']} → {row['end_time']}  |  components: {row['involved_components']}")
            st.dataframe(
                result.events_df[result.events_df["alert_incident_id"] == int(error_id)],
                width="stretch",
                hide_index=True,
            )


# ===========================================================================
# Entry point — static dashboard under Streamlit, comparison matrix under python
# ===========================================================================
if __name__ == "__main__":
    if _running_under_streamlit():
        render_dashboard()
    else:
        run_cli()
