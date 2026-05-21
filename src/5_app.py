"""
Syncident UI.
Run: streamlit run src/5_app.py
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import gc
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _load_pipeline_module(alias: str, filename: str):
    module_path = SRC / filename
    spec = importlib.util.spec_from_file_location(alias, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


ingestion = _load_pipeline_module("pipeline_ingestion", "1_ingestion.py")
baseline = _load_pipeline_module("pipeline_baseline", "2_baseline.py")
clustering = _load_pipeline_module("pipeline_clustering", "3_clustering.py")
evaluation = _load_pipeline_module("pipeline_evaluation", "4_evaluation.py")

DURATION_CAP_MULTIPLIER = 1.75
DURATION_CAP_FLOOR_SEC = 240.0
DEPTH_QUICK = "Quick (1 min)"
DEPTH_BALANCED = "Balanced (3 mins)"
DEPTH_DEEP = "Deep (No Limit)"
DEPTH_OPTIONS = [DEPTH_QUICK, DEPTH_BALANCED, DEPTH_DEEP]
PREVIEW_MAX_BYTES = 25 * 1024 * 1024
UI_TABLE_MAX_ROWS = 4000
UI_INCIDENT_EVENTS_MAX_ROWS = 2500
SESSION_MAX_SKIPPED_PREVIEW = 2000

CAPTION_W_TIME = "Weights how much the temporal proximity between logs matters for grouping."
CAPTION_W_COMPONENT = "Weights how much sharing the same Block ID contributes to the similarity score."
CAPTION_W_TEXT = "Weights how much the similarity of log message templates influences grouping."
CAPTION_ASSIGN = "Minimum similarity score required to join an existing incident (Higher = stricter grouping)."


# ---------- UI helpers ----------
def _inject_styles() -> None:
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
            html, body, [class*="css"]  { font-family: 'Inter', 'Segoe UI', sans-serif; }
            .stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] { background: #0E1117 !important; }
            [data-testid="stSidebar"] { background: #0B0E13 !important; border-right: 1px solid rgba(148,163,184,0.14); }
            .block-container { max-width: 1260px; padding-top: 1rem; }

            .hero-title {
                margin: 0 0 0.8rem 0;
                color: #E5E7EB;
                font-size: 2.0rem;
                font-weight: 800;
                letter-spacing: -0.03em;
            }
            .funnel-card {
                border: 1px solid rgba(255,255,255,0.16);
                border-radius: 14px;
                background: linear-gradient(180deg, rgba(17,24,39,0.62) 0%, rgba(15,23,42,0.52) 100%);
                backdrop-filter: blur(10px);
                padding: 1.1rem 1.2rem;
                min-height: 168px;
            }
            .funnel-label {
                margin: 0;
                color: #94A3B8;
                font-size: 0.74rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.1em;
            }
            .funnel-value {
                margin: 0.35rem 0 0 0;
                color: #9CA3AF;
                font-size: 2.4rem;
                font-weight: 800;
                letter-spacing: -0.03em;
            }
            .funnel-value-cyan {
                margin: 0.35rem 0 0 0;
                color: #00f2ff;
                font-size: 2.4rem;
                font-weight: 800;
                letter-spacing: -0.03em;
                text-shadow: 0 0 18px rgba(0,242,255,0.22);
            }
            .funnel-arrow {
                height: 58px;
                border-radius: 999px;
                margin-top: 2.3rem;
                background: linear-gradient(90deg, rgba(148,163,184,0.14) 0%, rgba(0,242,255,0.72) 45%, rgba(0,242,255,1) 100%);
                clip-path: polygon(0 24%, 80% 24%, 80% 0, 100% 50%, 80% 100%, 80% 76%, 0 76%);
                border: 1px solid rgba(255,255,255,0.14);
                box-shadow: 0 0 22px rgba(0,242,255,0.18) inset;
            }
            .eff-badge {
                margin-top: 0.9rem;
                display: inline-block;
                padding: 0.36rem 0.9rem;
                border-radius: 999px;
                border: 1px solid rgba(0,255,136,0.45);
                color: #00ff88;
                background: rgba(0,255,136,0.10);
                font-size: 0.83rem;
                font-weight: 700;
                letter-spacing: 0.01em;
            }

            .kpi-card {
                border: 1px solid rgba(255,255,255,0.16);
                border-radius: 14px;
                background: rgba(15,23,42,0.52);
                backdrop-filter: blur(10px);
                padding: 1rem 1.1rem;
                min-height: 122px;
            }
            .kpi-label {
                margin: 0;
                color: #94A3B8;
                font-size: 0.73rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.09em;
            }
            .kpi-value {
                margin: 0.45rem 0 0 0;
                color: #E5E7EB;
                font-size: 1.85rem;
                font-weight: 800;
                letter-spacing: -0.03em;
            }
            .kpi-value-green {
                margin: 0.45rem 0 0 0;
                color: #00ff88;
                font-size: 1.85rem;
                font-weight: 800;
                letter-spacing: -0.03em;
            }

            .summary-card {
                border: 1px solid rgba(255,255,255,0.16);
                border-radius: 14px;
                background: rgba(15,23,42,0.52);
                backdrop-filter: blur(10px);
                padding: 1rem 1.1rem;
                margin-top: 0.9rem;
            }
            .summary-title {
                margin: 0 0 0.45rem 0;
                color: #94A3B8;
                font-size: 0.73rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.09em;
            }
            .summary-text {
                margin: 0;
                color: #E5E7EB;
                font-size: 1.02rem;
                line-height: 1.6;
            }

            .ring-wrap {
                margin-top: 0.55rem;
                display: flex;
                align-items: center;
                gap: 0.75rem;
            }

            .pulse-track {
                width: 100%;
                height: 3px;
                border-radius: 999px;
                background: rgba(148,163,184,0.22);
                overflow: hidden;
            }
            .pulse-bar {
                width: 36%;
                height: 100%;
                background: linear-gradient(90deg, #00f2ff 0%, #00ff88 100%);
                animation: pulseMove 1.25s ease-in-out infinite;
                border-radius: 999px;
            }
            @keyframes pulseMove {
                0% { transform: translateX(-120%); }
                50% { transform: translateX(120%); }
                100% { transform: translateX(300%); }
            }

            /* Sidebar button styling */
            [data-testid="stSidebar"] button[kind="primary"] {
                background: linear-gradient(90deg, #00d9ff 0%, #00b8ff 100%) !important;
                color: #03131c !important;
                border: 1px solid rgba(0,242,255,0.55) !important;
                font-weight: 700 !important;
            }
            [data-testid="stSidebar"] button[kind="primary"]:hover {
                filter: brightness(1.04);
            }
            [data-testid="stSidebar"] button[kind="secondary"] {
                box-shadow: 0 0 0 1px rgba(0,242,255,0.25), 0 0 16px rgba(0,242,255,0.18) !important;
            }

            .stTabs [data-baseweb="tab-list"] {
                gap: 0.35rem;
                background: transparent;
            }
            .stTabs [data-baseweb="tab"] {
                color: #94A3B8;
                border-radius: 10px;
                background: transparent;
                border: 1px solid rgba(148,163,184,0.15);
                padding: 0.35rem 0.8rem;
            }
            .stTabs [aria-selected="true"] {
                color: #E5E7EB !important;
                border-color: rgba(0,242,255,0.45) !important;
                background: rgba(0,242,255,0.06) !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _show_pulse(progress_slot: Any) -> None:
    progress_slot.markdown(
        "<div class='pulse-track'><div class='pulse-bar'></div></div>",
        unsafe_allow_html=True,
    )


def _hide_pulse(progress_slot: Any) -> None:
    progress_slot.empty()


def _ring_svg(percent: float) -> str:
    p = max(0.0, min(100.0, percent))
    r = 27
    c = 2 * 3.14159265 * r
    dash = c * p / 100.0
    rest = c - dash
    return (
        "<svg width='74' height='74' viewBox='0 0 74 74' xmlns='http://www.w3.org/2000/svg'>"
        "<circle cx='37' cy='37' r='27' stroke='rgba(148,163,184,0.28)' stroke-width='7' fill='none'/>"
        f"<circle cx='37' cy='37' r='27' stroke='#00ff88' stroke-width='7' fill='none' "
        f"stroke-dasharray='{dash:.2f} {rest:.2f}' stroke-linecap='round' transform='rotate(-90 37 37)'/>"
        f"<text x='37' y='41' text-anchor='middle' fill='#00ff88' font-size='12' font-weight='700'>{p:.1f}%</text>"
        "</svg>"
    )


# ---------- Pipeline helpers ----------
def _init_state() -> None:
    if "r1" not in st.session_state:
        st.session_state.r1 = clustering.W_TIME
    if "r2" not in st.session_state:
        st.session_state.r2 = clustering.W_COMPONENT
    if "r3" not in st.session_state:
        st.session_state.r3 = clustering.W_TEXT
    if "thr" not in st.session_state:
        st.session_state.thr = clustering.ASSIGN_THRESHOLD
    if "opt_depth" not in st.session_state:
        st.session_state.opt_depth = DEPTH_BALANCED
    if "run_data" not in st.session_state:
        st.session_state.run_data = None
    if "manual_csv_mapping" not in st.session_state:
        st.session_state.manual_csv_mapping = False


def _iter_weights_grid_01() -> Iterator[tuple[float, float, float]]:
    for i in range(11):
        for j in range(11 - i):
            k = 10 - i - j
            yield i / 10.0, j / 10.0, k / 10.0


def _deterministic_recipe_subset(
    grid: list[tuple[float, float, float]],
    k: int,
) -> list[tuple[float, float, float]]:
    """Select k recipes deterministically with stable coverage over the full grid."""
    if k >= len(grid):
        return grid
    if k <= 1:
        return [grid[0]]

    picks: list[tuple[float, float, float]] = []
    used: set[int] = set()
    n = len(grid) - 1
    for i in range(k):
        idx = (i * n) // (k - 1)
        while idx in used and idx < len(grid) - 1:
            idx += 1
        if idx in used:
            idx = max(j for j in range(len(grid)) if j not in used)
        used.add(idx)
        picks.append(grid[idx])
    return picks


def _pick_weight_recipes(depth: str) -> list[tuple[float, float, float]]:
    grid = list(_iter_weights_grid_01())
    if depth == DEPTH_QUICK:
        return _deterministic_recipe_subset(grid, k=min(15, len(grid)))
    if depth == DEPTH_BALANCED:
        return _deterministic_recipe_subset(grid, k=min(35, len(grid)))
    return grid


def _safe_run_dir() -> Path:
    base = DATA / datetime.now().strftime("run_%Y%m%d_%H%M%S")
    out = base
    idx = 1
    while out.exists():
        out = Path(f"{base}_{idx}")
        idx += 1
    out.mkdir(parents=True, exist_ok=True)
    return out


def _downsample_rows(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    stride = int(math.ceil(len(df) / max_rows))
    return df.iloc[::stride].copy()


def _prepare_table_df(df: pd.DataFrame, max_rows: int) -> tuple[pd.DataFrame, bool]:
    sampled = len(df) > max_rows
    return (_downsample_rows(df, max_rows), sampled)


@st.cache_data(show_spinner=False)
def _load_ui_context(run_dir: str) -> dict[str, Any]:
    """Disk-backed dashboard context from incidents_summary.json only."""
    return evaluation.load_ui_context(Path(run_dir))


@st.cache_data(show_spinner=False)
def _load_incident_events(run_dir: str, incident_id: str) -> list[dict[str, str]]:
    """Lazy-load one incident cluster from incident_events_mapping.json."""
    mapping_path = Path(run_dir) / evaluation.INCIDENT_EVENTS_MAPPING_FILENAME
    return evaluation.load_incident_events_slice(mapping_path, incident_id)


def _load_known_issues(run_dir: str) -> pd.DataFrame | None:
    """Optional ground-truth overlay; kept small and loaded from disk on demand."""
    path = Path(run_dir) / "known_issues.json"
    if not path.is_file():
        return None
    gt_df, _skipped = evaluation.load_ground_truth(path)
    return gt_df if not gt_df.empty else None


def _normalize_upload(uploaded: Any, parser_mode: str, mapper: dict[str, str], regex_pattern: str):
    normalizer = ingestion.LogNormalizer()
    if parser_mode == "csv":
        uploaded.seek(0)
        return normalizer.normalize_csv_stream(
            uploaded,
            timestamp_col=mapper["timestamp"],
            component_col=mapper["component"],
            raw_message_col=mapper["raw_message"],
        )
    if parser_mode == "json":
        uploaded.seek(0)
        return normalizer.normalize_json(uploaded.read())
    if parser_mode == "text":
        uploaded.seek(0)
        text_stream = io.TextIOWrapper(uploaded, encoding="utf-8", errors="replace")
        try:
            return normalizer.normalize_text(text_stream, regex_pattern)
        finally:
            text_stream.detach()
    raise ValueError(f"Unsupported parser mode: {parser_mode}")


def _build_metrics(baseline_df: pd.DataFrame, smart_df: pd.DataFrame, n_raw: int) -> dict[str, Any]:
    return {
        "n_raw_logs": int(n_raw),
        "baseline": evaluation.summarize_method(baseline_df, n_raw, "baseline"),
        "alert_fusion": evaluation.summarize_method(smart_df, n_raw, "smart"),
    }


def _save_run_artifacts(
    run_dir: Path,
    normalized_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    smart_df: pd.DataFrame,
    metrics: dict[str, Any],
    metadata: dict[str, Any],
    baseline_event_ids: list[int],
    smart_event_ids: list[int],
) -> None:
    normalized_df.to_csv(run_dir / "normalized_events.csv", index=False)
    baseline_df.to_csv(run_dir / "baseline_incidents.csv", index=False)
    smart_df.to_csv(run_dir / "smart_incidents.csv", index=False)
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (run_dir / "run_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    evaluation.export_split_artifacts(
        run_dir,
        normalized_df,
        baseline_df,
        smart_df,
        metrics,
        baseline_event_ids=baseline_event_ids,
        smart_event_ids=smart_event_ids,
    )


def _run_pipeline(
    uploaded: Any,
    parser_mode: str,
    mapper: dict[str, str],
    regex_pattern: str,
    params: Any,
    known_issues_upload: Any | None = None,
) -> dict[str, Any]:
    result = _normalize_upload(uploaded, parser_mode, mapper, regex_pattern)
    if result.normalized_df.empty:
        raise ValueError("No valid normalized rows were produced.")

    run_dir = _safe_run_dir()
    normalized_path = run_dir / "normalized_events.csv"
    norm_df = result.normalized_df
    required = {"timestamp", "component", "raw_message"}
    missing = required - set(norm_df.columns)
    if missing:
        raise ValueError(f"Normalized data missing columns: {sorted(missing)}")
    # Reuse normalized frame in-memory to avoid a write/read roundtrip on large files.
    norm_df["_ts"] = pd.to_datetime(norm_df["timestamp"])
    norm_df["_ord"] = range(len(norm_df))
    norm_df.sort_values(["_ts", "_ord"], kind="mergesort", inplace=True)
    norm_df.reset_index(drop=True, inplace=True)
    norm_df["_row_id"] = range(len(norm_df))
    norm_df["raw_message"] = norm_df["raw_message"].fillna("").astype(str)
    norm_df.to_csv(normalized_path, index=False)

    skipped_records = result.skipped_records
    skipped_count = result.skipped_count
    skipped_truncated = int(result.skipped_overflow_count)
    if len(skipped_records) > SESSION_MAX_SKIPPED_PREVIEW:
        skipped_records = skipped_records[:SESSION_MAX_SKIPPED_PREVIEW]
    del result
    gc.collect()

    baseline_df = baseline.group_baseline_incidents(norm_df)
    incidents, alert_assign = clustering.run_alert_fusion(norm_df, params)
    smart_df = clustering.incidents_to_dataframe(incidents)
    del incidents
    gc.collect()
    baseline_path = run_dir / "baseline_incidents.csv"
    smart_path = run_dir / "smart_incidents.csv"
    baseline_df.to_csv(baseline_path, index=False)
    smart_df.to_csv(smart_path, index=False)

    known_issues_path: Path | None = None
    known_issues_df: pd.DataFrame | None = None
    known_issues_skipped = 0
    if known_issues_upload is not None:
        known_issues_path = run_dir / "known_issues.json"
        known_issues_upload.seek(0)
        known_issues_path.write_bytes(known_issues_upload.read())
        known_issues_df, known_issues_skipped = evaluation.load_ground_truth(known_issues_path)

    metrics = evaluation.build_metrics_from_frames(
        baseline_df,
        smart_df,
        norm_df,
        known_issues_path,
    )
    metadata = {
        "run_time": datetime.now().isoformat(timespec="seconds"),
        "source_file": str(uploaded.name),
        "mode": parser_mode,
        "skipped_count": skipped_count,
        "known_issues_file": str(known_issues_upload.name) if known_issues_upload is not None else None,
        "known_issues_skipped": int(known_issues_skipped),
        "params": {
            "w_time": params.w_time,
            "w_component": params.w_component,
            "w_text": params.w_text,
            "assign_threshold": params.assign_threshold,
        },
    }
    export_base_assign = baseline.event_to_baseline_incident_ids(norm_df).tolist()
    if len(alert_assign) != len(norm_df):
        raise ValueError("Internal assignment mismatch: AlertFusion assignments do not match event count.")
    _save_run_artifacts(
        run_dir,
        norm_df,
        baseline_df,
        smart_df,
        metrics,
        metadata,
        export_base_assign,
        alert_assign,
    )

    del norm_df
    del baseline_df
    del smart_df
    del alert_assign
    del export_base_assign
    gc.collect()

    return {
        "run_dir": str(run_dir),
        "skipped_records": skipped_records,
        "skipped_records_truncated": skipped_truncated,
    }


def _run_auto_tune(df: pd.DataFrame, assign_threshold: float, recipes: list[tuple[float, float, float]]) -> tuple[tuple[float, float, float], dict[str, Any]]:
    base_df = baseline.group_baseline_incidents(df)
    s = pd.to_datetime(base_df["start_time"])
    e = pd.to_datetime(base_df["end_time"])
    cap = max(float((e - s).dt.total_seconds().mean()) * DURATION_CAP_MULTIPLIER, DURATION_CAP_FLOOR_SEC)

    best = (clustering.W_TIME, clustering.W_COMPONENT, clustering.W_TEXT)
    best_nrr = -1.0
    feasible = 0
    for w1, w2, w3 in recipes:
        p = clustering.FusionParams(w1, w2, w3, assign_threshold)
        inc, _ = clustering.run_alert_fusion(df, p)
        smart_df = clustering.incidents_to_dataframe(inc)
        nrr = evaluation.noise_reduction_ratio(len(smart_df), len(df))
        avg_dur = float((pd.to_datetime(smart_df["end_time"]) - pd.to_datetime(smart_df["start_time"])).dt.total_seconds().mean())
        if avg_dur <= cap:
            feasible += 1
            if nrr > best_nrr:
                best_nrr = nrr
                best = (w1, w2, w3)

    return best, {"tested_count": len(recipes), "feasible_count": feasible}


# ---------- Rendering ----------
def _sidebar_ui() -> dict[str, Any]:
    _init_state()
    st.sidebar.markdown("### Input")
    uploaded = st.sidebar.file_uploader("Upload file", type=["csv", "txt", "log", "json"])
    with st.sidebar.expander("Validate Results (Optional)", expanded=False):
        known_issues_upload = st.sidebar.file_uploader("Upload Known Issues", type=["json"])
    run_clicked = st.sidebar.button("Run Analysis", type="primary", use_container_width=True)

    parser_mode = "text"
    mapper: dict[str, str] = {}
    regex_pattern = ingestion.HDFS_REGEX_TEMPLATE
    preview_df: pd.DataFrame | None = None
    csv_columns: list[str] = []
    csv_mapping_warning: str | None = None
    allow_manual_fix = False

    if uploaded is not None and str(uploaded.name).lower().endswith(".csv"):
        parser_mode = "csv"
        normalizer = ingestion.LogNormalizer()
        file_size = int(getattr(uploaded, "size", 0) or 0)
        sample_df = pd.DataFrame()
        try:
            uploaded.seek(0)
            sample_df = pd.read_csv(uploaded, nrows=ingestion.SCHEMA_DISCOVERY_SAMPLE_ROWS)
            uploaded.seek(0)
            csv_columns = [str(c) for c in sample_df.columns]
        except Exception:
            csv_columns = []
            sample_df = pd.DataFrame()

        if file_size <= PREVIEW_MAX_BYTES and not sample_df.empty:
            preview_df = sample_df.head(40).copy()
        else:
            if file_size > PREVIEW_MAX_BYTES:
                st.sidebar.caption("Preview disabled for large files to preserve memory.")

        mapper = normalizer.discover_schema(sample_df)
        if not mapper:
            csv_mapping_warning = "Could not auto-detect a valid CSV schema. CSV files must have at least 3 columns."
        else:
            ts_ok = normalizer.timestamp_column_looks_valid(sample_df, mapper["timestamp"])
            if not ts_ok:
                csv_mapping_warning = (
                    f"Detected timestamp column '{mapper['timestamp']}' does not appear to contain valid dates."
                )
                allow_manual_fix = True
            st.sidebar.caption(
                "Automatically detected schema: "
                f"[{mapper.get('timestamp', '?')}, {mapper.get('component', '?')}, {mapper.get('raw_message', '?')}]"
            )
    elif uploaded is not None and str(uploaded.name).lower().endswith(".json"):
        parser_mode = "json"
        st.session_state.manual_csv_mapping = False
    else:
        st.session_state.manual_csv_mapping = False

    with st.sidebar.expander("System Internals", expanded=False):
        if parser_mode == "text":
            regex_pattern = st.text_area("Regex Parser", value=ingestion.HDFS_REGEX_TEMPLATE, height=120)
        elif parser_mode == "csv":
            st.caption("Schema auto-discovery is enabled for one-click analysis.")
            if csv_mapping_warning:
                st.warning(csv_mapping_warning)
            if allow_manual_fix and st.button("Fix Mapping", use_container_width=True):
                st.session_state.manual_csv_mapping = True
            if st.session_state.manual_csv_mapping and csv_columns:
                mapper["timestamp"] = st.selectbox("Timestamp", csv_columns, key="map_ts")
                mapper["component"] = st.selectbox("Component/Source", csv_columns, key="map_comp")
                mapper["raw_message"] = st.selectbox("Raw Message", csv_columns, key="map_msg")
        else:
            st.caption("JSON mode uses automatic field detection for timestamp/component/message.")

        st.markdown("**W_TIME**")
        st.slider("W_TIME", 0.0, 1.0, step=0.1, key="r1", label_visibility="collapsed")
        st.caption(CAPTION_W_TIME)
        st.markdown("**W_COMPONENT**")
        st.slider("W_COMPONENT", 0.0, 1.0, step=0.1, key="r2", label_visibility="collapsed")
        st.caption(CAPTION_W_COMPONENT)
        st.markdown("**W_TEXT**")
        st.slider("W_TEXT", 0.0, 1.0, step=0.1, key="r3", label_visibility="collapsed")
        st.caption(CAPTION_W_TEXT)
        st.markdown("**ASSIGN_THRESHOLD**")
        st.slider("ASSIGN_THRESHOLD", 0.35, 0.95, step=0.05, key="thr", label_visibility="collapsed")
        st.caption(CAPTION_ASSIGN)
        st.select_slider("Optimization Depth", options=DEPTH_OPTIONS, key="opt_depth")
        optimize_clicked = st.button(
            "Optimize",
            help="Searches for the best weights to maximize noise reduction without breaking incident logic.",
            type="secondary",
            use_container_width=True,
        )

    s = float(st.session_state.r1) + float(st.session_state.r2) + float(st.session_state.r3)
    if s <= 0:
        s = 1.0
    params = clustering.FusionParams(
        float(st.session_state.r1) / s,
        float(st.session_state.r2) / s,
        float(st.session_state.r3) / s,
        float(st.session_state.thr),
    )

    return {
        "uploaded": uploaded,
        "run_clicked": run_clicked,
        "optimize_clicked": optimize_clicked,
        "parser_mode": parser_mode,
        "mapper": mapper,
        "regex_pattern": regex_pattern,
        "params": params,
        "preview_df": preview_df,
        "known_issues_upload": known_issues_upload,
        "csv_mapping_warning": csv_mapping_warning,
    }


def _hero_funnel(metrics: dict[str, Any], show_raw: bool = False) -> None:
    raw = int(metrics["n_raw_logs"])
    incidents = int(metrics["alert_fusion"]["n_incidents"])
    reduction = round(float(metrics["alert_fusion"]["nrr"]) * 100, 1)

    left, center, right = st.columns([1.2, 1.0, 1.2])
    with left:
        st.markdown(
            f"<div class='funnel-card'><p class='funnel-label'>RAW LOGS 👁</p><p class='funnel-value'>{raw:,}</p></div>",
            unsafe_allow_html=True,
        )
    with center:
        st.markdown("<div class='funnel-arrow'></div>", unsafe_allow_html=True)
        st.markdown(f"<div class='eff-badge'>-{reduction:.1f}% Noise Reduction</div>", unsafe_allow_html=True)
    with right:
        st.markdown(
            f"<div class='funnel-card'><p class='funnel-label'>SIGNAL INCIDENTS</p><p class='funnel-value-cyan'>{incidents:,}</p></div>",
            unsafe_allow_html=True,
        )


def _kpi_cards(metrics: dict[str, Any], smart_df: pd.DataFrame) -> None:
    raw = int(metrics["n_raw_logs"])
    incidents = int(metrics["alert_fusion"]["n_incidents"])
    reduction = round(float(metrics["alert_fusion"]["nrr"]) * 100, 1)
    compression = raw / incidents if incidents else 0.0
    if smart_df.empty:
        nrange_sec = 0.0
    else:
        starts = pd.to_datetime(smart_df["start_time"])
        ends = pd.to_datetime(smart_df["end_time"])
        nrange_sec = float((ends.max() - starts.min()).total_seconds())
    hours = max(nrange_sec / 3600.0, 1e-9)
    signal_density = incidents / hours

    cards = [
        ("Compression Ratio", f"{compression:.1f}x Compression", "kpi-value"),
        ("Reduction Efficiency", f"{reduction:.1f}%", "kpi-value-green"),
        ("Signal Density", f"{signal_density:.1f} incidents/hour", "kpi-value"),
    ]
    align = metrics.get("alignment_metrics")
    if isinstance(align, dict):
        af = align.get("alert_fusion", {})
        fault_coverage = float(af.get("recall_temporal", 0.0)) * 100.0
        detection_accuracy = float(af.get("precision_temporal", 0.0)) * 100.0
        cards.extend(
            [
                ("Fault Coverage", f"{fault_coverage:.1f}%", "kpi-value-green"),
                ("Detection Accuracy", f"{detection_accuracy:.1f}%", "kpi-value"),
            ]
        )

    cols = st.columns(len(cards))
    for col, (label, value, klass) in zip(cols, cards):
        col.markdown(
            f"<div class='kpi-card'><p class='kpi-label'>{label}</p><p class='{klass}'>{value}</p></div>",
            unsafe_allow_html=True,
        )


def _truncate_component_id(comp: str) -> str:
    if len(comp) <= 16:
        return comp
    return f"{comp[:4]}...{comp[-8:]}"


def _top_components_from_summary(smart_df: pd.DataFrame) -> None:
    with st.expander("🔍 Top Affected Components", expanded=False):
        if smart_df.empty:
            st.write("No component data available.")
            return
        counts: dict[str, int] = {}
        for row in smart_df.itertuples(index=False):
            weight = int(row.event_count)
            for part in str(row.involved_components).split(";"):
                comp = part.strip()
                if comp:
                    counts[comp] = counts.get(comp, 0) + weight
        if not counts:
            st.write("No component data available.")
            return
        for name, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:3]:
            short = _truncate_component_id(name)
            st.markdown(
                f"- <span title='{name}'><strong>{short}</strong></span>: {int(cnt):,} alerts",
                unsafe_allow_html=True,
            )


def _executive_summary(metrics: dict[str, Any], smart_df: pd.DataFrame) -> None:
    reduction = round(float(metrics["alert_fusion"]["nrr"]) * 100, 1)
    top_component = "Unknown"
    if not smart_df.empty:
        counts: dict[str, int] = {}
        for row in smart_df.itertuples(index=False):
            for part in str(row.involved_components).split(";"):
                comp = part.strip()
                if comp:
                    counts[comp] = counts.get(comp, 0) + int(row.event_count)
        if counts:
            top_component = max(counts.items(), key=lambda x: (x[1], x[0]))[0]
    top_short = _truncate_component_id(top_component)
    text = (
        f"Analysis complete. <span title='{top_component}'>{top_short}</span> was identified as the primary noise generator. "
        f"AlertFusion effectively suppressed background chatter, reducing analyst workload by an estimated {reduction:.1f}%."
    )
    align = metrics.get("alignment_metrics")
    if isinstance(align, dict):
        af = align.get("alert_fusion", {})
        n_known = int(af.get("n_faults", 0))
        coverage = float(af.get("recall_temporal", 0.0)) * 100.0
        text += f" The analysis successfully cross-referenced with {n_known:,} known issues, achieving a {coverage:.1f}% coverage rate."
    st.markdown(
        f"<div class='summary-card'><p class='summary-title'>Executive Summary</p><p class='summary-text'>{text}</p></div>",
        unsafe_allow_html=True,
    )


def _timeline_chart(smart_df: pd.DataFrame, known_issues_df: pd.DataFrame | None = None) -> None:
    """Plotly timeline from incidents_summary only (no raw event arrays in memory)."""
    if smart_df.empty:
        st.info("No incidents available for timeline visualization.")
        return

    s = smart_df.copy()
    s["start_time"] = pd.to_datetime(s["start_time"])
    volume = pd.DataFrame(
        {
            "x_time": s["start_time"],
            "track": "Event Volume",
            "size": s["event_count"].clip(lower=1, upper=70).astype(float),
            "hover_count": 1,
            "total_logs": s["event_count"].astype(int),
        }
    )
    grouped = (
        s.groupby("start_time", as_index=False)
        .agg(total_logs=("event_count", "sum"), clusters=("incident_id", "count"))
        .rename(columns={"start_time": "x_time"})
    )
    grouped["track"] = "Signal Clusters"
    grouped["size"] = grouped["total_logs"].clip(lower=1, upper=70)
    grouped["hover_count"] = grouped["clusters"]

    combined = pd.concat([volume, grouped[["x_time", "track", "size", "hover_count", "total_logs"]]], ignore_index=True)
    combined["total_logs"] = combined["total_logs"].fillna(1).astype(int)
    combined["hover_count"] = combined["hover_count"].fillna(1).astype(int)

    fig = px.scatter(
        combined,
        x="x_time",
        y="track",
        size="size",
        color="track",
        color_discrete_map={
            "Event Volume": "rgba(148,163,184,0.28)",
            "Signal Clusters": "#00f2ff",
        },
        hover_data={
            "x_time": "|%Y-%m-%d %H:%M:%S",
            "hover_count": True,
            "total_logs": True,
            "track": False,
        },
        size_max=34,
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0E1117",
        plot_bgcolor="#0E1117",
        height=390,
        margin=dict(l=8, r=8, t=10, b=8),
        legend_title_text="",
        xaxis_title="",
        yaxis_title="",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(148,163,184,0.18)")
    fig.update_yaxes(showgrid=False)
    fig.for_each_trace(
        lambda t: t.update(
            marker=dict(
                opacity=0.22 if t.name == "Event Volume" else 0.92,
                line=dict(width=0),
            )
        )
    )
    if known_issues_df is not None and not known_issues_df.empty:
        for row in known_issues_df.itertuples(index=False):
            fig.add_vrect(
                x0=pd.to_datetime(row.fault_start),
                x1=pd.to_datetime(row.fault_end),
                fillcolor="#f59e0b",
                opacity=0.09,
                line_width=0,
                layer="below",
            )
    st.plotly_chart(fig, use_container_width=True)


def _render_incident_events(run_dir: str, incident_id: str) -> None:
    """Lazy-load and render raw events for a single incident from disk."""
    with st.spinner(f"Loading events for incident {incident_id}…"):
        events = _load_incident_events(run_dir, incident_id)
    if not events:
        st.warning("No events found for this incident in the on-disk mapping artifact.")
        return
    view = pd.DataFrame(events)
    view["raw_message"] = view["raw_message"].astype(str).str.slice(0, 200)
    table_df, sampled = _prepare_table_df(view, UI_INCIDENT_EVENTS_MAX_ROWS)
    if sampled:
        st.caption(
            f"Showing sampled event rows ({len(table_df):,} of {len(view):,}) for this incident.",
        )
    st.dataframe(table_df, use_container_width=True, height=380, hide_index=True)
    del view
    del table_df
    gc.collect()


def _smart_table(smart_df: pd.DataFrame, run_dir: str) -> None:
    c1, c2 = st.columns(2)
    with c1:
        ids = sorted(smart_df["incident_id"].unique().tolist())
        pick = st.selectbox("Incident ID", ["(all)"] + [str(x) for x in ids], key="ex_id")
    with c2:
        query = st.text_input("Component contains", key="ex_comp", placeholder="blk_…")
    out = smart_df.copy()
    if pick != "(all)":
        out = out[out["incident_id"] == int(pick)]
    if query.strip():
        out = out[out["involved_components"].astype(str).str.contains(query.strip(), case=False, na=False)]
    table_df, sampled = _prepare_table_df(out, UI_TABLE_MAX_ROWS)
    if sampled:
        st.caption(f"Showing sampled incidents ({len(table_df):,} of {len(out):,}) to avoid UI overload.")
    st.dataframe(table_df, use_container_width=True, height=320, hide_index=True)
    del out
    gc.collect()

    if pick != "(all)":
        st.markdown("#### Incident event log (lazy-loaded from disk)")
        _render_incident_events(run_dir, pick)


def main() -> None:
    st.set_page_config(page_title="Syncident | Intelligence", page_icon="◆", layout="wide", initial_sidebar_state="expanded")
    _inject_styles()

    # Apply deferred optimizer updates before sidebar widgets are instantiated.
    pending_opt = st.session_state.pop("_pending_optimized_weights", None)
    if pending_opt is not None:
        st.session_state.r1, st.session_state.r2, st.session_state.r3 = pending_opt

    controls = _sidebar_ui()
    progress_slot = st.empty()

    if controls["optimize_clicked"]:
        run_data = st.session_state.get("run_data")
        if run_data:
            norm_path = Path(run_data["run_dir"]) / "normalized_events.csv"
            if not norm_path.is_file():
                st.sidebar.error("Normalized events artifact missing; re-run analysis first.")
            else:
                _show_pulse(progress_slot)
                try:
                    with st.spinner("Optimizing analysis settings for this dataset..."):
                        norm_df = clustering.load_normalized_events(norm_path)
                        recipes = _pick_weight_recipes(str(st.session_state.opt_depth))
                        best, _meta = _run_auto_tune(
                            norm_df,
                            float(st.session_state.thr),
                            recipes,
                        )
                        del norm_df
                        gc.collect()
                        st.session_state["_pending_optimized_weights"] = best
                        st.rerun()
                finally:
                    _hide_pulse(progress_slot)

    if controls["run_clicked"]:
        uploaded = controls["uploaded"]
        if uploaded is None:
            st.sidebar.error("Please upload a file.")
        elif controls["parser_mode"] == "csv" and controls.get("csv_mapping_warning") and (
            not controls["mapper"] or len(set(controls["mapper"].values())) < 3
        ):
            st.sidebar.error("Could not validate CSV schema automatically. Use 'Fix Mapping' to continue.")
        else:
            _show_pulse(progress_slot)
            try:
                with st.spinner("Ingesting logs and running analysis. Large files may take a while..."):
                    run_data = _run_pipeline(
                        uploaded=uploaded,
                        parser_mode=controls["parser_mode"],
                        mapper=controls["mapper"],
                        regex_pattern=controls["regex_pattern"],
                        params=controls["params"],
                        known_issues_upload=controls["known_issues_upload"],
                    )
                _load_ui_context.clear()
                _load_incident_events.clear()
                st.session_state.run_data = run_data
            except Exception as exc:
                st.sidebar.error(str(exc))
            finally:
                _hide_pulse(progress_slot)

    run_data = st.session_state.get("run_data")
    ui_ctx: dict[str, Any] | None = None
    if run_data:
        try:
            ui_ctx = _load_ui_context(run_data["run_dir"])
        except FileNotFoundError as exc:
            st.error(str(exc))

    tab_dash, tab_data = st.tabs(["Dashboard", "Data Explorer"])

    with tab_dash:
        st.markdown("<div class='hero-title'>Syncident | Intelligence</div>", unsafe_allow_html=True)
        if controls["preview_df"] is not None:
            with st.expander("👁 Raw Logs (upload preview only)", expanded=False):
                st.dataframe(controls["preview_df"], use_container_width=True, height=230, hide_index=True)
        if ui_ctx:
            metrics = ui_ctx["metrics"]
            smart_df = ui_ctx["smart_df"]
            known_issues_df = _load_known_issues(run_data["run_dir"])
            st.caption(
                f"Dashboard loaded from `{evaluation.INCIDENTS_SUMMARY_FILENAME}` "
                f"in `{run_data['run_dir']}`. Event payloads load on demand from disk."
            )
            _hero_funnel(metrics)
            _kpi_cards(metrics, smart_df)
            _top_components_from_summary(smart_df)
            _executive_summary(metrics, smart_df)
            st.divider()
            _timeline_chart(smart_df, known_issues_df)
            del known_issues_df
            gc.collect()

    with tab_data:
        if ui_ctx:
            _smart_table(ui_ctx["smart_df"], run_data["run_dir"])


if __name__ == "__main__":
    main()
