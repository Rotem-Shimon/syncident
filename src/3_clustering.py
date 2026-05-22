"""
AlertFusion primary grouping (FR8, Syncident Section 5.2): incremental correlation on normalized events.

Pipeline role: consumes the unified event CSV and produces *primary* incidents for
noise-reduction evaluation (FR13/FR14) against the fixed-window baseline. This stage
turns the “event stream” into explainable incident candidates using only time, shared
component context, and lightweight text similarity—no learning loop.

Assignment rule (Section 5.2.3): each event joins the *best* currently *active* incident if the
similarity score meets ASSIGN_THRESHOLD; otherwise a new incident opens. Active means the
incident received an event within ACTIVE_WINDOW_SEC (Section 5.2.1), capping candidate set size.

Similarity is a weighted sum of three signals in [0, 1] (Section 5.2.2), all configurable in spirit:
    score = W_TIME * s_time + W_COMPONENT * s_component + W_TEXT * s_text
Equal 1/3 weights make the theoretical maximum *without* a component match 2/3, so a
threshold of 0.7 would forbid almost all cross-block merges; slightly skewing mass toward
time and text keeps FR8’s “multi-signal correlation” operational while still privileging
shared block ids via W_COMPONENT.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from collections import Counter, deque
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import pandas as pd

# --- Parameters (deterministic; mirror Section 6 “Configuration Design”) ---
# Section 5.2.1: only incidents touched within this many seconds remain candidates—stops every
# historical incident from competing and models “recent failure context.”
ACTIVE_WINDOW_SEC = 600

# Section 5.2.3: interpretable merge gate; separates “strong match” from opening a new incident.
ASSIGN_THRESHOLD = 0.7

# Anti-Flapping post-processing window (Section 5.2.3 Step 4): two closed incident
# containers are eligible to merge when the gap between I_A.end_time and I_B.start_time
# does not exceed this value AND both containers share identical dominant-component and
# dominant-template identity. 1800 s (30 min) is wide enough to capture re-open flapping
# while remaining far below the inter-failure silence on healthy systems.
FLAPPING_WINDOW_SEC = 1800

# Section 5.2.2 “time signal”: exponential decay against incident end:
# s_time = exp(-lambda * delta_seconds). Lambda is computed adaptively per event from the
# sliding median of inter-arrival deltas in the active window (ln(2)/median_Δt), giving a
# half-life equal to the observed local event cadence. This constant serves as the safe
# fallback when the window has fewer than two events or when the median delta is exactly 0
# (e.g. simultaneous alert floods where no velocity signal is available).
TIME_DECAY_LAMBDA = 0.003

# Precomputed ln(2) for the adaptive half-life formula: λ = ln(2) / median(Δt).
_LN2: float = math.log(2)

# Weights sum to 1.0. See module docstring for why 0.35/0.30/0.35 beats equal thirds under 0.7.
W_TIME = 0.35
W_COMPONENT = 0.30
W_TEXT = 0.35

# Caps template-size work for deterministic bounded runtime (NFR3).
MAX_TEMPLATE_CHARS = 256
TEMPLATE_NGRAM_SIZE = 3

# Precompiled token masks keep normalization fast on large event streams.
UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}\b",
)
HEX_ADDRESS_PATTERN = re.compile(r"\b0x[0-9a-fA-F]+\b")
HEX_ID_PATTERN = re.compile(r"\b[0-9a-fA-F]{12,}\b")
ISO_TIMESTAMP_PATTERN = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T\s]"
    r"\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?"
    r"(?:Z|[+-]\d{2}:?\d{2})?\b",
)
SYSLOG_TIMESTAMP_PATTERN = re.compile(
    r"\b(?:\d{4}/\d{2}/\d{2}|\d{2}/\d{2}/\d{4})"
    r"[T\s]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\b",
)


@dataclass(frozen=True)
class FusionParams:
    """Tunable FR8 correlation knobs (Section 6). Shared by CLI defaults and the FR11 Streamlit lab."""

    w_time: float
    w_component: float
    w_text: float
    assign_threshold: float


def default_fusion_params() -> FusionParams:
    """Factory mirroring module defaults so batch CLI and Streamlit share one definition."""
    return FusionParams(
        w_time=W_TIME,
        w_component=W_COMPONENT,
        w_text=W_TEXT,
        assign_threshold=ASSIGN_THRESHOLD,
    )


def load_normalized_events(path: Path) -> pd.DataFrame:
    """Prepare FR8 input: enforce schema, chronological order, stable ties (Section 5.2 step 1)."""
    df = pd.read_csv(path)
    required = {"timestamp", "component", "raw_message"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {sorted(missing)}")
    df = df.copy()
    df["_ts"] = pd.to_datetime(df["timestamp"])
    df["_ord"] = range(len(df))
    df = df.sort_values(["_ts", "_ord"], kind="mergesort")
    df["raw_message"] = df["raw_message"].fillna("").astype(str)
    return df


def normalize_message_template(msg: str) -> str:
    """Replace volatile tokens so similar log lines get high template similarity."""
    s = msg
    s = UUID_PATTERN.sub("<UUID>", s)
    s = HEX_ADDRESS_PATTERN.sub("<HEX>", s)
    s = HEX_ID_PATTERN.sub("<HEXID>", s)
    s = ISO_TIMESTAMP_PATTERN.sub("<TS>", s)
    s = SYSLOG_TIMESTAMP_PATTERN.sub("<TS>", s)
    s = re.sub(r"blk_-?[0-9]+", "<BLK>", s)
    s = re.sub(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<IP>", s)
    s = re.sub(r"/[\w./-]+", "<PATH>", s)
    s = re.sub(r"\b\d+\b", "<N>", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


@lru_cache(maxsize=100_000)
def _cached_normalized_template(msg: str) -> str:
    return normalize_message_template(msg)[:MAX_TEMPLATE_CHARS]


@lru_cache(maxsize=100_000)
def _template_ngrams(template: str) -> frozenset[str]:
    if not template:
        return frozenset()
    if len(template) < TEMPLATE_NGRAM_SIZE:
        return frozenset({template})
    grams = {
        template[i : i + TEMPLATE_NGRAM_SIZE]
        for i in range(len(template) - TEMPLATE_NGRAM_SIZE + 1)
    }
    return frozenset(grams)


@lru_cache(maxsize=500_000)
def _cached_template_similarity_pair(ta: str, tb: str) -> float:
    """
    Hash-based memoized similarity for template pairs.
    Repeated event-vs-incident comparisons hit this cache instead of rebuilding n-gram overlap.
    """
    ga = _template_ngrams(ta)
    gb = _template_ngrams(tb)
    denom = len(ga) + len(gb)
    if denom == 0:
        return 1.0
    return float((2.0 * len(ga & gb)) / denom)


def template_similarity(ta: str, tb: str) -> float:
    """
    Fast normalized similarity in [0, 1] using Dice overlap over character n-grams.
    - 1.0 for identical templates
    - 0.0 for disjoint templates
    """
    if not ta and not tb:
        return 1.0
    if ta == tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    # Canonical ordering improves cache hit rate because similarity is symmetric.
    if ta > tb:
        ta, tb = tb, ta
    return _cached_template_similarity_pair(ta, tb)


def message_similarity(a: str, b: str) -> float:
    """Section 5.2.2 text signal: fast deterministic similarity on masked templates (s_text)."""
    ta = _cached_normalized_template(a)
    tb = _cached_normalized_template(b)
    return template_similarity(ta, tb)


def time_proximity_score(
    event_ts: pd.Timestamp,
    incident_end: pd.Timestamp,
    lam: float = TIME_DECAY_LAMBDA,
) -> float:
    """
    Section 5.2.2 time signal (s_time): exponential temporal affinity.
    s_time = exp(-lam * delta_seconds), clamped to [0, 1].
    Future/overlapping timestamps receive full score 1.0.
    lam defaults to TIME_DECAY_LAMBDA but is overridden by the adaptive mechanism in
    run_alert_fusion, which derives lam = ln(2) / median(inter-arrival delta).
    """
    delta = (event_ts - incident_end).total_seconds()
    if delta <= 0:
        return 1.0
    return float(math.exp(-lam * delta))


def component_score(event_component: str, component_counts: Counter[str]) -> float:
    """
    Section 5.2.2 component signal: pure Jaccard Similarity Index between the incoming
    event's singleton component set C_e = {event_component} and the set of all unique
    components already recorded in the incident cluster C_I:

        s_component = |C_e ∩ C_I| / |C_e ∪ C_I|

    Returns exactly 0.0 when the incoming component is entirely new to this incident,
    preventing merge-drift from cross-component false positives.
    """
    if not component_counts:
        return 0.0
    c_i: frozenset[str] = frozenset(component_counts.keys())
    c_e: frozenset[str] = frozenset({event_component})
    union = c_e | c_i
    intersection = c_e & c_i
    return float(len(intersection) / len(union))


@dataclass
class Incident:
    """Mutable cluster state for incremental AlertFusion: bounds, mass on components, last line."""
    incident_id: int
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    component_counts: Counter[str] = field(default_factory=Counter)
    message_counts: Counter[str] = field(default_factory=Counter)
    event_count: int = 0
    last_message: str = ""
    last_template: str = ""

    def dominant_component(self) -> str:
        """Most frequent block id in this incident—the comparison target for s_component (Section 5.2.2)."""
        if not self.component_counts:
            return ""
        return self.component_counts.most_common(1)[0][0]

    def add_event(
        self,
        ts: pd.Timestamp,
        component: str,
        raw_message: str,
    ) -> None:
        """Update cluster extent and the text anchor used for the next message similarity call."""
        self.end_time = ts
        normalized = _cached_normalized_template(raw_message)
        self.component_counts[component] += 1
        self.message_counts[normalized] += 1
        self.event_count += 1
        self.last_message = raw_message
        self.last_template = normalized

    def dominant_message(self) -> str:
        """
        Most frequent normalized message template in this incident.
        Empty string when no messages were registered.
        """
        if not self.message_counts:
            return ""
        return self.message_counts.most_common(1)[0][0]


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


def is_active(event_ts: pd.Timestamp, inc: Incident) -> bool:
    """Section 5.2.1 candidate filter: incident still “alive” for correlation if last event is recent enough."""
    gap_sec = (event_ts - inc.end_time).total_seconds()
    return gap_sec <= ACTIVE_WINDOW_SEC


def involved_components_cell(ctr: Counter[str]) -> str:
    """Export shape matches baseline/F7 reports: sorted multiset of blocks for FR10-style summaries."""
    uniq = sorted(ctr.keys())
    return ";".join(str(c) for c in uniq)


def _as_ts(val: object) -> pd.Timestamp:
    if isinstance(val, pd.Timestamp):
        return val
    return pd.Timestamp(val)


def merge_incidents(
    incidents: list[Incident],
    flapping_window_sec: int = FLAPPING_WINDOW_SEC,
) -> tuple[list[Incident], dict[int, int]]:
    """
    Mandatory Anti-Flapping post-processing pass (Section 5.2.3 Step 4).

    Two adjacent closed incident containers I_A and I_B are merged when ALL three
    conditions are simultaneously satisfied:

        1. Temporal proximity:
               I_B.start_time - I_A.end_time <= flapping_window_sec
        2. Topological identity (dominant component must be identical):
               I_A.dominant_component() == I_B.dominant_component()
        3. Structural identity (primary event-type template signature must be identical):
               I_A.dominant_message() == I_B.dominant_message()

    When a flapping condition is detected I_A absorbs I_B: event counts, component
    frequency maps, and message frequency maps are aggregated, temporal boundaries are
    extended to cover the full span, and the last-message anchor is advanced to the
    absorbed segment's tail. Conditions (2) and (3) are strict equality checks—no
    partial matching—to prevent merge-drift across topologically distinct failure modes.

    Returns:
        merged_incidents: deterministically ordered by (start_time, incident_id).
        old_to_final_id: full mapping from every pre-merge incident_id to its final id.
    """
    if not incidents:
        return [], {}

    ordered = sorted(incidents, key=lambda x: (x.start_time, x.incident_id))
    merged: list[Incident] = []

    # old_id -> root_old_id after merge chaining.
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

        flapping = gap_sec <= flapping_window_sec and same_component and same_template

        if flapping:
            # I_A absorbs I_B: extend span, aggregate all statistical counters.
            current.end_time = max(current.end_time, nxt.end_time)
            current.event_count += nxt.event_count
            current.component_counts.update(nxt.component_counts)
            current.message_counts.update(nxt.message_counts)
            # Advance last-message anchor to the absorbed segment's tail.
            current.last_message = nxt.last_message
            current.last_template = nxt.last_template
            old_to_root[nxt.incident_id] = current.incident_id
        else:
            merged.append(current)
            current = nxt

    merged.append(current)

    # Deterministic final re-indexing (1..k by start_time) and full old->final mapping.
    root_to_final: dict[int, int] = {}
    for i, inc in enumerate(merged, start=1):
        root_to_final[inc.incident_id] = i
        inc.incident_id = i

    old_to_final: dict[int, int] = {}
    for old_id, root_id in old_to_root.items():
        old_to_final[old_id] = root_to_final[root_id]

    return merged, old_to_final


def run_alert_fusion(
    df: pd.DataFrame,
    params: FusionParams | None = None,
) -> tuple[list[Incident], list[int]]:
    """
    Core FR8 engine (Section 5.2.3): each event scores only against *active* incidents, assigns to
    the argmax if score ≥ assign_threshold, else spawns a new cluster—deterministic order,
    explicit tie-break on lower incident_id.

    Returns:
        incidents: cluster list for export/summary.
        event_incident_ids: parallel to df row order after sorting—enables FR11 drill-down
        (event-level traceability without re-parsing messages).
    """
    if params is None:
        params = default_fusion_params()

    incidents: list[Incident] = []
    event_incident_ids: list[int] = []
    next_id = 1

    # Sliding deque of event timestamps within ACTIVE_WINDOW_SEC used to derive the
    # localized inter-arrival median for adaptive lambda (Section 5.2.2 time signal).
    window_ts: deque[pd.Timestamp] = deque()

    ts_arr = df["_ts"].to_numpy()
    comp_arr = df["component"].astype(str).to_numpy()
    msg_arr = df["raw_message"].astype(str).to_numpy()

    for i in range(len(df)):
        ts = _as_ts(ts_arr[i])
        comp = comp_arr[i]
        msg = msg_arr[i]
        msg_template = _cached_normalized_template(msg)

        # Maintain sliding window: evict timestamps older than ACTIVE_WINDOW_SEC.
        cutoff = ts - pd.Timedelta(seconds=ACTIVE_WINDOW_SEC)
        while window_ts and window_ts[0] < cutoff:
            window_ts.popleft()
        window_ts.append(ts)

        # Adaptive lambda: λ = ln(2) / median(Δt over active window).
        # Falls back to TIME_DECAY_LAMBDA when the window has fewer than two events
        # or when all events share the same timestamp (zero-delta alert flood).
        adaptive_lam: float = TIME_DECAY_LAMBDA
        if len(window_ts) >= 2:
            wlist = list(window_ts)
            deltas_sec = sorted(
                (wlist[j] - wlist[j - 1]).total_seconds()
                for j in range(1, len(wlist))
            )
            n_d = len(deltas_sec)
            median_delta = (
                deltas_sec[n_d // 2]
                if n_d % 2 == 1
                else (deltas_sec[n_d // 2 - 1] + deltas_sec[n_d // 2]) / 2.0
            )
            if median_delta > 0.0:
                adaptive_lam = _LN2 / median_delta

        active = [inc for inc in incidents if is_active(ts, inc)]
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
            event_incident_ids.append(inc.incident_id)
            next_id += 1

    # Mandatory Anti-Flapping post-processing pass (Section 5.2.3 Step 4):
    # collapses re-open flapping pairs that share identical dominant-component AND
    # dominant-template identity within the FLAPPING_WINDOW_SEC horizon.
    merged_incidents, id_map = merge_incidents(incidents, FLAPPING_WINDOW_SEC)
    final_event_ids = [id_map.get(i, i) for i in event_incident_ids]

    return merged_incidents, final_event_ids


def incidents_to_dataframe(incidents: list[Incident]) -> pd.DataFrame:
    """Serialize primary incidents for FR11 exports; sort by start_time per Section 5.2 step 5."""
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
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Hybrid Continuous Sampling for Auto-Tune (opt-in, deterministic, no random)
# ---------------------------------------------------------------------------

# Grid of ASSIGN_THRESHOLD values swept during auto-tune.
AUTOTUNE_THRESHOLDS: tuple[float, ...] = (0.65, 0.70, 0.75, 0.80)

# Minimum number of incidents a candidate parameter set must produce on the
# dense sample to avoid the "collapse everything into one bucket" failure mode.
AUTOTUNE_MIN_INCIDENTS = 3

# NRR penalty applied per missing incident below the minimum floor.
# Shaped so even one collapsed-to-single outcome is definitively rejected.
_AUTOTUNE_COLLAPSE_PENALTY = 0.5


def extract_dense_sample(
    df: pd.DataFrame,
    target_ratio: float = 0.10,
    max_rows: int = 50_000,
) -> pd.DataFrame:
    """
    Locate the chronological window with the highest log density via frequency
    binning on the datetime index, then extract a contiguous slice that covers
    `target_ratio` of the full dataset (capped at `max_rows`).

    Algorithm
    ---------
    1. Build a per-minute event count series with ``resample("1min")``.
    2. Compute a rolling sum over a window equal to the target row count
       (converted to minutes) to find the contiguous minute-band with the
       most events.
    3. Return the raw rows whose ``_ts`` falls inside that band, capped at
       ``max_rows`` by taking a stride-1 head (preserving temporal order).

    The function is fully deterministic: no shuffling, no random state.

    Parameters
    ----------
    df:
        Normalized event DataFrame as returned by ``load_normalized_events``.
        Must contain a ``_ts`` datetime column.
    target_ratio:
        Fraction of ``len(df)`` to target for the sample size before the
        ``max_rows`` cap is applied.  Default is 10 %.
    max_rows:
        Hard upper bound on rows returned regardless of ``target_ratio``.

    Returns
    -------
    pd.DataFrame
        A contiguous temporal slice of ``df``, reset-indexed.
    """
    if df.empty:
        return df.copy()

    n_total = len(df)
    target_n = max(1, min(int(n_total * target_ratio), max_rows))

    # Fast path: dataset is already small enough.
    if n_total <= target_n:
        return df.reset_index(drop=True)

    ts_col: pd.Series = df["_ts"] if "_ts" in df.columns else pd.to_datetime(df["timestamp"])

    # Build a 1-minute frequency count series anchored to the dataset start.
    # Using a copy avoids mutating the caller's frame.
    freq_series = (
        pd.Series(1, index=ts_col, name="count")
        .resample("1min")
        .sum()
        .fillna(0)
    )

    if freq_series.empty or freq_series.sum() == 0:
        # Degenerate dataset (all same timestamp): just return the head.
        return df.head(target_n).reset_index(drop=True)

    # Estimate how many minutes the target_n rows span on average.
    total_minutes = max(1, len(freq_series))
    mins_per_row = total_minutes / n_total
    window_minutes = max(1, int(math.ceil(target_n * mins_per_row)))

    if window_minutes >= total_minutes:
        return df.head(target_n).reset_index(drop=True)

    # Rolling sum to find the densest window.
    rolling_sums = freq_series.rolling(window=window_minutes, min_periods=1).sum()
    # argmax is deterministic on a sorted index.
    peak_end_ts: pd.Timestamp = rolling_sums.idxmax()
    peak_start_ts: pd.Timestamp = peak_end_ts - pd.Timedelta(minutes=window_minutes - 1)

    mask = (ts_col >= peak_start_ts) & (ts_col <= peak_end_ts + pd.Timedelta(seconds=59))
    slice_df = df.loc[mask]

    # If the window yielded fewer rows than target (sparse data), expand to head.
    if len(slice_df) < 1:
        slice_df = df

    # Cap at max_rows preserving temporal order (head, not stride-sample).
    result = slice_df.head(target_n).reset_index(drop=True)
    return result


def auto_tune_parameters(
    dense_df: pd.DataFrame,
) -> dict[str, object]:
    """
    Deterministic grid search over ``FusionParams`` on a dense sample slice.

    Search space
    ------------
    - ``ASSIGN_THRESHOLD``: values in ``AUTOTUNE_THRESHOLDS`` (4 points).
    - Weight triplets (w_time, w_component, w_text): all combinations on a
      0.1-step grid that sum to 1.0 (66 triplets), yielding
      4 × 66 = 264 candidate configurations total.

    Scoring
    -------
    score = NRR − collapse_penalty

    where ``collapse_penalty`` is non-zero only when ``n_incidents`` falls
    below ``AUTOTUNE_MIN_INCIDENTS``.  This hard-penalises configurations that
    over-merge the sample into fewer than three incident buckets, preventing
    degenerate "compress everything" solutions from winning.

    The search is fully deterministic (no ``random`` module usage) and safe to
    run on a small dense slice to avoid memory exhaustion on large datasets.

    Parameters
    ----------
    dense_df:
        A pre-extracted dense sample as returned by ``extract_dense_sample``.
        Must already have the ``_ts`` column present.

    Returns
    -------
    dict with keys:
        ``params``        – best ``FusionParams`` found.
        ``best_score``    – penalised NRR of the winning configuration.
        ``best_nrr``      – raw NRR (before penalty).
        ``n_incidents``   – incident count on the sample for the best params.
        ``n_sample``      – number of rows in ``dense_df``.
        ``total_tested``  – total parameter combinations evaluated.
        ``feasible``      – combinations that passed the collapse guard.
    """
    n_sample = len(dense_df)
    if n_sample == 0:
        return {
            "params": default_fusion_params(),
            "best_score": 0.0,
            "best_nrr": 0.0,
            "n_incidents": 0,
            "n_sample": 0,
            "total_tested": 0,
            "feasible": 0,
        }

    best_params: FusionParams = default_fusion_params()
    best_score: float = -1.0
    best_nrr: float = 0.0
    best_n_incidents: int = 0
    total_tested = 0
    feasible = 0

    # Weight grid: all (w_time, w_component, w_text) with 0.1 resolution that
    # sum to 1.0, normalised inside FusionParams construction.
    for threshold in AUTOTUNE_THRESHOLDS:
        for i in range(11):
            for j in range(11 - i):
                k = 10 - i - j
                w_time_raw = i / 10.0
                w_comp_raw = j / 10.0
                w_text_raw = k / 10.0
                weight_sum = w_time_raw + w_comp_raw + w_text_raw
                if weight_sum <= 0:
                    continue
                params = FusionParams(
                    w_time=w_time_raw / weight_sum,
                    w_component=w_comp_raw / weight_sum,
                    w_text=w_text_raw / weight_sum,
                    assign_threshold=threshold,
                )
                total_tested += 1

                incidents, _ = run_alert_fusion(dense_df, params)
                n_inc = len(incidents)

                nrr = 1.0 - (n_inc / n_sample) if n_sample > 0 else 0.0

                # Collapse penalty: reject configurations that obliterate all
                # structure in the sample.
                if n_inc < AUTOTUNE_MIN_INCIDENTS:
                    deficit = AUTOTUNE_MIN_INCIDENTS - n_inc
                    score = nrr - _AUTOTUNE_COLLAPSE_PENALTY * deficit
                else:
                    score = nrr
                    feasible += 1

                if score > best_score:
                    best_score = score
                    best_nrr = nrr
                    best_params = params
                    best_n_incidents = n_inc

    return {
        "params": best_params,
        "best_score": round(best_score, 6),
        "best_nrr": round(best_nrr, 6),
        "n_incidents": best_n_incidents,
        "n_sample": n_sample,
        "total_tested": total_tested,
        "feasible": feasible,
    }


def run_clustering(
    input_csv: Path,
    output_csv: Path,
    baseline_csv: Path | None,
) -> tuple[pd.DataFrame, int | None]:
    df = load_normalized_events(input_csv)
    incidents, _event_ids = run_alert_fusion(df)
    out = incidents_to_dataframe(incidents)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)

    baseline_n: int | None = None
    if baseline_csv is not None and baseline_csv.is_file():
        b = pd.read_csv(baseline_csv)
        baseline_n = len(b)
    return out, baseline_n


def main() -> None:
    """CLI entry: configurable paths (FR2 spirit) plus printed incident-count delta vs baseline (FR14)."""
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="AlertFusion incremental clustering (FR8) on normalized events.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=root / "data" / "normalized_events.csv",
        help="Normalized events CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "data" / "smart_incidents.csv",
        help="Output CSV for AlertFusion incidents",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=root / "data" / "baseline_incidents.csv",
        help="Baseline incidents CSV for comparison summary",
    )
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    out, baseline_n = run_clustering(args.input, args.output, args.baseline)
    n_smart = len(out)
    print(f"Wrote {args.output} ({n_smart} incidents).")
    print(
        f"Parameters: ACTIVE_WINDOW_SEC={ACTIVE_WINDOW_SEC}, "
        f"ASSIGN_THRESHOLD={ASSIGN_THRESHOLD}, "
        f"FLAPPING_WINDOW_SEC={FLAPPING_WINDOW_SEC}, "
        f"lambda=adaptive(ln2/median_Δt, fallback={TIME_DECAY_LAMBDA}), "
        f"weights=({W_TIME:.3f},{W_COMPONENT:.3f},{W_TEXT:.3f}).",
    )
    print()
    print("--- Summary vs baseline ---")
    if baseline_n is None:
        print(
            f"AlertFusion incidents: {n_smart}. "
            f"Baseline file not found ({args.baseline}); run 2_baseline.py first to compare.",
        )
    else:
        diff = n_smart - baseline_n
        print(f"Baseline incidents:   {baseline_n}")
        print(f"AlertFusion incidents: {n_smart}")
        if baseline_n:
            pct = (diff / baseline_n) * 100.0
            print(
                f"Difference: {diff:+d} ({pct:+.1f}% vs baseline incident count).",
            )
        else:
            print(f"Difference: {diff:+d}.")


if __name__ == "__main__":
    main()
