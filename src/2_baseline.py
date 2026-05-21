"""
Pipeline stage: **baseline grouping / FR7** (reference method for FR14 evaluation).

Role: a deliberately simple, deterministic partition of the normalized stream—only wall
clock and fixed windows—so “smart” grouping (FR8) can be debated against an understandable
floor. Same-window events always coalesce, even without shared components, which tends to
over-merge unrelated symptoms but yields stable metrics (NRR, Section 7).

Windowing follows Section 5.1: sort by time, slice the timeline into consecutive BASELINE_WINDOW_SEC
buckets from the first observed timestamp, one incident per non-empty bucket.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Section 5.1: interpretable unit (here 5 minutes); trade-off is bias-variance in incident count.
BASELINE_WINDOW_SEC = 300


def load_normalized_events(path: Path) -> pd.DataFrame:
    """Align FR7 with FR8 inputs: same chronological discipline before any grouping logic."""
    df = pd.read_csv(path)
    required = {"timestamp", "component"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {sorted(missing)}")
    df = df.copy()
    df["_ts"] = pd.to_datetime(df["timestamp"])
    df["_ord"] = range(len(df))
    df = df.sort_values(["_ts", "_ord"], kind="mergesort")
    return df


def event_to_baseline_incident_ids(df: pd.DataFrame) -> pd.Series:
    """
    Align each normalized event row with its FR7 incident_id (same numbering as group_baseline_incidents).
    Required columns: _ts. Used for FR11 drill-down against live AlertFusion assignments.
    """
    if df.empty:
        return pd.Series(dtype="int64")
    if "_ts" not in df.columns:
        raise ValueError("DataFrame must include _ts (use 3_clustering.load_normalized_events or match schema).")
    t_min = df["_ts"].min()
    win = assign_window_index(df["_ts"], t_min)
    ranked_windows = sorted(win.unique())
    id_map = {w: i + 1 for i, w in enumerate(ranked_windows)}
    return win.map(id_map).astype("int64")


def assign_window_index(timestamps: pd.Series, t_min: pd.Timestamp) -> pd.Series:
    """
    Map each event to a discrete time bucket anchored at t_min (Section 5.1 steps 3–4). Why anchor
    to data start, not Unix epoch: avoids arbitrary calendar phase and keeps windows aligned
    to the dataset’s own timeline for reproducible incident counts.
    """
    delta_sec = (timestamps - t_min).dt.total_seconds()
    return (delta_sec // BASELINE_WINDOW_SEC).astype("int64")


def group_baseline_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Materialize Section 5.1 steps 5–6: per-window incident rows with span, cardinality, components."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "incident_id",
                "start_time",
                "end_time",
                "event_count",
                "involved_components",
            ]
        )

    t_min = df["_ts"].min()
    df = df.copy()
    df["_window"] = assign_window_index(df["_ts"], t_min)

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
    return grouped[
        [
            "incident_id",
            "start_time",
            "end_time",
            "event_count",
            "involved_components",
        ]
    ]


def run_baseline(
    input_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """End-to-end FR7 run: normalized CSV → baseline_incidents.csv for AlertFusion comparison."""
    df = load_normalized_events(input_csv)
    out = group_baseline_incidents(df)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)
    return out


def main() -> None:
    """CLI: configurable dataset paths; prints incident count for quick sanity vs FR8 outputs."""
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Baseline fixed-window grouping (FR7) on normalized events.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=root / "data" / "normalized_events.csv",
        help="Normalized events CSV (timestamp, component, ...)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "data" / "baseline_incidents.csv",
        help="Output CSV for baseline incidents",
    )
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    result = run_baseline(args.input, args.output)
    print(
        f"Wrote {args.output} ({len(result)} incidents, "
        f"BASELINE_WINDOW_SEC={BASELINE_WINDOW_SEC}).",
    )


if __name__ == "__main__":
    main()
