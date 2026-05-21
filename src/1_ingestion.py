"""
Pipeline stage: **dynamic ingestion + normalization** (FR1, FR2, FR4, FR5).

This module makes Syncident input-agnostic:
- CSV input with configurable column mapping.
- Plain-text input with configurable regex extraction.

Normalization target (FR4):
    timestamp, component, raw_message
Robustness rule (FR5):
    malformed or incomplete rows are skipped, recorded, and reported without crashing.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError

BLOCK_ID_RE = re.compile(r"blk_-?[0-9]+")
HDFS_REGEX_TEMPLATE = (
    r"^(?P<timestamp>\d{6}\s+\d{6})\s+\S+\s+\S+\s+\S+:\s+"
    r"(?P<raw_message>.*?(?P<component>blk_-?\d+).*)$"
)
COMMON_TS_FORMATS = [
    "%y%m%d %H%M%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
]
TS_KEYWORDS = ("time", "date", "ts", "timestamp", "at")
COMP_KEYWORDS = ("comp", "serv", "source", "host", "node", "module")
MSG_KEYWORDS = ("msg", "message", "raw", "text", "content", "info")
SCHEMA_DISCOVERY_SAMPLE_ROWS = 500
MAX_PARSE_LINE_CHARS = 200_000
MAX_RAW_MESSAGE_CHARS = 12_000
CSV_NORMALIZE_CHUNK_SIZE = 100_000
MAX_SKIPPED_RECORDS = 20_000


@dataclass
class NormalizationResult:
    normalized_df: pd.DataFrame
    skipped_records: list[dict[str, Any]]
    skipped_overflow_count: int = 0

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_records) + int(self.skipped_overflow_count)


class LogNormalizer:
    def _append_skipped(
        self,
        skipped: list[dict[str, Any]],
        record: dict[str, Any],
        overflow_count: int,
    ) -> int:
        if len(skipped) < MAX_SKIPPED_RECORDS:
            skipped.append(record)
            return overflow_count
        return overflow_count + 1

    def _trim_message(self, text: str) -> str:
        if len(text) <= MAX_RAW_MESSAGE_CHARS:
            return text
        return text[:MAX_RAW_MESSAGE_CHARS]

    """
    Generic normalizer used by the UI and CLI.

    Design intent:
    - FR1/FR2: parsing behavior comes from user-provided mapping/regex (no hardcoded dataset lock-in).
    - FR4: always emit the same schema for downstream baseline/clustering.
    - FR5: bad rows are tracked in skipped_records, not treated as fatal.
    """

    def _parse_timestamp(self, raw_ts: Any) -> datetime | None:
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

    def _finalize_rows(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=["timestamp", "component", "raw_message"])
        if df.empty:
            return df
        # Unified timestamp serialization for reproducible downstream loading.
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
        df["component"] = df["component"].astype(str)
        df["raw_message"] = df["raw_message"].astype(str)
        return df

    def _pick_first(self, rec: dict[str, Any], keys: list[str]) -> Any:
        for key in keys:
            if key in rec and rec[key] is not None:
                val = rec[key]
                if isinstance(val, str):
                    if val.strip():
                        return val
                else:
                    return val
        return None

    def _pick_by_keywords(self, columns: list[str], keywords: tuple[str, ...], used: set[str]) -> str | None:
        best_col: str | None = None
        best_score = 0
        for col in columns:
            if col in used:
                continue
            name = str(col).strip().lower()
            score = sum(1 for kw in keywords if kw in name)
            if score > best_score:
                best_col = col
                best_score = score
        return best_col

    def discover_schema(self, sample_df: pd.DataFrame) -> dict[str, str]:
        columns = [str(c) for c in sample_df.columns]
        if len(columns) < 3:
            return {}

        used: set[str] = set()
        mapping: dict[str, str] = {}
        role_specs = [
            ("timestamp", TS_KEYWORDS, 0),
            ("component", COMP_KEYWORDS, 1),
            ("raw_message", MSG_KEYWORDS, 2),
        ]

        # Primary pass: keyword-based discovery.
        for role, keywords, _fallback_idx in role_specs:
            choice = self._pick_by_keywords(columns, keywords, used)
            if choice is not None:
                mapping[role] = choice
                used.add(choice)

        # Fallback pass: positional columns 0/1/2, then next available columns.
        for role, _keywords, fallback_idx in role_specs:
            if role in mapping:
                continue
            if fallback_idx < len(columns) and columns[fallback_idx] not in used:
                mapping[role] = columns[fallback_idx]
                used.add(columns[fallback_idx])
                continue
            for col in columns:
                if col not in used:
                    mapping[role] = col
                    used.add(col)
                    break

        return mapping if len(set(mapping.values())) == 3 else {}

    def timestamp_column_looks_valid(self, sample_df: pd.DataFrame, timestamp_col: str) -> bool:
        if sample_df.empty or timestamp_col not in sample_df.columns:
            return False
        parsed = pd.to_datetime(sample_df[timestamp_col], errors="coerce")
        return bool(parsed.notna().any())

    def normalize_csv(
        self,
        df: pd.DataFrame,
        timestamp_col: str,
        component_col: str,
        raw_message_col: str,
        row_index_offset: int = 0,
    ) -> NormalizationResult:
        if df.empty:
            empty = pd.DataFrame(columns=["timestamp", "component", "raw_message"])
            return NormalizationResult(normalized_df=empty, skipped_records=[], skipped_overflow_count=0)

        if timestamp_col in df.columns:
            ts_src = df[timestamp_col]
        else:
            ts_src = pd.Series([pd.NA] * len(df), index=df.index, dtype="string")
        if component_col in df.columns:
            comp_src = df[component_col]
        else:
            comp_src = pd.Series([pd.NA] * len(df), index=df.index, dtype="string")
        if raw_message_col in df.columns:
            msg_src = df[raw_message_col]
        else:
            msg_src = pd.Series([pd.NA] * len(df), index=df.index, dtype="string")

        ts_text = ts_src.astype("string").fillna("").str.strip()
        parsed_ts = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

        for fmt in COMMON_TS_FORMATS:
            missing_mask = parsed_ts.isna() & ts_text.ne("")
            if missing_mask.any():
                parsed_ts.loc[missing_mask] = pd.to_datetime(ts_text[missing_mask], format=fmt, errors="coerce")

        missing_mask = parsed_ts.isna() & ts_text.ne("")
        if missing_mask.any():
            parsed_ts.loc[missing_mask] = pd.to_datetime(ts_text[missing_mask], errors="coerce")

        component = comp_src.astype("string").fillna("").str.strip()
        raw_message = msg_src.astype("string").fillna("").str.strip().str.slice(0, MAX_RAW_MESSAGE_CHARS)

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
            ts_text.str.slice(0, 60)
            + " | "
            + component.str.slice(0, 60)
            + " | "
            + raw_message.str.slice(0, 120)
        ).str.slice(0, 250)

        invalid_idx = df.index[~valid_mask].tolist()
        skipped: list[dict[str, Any]] = []
        skipped_overflow = 0
        for idx in invalid_idx:
            if isinstance(idx, (int, np.integer)):
                global_idx: int | str = int(idx) + row_index_offset
            else:
                global_idx = str(idx)
            skipped_overflow = self._append_skipped(
                skipped,
                {
                    "index": global_idx,
                    "reason": reason_series.loc[idx],
                    "record_preview": preview_series.loc[idx],
                },
                skipped_overflow,
            )

        return NormalizationResult(
            normalized_df=normalized_df,
            skipped_records=skipped,
            skipped_overflow_count=skipped_overflow,
        )

    def normalize_csv_stream(
        self,
        csv_source: Any,
        timestamp_col: str,
        component_col: str,
        raw_message_col: str,
        chunksize: int = CSV_NORMALIZE_CHUNK_SIZE,
    ) -> NormalizationResult:
        normalized_chunks: list[pd.DataFrame] = []
        skipped: list[dict[str, Any]] = []
        skipped_overflow = 0
        row_offset = 0

        try:
            chunk_iter = pd.read_csv(csv_source, chunksize=chunksize)
            for chunk in chunk_iter:
                chunk_result = self.normalize_csv(
                    chunk,
                    timestamp_col=timestamp_col,
                    component_col=component_col,
                    raw_message_col=raw_message_col,
                    row_index_offset=row_offset,
                )
                if not chunk_result.normalized_df.empty:
                    normalized_chunks.append(chunk_result.normalized_df)
                for rec in chunk_result.skipped_records:
                    skipped_overflow = self._append_skipped(skipped, rec, skipped_overflow)
                skipped_overflow += int(chunk_result.skipped_overflow_count)
                row_offset += len(chunk)
        except EmptyDataError:
            pass

        if normalized_chunks:
            normalized_df = pd.concat(normalized_chunks, ignore_index=True)
        else:
            normalized_df = pd.DataFrame(columns=["timestamp", "component", "raw_message"])
        return NormalizationResult(
            normalized_df=normalized_df,
            skipped_records=skipped,
            skipped_overflow_count=skipped_overflow,
        )

    def normalize_text(
        self,
        lines: Iterable[str],
        regex_pattern: str,
    ) -> NormalizationResult:
        rows: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        skipped_overflow = 0
        pattern = re.compile(regex_pattern)

        for line_no, raw in enumerate(lines, start=1):
            line = raw.rstrip("\n\r")
            if not line.strip():
                continue
            if len(line) > MAX_PARSE_LINE_CHARS:
                skipped_overflow = self._append_skipped(
                    skipped,
                    {"line_no": line_no, "reason": "line_too_long", "raw": line[:250]},
                    skipped_overflow,
                )
                continue

            m = pattern.search(line)
            if not m:
                skipped_overflow = self._append_skipped(
                    skipped,
                    {"line_no": line_no, "reason": "regex_no_match", "raw": line[:250]},
                    skipped_overflow,
                )
                continue

            ts = self._parse_timestamp(m.groupdict().get("timestamp"))
            comp = (m.groupdict().get("component") or "").strip()
            msg = (m.groupdict().get("raw_message") or "").strip()

            if not comp:
                blk = BLOCK_ID_RE.search(line)
                if blk:
                    comp = blk.group(0)
            if not msg:
                msg = line
            msg = self._trim_message(msg)

            reason_parts: list[str] = []
            if ts is None:
                reason_parts.append("invalid_timestamp")
            if not comp:
                reason_parts.append("missing_component")
            if not msg:
                reason_parts.append("missing_raw_message")

            if reason_parts:
                skipped_overflow = self._append_skipped(
                    skipped,
                    {
                        "line_no": line_no,
                        "reason": ",".join(reason_parts),
                        "raw": line[:250],
                    },
                    skipped_overflow,
                )
                continue

            rows.append({"timestamp": ts, "component": comp, "raw_message": msg})

        return NormalizationResult(
            normalized_df=self._finalize_rows(rows),
            skipped_records=skipped,
            skipped_overflow_count=skipped_overflow,
        )

    def normalize_json(self, file_path_or_content: Any) -> NormalizationResult:
        """
        Normalize JSON records into the FR4 schema with FR5-safe skipping behavior.

        Accepted inputs:
        - Path to a JSON file
        - Raw JSON string/bytes
        - Parsed python object (list/dict)
        """
        rows: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        skipped_overflow = 0

        payload: Any
        try:
            if isinstance(file_path_or_content, Path):
                payload = json.loads(file_path_or_content.read_text(encoding="utf-8", errors="replace"))
            elif isinstance(file_path_or_content, (bytes, bytearray)):
                payload = json.loads(bytes(file_path_or_content).decode("utf-8", errors="replace"))
            elif isinstance(file_path_or_content, str):
                candidate = Path(file_path_or_content)
                if candidate.is_file():
                    payload = json.loads(candidate.read_text(encoding="utf-8", errors="replace"))
                else:
                    payload = json.loads(file_path_or_content)
            else:
                payload = file_path_or_content
        except Exception as exc:  # FR5: invalid parse should not crash the pipeline
            skipped_overflow = self._append_skipped(
                skipped,
                {"index": -1, "reason": f"invalid_json:{type(exc).__name__}", "record_preview": ""},
                skipped_overflow,
            )
            return NormalizationResult(
                normalized_df=self._finalize_rows(rows),
                skipped_records=skipped,
                skipped_overflow_count=skipped_overflow,
            )

        records: list[Any]
        if isinstance(payload, list):
            records = payload
        elif isinstance(payload, dict):
            nested = None
            for key in ("events", "records", "logs", "data", "items"):
                val = payload.get(key)
                if isinstance(val, list):
                    nested = val
                    break
            records = nested if nested is not None else [payload]
        else:
            skipped_overflow = self._append_skipped(
                skipped,
                {
                    "index": -1,
                    "reason": "unsupported_json_root",
                    "record_preview": str(type(payload).__name__),
                },
                skipped_overflow,
            )
            return NormalizationResult(
                normalized_df=self._finalize_rows(rows),
                skipped_records=skipped,
                skipped_overflow_count=skipped_overflow,
            )

        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                skipped_overflow = self._append_skipped(
                    skipped,
                    {
                        "index": idx,
                        "reason": "non_object_record",
                        "record_preview": str(rec)[:250],
                    },
                    skipped_overflow,
                )
                continue

            ts_raw = self._pick_first(rec, ["timestamp", "time", "ts", "datetime", "date"])
            comp_raw = self._pick_first(rec, ["component", "source", "service", "host", "block_id"])
            msg_raw = self._pick_first(rec, ["raw_message", "message", "log", "msg", "text"])

            ts = self._parse_timestamp(ts_raw)
            comp = "" if comp_raw is None else str(comp_raw).strip()
            if msg_raw is None:
                msg = json.dumps(rec, ensure_ascii=False)
            else:
                msg = str(msg_raw).strip()
                if not msg:
                    msg = json.dumps(rec, ensure_ascii=False)
            msg = self._trim_message(msg)

            if not comp:
                blk = BLOCK_ID_RE.search(msg)
                if blk:
                    comp = blk.group(0)

            reason_parts: list[str] = []
            if ts is None:
                reason_parts.append("invalid_timestamp")
            if not comp:
                reason_parts.append("missing_component")
            if not msg:
                reason_parts.append("missing_raw_message")

            if reason_parts:
                skipped_overflow = self._append_skipped(
                    skipped,
                    {
                        "index": idx,
                        "reason": ",".join(reason_parts),
                        "record_preview": json.dumps(rec, ensure_ascii=False)[:250],
                    },
                    skipped_overflow,
                )
                continue

            rows.append({"timestamp": ts, "component": comp, "raw_message": msg})

        return NormalizationResult(
            normalized_df=self._finalize_rows(rows),
            skipped_records=skipped,
            skipped_overflow_count=skipped_overflow,
        )


def ingest_hdfs_log(input_path: Path, output_path: Path) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Backward-compatible CLI helper: parse HDFS text using the built-in template.
    """
    normalizer = LogNormalizer()
    with input_path.open("r", encoding="utf-8", errors="replace") as f:
        result = normalizer.normalize_text(f, HDFS_REGEX_TEMPLATE)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.normalized_df.to_csv(output_path, index=False)
    stats = {"written": len(result.normalized_df), "skipped_records": result.skipped_count}
    return result.normalized_df, stats


def main() -> None:
    """CLI entry for FR2-style reproducible runs from files."""
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Normalize log data into FR4 event schema.")
    parser.add_argument("--input", type=Path, default=root / "data" / "HDFS_2k.log")
    parser.add_argument("--output", type=Path, default=root / "data" / "normalized_events.csv")
    parser.add_argument(
        "--mode",
        choices=["text", "csv", "json", "auto"],
        default="auto",
        help="Input parsing mode",
    )
    parser.add_argument("--timestamp-col", default="timestamp")
    parser.add_argument("--component-col", default="component")
    parser.add_argument("--message-col", default="raw_message")
    parser.add_argument("--regex", default=HDFS_REGEX_TEMPLATE)
    args = parser.parse_args()

    if not args.input.is_file():
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    normalizer = LogNormalizer()
    mode = args.mode
    if mode == "auto":
        suffix = args.input.suffix.lower()
        if suffix == ".csv":
            mode = "csv"
        elif suffix == ".json":
            mode = "json"
        else:
            mode = "text"

    if mode == "csv":
        try:
            sample_df = pd.read_csv(args.input, nrows=SCHEMA_DISCOVERY_SAMPLE_ROWS)
        except EmptyDataError:
            sample_df = pd.DataFrame(columns=[args.timestamp_col, args.component_col, args.message_col])
        discovered_mapping = normalizer.discover_schema(sample_df)
        timestamp_col = args.timestamp_col
        component_col = args.component_col
        message_col = args.message_col
        explicit_cols_exist = all(col in sample_df.columns for col in [timestamp_col, component_col, message_col])
        if not explicit_cols_exist and discovered_mapping:
            timestamp_col = discovered_mapping["timestamp"]
            component_col = discovered_mapping["component"]
            message_col = discovered_mapping["raw_message"]
            print(
                "Auto-discovered CSV schema: "
                f"timestamp={timestamp_col}, component={component_col}, raw_message={message_col}",
                file=sys.stderr,
            )
        result = normalizer.normalize_csv_stream(
            args.input,
            timestamp_col=timestamp_col,
            component_col=component_col,
            raw_message_col=message_col,
        )
    elif mode == "json":
        result = normalizer.normalize_json(args.input)
    else:
        with args.input.open("r", encoding="utf-8", errors="replace") as f:
            result = normalizer.normalize_text(f, args.regex)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.normalized_df.to_csv(args.output, index=False)
    print(f"Wrote {args.output} ({len(result.normalized_df)} rows).")
    if result.skipped_count:
        print(f"Skipped records: {result.skipped_count}", file=sys.stderr)
        if result.skipped_overflow_count:
            print(
                f"Skipped preview truncated to first {MAX_SKIPPED_RECORDS} records.",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()
