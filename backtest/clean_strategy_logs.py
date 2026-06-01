"""Create readable strategy-log copies without modifying raw logs.

The raw logs mix English market snapshots and Chinese market snapshots. This
script normalizes both shapes into one stable English form:

``price=...  Boll[lower | mid | upper]  position=...  equity=...``
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path


DEFAULT_IN_DIR = Path("logs")
DEFAULT_OUT_DIR = Path("logs_cleaned")

MARKET_RE = re.compile(
    r"(?:price|\u4ef7\u683c)=(?P<price>\d+(?:\.\d+)?)\s+"
    r"(?:Boll|\u5e03\u6797)\[(?P<lower>\d+(?:\.\d+)?)\s+\|\s+"
    r"(?P<mid>\d+(?:\.\d+)?)\s+\|\s+"
    r"(?P<upper>\d+(?:\.\d+)?)\]\s+"
    r"(?:position|\u6301\u4ed3)=(?P<position>\S+)\s+"
    r"(?:equity|\u6743\u76ca)=(?P<equity>\d+(?:\.\d+)?)"
)


def normalize_line(line: str) -> tuple[str, bool]:
    """Return a normalized log line and whether it changed."""
    match = MARKET_RE.search(line)
    if not match:
        return line, False

    replacement = (
        f"price={match.group('price')}  "
        f"Boll[{match.group('lower')} | {match.group('mid')} | {match.group('upper')}]  "
        f"position={match.group('position')}  equity={match.group('equity')}"
    )
    cleaned = MARKET_RE.sub(replacement, line)
    return cleaned, cleaned != line


def clean_file(src: Path, dst: Path) -> tuple[int, int]:
    """Clean one log file and return ``(line_count, changed_count)``."""
    line_count = 0
    changed_count = 0
    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("r", encoding="utf-8", errors="replace") as reader, dst.open(
        "w", encoding="utf-8", newline=""
    ) as writer:
        for line in reader:
            line_count += 1
            cleaned, changed = normalize_line(line)
            if changed:
                changed_count += 1
            writer.write(cleaned)
    return line_count, changed_count


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Normalize strategy logs into readable copies.")
    parser.add_argument("--log-dir", default=str(DEFAULT_IN_DIR))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--pattern", default="boll_pin_*.log")
    args = parser.parse_args()

    in_dir = Path(args.log_dir)
    out_dir = Path(args.out_dir)
    files = sorted(in_dir.glob(args.pattern))
    if not files:
        raise SystemExit(f"No log files matched {in_dir / args.pattern}")

    total_lines = 0
    total_changed = 0
    for src in files:
        dst = out_dir / src.name
        line_count, changed_count = clean_file(src, dst)
        total_lines += line_count
        total_changed += changed_count
        print(f"{src.name}: lines={line_count} changed={changed_count} -> {dst}")

    print(f"Done. files={len(files)} lines={total_lines} changed={total_changed} out_dir={out_dir}")


if __name__ == "__main__":
    main()
