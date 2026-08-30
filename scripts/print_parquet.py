#!/usr/bin/env python3
"""Print every Parquet file found under a folder using DataFusion Python."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

try:
    from datafusion import SessionContext
except ImportError:  # Report a concise installation hint from main().
    SessionContext = None  # type: ignore[assignment,misc]


PARQUET_MAGIC = b"PAR1"


def is_parquet_file(path: Path) -> bool:
    """Identify Parquet files by their magic bytes, not their extension."""
    try:
        with path.open("rb") as file:
            return file.read(len(PARQUET_MAGIC)) == PARQUET_MAGIC
    except OSError:
        return False


def parse_folder(value: str) -> Path:
    """Convert a local path or file:// URI to a Path."""
    parsed = urlparse(value)

    if not parsed.scheme:
        return Path(value).expanduser().resolve()

    if parsed.scheme != "file":
        raise ValueError(f"unsupported URI scheme: {parsed.scheme}")

    if parsed.netloc not in ("", "localhost"):
        raise ValueError(f"unsupported file URI host: {parsed.netloc}")

    return Path(unquote(parsed.path)).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recursively find and print Parquet files in a folder using the "
            "DataFusion Python API. Extensionless files are supported."
        )
    )
    parser.add_argument(
        "folder",
        help="Folder containing Parquet files, as a path or file:// URI",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        folder = parse_folder(args.folder)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if not folder.is_dir():
        print(f"error: not a directory: {folder}", file=sys.stderr)
        return 2

    if SessionContext is None:
        print(
            "error: DataFusion Python is not installed; run: pip install datafusion",
            file=sys.stderr,
        )
        return 127

    parquet_files = sorted(path.resolve() for path in folder.rglob("*") if path.is_file() and is_parquet_file(path))

    if not parquet_files:
        print(f"error: no Parquet files found under: {folder}", file=sys.stderr)
        return 1

    context = SessionContext()
    dataframe = context.read_parquet(
        str(parquet_files[0]),
        file_extension="",
    )

    for parquet_file in parquet_files[1:]:
        dataframe = dataframe.union(context.read_parquet(str(parquet_file), file_extension=""))
    count = dataframe.count()
    dataframe.show(count)
    print(f"Nb of rows: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
