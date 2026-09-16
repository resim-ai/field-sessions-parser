from __future__ import annotations

import argparse
import json
import sys

from .logs import inspect, summarize


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect an MCAP recording for a session metrics build")
    parser.add_argument(
        "command",
        choices=["summary", "inspect"],
        help="summary: recorded metadata only; inspect: scan and decode every message",
    )
    parser.add_argument("source")
    args = parser.parse_args()
    try:
        result = summarize(args.source) if args.command == "summary" else inspect(args.source)
    except Exception as error:
        print(json.dumps({"complete": False, "error": str(error)}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
