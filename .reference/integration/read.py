#!/usr/bin/env python3
"""Read one #964 reference packet, section or family; never execute product work."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MAX_CHARS = 12_000
DEFAULT_CHARS = 8_000


def safe_path(relative: str, root: Path = ROOT) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError(f"Reference file unavailable or outside bundle: {relative}")
    return path


def packets(root: Path = ROOT) -> list[dict[str, Any]]:
    data = json.loads(safe_path("integration/routing.json", root).read_text(encoding="utf-8"))
    if data.get("format") != "reference-reading-router/v1":
        raise ValueError("Unsupported reading-router format")
    return data["packets"]


def family_entries(root: Path = ROOT) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    meta = json.loads(safe_path("v1_inventory/metadata.json", root).read_text(encoding="utf-8"))
    for part in meta["publication_representation"]["parts"]:
        for item in json.loads(safe_path(f"v1_inventory/{part}", root).read_text(encoding="utf-8")):
            if item["id"] in result:
                raise ValueError(f"Duplicate inventory family {item['id']}")
            result[item["id"]] = item
    return result


def section(text: str, heading: str | None) -> str:
    if heading is None:
        return text
    lines = text.splitlines(keepends=True)
    found = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == heading]
    if len(found) != 1:
        raise ValueError(f"Expected one exact heading: {heading}")
    start = found[0]
    level = len(heading) - len(heading.lstrip("#"))
    end = len(lines)
    for i in range(start + 1, len(lines)):
        match = re.match(r"^(#{1,6}) ", lines[i])
        if match and len(match[1]) <= level:
            end = i
            break
    return "".join(lines[start:end])


def page(text: str, offset: int, chars: int) -> tuple[str, int | None]:
    if offset < 0 or offset > len(text) or not 1 <= chars <= MAX_CHARS:
        raise ValueError("Use a valid character offset and 1..12000 characters")
    end = min(len(text), offset + chars)
    return text[offset:end], end if end < len(text) else None


def check(root: Path = ROOT) -> dict[str, Any]:
    rows = packets(root)
    by_id = {row["id"]: row for row in rows}
    if len(rows) != len(by_id):
        raise ValueError("Duplicate packet ID")
    seen_families: list[str] = []
    max_words = 0
    edges = 0
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(pid: str) -> None:
        nonlocal edges
        if pid in visiting:
            raise ValueError("Packet dependency cycle")
        if pid in visited:
            return
        if pid not in by_id:
            raise ValueError(f"Unknown predecessor {pid}")
        visiting.add(pid)
        for previous in by_id[pid]["needs"]:
            edges += 1
            visit(previous)
        visiting.remove(pid)
        visited.add(pid)

    for row in rows:
        visit(row["id"])
        card = safe_path(row["card"], root).read_text(encoding="utf-8")
        max_words = max(max_words, len(card.split()))
        if len(card.split()) > 350:
            raise ValueError(f"Packet card exceeds reading budget: {row['id']}")
        for required in ("## Outcome", "## First focused slice", "## Exit evidence", "## Stop and resume"):
            if required not in card:
                raise ValueError(f"Missing packet section {required}: {row['id']}")
        for source in row["reads"]:
            section(safe_path(source["path"], root).read_text(encoding="utf-8"), source["heading"])
        seen_families.extend(row["families"])
    expected = set(family_entries(root))
    if len(seen_families) != len(set(seen_families)) or set(seen_families) != expected:
        raise ValueError("Every existing family must route exactly once")
    return {"packets": len(rows), "families_routed": len(expected),
            "dependency_edges": edges, "max_card_words": max_words,
            "max_excerpt_characters": MAX_CHARS, "runtime_conformance_tested": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", nargs="?")
    parser.add_argument("--list", action="store_true", help="List packet titles only")
    parser.add_argument("--check", action="store_true", help="Check reference navigation only")
    parser.add_argument("--read", type=int, help="Select one numbered design read")
    parser.add_argument("--family", help="Show one existing inventory family")
    parser.add_argument("--offset", type=int, default=0, help="Character offset within selected text")
    parser.add_argument("--chars", type=int, default=DEFAULT_CHARS)
    args = parser.parse_args()
    try:
        if sum((args.list, args.check, bool(args.family), bool(args.packet))) > 1:
            raise ValueError("Choose one packet, --family, --list or --check")
        rows = packets()
        if args.check:
            if args.read is not None:
                raise ValueError("--read requires a packet")
            print(json.dumps(check(), sort_keys=True))
            return
        if not args.packet and not args.family:
            if args.read is not None:
                raise ValueError("--read requires a packet")
            for row in rows:
                print(f"{row['id']}: {row['title']}")
            print("Choose ONE packet. This is reference routing, not live progress.")
            return
        if args.family:
            if args.read is not None:
                raise ValueError("--read cannot be combined with --family")
            item = family_entries().get(args.family)
            if item is None:
                raise ValueError("Unknown inventory family")
            owner = next(row["id"] for row in rows if args.family in row["families"])
            title = f"{args.family} -> {owner}; proposed bounds, not runtime conformance"
            text = json.dumps(item, ensure_ascii=False, indent=2) + "\n"
            command = f"--family {args.family}"
        else:
            row = next((r for r in rows if r["id"] == args.packet), None)
            if row is None:
                raise ValueError("Unknown packet")
            command = args.packet
            if args.read is None:
                title = row["card"]
                text = safe_path(title).read_text(encoding="utf-8")
            else:
                if not 1 <= args.read <= len(row["reads"]):
                    raise ValueError("Read number out of range")
                source = row["reads"][args.read - 1]
                title = source["path"] + (" :: " + source["heading"] if source["heading"] else "")
                text = section(safe_path(source["path"]).read_text(encoding="utf-8"), source["heading"])
                command += f" --read {args.read}"
        excerpt, next_offset = page(text, args.offset, args.chars)
        print(f"REFERENCE ONLY | {title} | characters {args.offset}..{args.offset + len(excerpt)} of {len(text)}")
        print(excerpt, end="" if excerpt.endswith("\n") else "\n")
        if next_offset is not None:
            print(f"NEXT: python .reference/integration/read.py {command} --offset {next_offset} --chars {args.chars}")
        else:
            print("END selected text. Follow only a needed link; do not preload adjacent packets.")
    except (ValueError, KeyError, StopIteration, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
