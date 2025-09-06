#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Split Chance CSV into 10k chunks and write parts/index.json manifest.

- Reads chance.csv (kept NEW→OLD in repo), sorts internally OLD→NEW for correct slicing.
- Writes chance{N}.csv … chance0.csv (each <=10,000 rows) with identical header.
- Writes parts/index.json with order (old→new) + per-file summary.
"""

import csv, json, pathlib, sys

SRC = pathlib.Path("chance.csv")
PART_SIZE = 10000
HEADER_STD = ["תאריך","הגרלה","תלתן","יהלום","לב","עלה"]

# If True, keeps chance0.csv as an EMPTY "reservoir" (only header).
RESERVE_LATEST_EMPTY = False   # Recommended: False

def read_rows():
    """Return (header:list[str], rows:list[list[str]]) from chance.csv with autodetect."""
    for enc in ("utf-8-sig", "windows-1255", "cp1255", "utf-8"):
        try:
            with open(SRC, "r", encoding=enc, newline="") as f:
                sample = f.read(4096); f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
                except Exception:
                    # Fallback: comma
                    class _D: delimiter = ","; quotechar='"'; doublequote=True; skipinitialspace=True
                    dialect = _D()
                r = csv.reader(f, dialect)
                header = [h.strip().strip('"') for h in next(r)]
                rows = []
                for row in r:
                    if not row: continue
                    rows.append([c.strip().strip('"') for c in row])
                print(f"[INFO] Read {len(rows)} rows using encoding={enc} delimiter='{getattr(dialect,'delimiter',',')}'")
                return header, rows
        except Exception as e:
            print(f"[WARN] Failed with encoding {enc}: {e}")
    raise RuntimeError("Nu pot citi chance.csv (encoding/separator).")

def write_csv(path, header, rows):
    p = pathlib.Path(path)
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"[WRITE] {p} rows={len(rows)}")

def sort_old_to_new(rows):
    """Sort DD/MM/YYYY then draw_id."""
    def key_fn(r):
        d = r[0]  # תאריך
        dd, mm, yy = d.split("/")
        try:
            draw = int(r[1])
        except:
            draw = r[1]
        return (int(yy), int(mm), int(dd), draw)
    rows.sort(key=key_fn)

def build_manifest(order_files, header, parts_summary):
    return {
        "schema": "1.2",
        "order": order_files,                 # old -> new
        "header": header,
        "notes": "Ordinea este vechi→nou. Fișierele chanceX.csv au antet identic.",
        "parts": parts_summary                # [{file, rows, first_date, first_draw, last_date, last_draw}]
    }

def main():
    header, rows = read_rows()
    use_header = header if header[:6] != HEADER_STD else HEADER_STD

    if not rows:
        print("[INFO] Empty CSV. Creating empty chance0.csv + manifest.")
        write_csv("chance0.csv", use_header, [])
        pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
        (pathlib.Path("parts")/"index.json").write_text(
            json.dumps(build_manifest(["chance0.csv"], use_header, [{
                "file":"chance0.csv","rows":0,"first_date":None,"first_draw":None,"last_date":None,"last_draw":None
            }]), ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        return

    # Ensure chronological processing (OLD→NEW) for correct gaps/streaks.
    sort_old_to_new(rows)
    total = len(rows)
    print(f"[INFO] Sorted OLD→NEW. Total rows={total}")

    # Slice into 10k chunks (OLD→NEW).
    chunks = [rows[i:i+PART_SIZE] for i in range(0, total, PART_SIZE)]
    print(f"[INFO] Split into {len(chunks)} part(s) of <= {PART_SIZE} rows")

    names = []
    parts_summary = []

    if RESERVE_LATEST_EMPTY:
        # Shift indices so chance0.csv stays empty.
        total_files = len(chunks) + 1
        for i, chunk in enumerate(chunks):
            name = f"chance{total_files - 1 - (i+1)}.csv"   # chanceN..chance1
            write_csv(name, use_header, chunk)
            names.append(name)
            first, last = chunk[0], chunk[-1]
            parts_summary.append({
                "file": name, "rows": len(chunk),
                "first_date": first[0], "first_draw": first[1],
                "last_date":  last[0],  "last_draw":  last[1]
            })
        write_csv("chance0.csv", use_header, [])
        names.append("chance0.csv")
        parts_summary.append({"file":"chance0.csv","rows":0,
                              "first_date":None,"first_draw":None,
                              "last_date":None,"last_draw":None})
    else:
        # Normal: chance0.csv contains the NEWEST ≤10,000 rows.
        total_files = len(chunks)
        for i, chunk in enumerate(chunks):
            name = f"chance{total_files - 1 - i}.csv"       # ... chance2, chance1, chance0
            write_csv(name, use_header, chunk)
            names.append(name)
            first, last = chunk[0], chunk[-1]
            parts_summary.append({
                "file": name, "rows": len(chunk),
                "first_date": first[0], "first_draw": first[1],
                "last_date":  last[0],  "last_draw":  last[1]
            })

    # Manifest (ORDER = old→new)
    order = names[::-1]
    pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
    manifest_path = pathlib.Path("parts")/"index.json"
    manifest = build_manifest(order, use_header, parts_summary)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[WRITE] {manifest_path} (order OLD→NEW)")

    print("[SUMMARY]")
    for p in parts_summary:
        print(f"  - {p['file']:>10}: rows={p['rows']:>5}  "
              f"[{p['first_date']}#{p['first_draw']} → {p['last_date']}#{p['last_draw']}]")
    print("[DONE] regenerate parts + manifest")

if __name__ == "__main__":
    main()
