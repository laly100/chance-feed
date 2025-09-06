#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, pathlib, sys

SRC = pathlib.Path("chance.csv")     # intrarea (complet, în repo)
PART_SIZE = 10000                    # rânduri per fișier (≈ 5 ani)
HEADER_STD = ["תאריך","הגרלה","תלתן","יהלום","לב","עלה"]

# Dacă vrei ca chance0.csv să fie GOL (rezervor), pune True.
RESERVE_LATEST_EMPTY = False

def read_rows():
    """Citește chance.csv cu auto-detect la encoding și delimitator, returnează (header, rows)."""
    for enc in ("utf-8-sig","windows-1255","cp1255","utf-8"):
        try:
            with open(SRC, "r", encoding=enc, newline="") as f:
                sample = f.read(4096); f.seek(0)
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
                r = csv.reader(f, dialect)
                header = [h.strip().strip('"') for h in next(r)]
                rows = []
                for row in r:
                    if not row: continue
                    rows.append([c.strip().strip('"') for c in row])
                print(f"[INFO] Read {len(rows)} rows from chance.csv using {enc}")
                return header, rows
        except Exception as e:
            print(f"[WARN] Failed with {enc}: {e}")
            continue
    raise RuntimeError("Nu pot citi chance.csv (encoding/separator).")

def write_csv(path, header, rows):
    path = pathlib.Path(path)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"[WRITE] {path} rows={len(rows)}")

def main():
    header, rows = read_rows()
    if header[:6] != HEADER_STD:
        print(f"[WARN] Antetul găsit diferă de cel standard. Folosesc antetul detectat: {header}")
        use_header = header
    else:
        use_header = HEADER_STD

    # chance.csv poate fi nou→vechi. Sortăm VECHI→NOU pentru împărțire corectă.
    def key_fn(r):
        d = r[0]  # תאריך (DD/MM/YYYY)
        dd, mm, yy = d.split("/")
        try:
            di = int(r[1])  # הַגְרָלָה
        except:
            di = r[1]
        return (int(yy), int(mm), int(dd), di)

    rows.sort(key=key_fn)  # vechi→nou
    n = len(rows)
    if n == 0:
        print("[INFO] Nu sunt rânduri în chance.csv")
        write_csv("chance0.csv", use_header, [])
        pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
        (pathlib.Path("parts")/"index.json").write_text(
            '{ "schema":"1.0", "order":["chance0.csv"], "header":'+str(use_header)+', "notes":"gol" }\n',
            encoding="utf-8"
        )
        print("[DONE] parts/index.json (gol) creat.")
        sys.exit(0)

    # Împărțire în bucăți de 10.000 (vechi→nou)
    chunks = [rows[i:i+PART_SIZE] for i in range(0, n, PART_SIZE)]
    print(f"[INFO] Total rows={n} -> {len(chunks)} parts of {PART_SIZE}")

    names = []
    # Construim și un mic rezumat pentru manifest (first/last date/draw_id per fișier)
    parts_summary = []

    if RESERVE_LATEST_EMPTY:
        total_files = len(chunks) + 1
        for i, chunk in enumerate(chunks):
            name = f"chance{total_files - 1 - (i+1)}.csv"  # shift pentru a lăsa chance0 gol
            write_csv(name, use_header, chunk)
            names.append(name)
            # summary
            first, last = chunk[0], chunk[-1]
            parts_summary.append({
                "file": name,
                "rows": len(chunk),
                "first_date": first[0], "first_draw": first[1],
                "last_date":  last[0], "last_draw":  last[1]
            })
        write_csv("chance0.csv", use_header, [])
        names.append("chance0.csv")
        parts_summary.append({
            "file": "chance0.csv", "rows": 0,
            "first_date": None, "first_draw": None,
            "last_date":  None, "last_draw":  None
        })
    else:
        total_files = len(chunks)
        for i, chunk in enumerate(chunks):
            name = f"chance{total_files - 1 - i}.csv"  # 0 = cel mai nou
            write_csv(name, use_header, chunk)
            names.append(name)
            first, last = chunk[0], chunk[-1]
            parts_summary.append({
                "file": name,
                "rows": len(chunk),
                "first_date": first[0], "first_draw": first[1],
                "last_date":  last[0], "last_draw":  last[1]
            })

    # Manifest (ORDINE: vechi→nou; GPT va parcurge exact în această ordine)
    order = names[::-1]
    pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
    manifest_path = pathlib.Path("parts")/"index.json"
    import json
    manifest_obj = {
        "schema": "1.2",
        "order": order,
        "header": use_header,
        "notes": "Ordinea este vechi→nou. Fișierele chanceX.csv au antet identic.",
        "parts": parts_summary
    }
    manifest_path.write_text(json.dumps(manifest_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[WRITE] {manifest_path} (order old→new)")

    print("[SUMMARY]")
    for p in parts_summary:
        print(f"  - {p['file']}: rows={p['rows']}  [{p['first_date']}#{p['first_draw']} → {p['last_date']}#{p['last_draw']}]")
    print("[DONE] regenerate parts + manifest")

if __name__ == "__main__":
    main()
