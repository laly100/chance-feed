#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, pathlib, sys

SRC = pathlib.Path("chance.csv")        # intrarea (complet)
PARTS = 10000                           # câte rânduri/extrageri per fișier
HEADER_EXPECT = ["תאריך","הגרלה","תלתן","יהלום","לב","עלה"]

def read_rows():
    # încercăm encodări uzuale ale fișierului oficial
    for enc in ("utf-8-sig","windows-1255","cp1255","utf-8"):
        try:
            with open(SRC, "r", encoding=enc, newline="") as f:
                sample = f.read(4096); f.seek(0)
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
                r = csv.reader(f, dialect)
                header = next(r)
                header = [h.strip().strip('"') for h in header]
                rows = []
                for row in r:
                    if not row: continue
                    row = [c.strip().strip('"') for c in row]
                    rows.append(row)
                return header, rows
        except Exception:
            continue
    raise RuntimeError("Nu pot citi chance.csv (encoding/separator).")

def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def main():
    header, rows = read_rows()

    # sortăm vechi → nou (după dată + draw_id). Data este DD/MM/YYYY.
    def key_fn(r):
        d = r[0]  # תאריך
        dd, mm, yy = d.split("/")
        try:
            di = int(r[1])  # הַגְרָלָה
        except:
            di = r[1]
        return (int(yy), int(mm), int(dd), di)
    rows.sort(key=key_fn)

    n = len(rows)
    if n == 0:
        print("Nu sunt rânduri în chance.csv")
        sys.exit(0)

    # împărțim în bucăți de 10.000; ultimul chunk e cel mai nou
    parts = []
    for start in range(0, n, PARTS):
        end = min(start + PARTS, n)
        chunk = rows[start:end]
        parts.append(chunk)

    # vom numi fișierele astfel încât:
    #  - chance5.csv = cel mai vechi
    #  - ...
    #  - chance0.csv = cel mai nou
    total_files = len(parts)
    names = []
    for i, chunk in enumerate(parts):
        # index inversat: 0 = cel mai nou
        name = f"chance{total_files - 1 - i}.csv"
        names.append(name)
        write_csv(name, header, chunk)

    # scriem manifestul cu ordinea corectă (vechi → nou)
    order = names[::-1]  # inversăm ca să fie vechi → nou
    pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
    (pathlib.Path("parts") / "index.json").write_text(
        '{\n'
        '  "schema": "1.0",\n'
        f'  "order": {order!r},\n'
        f'  "header": {HEADER_EXPECT!r},\n'
        '  "notes": "Ordinea este vechi→nou. Toate fișierele au antet identic."\n'
        '}\n',
        encoding="utf-8"
    )
    print("OK: generate parts and manifest.")

if __name__ == "__main__":
    main()
