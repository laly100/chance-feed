#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, pathlib, sys

SRC = pathlib.Path("chance.csv")    # intrarea (complet, nou→vechi în fișier)
PART_SIZE = 10000                   # rânduri per fișier
HEADER = ["תאריך","הגרלה","תלתן","יהלום","לב","עלה"]

# Dacă vrei ca chance0.csv să fie GOL (rezervor), pune True.
RESERVE_LATEST_EMPTY = False

def read_rows():
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
                return header, rows
        except Exception:
            continue
    raise RuntimeError("Nu pot citi chance.csv (encoding/separator).")

def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)

def main():
    header, rows = read_rows()
    if header[:6] != HEADER:
        print("Avertisment: antetul diferă de cel așteptat. Continuăm cu antetul găsit.")

    # chance.csv este nou→vechi. Sortăm în memorie VECHI→NOU pentru împărțire corectă.
    def key_fn(r):
        d = r[0]  # תאריך (DD/MM/YYYY)
        dd, mm, yy = d.split("/")
        try: di = int(r[1])
        except: di = r[1]
        return (int(yy), int(mm), int(dd), di)

    rows.sort(key=key_fn)  # vechi→nou
    n = len(rows)
    if n == 0:
        print("Nu sunt rânduri în chance.csv")
        write_csv("chance0.csv", header, [])
        pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
        (pathlib.Path("parts")/"index.json").write_text(
            '{ "schema":"1.0", "order":["chance0.csv"], "header":'+str(HEADER)+', "notes":"gol" }\n',
            encoding="utf-8"
        )
        sys.exit(0)

    # împărțim în bucăți de 10.000 (vechi→nou)
    chunks = [rows[i:i+PART_SIZE] for i in range(0, n, PART_SIZE)]

    names = []
    if RESERVE_LATEST_EMPTY:
        total_files = len(chunks) + 1
        # scriem bucățile începând de la cel mai vechi, dar numim astfel încât chance0 să rămână gol
        for i, chunk in enumerate(chunks):
            name = f"chance{total_files - 1 - (i+1)}.csv"  # 1..N devin chanceN..chance1
            write_csv(name, header, chunk)
            names.append(name)
        write_csv("chance0.csv", header, [])               # rezervor gol
        names.append("chance0.csv")
    else:
        total_files = len(chunks)
        for i, chunk in enumerate(chunks):
            name = f"chance{total_files - 1 - i}.csv"      # 0 = cel mai nou
            write_csv(name, header, chunk)
            names.append(name)

    # manifest (ORDINE: vechi→nou; GPT va parcurge exact în această ordine)
    order = names[::-1]
    pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
    manifest = (
        '{\n'
        '  "schema": "1.1",\n'
        f'  "order": {order!r},\n'
        f'  "header": {HEADER!r},\n'
        '  "notes": "Ordinea este vechi→nou. Fișierele chanceX.csv au antet identic."\n'
        '}\n'
    )
    (pathlib.Path("parts")/"index.json").write_text(manifest, encoding="utf-8")

    print("OK: regenerate parts + manifest")
    print("Order (old→new):", order)

if __name__ == "__main__":
    main()
