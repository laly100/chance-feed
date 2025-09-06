#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv, json, pathlib, sys

SRC = pathlib.Path("chance.csv")
PART_SIZE = 10000
HEADER_STD = ["תאריך","הגרלה","תלתן","יהלום","לב","עלה"]
RESERVE_LATEST_EMPTY = False  # lasă False

def read_rows():
    for enc in ("utf-8-sig","windows-1255","cp1255","utf-8"):
        try:
            with open(SRC,"r",encoding=enc,newline="") as f:
                sample=f.read(4096); f.seek(0)
                try:
                    dialect=csv.Sniffer().sniff(sample,delimiters=",;\t")
                except Exception:
                    class D: delimiter=","; quotechar='"'; doublequote=True; skipinitialspace=True
                    dialect=D()
                r=csv.reader(f,dialect)
                header=[h.strip().strip('"') for h in next(r)]
                rows=[]
                for row in r:
                    if not row: continue
                    rows.append([c.strip().strip('"') for c in row])
                print(f"[INFO] Read {len(rows)} rows using {enc}")
                return header, rows
        except Exception as e:
            print(f"[WARN] {enc}: {e}")
    raise RuntimeError("Nu pot citi chance.csv")

def write_csv(path, header, rows):
    p=pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p,"w",encoding="utf-8",newline="") as f:
        w=csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"[WRITE] {p} rows={len(rows)}")

def sort_old_to_new(rows):
    def key_fn(r):
        dd,mm,yy = r[0].split("/")  # תאריך DD/MM/YYYY
        try: draw=int(r[1])         # הַגְרָלָה
        except: draw=r[1]
        return (int(yy),int(mm),int(dd),draw)
    rows.sort(key=key_fn)

def main():
    header, rows = read_rows()
    use_header = HEADER_STD if header[:6]==HEADER_STD else header
    if not rows:
        write_csv("chance0.csv", use_header, [])
        write_csv("parts/index.json", use_header, [])
        return
    sort_old_to_new(rows)  # OLD→NEW
    total=len(rows)
    chunks=[rows[i:i+PART_SIZE] for i in range(0,total,PART_SIZE)]
    print(f"[INFO] Split into {len(chunks)} parts of <= {PART_SIZE}")

    names=[]
    parts_summary=[]
    if RESERVE_LATEST_EMPTY:
        total_files=len(chunks)+1
        for i,chunk in enumerate(chunks):
            name=f"chance{total_files-1-(i+1)}.csv"
            write_csv(name,use_header,chunk); names.append(name)
            first,last=chunk[0],chunk[-1]
            parts_summary.append({"file":name,"rows":len(chunk),
                                  "first_date":first[0],"first_draw":first[1],
                                  "last_date":last[0],"last_draw":last[1]})
        write_csv("chance0.csv",use_header,[])
        names.append("chance0.csv")
        parts_summary.append({"file":"chance0.csv","rows":0,
                              "first_date":None,"first_draw":None,"last_date":None,"last_draw":None})
    else:
        total_files=len(chunks)
        for i,chunk in enumerate(chunks):
            name=f"chance{total_files-1-i}.csv"  # ...2,1,0 (0 = cele mai noi)
            write_csv(name,use_header,chunk); names.append(name)
            first,last=chunk[0],chunk[-1]
            parts_summary.append({"file":name,"rows":len(chunk),
                                  "first_date":first[0],"first_draw":first[1],
                                  "last_date":last[0],"last_draw":last[1]})

    order = names[::-1]  # manifest old→new
    manifest = {
        "schema":"1.2",
        "order":order,
        "header":use_header,
        "notes":"Ordine VECHI→NOU; fișierele chanceX.csv au același antet.",
        "parts":parts_summary
    }

    # mini: ultimele 1000 / 2000 rânduri
    mini_dir=pathlib.Path("mini"); mini_dir.mkdir(parents=True, exist_ok=True)
    def tail(seq,k): return seq[-k:] if len(seq)>=k else seq
    latest_1000=tail(rows,1000); latest_2000=tail(rows,2000)
    write_csv(mini_dir/"latest_1000.csv", use_header, latest_1000)
    write_csv(mini_dir/"latest_2000.csv", use_header, latest_2000)
    manifest["mini"]={"latest_1000":"mini/latest_1000.csv","latest_2000":"mini/latest_2000.csv"}

    pathlib.Path("parts").mkdir(parents=True, exist_ok=True)
    (pathlib.Path("parts")/"index.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("[DONE] parts/index.json + mini files generated")

if __name__=="__main__":
    main()
