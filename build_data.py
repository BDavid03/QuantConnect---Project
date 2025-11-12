import re, json, time, zipfile, shutil, requests
import pandas as pd, numpy as np
from pathlib import Path
from urllib.parse import urlparse

OUT = Path("Data/alternative/sec/failstodeliver")
prefixes = ("cnsfails", "cnsp_sec")
base_url = "https://catalog.data.gov/dataset/fails-to-deliver-data"


# --- Find meta_data json and Extracts .zip links ---
def extract(base_url, out_dir=OUT, ledger_path="previous.json", email="bondquantconnect@gmail.com"):
    OUT.mkdir(parents=True, exist_ok=True)
    LEDGER = Path(ledger_path)
    try:
        state = json.loads(LEDGER.read_text(encoding="utf-8")) if LEDGER.exists() else {"downloaded": []}
    except Exception:
        state = {"downloaded": []}
    if not LEDGER.exists():
        LEDGER.write_text(json.dumps(state, indent=2), encoding="utf-8")

    S = requests.Session()
    S.headers.update({
        "User-Agent": f"Mozilla/5.0 (compatible; SECDataBot/1.0; +{email})",
        "From": email,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": base_url,
        "Connection": "keep-alive",
    })

    print(f"🔎 fetching harvest metadata list from {base_url}")
    LINK = S.get(base_url, timeout=30).text
    meta_data = re.findall(r'href="/harvest/object/([a-f0-9\-]+)"', LINK, re.I)
    meta_data = list(dict.fromkeys(meta_data))
    print(f"Found {len(meta_data)} harvest objects")

    zip_urls = []
    for hid in meta_data:
        meta_url = f"https://catalog.data.gov/harvest/object/{hid}"
        try:
            r = S.get(meta_url, timeout=30); r.raise_for_status()
            found = re.findall(r'https://www\.sec\.gov/files/[^\s"<>]+?\.zip\b', r.text, re.I)
            zip_urls.extend(found); time.sleep(0.2)
        except Exception:
            continue

    zip_urls = list(dict.fromkeys(zip_urls))
    print(f"📦 Found {len(zip_urls)} SEC zip files total")

    for url in zip_urls:
        if url in state["downloaded"]:
            continue
        fn = Path(urlparse(url).path).name or "file.zip"
        fp = OUT / fn
        try:
            time.sleep(0.5)
            r = S.get(url, stream=True, timeout=120)
            if r.status_code == 403:
                r.close()
                r = S.get(url, headers={"Referer": "https://www.sec.gov/files/data/fails-deliver-data/"}, stream=True, timeout=120)
            r.raise_for_status()
            with fp.open("wb") as f:
                for chunk in r.iter_content(262144):
                    if chunk: f.write(chunk)
            if fp.stat().st_size > 0:
                state["downloaded"].append(url)
                LEDGER.write_text(json.dumps(state, indent=2), encoding="utf-8")
                print(f"Downloaded {fn}")
            else:
                fp.unlink(missing_ok=True)
        except Exception:
            fp.unlink(missing_ok=True); continue

# --- Unzips Zip Files Found in extract() ---
def unZIP(OUT, prefixes):
    for z in OUT.glob("*.zip"):
        if not z.name.lower().startswith(prefixes): continue
        with zipfile.ZipFile(z) as zf:
            for info in zf.infolist():
                if info.is_dir(): continue
                dest = z.parent / Path(info.filename).name
                with zf.open(info) as src, open(dest, "wb") as dst:
                    shutil.copyfileobj(src, dst)
        z.unlink()

# --- Fixes instances where delimiter is found in row ---
def fix_lines(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "cp1252", "latin1"):
        try:
            lines = path.read_text(encoding=enc, errors="replace").splitlines(); break
        except UnicodeDecodeError:
            continue
    else:
        return pd.DataFrame()
    if not lines: return pd.DataFrame()
    header = [h.strip() for h in lines[0].split("|")][:6]
    rows = []
    for ln in lines[1:]:
        parts = [s.strip() for s in ln.split("|")]; n = len(parts)
        if n == 6: row = parts
        elif n == 7: parts[4] = f"{parts[4]}-{parts[5]}"; row = parts[:5] + [parts[6]]
        elif n > 7: row = parts[:4] + ["-".join(parts[4:-1]), parts[-1]]
        else: continue
        if not row[1].strip(): continue
        rows.append(row)
    return pd.DataFrame(rows, columns=header)

# --- CUSIP Filtering, [0] for US equity and [6,7] = 10 for equity ---
def is_equity_cusip(cusip):
    c = str(cusip).strip().upper()
    return len(c) >= 8 and c[0] == '0' and c[6:8] == '10'


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    extract(base_url=base_url, out_dir=OUT)
    unZIP(OUT, prefixes)

    for p in OUT.iterdir():
        if not p.is_file() or not (p.suffix.lower() == ".txt" or p.suffix == ""):
            continue

        data = fix_lines(p)
        if data.empty:
            p.unlink(missing_ok=True)
            continue
        # numeric conversions
        if "PRICE" in data.columns:
            data["PRICE"] = pd.to_numeric(data["PRICE"], errors="coerce")
            data = data[data["PRICE"].notna() & (data["PRICE"] != 0)]
        if "QUANTITY (FAILS)" in data.columns:
            data["QUANTITY (FAILS)"] = pd.to_numeric(data["QUANTITY (FAILS)"], errors="coerce")

        req = ["CUSIP", "SYMBOL", "SETTLEMENT DATE", "QUANTITY (FAILS)", "PRICE"]
        if not all(c in data.columns for c in req):
            p.unlink(missing_ok=True)
            continue

        data["CUSIP"] = data["CUSIP"].astype(str)
        data = data[data["CUSIP"].apply(is_equity_cusip)]
        if data.empty:
            p.unlink(missing_ok=True)
            continue

        data["SETTLEMENT DATE"] = pd.to_datetime(data["SETTLEMENT DATE"], errors="coerce", format="%Y%m%d")
        data = data[data["SETTLEMENT DATE"] >= pd.Timestamp("2009-07-01")]
        if data.empty:
            p.unlink(missing_ok=True)
            continue

        # Offset date = release date logic: 1–15 → month end, 16–31 → next month 15th
        def offset_date(d):
            if pd.isna(d):
                return pd.NaT
            return (d + pd.offsets.MonthEnd(0)) if d.day <= 15 else (d + pd.offsets.MonthBegin(1)).replace(day=15)

        data["OFFSET_DATE"] = data["SETTLEMENT DATE"].apply(offset_date)
        if data["OFFSET_DATE"].isna().all():
            p.unlink(missing_ok=True)
            continue

        data["DATE"] = data["SETTLEMENT DATE"].dt.strftime("%Y%m%d")
        out_cols = ["DATE", "SYMBOL", "QUANTITY (FAILS)", "PRICE"]

        for off_dt, chunk in data.groupby(data["OFFSET_DATE"]):
            period = pd.to_datetime(off_dt).strftime("%Y%m%d")
            zip_path = OUT / f"{period}.zip"
            csv_name = f"{period}.csv"

            out = chunk[out_cols].copy()

            if zip_path.exists():
                try:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        if csv_name in zf.namelist():
                            with zf.open(csv_name) as f:
                                old = pd.read_csv(f, header=None)
                            old.columns = out_cols[:len(old.columns)] + [c for c in out_cols[len(old.columns):]]
                            out = pd.concat([old, out], ignore_index=True)
                            out.drop_duplicates(subset=out_cols, inplace=True)
                except Exception:
                    pass

            def fmt_row(r):
                q = r["QUANTITY (FAILS)"]
                q_str = "" if pd.isna(q) else (str(int(q)) if float(q).is_integer() else f"{q:.2f}")
                p_str = "" if pd.isna(r["PRICE"]) else f"{r['PRICE']:.2f}"
                return f"{r['DATE']},{r['SYMBOL']},{q_str},{p_str}"

            lines = [fmt_row(r) for _, r in out.iterrows()]
            csv_bytes = ("\n".join(lines) + "\n").encode("utf-8")

            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(csv_name, csv_bytes)

            print(f"✅ {zip_path.name} — {len(out)} rows")

        p.unlink(missing_ok=True)

    for f in OUT.glob("*"):
        if f.suffix.lower() in (".txt", ".csv"):
            f.unlink(missing_ok=True)
