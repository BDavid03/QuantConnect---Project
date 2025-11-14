import re, json, time, zipfile, shutil, requests
import pandas as pd, numpy as np
from pathlib import Path
from urllib.parse import urlparse
import logging


# ============================================================
# LOGGER
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("secftd")


OUT = Path("output/alternative/sec/failstodeliver")
PREFIXES = ("cnsfails", "cnsp_sec")
BASE_URL = "https://catalog.data.gov/dataset/fails-to-deliver-data"
LEDGER_PATH = Path("previous.json")

# ============================================================
# LEDGER
# ============================================================

class Ledger:
    def __init__(self, path: Path):
        self.path = path
        self.state = self._load()

    def _load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return {"downloaded": []}
        return {"downloaded": []}

    def save(self):
        self.path.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def add(self, url: str):
        if url not in self.state["downloaded"]:
            self.state["downloaded"].append(url)
            self.save()

    def contains(self, url: str) -> bool:
        return url in self.state["downloaded"]


# ============================================================
# HTTP SCRAPER
# ============================================================

class SECDownloader:
    def __init__(self, email: str, base_url: str):
        self.email = email
        self.base_url = base_url

        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": f"Mozilla/5.0 (compatible; SECDataBot/1.0; +{email})",
            "From": email,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": base_url,
            "Connection": "keep-alive",
        })

    def get(self, url: str, **kw):
        return self.s.get(url, **kw)


# ============================================================
# METADATA EXTRACTION
# ============================================================

class MetadataExtractor:
    def __init__(self, downloader: SECDownloader):
        self.dl = downloader

    def get_harvest_ids(self) -> list[str]:
        log.info(f"🔎 fetching harvest metadata list from {self.dl.base_url}")
        html = self.dl.get(self.dl.base_url, timeout=30).text
        ids = re.findall(r'href="/harvest/object/([a-f0-9\-]+)"', html, re.I)
        ids = list(dict.fromkeys(ids))
        log.info(f"Found {len(ids)} harvest objects")
        return ids

    def extract_zip_urls(self, harvest_ids: list[str]) -> list[str]:
        zip_urls = []

        for hid in harvest_ids:
            meta_url = f"https://catalog.data.gov/harvest/object/{hid}"
            try:
                r = self.dl.get(meta_url, timeout=30)
                r.raise_for_status()
                found = re.findall(
                    r'https://www\.sec\.gov/files/[^\s"<>]+?\.zip\b',
                    r.text,
                    re.I
                )
                zip_urls.extend(found)
                time.sleep(0.2)
            except Exception:
                continue

        zip_urls = list(dict.fromkeys(zip_urls))
        log.info(f"📦 Found {len(zip_urls)} SEC zip files total")

        return zip_urls


# ============================================================
# ZIP DOWNLOADER
# ============================================================

class ZipFetcher:
    def __init__(self, dl: SECDownloader, ledger: Ledger, out_dir: Path):
        self.dl = dl
        self.ledger = ledger
        self.out = out_dir

    def download_all(self, urls: list[str]):
        for url in urls:
            if self.ledger.contains(url):
                continue

            fn = Path(urlparse(url).path).name or "file.zip"
            fp = self.out / fn

            log.info(f"⬇ downloading {fn}")
            time.sleep(0.5)

            try:
                r = self.dl.get(url, stream=True, timeout=120)
                if r.status_code == 403:
                    r.close()
                    r = self.dl.get(
                        url,
                        headers={"Referer": "https://www.sec.gov/files/data/fails-deliver-data/"},
                        stream=True,
                        timeout=120,
                    )

                r.raise_for_status()

                with fp.open("wb") as f:
                    for chunk in r.iter_content(262144):
                        if chunk:
                            f.write(chunk)

                if fp.stat().st_size > 0:
                    # atomic ledger update
                    self.ledger.state["downloaded"].append(url)

                    tmp = self.ledger.path.with_suffix(".tmp")
                    tmp.write_text(json.dumps(self.ledger.state, indent=2), encoding="utf-8")
                    tmp.replace(self.ledger.path)

                    log.info(f"✔ ledger updated ({url})")
                else:
                    fp.unlink(missing_ok=True)

            except Exception:
                fp.unlink(missing_ok=True)
                continue


# ============================================================
# UNZIP
# ============================================================

class Unzipper:
    def __init__(self, out_dir: Path, prefixes: tuple[str, ...]):
        self.out = out_dir
        self.prefixes = prefixes

    def run(self):
        for z in self.out.glob("*.zip"):
            if not z.name.lower().startswith(self.prefixes):
                continue
            with zipfile.ZipFile(z) as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    dest = z.parent / Path(info.filename).name
                    with zf.open(info) as src, open(dest, "wb") as dst:
                        shutil.copyfileobj(src, dst)
            z.unlink()




# ============================================================
# CLEAN + TRANSFORM
# ============================================================

class Cleaner:
    @staticmethod
    def fix_lines(path: Path) -> pd.DataFrame:
        for enc in ("utf-8", "cp1252", "latin1"):
            try:
                text = path.read_text(encoding=enc, errors="replace")
                break
            except Exception:
                continue
        else:
            return pd.DataFrame()

        lines = text.splitlines()
        if not lines:
            return pd.DataFrame()

        header = [h.strip() for h in lines[0].split("|")][:6]
        rows = []

        for ln in lines[1:]:
            parts = [s.strip() for s in ln.split("|")]
            n = len(parts)

            if n == 6:
                row = parts
            elif n == 7:
                parts[4] = f"{parts[4]}-{parts[5]}"
                row = parts[:5] + [parts[6]]
            elif n > 7:
                row = parts[:4] + ["-".join(parts[4:-1]), parts[-1]]
            else:
                continue

            if not row[1].strip():
                continue

            rows.append(row)

        return pd.DataFrame(rows, columns=header)

    @staticmethod
    def is_equity_cusip(cusip: str) -> bool:
        c = str(cusip).strip().upper()
        return len(c) >= 8 and c[0] == "0" and c[6:8] == "10"

    @staticmethod
    def offset_date(d):
        if pd.isna(d):
            return pd.NaT
        return (d + pd.offsets.MonthEnd(0)) if d.day <= 15 else (d + pd.offsets.MonthBegin(1)).replace(day=15)


class Aggregator:
    def __init__(self, out_dir: Path):
        self.out = out_dir
        self.cleaner = Cleaner()

    def run(self):
        for p in self.out.iterdir():
            if not p.is_file() or p.suffix.lower() not in (".txt", "", ".csv"):
                continue

            df = self.cleaner.fix_lines(p)
            if df.empty:
                p.unlink(missing_ok=True)
                continue

            if "PRICE" in df.columns:
                df["PRICE"] = pd.to_numeric(df["PRICE"], errors="coerce")
                df = df[df["PRICE"].notna() & (df["PRICE"] != 0)]

            if "QUANTITY (FAILS)" in df.columns:
                df["QUANTITY (FAILS)"] = pd.to_numeric(df["QUANTITY (FAILS)"], errors="coerce")

            req = ["CUSIP", "SYMBOL", "SETTLEMENT DATE", "QUANTITY (FAILS)", "PRICE"]
            if not all(c in df.columns for c in req):
                p.unlink(missing_ok=True)
                continue

            df["CUSIP"] = df["CUSIP"].astype(str)
            df = df[df["CUSIP"].apply(self.cleaner.is_equity_cusip)]
            if df.empty:
                p.unlink(missing_ok=True)
                continue

            df["SETTLEMENT DATE"] = pd.to_datetime(df["SETTLEMENT DATE"], format="%Y%m%d", errors="coerce")
            df = df[df["SETTLEMENT DATE"] >= pd.Timestamp("2009-07-01")]
            if df.empty:
                p.unlink(missing_ok=True)
                continue

            df["OFFSET_DATE"] = df["SETTLEMENT DATE"].apply(self.cleaner.offset_date)
            df["DATE"] = df["SETTLEMENT DATE"].dt.strftime("%Y%m%d")

            out_cols = ["DATE", "SYMBOL", "QUANTITY (FAILS)", "PRICE"]

            for off_dt, chunk in df.groupby(df["OFFSET_DATE"]):
                period = pd.to_datetime(off_dt).strftime("%Y%m%d")
                zip_path = self.out / f"{period}.zip"
                csv_name = f"{period}.csv"

                data = chunk[out_cols].copy()

                if zip_path.exists():
                    try:
                        with zipfile.ZipFile(zip_path, "r") as zf:
                            if csv_name in zf.namelist():
                                old = pd.read_csv(zf.open(csv_name), header=None)
                                old.columns = out_cols[:len(old.columns)]
                                data = pd.concat([old, data], ignore_index=True).drop_duplicates()
                    except Exception:
                        pass

                lines = []
                for _, r in data.iterrows():
                    q = r["QUANTITY (FAILS)"]
                    q_str = "" if pd.isna(q) else (str(int(q)) if float(q).is_integer() else f"{q:.2f}")
                    p_str = "" if pd.isna(r["PRICE"]) else f"{r['PRICE']:.2f}"
                    lines.append(f"{r['DATE']},{r['SYMBOL']},{q_str},{p_str}")

                with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(csv_name, "\n".join(lines) + "\n")

                log.info(f"✓ {zip_path.name} — {len(data)} rows")

            p.unlink(missing_ok=True)



# ============================================================
# PIPELINE
# ============================================================

class SECFTDPipeline:
    def __init__(self):
        OUT.mkdir(parents=True, exist_ok=True)

        self.ledger = Ledger(LEDGER_PATH)
        self.downloader = SECDownloader(email="bondquantconnect@gmail.com", base_url=BASE_URL)
        self.meta = MetadataExtractor(self.downloader)
        self.fetcher = ZipFetcher(self.downloader, self.ledger, OUT)
        self.unzipper = Unzipper(OUT, PREFIXES)
        self.aggregator = Aggregator(OUT)

    def run(self):
        harvest_ids = self.meta.get_harvest_ids()
        zip_urls = self.meta.extract_zip_urls(harvest_ids)

        self.fetcher.download_all(zip_urls)
        self.unzipper.run()
        self.aggregator.run()

        for f in OUT.glob("*"):
            if f.suffix.lower() in (".txt", ".csv"):
                f.unlink(missing_ok=True)


# ============================================================
# MAIN
# ============================================================
#                              ._ o o
#                               \_`-)|_
#                            ,""       \ 
#                          ,"  ## |   ಠ ಠ. 
#                        ," ##   ,-\__    `.
#                      ,"       /     `--._;)
#                    ,"     ## /
#                  ,"   ##    /


if __name__ == "__main__":
    SECFTDPipeline().run()
