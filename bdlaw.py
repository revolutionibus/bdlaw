#!/usr/bin/env python3
"""Offline, searchable copy of https://bdlaws.minlaw.gov.bd (stdlib only).

  python3 bdlaw.py update          # fetch acts not yet stored (first run = full download)
  python3 bdlaw.py update --all    # re-download every act, replace the ones that changed
  python3 bdlaw.py search "words"  # search from the terminal
  python3 bdlaw.py serve           # search UI at http://127.0.0.1:8000
  python3 bdlaw.py split           # after an update: rewrite bdlaws.db.gz.* parts for git
"""
import argparse, gzip, hashlib, html, io, re, shutil, sqlite3, subprocess, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SITE = "https://bdlaws.minlaw.gov.bd"
DB = Path(__file__).with_name("bdlaws.db")


def fetch(path, tries=4):
    for i in range(tries):
        try:
            # curl, not urllib: python.org macOS builds ship without CA certs; curl uses the system store
            raw = subprocess.run(["curl", "-sSfL", "-m", "120", "-A", "Mozilla/5.0 (offline-law-archive)", SITE + path],
                                 capture_output=True, check=True).stdout
            # site serves UTF-16 with a BOM; fall back to UTF-8 if that ever changes
            return raw.decode("utf-16") if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else raw.decode("utf-8", "replace")
        except Exception as e:
            if i == tries - 1:
                raise
            time.sleep(2 ** i * 3)


def to_text(fragment):
    fragment = re.sub(r"(?is)<(script|style)\b.*?</\1>", "", fragment)
    fragment = re.sub(r"(?i)<br\s*/?>|</(p|div|tr|li|h\d)>", "\n", fragment)
    text = html.unescape(re.sub(r"<[^>]+>", " ", fragment))
    lines = (re.sub(r"[ \t ]+", " ", l).strip() for l in text.splitlines())
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def list_acts():
    """[(act_id, title, year)] from the chronological index."""
    page = fetch("/laws-of-bangladesh-chronological-index.html")
    acts = {}
    for row in re.findall(r"(?is)<tr\b.*?</tr>", page):
        ids = re.findall(r'href="/act-(\d+)\.html"', row)
        if not ids:
            continue
        cells = [to_text(c) for c in re.findall(r"(?is)<td\b.*?</td>", row)]
        year = next((c for c in cells if re.fullmatch(r"\d{4}", c)), "")
        title = max(cells, key=len) if cells else ""
        acts[int(ids[0])] = (title, year)
    return [(k, *v) for k, v in sorted(acts.items())]


def fetch_act(act_id):
    page = fetch(f"/act-details-{act_id}.html")
    title = to_text(re.search(r"(?is)<title>(.*?)</title>", page).group(1))
    start = page.find("<h3>")
    end = page.find("<footer")
    text = to_text(page[start if start >= 0 else 0 : end if end >= 0 else len(page)])
    return title, text


CHUNK = 45 * 1024 * 1024  # GitHub rejects files >100 MB and warns >50 MB


def parts():
    return sorted(DB.parent.glob(DB.name + ".gz.*"))


def split_db():
    """Compress bdlaws.db into bdlaws.db.gz.000, .001, ... small enough for GitHub."""
    db = sqlite3.connect(DB)
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # flush pending writes into the main file
    db.close()
    for p in parts():
        p.unlink()
    with open(DB, "rb") as src:
        data = gzip.compress(src.read(), 9)
    for n, i in enumerate(range(0, len(data), CHUNK)):
        DB.with_name(f"{DB.name}.gz.{n:03}").write_bytes(data[i:i + CHUNK])
    DB.touch()  # keep db newer than its own parts so the next start doesn't rebuild
    print(f"wrote {len(parts())} parts")


def join_db():
    """Rebuild bdlaws.db from the parts (done automatically when missing or stale)."""
    tmp = DB.with_name(DB.name + ".tmp")
    with open(tmp, "wb") as out, gzip.GzipFile(fileobj=io.BytesIO(b"".join(p.read_bytes() for p in parts()))) as gz:
        shutil.copyfileobj(gz, out)
    tmp.replace(DB)  # only appears once fully written


def connect():
    ps = parts()  # rebuild if db missing or parts are newer (e.g. after git pull)
    if ps and (not DB.exists() or max(p.stat().st_mtime for p in ps) > DB.stat().st_mtime):
        print("building bdlaws.db from parts ...", file=sys.stderr)
        join_db()
    db = sqlite3.connect(DB, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")  # lets the web UI read while an update is writing
    db.executescript("""
        CREATE TABLE IF NOT EXISTS acts(id INTEGER PRIMARY KEY, title TEXT, year TEXT, text TEXT,
                                        sha TEXT, fetched TEXT);
        CREATE VIRTUAL TABLE IF NOT EXISTS acts_fts USING fts5(title, text, content='acts',
                                        content_rowid='id', tokenize='trigram');
        CREATE TRIGGER IF NOT EXISTS acts_ai AFTER INSERT ON acts BEGIN
          INSERT INTO acts_fts(rowid, title, text) VALUES (new.id, new.title, new.text); END;
        CREATE TRIGGER IF NOT EXISTS acts_ad AFTER DELETE ON acts BEGIN
          INSERT INTO acts_fts(acts_fts, rowid, title, text) VALUES ('delete', old.id, old.title, old.text); END;
    """)
    return db


def update(refresh_all, workers):
    db = connect()
    index = list_acts()
    have = {r[0]: r[1] for r in db.execute("SELECT id, sha FROM acts")}
    todo = [a for a in index if refresh_all or a[0] not in have]
    print(f"{len(index)} acts on site, {len(have)} stored, fetching {len(todo)}")
    new = changed = failed = 0

    def job(a):
        try:
            return a, fetch_act(a[0]), None
        except Exception as e:
            return a, None, e

    with ThreadPoolExecutor(workers) as pool:  # small pool: be polite to a government server
        for n, ((act_id, idx_title, year), res, err) in enumerate(pool.map(job, todo), 1):
            if err:
                failed += 1
                print(f"  ! act {act_id}: {err}", file=sys.stderr)
                continue
            title, text = res
            sha = hashlib.sha256(text.encode()).hexdigest()
            if have.get(act_id) == sha:
                continue
            changed += act_id in have
            new += act_id not in have
            db.execute("DELETE FROM acts WHERE id=?", (act_id,))
            db.execute("INSERT INTO acts VALUES(?,?,?,?,?,datetime('now'))",
                       (act_id, title or idx_title, year, text, sha))
            if n % 25 == 0:
                db.commit()
                print(f"  {n}/{len(todo)}")
    db.commit()
    gone = set(have) - {a[0] for a in index}
    print(f"done: {new} new, {changed} updated, {failed} failed"
          + (f"; {len(gone)} stored acts no longer listed on site (kept): {sorted(gone)}" if gone else ""))


def search(db, q, limit=50):
    q = q.strip()
    if len(q) < 3:  # trigram index needs 3+ chars; fall back to a scan
        return db.execute("SELECT id, title, year, substr(text, 1, 200) FROM acts WHERE title LIKE ? OR text LIKE ? LIMIT ?",
                          (f"%{q}%", f"%{q}%", limit)).fetchall()
    phrase = '"' + q.replace('"', '""') + '"'
    return db.execute("""SELECT a.id, a.title, a.year, snippet(acts_fts, 1, '[[', ']]', ' … ', 64)
                         FROM acts_fts JOIN acts a ON a.id = acts_fts.rowid
                         WHERE acts_fts MATCH ? ORDER BY a.title LIKE ? DESC, bm25(acts_fts, 10.0, 1.0) LIMIT ?""",
                      (phrase, f"%{q}%", limit)).fetchall()


SEC = re.compile(r"^(\d+\s*\[\s*)?[\d০-৯]+[A-Za-z]{0,2}\s*[.।৷]")  # "12.", "4A.", "১৭।", "7 [4A."
PART = re.compile(r"(?i)^(part|chapter|schedule|ভাগ|অধ্যায়|পরিচ্ছেদ|তফসিল)")
OPEN_END = re.compile(r"[.;:,।৷\-–—)]$")


def render_act(text, q):
    """Plain stored text -> structured HTML (title block, parts, section headings, clauses, footnotes)."""
    def fmt(s):
        s = html.escape(s)
        s = re.sub(r"(?<![\w.])(\d{1,3}) (?=\[)", r"<sup><a href='#fn\1'>\1</a></sup>", s)  # "4 [text]" amendment markers
        if q.strip():  # highlight outside of tags only
            s = re.sub(re.escape(html.escape(q.strip())) + r"(?![^<]*>)", lambda x: f"<mark>{x[0]}</mark>", s, flags=re.I)
        return s

    paras = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()][1:]  # [0] is the title, shown as <h1>
    date = next((i for i, p in enumerate(paras[:8]) if re.fullmatch(r"\[.*\]", p)), -1)
    front, paras = paras[:date + 1], paras[date + 1:]
    out = []
    if front:
        meta = re.sub(r"\(\s+", "(", re.sub(r"\s+\)", ")", " ".join(front)))
        out.append(f"<p class=meta>{fmt(meta)}</p>")
    toc, prev = [], ""
    for i, p in enumerate(paras):
        nxt = paras[i + 1] if i + 1 < len(paras) else ""
        short = "\n" not in p and len(p) <= 160 and not OPEN_END.search(p)
        lines = p.splitlines()
        if len(p) <= 40 and PART.match(p):
            toc.append((len(out), p))
            out.append(f"<h2 class=part id=p{len(out)}>{fmt(p)}</h2>")
        elif short and prev == "part":
            out.append(f"<p class=part-name>{fmt(p)}</p>")
            toc[-1] = (toc[-1][0], f"{toc[-1][1]} — {p}")
        elif short and SEC.match(nxt):
            out.append(f"<h3>{fmt(p)}</h3>")
        elif len(lines) > 1 and lines[0].strip().isdigit():  # footnote list: "n" line then its text
            out.append("<section class=fn><h2>Footnotes</h2>" + "".join(
                f"<p id=fn{n.strip()}><b>{n.strip()}</b> {fmt(t)}</p>" for n, t in zip(lines[::2], lines[1::2])) + "</section>")
        elif p.startswith(("Copyright", "Ministry of Law")):
            out.append(f"<p class=copy>{fmt(p)}</p>")
        elif p.startswith("("):
            out.append(f"<p class=sub>{fmt(p)}</p>")
        elif p in ("Illustrations", "Illustration", "Explanation", "দৃষ্টান্ত", "ব্যাখ্যা"):
            out.append(f"<p class=label>{fmt(p)}</p>")
        else:
            out.append(f"<p>{fmt(p)}</p>")
        prev = "part" if out[-1].startswith("<h2 class=part") else ""
    if len(toc) > 1:
        out.insert(1 if front else 0, "<details class=toc><summary>Contents</summary>" + "".join(
            f"<a href='#p{k}'>{html.escape(t)}</a>" for k, t in toc) + "</details>")
    return "".join(out)


PAGE = """<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Bangladesh Laws (offline)</title>
<style>
:root{{--bg:#f6f7f9;--card:#fff;--fg:#1d2330;--muted:#667085;--line:#e4e7ec;--accent:#006a4e;--accent-soft:#e6f2ee;--mark:#fde68a}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0f1318;--card:#171c23;--fg:#e6e8eb;--muted:#98a2b3;--line:#2a313b;
  --accent:#4cc39a;--accent-soft:#16302a;--mark:#7a5d00}}}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--fg);
  font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans Bengali","Kohinoor Bangla",Roboto,sans-serif}}
a{{color:var(--accent);text-decoration:none}} a:hover{{text-decoration:underline}}
header{{position:sticky;top:0;z-index:1;background:var(--card);border-bottom:1px solid var(--line)}}
.bar{{max-width:960px;margin:auto;padding:12px 16px;display:flex;gap:16px;align-items:center;flex-wrap:wrap}}
.brand{{font-weight:700;font-size:18px;color:var(--fg);white-space:nowrap}} .brand span{{color:var(--accent)}}
form{{flex:1;display:flex;min-width:240px}}
input{{flex:1;min-width:0;font:inherit;padding:10px 14px;border:1px solid var(--line);border-right:0;border-radius:8px 0 0 8px;
  background:var(--bg);color:var(--fg)}}
input:focus{{outline:2px solid var(--accent);outline-offset:-1px}}
button{{font:inherit;padding:10px 18px;border:0;border-radius:0 8px 8px 0;background:var(--accent);color:#fff;cursor:pointer}}
main{{max-width:960px;margin:auto;padding:20px 16px 60px}}
.m{{color:var(--muted);font-size:14px}}
mark{{background:var(--mark);color:inherit;border-radius:3px;padding:0 2px}}
.tabs{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:16px}}
.tabs a,.tabs b{{padding:6px 14px;border-radius:999px;font-size:14px;border:1px solid var(--line);background:var(--card)}}
.tabs b{{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}}
.tabs .m{{margin-right:auto}}
.list{{background:var(--card);border:1px solid var(--line);border-radius:10px;overflow:hidden}}
.list a{{display:flex;justify-content:space-between;gap:16px;padding:10px 16px;border-top:1px solid var(--line);color:var(--fg)}}
.list a:first-child{{border-top:0}} .list a:hover{{background:var(--accent-soft);text-decoration:none}}
.list .m{{white-space:nowrap}}
.r{{display:block;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin:10px 0;color:var(--fg)}}
.r:hover{{border-color:var(--accent);text-decoration:none}} .r b{{color:var(--accent);font-weight:600}}
.r div{{margin-top:4px}}
html{{scroll-padding-top:80px}}
.jump{{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 8px}}
.jump a{{padding:3px 10px;border-radius:6px;font-size:14px;background:var(--card);border:1px solid var(--line)}}
.jump a:hover{{background:var(--accent-soft);text-decoration:none}}
h2.group{{font-size:20px;margin:28px 0 8px;padding-left:10px;border-left:4px solid var(--accent)}}
h2.group .m{{font-weight:400;margin-left:6px}}
.act{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:32px clamp(16px,5vw,64px);
  font-size:17px;line-height:1.75}}
.act>*{{max-width:75ch;margin-left:auto;margin-right:auto}}
.act h1{{font-size:26px;line-height:1.3;text-align:center;margin:0 auto 6px}}
.act .src{{text-align:center;margin-top:0}}
.act .meta{{text-align:center;color:var(--muted);font-size:15px;padding-bottom:20px;border-bottom:1px solid var(--line)}}
.act p{{margin:0 auto 12px}}
.act h2.part{{text-align:center;font-size:15px;letter-spacing:.12em;text-transform:uppercase;color:var(--accent);
  margin:48px auto 0;padding-top:24px;border-top:2px solid var(--accent-soft)}}
.act .part-name{{text-align:center;font-weight:700;font-size:19px;margin-bottom:24px}}
.act h3{{font-size:17px;margin:28px auto 6px;color:var(--fg)}}
.act .sub{{padding-left:2em}}
.act .label{{font-style:italic;color:var(--muted);margin-top:16px}}
.act sup a{{font-size:11px;padding:0 2px;border-radius:3px;background:var(--accent-soft)}}
.toc{{margin:20px auto;border:1px solid var(--line);border-radius:8px;padding:10px 16px;font-size:15px}}
.toc summary{{cursor:pointer;font-weight:600}}
.toc a{{display:block;padding:3px 0}}
.fn{{margin-top:48px;padding-top:16px;border-top:1px solid var(--line);font-size:14px;line-height:1.55;color:var(--muted)}}
.fn h2{{font-size:16px;color:var(--fg)}}
.fn p{{margin-bottom:8px}} .fn p:target{{background:var(--mark);color:var(--fg);border-radius:4px}}
.fn b{{color:var(--accent);margin-right:6px}}
.act .copy{{text-align:center;font-size:13px;color:var(--muted);margin-bottom:0}}
</style>
<header><div class=bar><a class=brand href=/>Bangladesh <span>Laws</span></a>
<form action=/><input name=q value="{q}" placeholder="Search title or text (English / বাংলা)" aria-label=Search> <button>Search</button></form></div></header>
<main>{body}</main>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(url.query)
        q = qs.get("q", [""])[0]
        db = connect()
        if m := re.fullmatch(r"/act/(\d+)", url.path):
            row = db.execute("SELECT title, year, text FROM acts WHERE id=?", (int(m[1]),)).fetchone()
            if not row:
                return self.send_error(404)
            body = (f"<p class=m><a href=/>← All laws</a></p><article class=act><h1>{html.escape(row[0])}</h1>"
                    f"<p class='m src'>{row[1]} · <a href='{SITE}/act-details-{m[1]}.html'>View on bdlaws.minlaw.gov.bd ↗</a></p>"
                    f"{render_act(row[2], q)}</article>")
        elif q:
            rows = search(db, q)
            body = f"<p class=m>{len(rows)} result(s){' (showing first 50)' if len(rows) == 50 else ''}</p>" + "".join(
                f"<a class=r href='/act/{i}?q={urllib.parse.quote(q)}'><b>{html.escape(t)}</b> <span class=m>{y}</span>"
                f"<div class=m>{html.escape(s).replace('[[', '<mark>').replace(']]', '</mark>')}</div></a>"
                for i, t, y, s in rows)
        else:
            alpha = qs.get("sort", [""])[0] == "alpha"
            rows = db.execute("SELECT id, title, year, fetched FROM acts").fetchall()
            bn = str.maketrans("০১২৩৪৫৬৭৮৯", "0123456789")  # Bangla acts store years in Bangla digits
            if alpha:  # ignore leading "The", "[***]", "*", quotes, as the site's own alphabetical index does
                key = lambda r: re.sub(r"(?i)^[\W_]*(the\b)?[\W_]*", "", r[1]).casefold()
                rows.sort(key=key)
                group = lambda r: (key(r)[:1] or "#").upper()
                jump = lambda g: g
            else:
                year = lambda r: int(y) if (y := r[2].translate(bn)).isdigit() else 9999
                rows.sort(key=lambda r: (year(r), r[0]))
                group = lambda r: str(year(r)) if year(r) != 9999 else "Undated"
                jump = lambda g: g[:3] + "0s" if g.isdigit() else g  # jump bar by decade
            groups = {}
            for r in rows:
                groups.setdefault(group(r), []).append(r)
            jumps = {}
            for g in groups:
                jumps.setdefault(jump(g), g)
            body = (f"<nav class=tabs><span class=m>{len(rows)} acts · updated {max((r[3] for r in rows), default='never')} UTC</span>"
                    + ("<a href=/>Chronological</a><b>Alphabetical</b>" if alpha else
                       "<b>Chronological</b><a href='/?sort=alpha'>Alphabetical</a>") + "</nav>"
                    + "<nav class=jump>" + "".join(f"<a href='#g-{urllib.parse.quote(g)}'>{html.escape(j)}</a>"
                                                   for j, g in jumps.items()) + "</nav>"
                    + "".join(f"<h2 class=group id='g-{urllib.parse.quote(g)}'>{html.escape(g)} <span class=m>{len(rs)}</span></h2>"
                              "<div class=list>" + "".join(f"<a href='/act/{i}'><span>{html.escape(t)}</span><span class=m>{y}</span></a>"
                                                           for i, t, y, _ in rs) + "</div>"
                              for g, rs in groups.items()))
        out = PAGE.format(q=html.escape(q), body=body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(out)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("update"); u.add_argument("--all", action="store_true"); u.add_argument("--workers", type=int, default=4)
    s = sub.add_parser("search"); s.add_argument("query")
    v = sub.add_parser("serve"); v.add_argument("--port", type=int, default=8000)
    sub.add_parser("split")
    a = p.parse_args()
    if a.cmd == "update":
        update(a.all, a.workers)
    elif a.cmd == "split":
        split_db()
    elif a.cmd == "search":
        for i, t, y, s in search(connect(), a.query):
            print(f"[{i}] {t} ({y})\n    {s.replace(chr(10), ' ')}\n")
    else:
        print(f"Open http://127.0.0.1:{a.port}")
        ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
