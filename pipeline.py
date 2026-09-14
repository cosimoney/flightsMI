#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Usage: pipeline.py PAGE_HTML  — rescans flights and rewrites DATA/RETURNS/PAIRS + date in the HTML.
import json, random, re, sys, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from primp import Client
from fast_flights import FlightQuery, Passengers, create_query
from fast_flights.parser import parse

DESTS_LIN = {"DUB":"Dublino","CAG":"Cagliari","CIY":"Comiso","FOG":"Foggia","OLB":"Olbia","TRS":"Trieste","FRA":"Francoforte","MUC":"Monaco di Baviera","CDG":"Parigi CDG","ORY":"Parigi Orly","VIE":"Vienna","LHR":"Londra Heathrow","LCY":"Londra City","LGW":"Londra Gatwick","BRU":"Bruxelles","AOI":"Ancona","AMS":"Amsterdam","BCN":"Barcellona","BER":"Berlino","CPH":"Copenaghen","EDI":"Edimburgo","LIS":"Lisbona","MAN":"Manchester","OPO":"Porto","TFS":"Tenerife Sud","BDS":"Brindisi","GVA":"Ginevra","LPA":"Gran Canaria","IBZ":"Ibiza","LUX":"Lussemburgo","PMI":"Palma di Maiorca","SPU":"Spalato","DUS":"Dusseldorf","STR":"Stoccarda","HEL":"Helsinki","MAD":"Madrid","AHO":"Alghero","BRI":"Bari","CTA":"Catania","SUF":"Lamezia Terme","NAP":"Napoli","PMO":"Palermo","REG":"Reggio Calabria","FCO":"Roma Fiumicino","MLA":"Malta","ARN":"Stoccolma"}
DESTS_MXP = {"BCN":"Barcellona","BIO":"Bilbao","MAD":"Madrid","AGP":"Malaga","PMI":"Palma di Maiorca","SVQ":"Siviglia","LIS":"Lisbona","OPO":"Porto","VLC":"Valencia","ALC":"Alicante","CDG":"Parigi CDG","ORY":"Parigi Orly","BVA":"Parigi Beauvais","BOD":"Bordeaux","TLS":"Tolosa","NTE":"Nantes","SXB":"Strasburgo","LGW":"Londra Gatwick","LTN":"Londra Luton","STN":"Londra Stansted","DUB":"Dublino","EDI":"Edimburgo","MAN":"Manchester","BRS":"Bristol","FRA":"Francoforte","MUC":"Monaco di Baviera","BER":"Berlino","DUS":"Dusseldorf","CGN":"Colonia","HAM":"Amburgo","NUE":"Norimberga","AMS":"Amsterdam","BRU":"Bruxelles","LUX":"Lussemburgo","ATH":"Atene","SKG":"Salonicco","PRG":"Praga","VIE":"Vienna","BUD":"Budapest","WAW":"Varsavia","KRK":"Cracovia","GDN":"Danzica","OTP":"Bucarest","BTS":"Bratislava","CLJ":"Cluj-Napoca","VNO":"Vilnius","RIX":"Riga","TLL":"Tallinn","SOF":"Sofia","BEG":"Belgrado","TIA":"Tirana","TGD":"Podgorica","PRN":"Pristina","OSL":"Oslo","ARN":"Stoccolma","CPH":"Copenaghen","HEL":"Helsinki","ZRH":"Zurigo","RAK":"Marrakech","RBA":"Rabat","TUN":"Tunisi","LCA":"Larnaca","KEF":"Reykjavik","MLA":"Malta","NAP":"Napoli","PMO":"Palermo","CTA":"Catania","CAG":"Cagliari","BRI":"Bari","BDS":"Brindisi","SUF":"Lamezia Terme","TPS":"Trapani"}
MESI_IT = ["gennaio","febbraio","marzo","aprile","maggio","giugno","luglio","agosto","settembre","ottobre","novembre","dicembre"]
PRICE_KEEP = 115
URL = "https://www.google.com/travel/flights"
SOCS = {"SOCS": "CAISHAgBEhJnd3NfMjAyNDA0MDktMF9SQzIaAml0IAEaBgiA_LyaBg"}
_tls = threading.local()

def client():
    if getattr(_tls, "c", None) is None:
        c = Client(impersonate="chrome_145", impersonate_os="macos", referer=True, cookie_store=True)
        c.set_cookies("https://www.google.com", SOCS)
        _tls.c = c
    return _tls.c

def fetch(q):
    last = None
    for a in range(2):
        try:
            res = client().get(URL, params=q.params())
            if res.status_code != 200:
                raise RuntimeError("HTTP %s" % res.status_code)
            return parse(res.text)
        except (TypeError, AttributeError):
            return None  # no flight data on page (route/date not served)
        except Exception as e:
            last = e
            time.sleep(1 + a * 2 + random.random())
    raise last

def qualifies(wd, t):
    h, m = t
    if wd == 3:
        return h >= 19
    return h < 8 or h > 18 or (h == 18 and m >= 30)

def pool(tasks, fn, label):
    out, lock, done = [], threading.Lock(), [0]
    def work(t):
        time.sleep(random.random() * 0.4)
        try:
            r = fn(t)
        except Exception:
            r = None
        with lock:
            done[0] += 1
            if done[0] % 200 == 0:
                print("%s %d/%d" % (label, done[0], len(tasks)), flush=True)
        return t, r
    with ThreadPoolExecutor(max_workers=8) as ex:
        return [f.result() for f in as_completed([ex.submit(work, t) for t in tasks])]

def main():
    page = sys.argv[1]
    today = date.today()
    thursdays = []
    d = date(2026, 10, 15)
    while d <= date(2027, 4, 29):
        if d > today:
            thursdays.append(d)
        d += timedelta(days=7)
    if not thursdays:
        print("ABORT: no future weekends left in the Oct 2026 - Apr 2027 window; disable this workflow.")
        sys.exit(1)

    scan_tasks = []
    for origin, dests in (("LIN", DESTS_LIN), ("MXP", DESTS_MXP)):
        for dest in dests:
            for thu in thursdays:
                fri, sun = thu + timedelta(days=1), thu + timedelta(days=3)
                scan_tasks.append((origin, dests[dest], dest, thu, sun))
                scan_tasks.append((origin, dests[dest], dest, fri, sun))
    random.shuffle(scan_tasks)

    def scan_one(t):
        origin, city, dest, dep, ret = t
        q = create_query(flights=[FlightQuery(date=dep.isoformat(), from_airport=origin, to_airport=dest, max_stops=0), FlightQuery(date=ret.isoformat(), from_airport=dest, to_airport=origin, max_stops=0)], trip="round-trip", passengers=Passengers(adults=1), currency="EUR", language="it")
        res = fetch(q)
        rows = []
        for fl in res or []:
            try:
                if not fl.flights or not fl.price or fl.price <= 0 or fl.price > PRICE_KEEP:
                    continue
                leg = fl.flights[0]
                t0 = leg.departure.time
                if not qualifies(dep.weekday(), t0):
                    continue
                rows.append({"origin": origin, "dest": dest, "city": city, "dep": dep.isoformat(), "ret": ret.isoformat(), "dep_time": "%02d:%02d" % t0, "arr_time": "%02d:%02d" % leg.arrival.time, "airline": ", ".join(fl.airlines), "price": fl.price})
            except Exception:
                continue
        return rows

    hits = []
    for _, r in pool(scan_tasks, scan_one, "scan"):
        hits.extend(r or [])
    hits.sort(key=lambda h: (h["price"], h["dep"]))
    print("scan done: %d hits" % len(hits), flush=True)
    if len(hits) < 100:
        print("ABORT: only %d hits — Google is probably blocking this IP. Not publishing." % len(hits))
        sys.exit(1)

    combos = sorted({(h["origin"], h["dest"], h["ret"]) for h in hits})
    def ret_one(c):
        origin, dest, ret = c
        q = create_query(flights=[FlightQuery(date=ret, from_airport=dest, to_airport=origin, max_stops=0)], trip="one-way", passengers=Passengers(adults=1), currency="EUR", language="it")
        res = fetch(q)
        fls = []
        for fl in res or []:
            try:
                if not fl.flights:
                    continue
                leg = fl.flights[0]
                fls.append({"dep_time": "%02d:%02d" % leg.departure.time, "arr_time": "%02d:%02d" % leg.arrival.time, "airline": ", ".join(fl.airlines)})
            except Exception:
                continue
        seen, out = set(), []
        for f in sorted(fls, key=lambda x: x["dep_time"]):
            k = (f["dep_time"], f["airline"])
            if k not in seen:
                seen.add(k)
                out.append(f)
        return out
    returns = {}
    for c, r in pool(combos, ret_one, "returns"):
        returns["%s|%s|%s" % c] = r or []
    print("returns done: %d combos" % len(returns), flush=True)

    pair_tasks = []
    seen_keys = set()
    for h in hits:
        rk = "%s|%s|%s|%s" % (h["origin"], h["dest"], h["dep"], h["dep_time"])
        rets = returns.get("%s|%s|%s" % (h["origin"], h["dest"], h["ret"])) or []
        dh = int(h["dep_time"][:2])
        for rh in sorted({int(f["dep_time"][:2]) for f in rets}):
            key = "%s|%d" % (rk, rh)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            pair_tasks.append((key, h["origin"], h["dest"], h["dep"], h["ret"], dh, rh))
    def pair_one(t):
        key, origin, dest, dep, ret, dh, rh = t
        q = create_query(flights=[FlightQuery(date=dep, from_airport=origin, to_airport=dest, max_stops=0, earliest_departure_hour=dh, latest_departure_hour=min(dh + 1, 23)), FlightQuery(date=ret, from_airport=dest, to_airport=origin, max_stops=0, earliest_departure_hour=rh, latest_departure_hour=min(rh + 1, 23))], trip="round-trip", passengers=Passengers(adults=1), currency="EUR", language="it")
        res = fetch(q)
        ps = [fl.price for fl in res or [] if fl.flights and fl.price and fl.price > 0]
        return min(ps) if ps else None
    pairs = {}
    n_priced = 0
    for t, v in pool(pair_tasks, pair_one, "pairs"):
        rk, rh = t[0].rsplit("|", 1)
        pairs.setdefault(rk, {})[rh] = v
        if isinstance(v, (int, float)):
            n_priced += 1
    print("pairs done: %d priced" % n_priced, flush=True)

    html = open(page, encoding="utf-8").read()
    reps = [("const DATA = ", json.dumps(hits, ensure_ascii=False, separators=(",", ":"))), ("const RETURNS = ", json.dumps(returns, ensure_ascii=False, separators=(",", ":"))), ("const PAIRS = ", json.dumps(pairs, ensure_ascii=False, separators=(",", ":")))]
    lines = html.split("\n")
    found = 0
    for i, ln in enumerate(lines):
        for prefix, blob in reps:
            if ln.strip().startswith(prefix):
                lines[i] = prefix + blob + ";"
                found += 1
    if found != 3:
        print("ABORT: expected 3 const lines, found %d — page layout changed, not publishing." % found)
        sys.exit(1)
    html = "\n".join(lines)
    html = re.sub(r"Prezzi Google Flights al \d+ \w+ 202\d", "Prezzi Google Flights al %d %s %d" % (today.day, MESI_IT[today.month - 1], today.year), html)
    open(page, "w", encoding="utf-8").write(html)
    inb = sum(1 for h in hits if h["price"] <= 100)
    print("OK-PUBLISH hits=%d in_budget=%d priced_pairs=%d" % (len(hits), inb, n_priced))

if __name__ == "__main__":
    main()
