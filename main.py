"""Acme Appliances ledger Q&A service.

POST {"question": "..."}  ->  {"answer": ...}

Data rules (from the API's own notes):
  * an order id can appear more than once -> keep the row with the latest updated_at (compared as instants)
  * only status 'paid' counts as revenue
  * business dates use Asia/Kolkata, so created_at is converted to IST before bucketing
  * money is converted to USD with rates.json (usd_per_unit)
"""
import json
import os
import re
import threading
import time
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from fastapi import FastAPI, Request
from starlette.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

BASE = Path(__file__).parent
IST = timezone(timedelta(hours=5, minutes=30))
DEFAULT_YEAR = 2026
CENT = Decimal("0.01")

# --------------------------------------------------------------------------- data
STATE = {"orders": [], "rates": {}, "loaded_at": 0.0}


def _fetch(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return r.read().decode("utf-8")


def _parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def build_orders(rows, rates):
    latest = {}
    for r in rows:
        cur = latest.get(r["id"])
        if cur is None or _parse_ts(r["updated_at"]) > _parse_ts(cur["updated_at"]):
            latest[r["id"]] = r
    out = []
    for r in latest.values():
        rate = Decimal(str(rates[r["currency"]]))
        out.append({
            "id": r["id"],
            "customer": r["customer"],
            "region": r["region"],
            "product": r["product"],
            "qty": int(r["qty"]),
            "currency": r["currency"],
            "status": r["status"],
            "usd": Decimal(str(r["amount"])) * rate,
            "day": _parse_ts(r["created_at"]).astimezone(IST).date(),
        })
    return out


def load_data():
    export_url, rates_url = os.environ.get("EXPORT_URL"), os.environ.get("RATES_URL")
    rows = rates = None
    if export_url and rates_url:
        try:
            rows = [json.loads(l) for l in _fetch(export_url).splitlines() if l.strip()]
            rates = json.loads(_fetch(rates_url))["usd_per_unit"]
            if len(rows) < 100:
                raise ValueError("export too small")
        except Exception as e:  # fall back to the bundled snapshot
            print("live load failed, using bundled data:", e)
            rows = rates = None
    if rows is None:
        d = BASE / "data" if (BASE / "data").exists() else BASE  # works with or without a data/ folder
        rows = [json.loads(l) for l in (d / "export.ndjson").read_text().splitlines() if l.strip()]
        rates = json.loads((d / "rates.json").read_text())["usd_per_unit"]
    STATE["orders"] = build_orders(rows, rates)
    STATE["rates"] = {k: Decimal(str(v)) for k, v in rates.items()}
    STATE["loaded_at"] = time.time()


def refresher():
    while True:
        time.sleep(600)
        if os.environ.get("EXPORT_URL"):
            try:
                load_data()
            except Exception as e:
                print("refresh failed:", e)


load_data()
threading.Thread(target=refresher, daemon=True).start()

REGIONS = sorted({o["region"] for o in STATE["orders"]})
PRODUCTS = sorted({o["product"] for o in STATE["orders"]})
CUSTOMERS = {o["customer"] for o in STATE["orders"]}

# --------------------------------------------------------------------------- engine
def money(x, cur="USD"):
    if cur != "USD":
        x = x / STATE["rates"][cur]
    return float(Decimal(x).quantize(CENT, rounding=ROUND_HALF_UP))


def run(spec):
    status = spec.get("status", "paid")
    rows = []
    for o in STATE["orders"]:
        if status != "all" and o["status"] != status:
            continue
        if spec.get("regions") and o["region"] not in spec["regions"]:
            continue
        if spec.get("products") and o["product"] not in spec["products"]:
            continue
        if spec.get("customers") and o["customer"] not in spec["customers"]:
            continue
        if spec.get("currencies") and o["currency"] not in spec["currencies"]:
            continue
        iv = spec.get("intervals")
        if iv and not any(s <= o["day"] <= e for s, e in iv):
            continue
        rows.append(o)

    out_cur = spec.get("out_currency", "USD")
    measure = spec.get("measure", "amount")

    def value(rs):
        if measure == "amount":
            return money(sum((o["usd"] for o in rs), Decimal(0)), out_cur)
        if measure == "count":
            return len(rs)
        if measure == "units":
            return sum(o["qty"] for o in rs)
        if measure == "aov":
            return money(sum((o["usd"] for o in rs), Decimal(0)) / len(rs), out_cur) if rs else 0
        if measure == "avg_units":
            return round(sum(o["qty"] for o in rs) / len(rs), 2) if rs else 0
        if measure == "per_customer":
            n = len({o["customer"] for o in rs})
            return money(sum((o["usd"] for o in rs), Decimal(0)) / n, out_cur) if n else 0
        if measure == "distinct_customers":
            return len({o["customer"] for o in rs})
        if measure == "distinct_products":
            return len({o["product"] for o in rs})
        raise ValueError("bad measure " + measure)

    gb = spec.get("group_by")
    if not gb:
        return value(rows)
    groups = defaultdict(list)
    for o in rows:
        key = o["day"].strftime("%Y-%m") if gb == "month" else o[gb]
        groups[key].append(o)
    vals = {k: value(v) for k, v in groups.items()}
    ext = spec.get("extremum")
    if ext:
        sign = -1 if ext == "max" else 1
        ranked = sorted(vals.items(), key=lambda kv: (sign * kv[1], kv[0]))
        n = spec.get("top_n")
        if n:
            return [k for k, _ in ranked[:n]]
        return ranked[0][0] if ranked else None
    return dict(sorted(vals.items()))


# --------------------------------------------------------------------------- parser
MONTH_RE = (r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|"
            r"sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)")
MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8,
          "sep": 9, "oct": 10, "nov": 11, "dec": 12}
ORD = {"first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4}
NUMW = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "ten": 10}


def mnum(word):
    return MONTHS[word.lower()[:3]]


def month_span(y, m):
    start = date(y, m, 1)
    end = (date(y + (m == 12), m % 12 + 1, 1)) - timedelta(days=1)
    return start, end


def add_months(y, m, k):
    t = y * 12 + (m - 1) + k
    return t // 12, t % 12 + 1


def parse_time(q):
    """Return (intervals, remaining_text). Intervals are inclusive IST date ranges."""
    text = q
    years = sorted({int(y) for y in re.findall(r"\b(20\d{2})\b", text)})
    gy = years[0] if len(years) == 1 else None
    ivs = []

    # 1) ISO dates
    isos = re.findall(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if isos:
        ds = [date(int(a), int(b), int(c)) for a, b, c in isos]
        ivs.append((ds[0], ds[-1]) if len(ds) > 1 else (ds[0], ds[0]))
        text = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", " ", text)
        return ivs, text

    # 2) specific days: "March 5, 2026" / "5th of March 2026"
    days = []
    for m in re.finditer(MONTH_RE + r"\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?!\d)(?:,?\s*(20\d{2}))?", text, re.I):
        if m.group(1).lower() == "may" and m.group(1)[0] == "m":
            continue
        days.append((m.start(), m.end(), int(m.group(3) or gy or DEFAULT_YEAR), mnum(m.group(1)), int(m.group(2))))
    for m in re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?" + MONTH_RE + r"\b(?:,?\s*(20\d{2}))?", text, re.I):
        if m.group(2).lower() == "may" and m.group(2)[0] == "m":
            continue
        days.append((m.start(), m.end(), int(m.group(3) or gy or DEFAULT_YEAR), mnum(m.group(2)), int(m.group(1))))
    if days:
        days.sort()
        ds = [date(y, mo, d) for _, _, y, mo, d in days]
        ivs.append((ds[0], ds[-1]))
        for s, e, *_ in sorted(days, reverse=True):
            text = text[:s] + " " + text[e:]
        return ivs, text

    # 3) quarters / halves
    qs = []
    for m in re.finditer(r"\bq([1-4])\b(?:\s*(?:of\s+|,)?\s*(20\d{2}))?", text, re.I):
        qs.append((m.start(), m.end(), int(m.group(1)), int(m.group(2) or gy or DEFAULT_YEAR)))
    for m in re.finditer(r"\b(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter\b(?:\s*(?:of|in)?\s*(20\d{2}))?", text, re.I):
        qs.append((m.start(), m.end(), ORD[m.group(1).lower()], int(m.group(2) or gy or DEFAULT_YEAR)))
    for m in re.finditer(r"\bh([12])\b(?:\s*(20\d{2}))?|\b(first|second|1st|2nd)\s+half\b(?:\s*(?:of|in)?\s*(20\d{2}))?", text, re.I):
        h = int(m.group(1)) if m.group(1) else ORD[m.group(3).lower()]
        y = int(m.group(2) or m.group(4) or gy or DEFAULT_YEAR)
        a = date(y, 1 if h == 1 else 7, 1)
        b = month_span(y, 6 if h == 1 else 12)[1]
        ivs.append((a, b))
        qs.append((m.start(), m.end(), None, None))
    for _, _, qn, y in [x for x in qs if x[2]]:
        a = month_span(y, 3 * (qn - 1) + 1)[0]
        b = month_span(y, 3 * (qn - 1) + 3)[1]
        ivs.append((a, b))
    if qs:
        for s, e, *_ in sorted(qs, reverse=True):
            text = text[:s] + " " + text[e:]
        return ivs, text

    # 4) month names (optionally with years), also 2026-03 and 03/2026
    ms = []
    for m in re.finditer(r"\b(20\d{2})[-/](0?[1-9]|1[0-2])\b", text):
        ms.append((m.start(), m.end(), int(m.group(1)), int(m.group(2))))
    for m in re.finditer(r"\b(0?[1-9]|1[0-2])/(20\d{2})\b", text):
        ms.append((m.start(), m.end(), int(m.group(2)), int(m.group(1))))
    for m in re.finditer(r"\b" + MONTH_RE + r"\b\.?(?:\s*,?\s*(?:of\s+)?(20\d{2}))?", text, re.I):
        w = m.group(1)
        if w.lower() == "may" and w[0] == "m" and not m.group(2) and not re.match(r"\s*\d", text[m.end():]):
            continue  # the verb "may"
        ms.append((m.start(), m.end(), int(m.group(2) or gy or DEFAULT_YEAR), mnum(w)))
    if ms:
        ms.sort()
        rng = re.search(r"\b(from|between|through|thru|until|till|to)\b|\s-\s|–", text, re.I)
        if rng and len(ms) >= 2:
            (y1, m1), (y2, m2) = (ms[0][2], ms[0][3]), (ms[-1][2], ms[-1][3])
            y, m = y1, m1
            while (y, m) <= (y2, m2):
                ivs.append(month_span(y, m))
                y, m = add_months(y, m, 1)
        else:
            ivs.extend(month_span(y, m) for _, _, y, m in ms)
        for s, e, *_ in sorted(ms, reverse=True):
            text = text[:s] + " " + text[e:]
        return ivs, text

    # 5) bare year
    if years:
        ivs.extend((date(y, 1, 1), date(y, 12, 31)) for y in years)
        text = re.sub(r"\b20\d{2}\b", " ", text)
    return ivs, text


def parse(question):
    q0 = question.strip()
    ivs, rest = parse_time(q0)
    t = " " + re.sub(r"[_\-]", " ", rest.lower()) + " "
    t = re.sub(r"[?!,;:()\"'’]", " ", t)
    t = re.sub(r"\s+", " ", t)
    spec = {}
    if ivs:
        spec["intervals"] = ivs

    # ---- filters
    regs = [r for r in REGIONS if re.search(r"\b" + r.lower() + r"\b", t)]
    if regs:
        spec["regions"] = regs
    prods = [p for p in PRODUCTS if re.search(r"\b" + p.lower().replace(" ", r"\s?") + r"s?\b", t)]
    if prods:
        spec["products"] = prods
    custs = [c.upper() for c in re.findall(r"\bc\d{3,5}\b", t) if c.upper() in CUSTOMERS]
    custs += ["C" + n for n in re.findall(r"\bcustomer\s*(?:id\s*|number\s*|no\.?\s*)?#?(\d{4})\b", t) if "C" + n in CUSTOMERS]
    if custs:
        spec["customers"] = sorted(set(custs))

    # ---- currency filter vs. output currency
    curmap = {"usd": "USD", "eur": "EUR", "inr": "INR", "euro": "EUR", "euros": "EUR", "rupee": "INR", "rupees": "INR"}
    cw = r"(usd|eur|inr|euros?|rupees?)"
    cur_filter = set()
    for m in re.finditer(r"\b" + cw + r" (?:denominated |currency )?(?:orders|transactions|purchases|sales|payments)\b", t):
        cur_filter.add(curmap[m.group(1)])
    for m in re.finditer(r"\b(?:orders|transactions|purchases|payments|sales)\s+(?:that were |which were )?(?:paid |made |placed |billed |denominated )?(?:in|using|with) " + cw + r"\b", t):
        cur_filter.add(curmap[m.group(1)])
    for m in re.finditer(r"\b(?:paid|billed|denominated|priced) in " + cw + r"\b", t):
        cur_filter.add(curmap[m.group(1)])
    if cur_filter:
        spec["currencies"] = sorted(cur_filter)
        t = re.sub(r"\b" + cw + r" (denominated |currency )?(orders|transactions|purchases|sales|payments)\b", " ", t)
    m = re.search(r"\b(?:in|into|as) (?:us )?(usd|eur|inr|dollars?|euros?|rupees?)\b", t)
    if m and not cur_filter:
        w = m.group(1)
        spec["out_currency"] = {"dollar": "USD", "dollars": "USD"}.get(w) or curmap.get(w, "USD")

    # ---- status
    refund = bool(re.search(r"refund", t))
    void = bool(re.search(r"\bvoid(?:ed)?\b|cancel", t))
    spec["status"] = "refunded" if refund else "void" if void else "paid"
    if re.search(r"regardless of status|all orders|any status|every order", t):
        spec["status"] = "all"

    # ---- measure
    money_words = r"(?:us |u\.s\. )?(?:dollars?|usd|euros?|eur|rupees?|inr|money|bucks)"
    how_many = bool(re.search(r"how many|number of|count of|\bcount\b|total number", t))
    if re.search(r"how (?:many|much) " + money_words, t):
        how_many = False  # "how many US dollars ..." asks for an amount, not a count
    avg_word = re.search(r"\b(average|avg|mean|typical|on average)\b", t)
    units_word = re.search(r"\b(units?|quantit\w*|items?|pieces|qty)\b", t)
    per_cust = re.search(r"per customer|for each customer|per buyer|per client", t) and not re.search(r"which customer|top customer|what customer", t)
    if avg_word and units_word:
        spec["measure"] = "avg_units"
    elif per_cust:
        spec["measure"] = "per_customer"
    elif avg_word or re.search(r"\baov\b|average order|avg order|mean order|(?:order|sale|purchase|transaction)s? (?:is |are )?worth|order value|order size|per order|each order|ticket size|basket", t):
        spec["measure"] = "aov"
    elif re.search(r"(?:unique|distinct|different) customers|(?:how many|number of|count of) (?:\w+ )?customers", t) and not re.search(r"which customer|what customer|top customer", t):
        spec["measure"] = "distinct_customers"
    elif re.search(r"(?:unique|distinct|different) products|(?:how many|number of) (?:\w+ )?products", t) and not re.search(r"which product|what product", t):
        spec["measure"] = "distinct_products"
    elif re.search(r"\bunits?\b|\bquantit|\bitems?\b|\bpieces\b|\bqty\b", t):
        spec["measure"] = "units"
    elif how_many and re.search(r"orders?|refunds?|transactions?|sales|purchases|cancellations", t):
        spec["measure"] = "count"
    elif re.search(r"refund rate|percent|percentage|ratio|proportion|share of", t) and refund:
        spec["measure"] = "refund_rate"
    else:
        spec["measure"] = "amount"
    if spec["measure"] == "refund_rate":
        raise ValueError("refund rate is not supported by the rules parser")

    # ---- grouping / ranking
    ENT = r"(product|customer|region|month)s?"
    hi = re.search(r"\b(highest|most|top|best|largest|biggest|maximum|max|greatest|leading|higher|more)\b", t)
    lo = re.search(r"\b(lowest|least|fewest|smallest|minimum|min|worst|bottom|lower|fewer)\b", t)
    ent = None
    rank_m = (re.search(r"\b(?:which|what)\s+(?:\w+\s+){0,3}?" + ENT + r"\b", t)
              or re.search(r"\b(?:top|bottom|best|worst)\s+(?:\d+|one|two|three|four|five|six|seven|eight|ten)\s+" + ENT + r"\b", t)
              or re.search(r"\b(?:best|top)\s+selling\s+" + ENT + r"\b", t)
              or re.search(r"\b" + ENT + r"\s+(?:with|had|has|having|that|who|generating|generated|brought|made)\b", t))
    by_m = re.search(r"\b(?:by|per|each|every|for each|breakdown by|split by)\s+(?:\w+\s+)?" + ENT + r"\b", t)
    if rank_m and (hi or lo):
        ent = rank_m.group(1)
        spec["group_by"] = ent
        spec["extremum"] = "min" if (lo and not hi) else "max"
        m = re.search(r"\b(?:top|bottom|best|worst)\s+(\d+|one|two|three|four|five|six|seven|eight|ten)\b", t)
        if m:
            spec["top_n"] = int(m.group(1)) if m.group(1).isdigit() else NUMW[m.group(1)]
            if re.search(r"\b(bottom|worst)\b", t):
                spec["extremum"] = "min"
        if re.search(r"best selling|top selling|most sold|most popular", t) and spec["measure"] == "amount":
            spec["measure"] = "units"
    elif by_m and not (spec["measure"] == "per_customer" and by_m.group(1) == "customer"):
        ent = by_m.group(1)
        spec["group_by"] = ent
    if ent:  # the thing being ranked/grouped must not also act as a filter on itself
        spec.pop({"product": "products", "region": "regions", "customer": "customers"}.get(ent, "_"), None)

    # ---- sanity: need at least something recognisable
    known = re.search(r"revenue|sales|refund|order|income|earn|sold|spen[dt]|units?|quantit|customers?|products?|void|cancel|amount|worth|gmv|turnover|paid|bought|purchas|transaction|items?|money|bring|brought|made|make|generat|gross|total|collect|bring in|took in|rake|cash|dollars|usd", t)
    if not known and not any(k in spec for k in ("regions", "products", "customers", "intervals")):
        raise ValueError("could not understand the question")
    return spec


# --------------------------------------------------------------------------- optional LLM fallback
LLM_PROMPT = """You convert questions about an order ledger into a JSON query spec. Reply with JSON only.
Schema (omit keys you do not need):
{"status": "paid"|"refunded"|"void"|"all",   // default "paid"; revenue = paid orders; refunds = refunded orders
 "measure": "amount"|"count"|"units"|"aov"|"distinct_customers"|"distinct_products",
 "regions": [one or more of %s], "products": [one or more of %s], "customers": ["C1234", ...],
 "currencies": ["USD"|"EUR"|"INR"],      // only to FILTER orders by their own currency
 "out_currency": "USD"|"EUR"|"INR",       // currency to express money in (default USD)
 "intervals": [["YYYY-MM-DD","YYYY-MM-DD"], ...],   // inclusive date ranges, business dates; data covers 2026-01-01..2026-06-30; assume 2026 if no year
 "group_by": "product"|"customer"|"region"|"month", "extremum": "max"|"min", "top_n": integer}
Use group_by+extremum for "which X had the most/least ..."; use group_by alone for "by X" breakdowns.
Ranking by units for "best selling". Return only the JSON object."""


def llm_spec(question):
    key = os.environ.get("LLM_API_KEY")
    if not key:
        raise RuntimeError("no LLM key")
    base = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    model = os.environ.get("LLM_MODEL", "gpt-4o-mini")
    body = json.dumps({
        "model": model, "temperature": 0, "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": LLM_PROMPT % (REGIONS, PRODUCTS)},
                     {"role": "user", "content": question}]}).encode()
    req = urllib.request.Request(base + "/chat/completions", data=body,
                                 headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=9) as r:
        content = json.loads(r.read())["choices"][0]["message"]["content"]
    spec = json.loads(content)
    if "intervals" in spec:
        spec["intervals"] = [(date.fromisoformat(a), date.fromisoformat(b)) for a, b in spec["intervals"]]
    return spec


def answer(question):
    try:
        return run(parse(question))
    except Exception as first_error:
        if os.environ.get("LLM_API_KEY"):
            return run(llm_spec(question))
        raise first_error


# --------------------------------------------------------------------------- web
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.post("/")
@app.post("/ask")
@app.post("/answer")
async def ask(request: Request):
    try:
        body = await request.json()
        question = body.get("question") or body.get("q") or ""
    except Exception:
        return {"answer": None, "error": "send JSON like {\"question\": \"...\"}"}
    try:
        return {"answer": await run_in_threadpool(answer, question)}
    except Exception as e:
        return {"answer": None, "error": str(e)}


@app.get("/")
async def health():
    return {"status": "ok", "orders": len(STATE["orders"])}
