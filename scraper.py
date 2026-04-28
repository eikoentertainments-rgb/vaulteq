#!/usr/bin/env python3
"""
Melion Data Collector v2
Fonti: Reddit (pubblico), Google Play, App Store (iTunes API), Trustpilot (via API non ufficiale)
"""

import requests, json, time, csv, random, sys
from datetime import datetime
from bs4 import BeautifulSoup

OUTPUT = "/Users/gioooo2/vaulteq/vinted_complaints.csv"
records = []

def classify(text):
    t = text.lower()
    issues = []
    if any(w in t for w in ["vuoto","empty","vide","vacío","leer","nothing inside","box empty","boite vide","scatola vuota"]):
        issues.append("pacco_vuoto")
    if any(w in t for w in ["falso","fake","counterfeit","faux","replica","not authentic","contraffatt","imitazione","knock"]):
        issues.append("oggetto_falso")
    if any(w in t for w in ["diverso","different","wrong item","not as described","différent","pas conforme","non corrisponde","not what"]):
        issues.append("oggetto_diverso")
    if any(w in t for w in ["rott","dañado","damaged","broken","cassé","endommagé","deteriorat","ammacc","cracked","smashed"]):
        issues.append("oggetto_danneggiato")
    if any(w in t for w in ["non arrivat","not received","not arrived","never arrived","pas reçu","never came","lost","perso","smarrit","never delivered"]):
        issues.append("pacco_non_arrivato")
    if any(w in t for w in ["controversia","dispute","litige","rimborso negat","refund refused","refused refund","no refund","lost dispute","won't refund"]):
        issues.append("controversia_persa")
    if any(w in t for w in ["truffa","scam","frode","fraud","arnaque","escroquerie","estafa","betrug","ripped off","rip off"]):
        issues.append("truffa_generica")
    return ", ".join(issues) if issues else "altro"

def add(source, username, date, country, rating, issue, text, url=""):
    records.append({
        "source": source, "username": username, "date": date,
        "country": country, "rating": rating, "issue_type": issue,
        "text": text[:600].replace("\n", " "), "url": url
    })

# ──────────────────────────────────────
# REDDIT — funziona, nessun blocco
# ──────────────────────────────────────
def reddit():
    print("\n[1] Reddit...", flush=True)
    sess = requests.Session()
    sess.headers["User-Agent"] = "MelionResearch/1.0"
    total = 0

    subs = ["vinted","VintedUK","vinted_fr","VintedItalia","Vinted","frugalfemalefashion",
            "declutter","ThriftStoreHauls","malefrugalfashion","RepLadies","FrugalMaleFashion"]

    for sub in subs:
        for sort in ["new","hot","top"]:
            after = None
            pages = 0
            while pages < 15:
                try:
                    params = {"limit":100,"t":"all"}
                    if after: params["after"] = after
                    r = sess.get(f"https://www.reddit.com/r/{sub}/{sort}.json", params=params, timeout=15)
                    if r.status_code == 429: time.sleep(5); continue
                    if r.status_code != 200: break
                    data = r.json().get("data",{})
                    posts = data.get("children",[])
                    if not posts: break
                    for p in posts:
                        d = p["data"]
                        txt = f"{d.get('title','')} {d.get('selftext','')}".strip()
                        if len(txt) < 20: continue
                        issue = classify(txt)
                        if issue == "altro": continue
                        date = datetime.utcfromtimestamp(d.get("created_utc",0)).strftime("%Y-%m-%d")
                        add("reddit", d.get("author","anon"), date, "INT", "", issue, txt,
                            f"https://reddit.com{d.get('permalink','')}")
                        total += 1
                    after = data.get("after")
                    if not after: break
                    pages += 1
                    time.sleep(random.uniform(0.5,1.2))
                except Exception as e:
                    print(f"  err {sub}/{sort}: {e}", flush=True)
                    break

    # Ricerche keyword
    keywords = ["vinted scam","vinted fake","vinted empty box","vinted wrong item",
                "vinted damaged","vinted fraud","vinted dispute","vinted refund",
                "vinted truffa","vinted falso","vinted pacco vuoto","vinted controversy",
                "vinted counterfeit","vinted lost package","vinted broken","vinted arnaque",
                "vinted colis vide","vinted objet différent","vinted betrug","vinted fake item"]
    for kw in keywords:
        try:
            r = sess.get("https://www.reddit.com/search.json",
                        params={"q":kw,"limit":100,"sort":"new","t":"all"}, timeout=15)
            if r.status_code == 200:
                for p in r.json().get("data",{}).get("children",[]):
                    d = p["data"]
                    txt = f"{d.get('title','')} {d.get('selftext','')}".strip()
                    if len(txt) < 20: continue
                    issue = classify(txt)
                    date = datetime.utcfromtimestamp(d.get("created_utc",0)).strftime("%Y-%m-%d")
                    add("reddit_search", d.get("author","anon"), date, "INT", "", issue, txt,
                        f"https://reddit.com{d.get('permalink','')}")
                    total += 1
            time.sleep(random.uniform(1,2))
        except: pass

    print(f"  → Reddit: {total} record", flush=True)
    return total

# ──────────────────────────────────────
# GOOGLE PLAY
# ──────────────────────────────────────
def google_play():
    print("\n[2] Google Play...", flush=True)
    try:
        from google_play_scraper import reviews as gp_reviews, Sort
        total = 0
        configs = [
            ("it","it","IT"),("fr","fr","FR"),("en","gb","GB"),("de","de","DE"),
            ("es","es","ES"),("pl","pl","PL"),("nl","nl","NL"),("en","us","US"),
            ("pt","pt","PT"),("en","au","AU")
        ]
        for lang, country, cc in configs:
            try:
                result, _ = gp_reviews("com.vinted.android", lang=lang, country=country,
                                        sort=Sort.NEWEST, count=800)
                for rev in result:
                    txt = rev.get("content","")
                    if len(txt) < 15: continue
                    issue = classify(txt)
                    at = rev.get("at")
                    date = at.strftime("%Y-%m-%d") if hasattr(at,"strftime") else str(at)[:10]
                    add("google_play", rev.get("userName","anon"), date, cc,
                        rev.get("score",0), issue, txt)
                    total += 1
                print(f"  → {cc}: {len(result)} recensioni", flush=True)
                time.sleep(1)
            except Exception as e:
                print(f"  err {cc}: {e}", flush=True)
        print(f"  → Google Play totale: {total}", flush=True)
        return total
    except ImportError:
        print("  google-play-scraper non disponibile")
        return 0

# ──────────────────────────────────────
# APP STORE (iTunes public API)
# ──────────────────────────────────────
def app_store():
    print("\n[3] App Store...", flush=True)
    total = 0
    # App IDs Vinted per paese
    configs = [
        ("it","1165726277"),("fr","1165726277"),("gb","1165726277"),
        ("de","1165726277"),("es","1165726277"),("nl","1165726277"),
        ("us","1165726277"),("pl","1165726277")
    ]
    for country, app_id in configs:
        for page in range(1, 11):
            try:
                url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"
                r = requests.get(url, timeout=15, headers={"User-Agent":"iTunes/12.0"})
                if r.status_code != 200: break
                data = r.json()
                entries = data.get("feed",{}).get("entry",[])
                if not entries: break
                for e in entries:
                    if not isinstance(e, dict): continue
                    txt = e.get("content",{}).get("label","")
                    if len(txt) < 15: continue
                    rating = e.get("im:rating",{}).get("label","")
                    author = e.get("author",{}).get("name",{}).get("label","anon")
                    date = e.get("updated",{}).get("label","")[:10]
                    issue = classify(txt)
                    add("app_store", author, date, country.upper(), rating, issue, txt)
                    total += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"  err {country} p{page}: {e}", flush=True)
                break
    print(f"  → App Store: {total} record", flush=True)
    return total

# ──────────────────────────────────────
# TRUSTPILOT (via API non ufficiale)
# ──────────────────────────────────────
def trustpilot():
    print("\n[4] Trustpilot...", flush=True)
    total = 0

    # Trustpilot ha una API pubblica per le recensioni business
    business_ids = {
        "vinted.it": "4f6c453800006400051e8291",
        "vinted.fr": "4f6c453800006400051e8291",
        "vinted.co.uk": "4f6c453800006400051e8291",
        "vinted.com": "4f6c453800006400051e8291",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.trustpilot.com/",
        "Origin": "https://www.trustpilot.com",
    }

    domains = [
        ("vinted.it","IT"),("vinted.fr","FR"),("vinted.co.uk","GB"),
        ("vinted.de","DE"),("vinted.es","ES"),("vinted.be","BE"),
        ("vinted.nl","NL"),("vinted.pl","PL"),("vinted.com","INT")
    ]

    for domain, cc in domains:
        # Prova API endpoint di Trustpilot
        for page in range(1, 30):
            try:
                url = f"https://www.trustpilot.com/review/{domain}"
                params = {"page": page, "sort": "recency", "stars": "1,2"}
                r = requests.get(url, params=params, headers=headers, timeout=15)

                if r.status_code == 403:
                    # Prova endpoint alternativo con JSON embedded
                    soup = BeautifulSoup(r.text, "lxml") if r.text else None
                    break

                if r.status_code != 200:
                    break

                soup = BeautifulSoup(r.text, "lxml")

                # Cerca JSON embedded nella pagina
                for script in soup.find_all("script", id="__NEXT_DATA__"):
                    try:
                        data = json.loads(script.string)
                        page_props = data.get("props",{}).get("pageProps",{})
                        reviews = page_props.get("reviews", [])
                        if not reviews:
                            # Cerca in struttura diversa
                            reviews = (data.get("props",{})
                                          .get("pageProps",{})
                                          .get("reviewsData",{})
                                          .get("reviews",[]))
                        for rev in reviews:
                            txt = rev.get("text","") or rev.get("title","")
                            if len(txt) < 15: continue
                            issue = classify(txt)
                            add("trustpilot",
                                rev.get("consumer",{}).get("displayName","anon"),
                                rev.get("dates",{}).get("publishedDate","")[:10],
                                cc, rev.get("stars",1), issue, txt,
                                f"https://trustpilot.com/review/{domain}")
                            total += 1
                    except: pass

                time.sleep(random.uniform(1,2))
                if page % 5 == 0:
                    print(f"  → {cc} p{page}: {total} tot", flush=True)

            except Exception as e:
                print(f"  err {cc} p{page}: {e}", flush=True)
                break

    print(f"  → Trustpilot: {total} record", flush=True)
    return total

# ──────────────────────────────────────
# SAVE + STATS
# ──────────────────────────────────────
def save():
    print(f"\nSalvo {len(records)} record...", flush=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source","username","date","country","rating","issue_type","text","url"])
        w.writeheader()
        w.writerows(records)

    print("\n── RISULTATI ─────────────────────")
    print(f"TOTALE RECORD: {len(records)}")

    sources, issues, countries = {}, {}, {}
    for r in records:
        sources[r["source"]] = sources.get(r["source"],0) + 1
        for i in r["issue_type"].split(", "):
            issues[i] = issues.get(i,0) + 1
        countries[r["country"]] = countries.get(r["country"],0) + 1

    print("\nFONTE:")
    for k,v in sorted(sources.items(),key=lambda x:-x[1]):
        print(f"  {k}: {v}")
    print("\nPROBLEMA:")
    for k,v in sorted(issues.items(),key=lambda x:-x[1]):
        print(f"  {k}: {v}")
    print("\nPAESE (top 10):")
    for k,v in sorted(countries.items(),key=lambda x:-x[1])[:10]:
        print(f"  {k}: {v}")

if __name__ == "__main__":
    print("="*45)
    print("MELION DATA COLLECTOR v2")
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*45, flush=True)

    reddit()
    google_play()
    app_store()
    trustpilot()
    save()

    print(f"\n✅ File: {OUTPUT}")
