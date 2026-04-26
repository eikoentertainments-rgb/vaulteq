#!/usr/bin/env python3
"""Aggiunge record manuali al CSV principale, riusando classify/dedup del collector."""
import sys, os, csv, json, hashlib
sys.path.insert(0, os.path.dirname(__file__))
from datetime import datetime

BASE = "/Users/gioooo2/gio-platform"
OUTPUT = f"{BASE}/data/vinted_complaints.csv"

# Importa funzioni dal main per coerenza
sys.path.insert(0, BASE)
from collector.main import (
    classify, classify_product, classify_platform,
    is_negative, is_valid, make_id, FIELDNAMES
)

def load_seen_ids():
    seen = set()
    with open(OUTPUT) as f:
        for row in csv.DictReader(f):
            seen.add(make_id(row.get("text",""), row.get("username",""), row.get("date","")))
    return seen

def add_records(records):
    """records = list of dicts: source, username, date, country, text, url"""
    seen = load_seen_ids()
    added = 0
    skipped = 0
    with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        for r in records:
            text = r.get("text", "").strip()
            if not text or len(text.split()) < 5:
                skipped += 1; continue
            issue = r.get("issue_type") or classify(text)
            if not r.get("issue_type") and not is_negative(text):
                # solo se non sai già che issue è (curatela manuale = bypass)
                skipped += 1; continue
            if issue == "altro":
                skipped += 1; continue
            uid = make_id(text, r.get("username",""), r.get("date",""))
            if uid in seen:
                skipped += 1; continue
            seen.add(uid)
            writer.writerow({
                "source": r["source"],
                "username": r.get("username","anon")[:80],
                "date": r.get("date","") or datetime.now().strftime("%Y-%m-%d"),
                "country": r.get("country","INT"),
                "rating": "",
                "issue_type": issue,
                "product_category": classify_product(text),
                "platform": classify_platform(text, r.get("url","")),
                "text": text[:600].replace("\n"," ").replace("\r",""),
                "url": r.get("url",""),
                "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            added += 1
    return added, skipped

if __name__ == "__main__":
    records = json.load(sys.stdin)
    added, skipped = add_records(records)
    print(f"Aggiunti: {added} | Scartati: {skipped}")
