#!/usr/bin/env python3
"""
Melion Data Collector — versione continua
Salva progressivamente, gira 24/7, si riprende da dove si è fermato
"""

import requests, json, time, csv, random, os, sys, hashlib, threading
from datetime import datetime
from bs4 import BeautifulSoup

_lock = threading.Lock()  # protegge state e CSV da scritture simultanee
_cycle_counts = {}        # record aggiunti per fonte nel ciclo corrente
_existing_counts = {}     # record già presenti nel CSV per fonte (a inizio ciclo)
SOURCE_HARD_CAP = 1000    # max record totali per fonte (esistenti + nuovi)
MAX_PER_SOURCE = {        # limite per ciclo per bilanciare le fonti
    "youtube_comment": 600,
    "youtube_video":   150,
    "reddit":          500,
    "reddit_comment":  500,
    "reddit_search":   300,
    "sitejabber":      400,
    "pissedconsumer":  400,
    "trustpilot":      400,
    "hackernews":      400,
    "appstore":        300,
    "playstore":       300,
    "twitter":         300,
    "facebook_search":   400,
    "instagram_search":  400,
    "tiktok_search":     400,
    "trustpilot_search": 400,
    "google_news":     300,
    "forum_it":        300,
    "forum_fr":        300,
    "forum_de":        300,
}

BASE = "/Users/gioooo2/gio-platform"
DATA_DIR = f"{BASE}/data"
OUTPUT = f"{DATA_DIR}/vinted_complaints.csv"
STATE_FILE = f"{DATA_DIR}/state.json"
CONFIG_FILE = f"{BASE}/collector/config.json"
LOG_FILE = f"{DATA_DIR}/collector.log"

os.makedirs(DATA_DIR, exist_ok=True)

# ── CONFIG ──────────────────────────────
def _load_env():
    env_file = os.path.join(BASE, ".env")
    env = {}
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    return env

def load_config():
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    env = _load_env()
    # Chiavi sensibili: prima env var, poi .env file, poi config (legacy)
    cfg["youtube_api_key"]       = os.environ.get("YOUTUBE_API_KEY")       or env.get("YOUTUBE_API_KEY",       "")
    cfg["twitter_bearer_token"]  = os.environ.get("TWITTER_BEARER_TOKEN")  or env.get("TWITTER_BEARER_TOKEN",  "")
    cfg["facebook_cookies"]      = os.environ.get("FACEBOOK_COOKIES")      or env.get("FACEBOOK_COOKIES",      "")
    cfg["trustpilot_api_key"]    = os.environ.get("TRUSTPILOT_API_KEY")    or env.get("TRUSTPILOT_API_KEY",    "")
    if os.environ.get("OUTPUT_DIR"):
        cfg["output_dir"] = os.environ["OUTPUT_DIR"]
    return cfg

# ── LOGGING ─────────────────────────────
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ── STATE (per riprendere dove ci si è fermati) ──
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"seen_ids": [], "reddit_after": {}, "gplay_done": [], "appstore_done": []}

def save_state(state):
    with _lock:
        if len(state["seen_ids"]) > 50000:
            state["seen_ids"] = state["seen_ids"][-50000:]
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)

MIN_WORDS   = 10
MIN_DATE    = "2022-01-01"   # post 2022+; ante-2022 accettati solo se >= 50 parole e issue != altro
PRE22_WORDS = 50

# ── CSV WRITER (append progressivo) ─────
FIELDNAMES = ["source","username","date","country","rating","issue_type","product_category","platform","text","url","collected_at"]

def init_csv():
    if not os.path.exists(OUTPUT):
        with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

def write_record(record):
    with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES).writerow(record)

def count_records():
    if not os.path.exists(OUTPUT):
        return 0
    with open(OUTPUT, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1

def count_per_source():
    counts = {}
    if not os.path.exists(OUTPUT):
        return counts
    with open(OUTPUT, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            s = row.get("source", "")
            counts[s] = counts.get(s, 0) + 1
    return counts

def make_id(text, username, date):
    return hashlib.md5(f"{text[:100]}{username}{date}".encode()).hexdigest()

def classify_platform(text, url=""):
    t = (text + " " + url).lower()
    if "kleinanzeigen" in t: return "kleinanzeigen"
    if "leboncoin" in t: return "leboncoin"
    if "facebook marketplace" in t or "fb marketplace" in t or "marketplace facebook" in t: return "facebook_marketplace"
    if "wallapop" in t: return "wallapop"
    if "subito.it" in t or " subito " in t: return "subito"
    if "depop" in t: return "depop"
    if "vinted" in t: return "vinted"
    if "ebay" in t: return "ebay"
    if "vestiaire" in t: return "vestiaire"
    if "back market" in t or "backmarket" in t: return "back_market"
    if "vide dressing" in t or "videdressing" in t: return "vide_dressing"
    return "other"

# ── CLASSIFICATORE ───────────────────────
def classify(text):
    t = text.lower()
    issues = []

    # 1. PACCO VUOTO
    if any(w in t for w in [
        # EN
        "empty box","box was empty","box empty","nothing inside","arrived empty",
        "package was empty","parcel was empty","sent empty","opened it and nothing",
        # IT
        "scatola vuota","pacco vuoto","arrivato vuoto","busta vuota",
        # FR
        "boite vide","colis vide","paquet vide","arrivé vide",
        # ES
        "caja vacía","paquete vacío","llegó vacío","caja estaba vacía",
        # DE
        "leere packung","leeres paket","leer angekommen",
        # NL
        "lege doos","lege pakket","leeg pakket",
        # PL
        "puste pudełko","paczka była pusta","przyszło puste","pusta paczka",
        # LT
        "tuščia dėžė","tuščias paketas",
        # CZ/SK
        "prázdná krabice","prázdný balíček","prišlo prázdne"
    ]):
        issues.append("pacco_vuoto")

    # 2. OGGETTO CONTRAFFATTO
    if any(w in t for w in [
        # EN
        "fake","counterfeit","replica","not authentic","not genuine","not real",
        "it's a fake","it was fake","clearly fake","obvious fake","definitely fake",
        "knock off","knockoff","not the real","not original",
        # IT
        "falso","contraffatto","non originale","non autentico","imitazione",
        # FR
        "faux","contrefaçon","pas authentique","pas original","c'est un faux",
        # ES
        "falso","falsificado","no es original","no es auténtico","imitación",
        # DE
        "gefälscht","fälschung","nicht echt","nicht original","fälschung",
        # NL
        "nep","namaak","vals","niet echt","niet origineel",
        # PL
        "podróbka","podrobiony","fałszywy","nie oryginalny","falsyfikat",
        # LT
        "suklastotas","netikras","padirbtas",
        # CZ/SK
        "falzum","padělok","falošný","není originál"
    ]):
        issues.append("oggetto_falso")

    # 3. OGGETTO NON COME DESCRITTO
    if any(w in t for w in [
        # EN
        "not as described","not what i ordered","not what was listed","wrong item",
        "different item","different from","different to what","completely different",
        "nothing like","looked nothing like","photos were fake","fake photos",
        "misrepresented","false advertising","sent the wrong","received the wrong",
        # IT
        "non corrisponde","diverso dall'annuncio","diverso da","articolo diverso",
        "non era come","foto false","non è quello che",
        # FR
        "pas conforme","pas comme","différent de","pas ce qui était","photos truquées",
        # ES
        "no es lo que","diferente a","no corresponde","fotos falsas","no como se describía",
        # DE
        "nicht wie beschrieben","falsch beschrieben","anders als","fotos gefälscht",
        # NL
        "anders dan","niet zoals beschreven","fotos klopten niet","niet wat ik bestelde",
        # PL
        "niezgodny z opisem","inny niż","zdjęcia były fałszywe","nie to co zamawiałem",
        "towar inny niż","nie odpowiada opisowi",
        # LT
        "neatitinka aprašymo","kitoks nei","nuotraukos buvo melagingos",
        # CZ/SK
        "nesouhlasí s popisem","jiný než","fotky byly falešné"
    ]):
        issues.append("oggetto_diverso")

    # 4. OGGETTO DANNEGGIATO
    if any(w in t for w in [
        # EN
        "arrived damaged","arrived broken","arrived smashed","arrived cracked",
        "received damaged","received broken","it was broken","it was damaged",
        "completely broken","smashed","shattered","damaged in transit",
        # IT
        "arrivato rotto","arrivato danneggiato","rotto","danneggiato","ammaccato",
        # FR
        "arrivé cassé","arrivé endommagé","cassé","endommagé","abîmé",
        # ES
        "llegó roto","llegó dañado","roto","dañado","destrozado",
        # DE
        "beschädigt angekommen","kaputt angekommen","beschädigt","kaputt","zerbrochen",
        # NL
        "beschadigd","kapot","gebroken","stuk aangekomen",
        # PL
        "przyszło uszkodzone","przyszło zniszczone","uszkodzone","zniszczone","połamane",
        "paczka uszkodzona","towar uszkodzony",
        # LT
        "atvyko sugadinta","sugadintas","sulaužytas",
        # CZ/SK
        "dorazilo poškozené","poškozené","rozbité","zničené"
    ]):
        issues.append("oggetto_danneggiato")

    # 5. PACCO NON ARRIVATO
    if any(w in t for w in [
        # EN
        "never arrived","never received","never delivered","never came",
        "not received","not arrived","not delivered","didn't arrive","didn't receive",
        "package lost","parcel lost","lost in transit","tracking shows delivered but",
        "says delivered but","marked as delivered but","delivered but not received",
        # IT
        "mai arrivato","non arrivato","non ricevuto","pacco perso","perso in transito",
        "smarrito","tracciatura dice consegnato ma",
        # FR
        "jamais reçu","jamais arrivé","perdu","pas reçu","livré mais pas reçu","colis perdu",
        # ES
        "nunca llegó","nunca recibí","no llegó","perdido","paquete perdido",
        # DE
        "nie angekommen","nie erhalten","verloren gegangen","als zugestellt markiert aber",
        "paket verloren","nicht angekommen",
        # NL
        "nooit ontvangen","nooit aangekomen","verloren","pakket kwijt",
        # PL
        "nie dotarło","nie otrzymałem","nie przyszło","paczka zaginęła","zaginiona paczka",
        "paczka nie dotarła","śledzenie mówi dostarczone ale",
        # LT
        "niekada negavo","neprisijungė","paketas dingo","negavau",
        # CZ/SK
        "nikdy nedorazilo","nedostal jsem","ztracený balíček","zásilka se ztratila"
    ]):
        issues.append("pacco_non_arrivato")

    # 6. CONTROVERSIA PERSA
    if any(w in t for w in [
        # EN
        "lost the dispute","lost my dispute","vinted sided with","vinted took their side",
        "vinted ruled against","refused my refund","denied my claim","denied my refund",
        "refund refused","refund denied","won't refund","wont refund","no refund",
        "closed the case against me","closed my case","dispute closed against",
        # IT
        "ho perso la controversia","vinted ha dato torto","rimborso negato","rimborso rifiutato",
        "non mi hanno rimborsato","controversia chiusa","hanno dato ragione al venditore",
        # FR
        "remboursement refusé","refus de remboursement","litige perdu","vinted a donné raison",
        "pas de remboursement","ils ont refusé",
        # ES
        "reembolso rechazado","vinted se puso del lado","perdí la disputa","no me reembolsaron",
        # DE
        "dispute verloren","erstattung verweigert","haben entschieden gegen",
        "keine erstattung","rückerstattung abgelehnt",
        # NL
        "terugbetaling geweigerd","dispute verloren","vinted koos voor",
        # PL
        "przegrałem spór","vinted stanął po stronie","odmówiono zwrotu","brak zwrotu",
        "spór przegrany","nie oddali pieniędzy",
        # LT
        "ginčas prarastas","atsisakė grąžinti","vinted palaikė",
        # CZ/SK
        "spor prohrán","odmítli vrátit","vinted se přiklonilo"
    ]):
        issues.append("controversia_persa")

    # 7. TRUFFA ORGANIZZATA
    if any(w in t for w in [
        # EN
        "scammed","got scammed","been scammed","i was scammed","i got scammed",
        "ripped off","got ripped off","been ripped off",
        "it's a scam","it was a scam","total scam","complete scam","absolute scam",
        "fraudster","con artist","swindled","taken my money","stole my money",
        "took my money","money gone","seller disappeared","seller vanished",
        "account deleted after","account gone after",
        # IT
        "truffato","mi hanno truffato","è una truffa","truffa bella e buona",
        "venditore sparito","soldi spariti","mi hanno rubato","fregato","mi ha fregato",
        # FR
        "arnaqué","je me suis fait arnaquer","c'est une arnaque","arnaque totale",
        "vendeur disparu","argent perdu","ils ont pris mon argent","escroquerie",
        # ES
        "me estafaron","es una estafa","estafador","me robaron","vendedor desapareció",
        "dinero perdido","timo","me timaron",
        # DE
        "betrogen","abgezockt","betrug","verkäufer verschwunden","geld weg",
        "ich wurde betrogen","das ist betrug","abzocke",
        # NL
        "opgelicht","oplichting","geld kwijt","verkoper verdwenen",
        "ik ben opgelicht","dit is oplichting",
        # PL
        "oszukano mnie","to oszustwo","sprzedawca zniknął","pieniądze przepadły",
        "zostałem oszukany","wyłudzenie","złodziej","ukradli mi",
        # LT
        "apgavo mane","tai sukčiavimas","pardavėjas dingo","pinigai dingo",
        # CZ/SK
        "podvedli mě","je to podvod","prodejce zmizel","peníze jsou pryč","podvod"
    ]):
        issues.append("truffa_generica")

    # 8. VENDITORE NON RISPONDE
    if any(w in t for w in [
        "seller not responding","seller doesn't respond","seller won't reply","no response from seller",
        "seller ignoring","seller blocked me","blocked by seller","seller never replied",
        "venditore non risponde","non mi risponde","mi ha bloccato","non risponde",
        "vendeur ne répond pas","il ne répond plus","m'a bloqué","ne répond plus",
        "verkäufer antwortet nicht","hat mich blockiert","antwortet nicht mehr",
        "sprzedawca nie odpowiada","zablokował mnie","nie odpowiada",
        "verkoper reageert niet","heeft me geblokkeerd","reageert niet"
    ]):
        issues.append("venditore_non_risponde")

    # 9. PAGAMENTO NON RICEVUTO (venditore)
    if any(w in t for w in [
        "never paid","payment not received","didn't receive payment","buyer didn't pay",
        "still waiting for payment","payment pending forever","funds not released",
        "non ho ricevuto il pagamento","pagamento non arrivato","soldi non arrivati",
        "pas reçu le paiement","acheteur n'a pas payé","fonds non libérés",
        "zahlung nicht erhalten","käufer hat nicht bezahlt","geld nicht freigegeben",
        "nie otrzymałem płatności","kupujący nie zapłacił","środki nie zostały zwolnione"
    ]):
        issues.append("pagamento_mancante")

    return issues[0] if issues else "altro"


def classify_product(text):
    t = text.lower()
    if any(w in t for w in [
        "shoes","sneakers","boots","heels","trainers","scarpe","stivali","tacchi","scarpa",
        "chaussures","baskets","bottes","zapatos","zapatillas","botas","schuhe","stiefel",
        "buty","sneakersy","botki"
    ]):
        return "scarpe"
    if any(w in t for w in [
        "bag","handbag","purse","wallet","backpack","belt","jewelry","jewellery","watch","sunglasses",
        "borsa","portafoglio","zaino","cintura","gioielli","orologio","occhiali",
        "sac","portefeuille","sac à dos","ceinture","bijoux","montre","lunettes",
        "bolso","cartera","mochila","cinturón","joyas","reloj","gafas",
        "tasche","geldbörse","rucksack","gürtel","schmuck","uhr","brille",
        "torebka","portfel","plecak","pasek","biżuteria","zegarek"
    ]):
        return "borse_accessori"
    if any(w in t for w in [
        "phone","iphone","samsung","android","laptop","tablet","ipad","headphones","airpods",
        "console","playstation","xbox","nintendo","gaming","computer","keyboard","mouse",
        "telefono","cellulare","cuffie","consolle","tastiera",
        "téléphone","ordinateur","écouteurs","manette",
        "telefon","kopfhörer","spielekonsole","tastatur",
        "telefon","słuchawki","konsola","komputer"
    ]):
        return "elettronica"
    if any(w in t for w in [
        "perfume","perfume","cologne","makeup","lipstick","foundation","skincare","cream","serum",
        "profumo","trucco","rossetto","crema","skincare",
        "parfum","maquillage","rouge à lèvres","crème","soin",
        "parfüm","make-up","lippenstift","creme",
        "perfumy","makijaż","szminka","krem"
    ]):
        return "cosmetici_profumi"
    if any(w in t for w in [
        "furniture","sofa","lamp","decoration","vase","plant pot","frame","mirror","bedding","towel",
        "mobile","divano","lampada","decorazione","specchio","lenzuola","asciugamano",
        "meuble","canapé","lampe","décoration","miroir","literie","serviette",
        "möbel","sofa","lampe","dekoration","spiegel","bettwäsche",
        "meble","sofa","lampa","dekoracja","lustro","pościel"
    ]):
        return "casa_arredamento"
    if any(w in t for w in [
        "bike","bicycle","helmet","football","tennis","yoga","gym","sports","fitness","ski","surf",
        "bici","bicicletta","casco","calcio","tennis","palestra","sport","sci",
        "vélo","casque","football","tennis","gym","sport","ski",
        "fahrrad","helm","fußball","sport","fitness","ski",
        "rower","kask","piłka","sport","siłownia","narty"
    ]):
        return "sport_outdoor"
    if any(w in t for w in [
        "toy","lego","doll","baby","children","kids","stroller","pram","child","infant",
        "giocattolo","lego","bambola","bambini","passeggino","bimbo","neonato",
        "jouet","poupée","enfants","poussette","bébé","nourrisson",
        "spielzeug","puppe","kinder","kinderwagen","baby","kleinkind",
        "zabawka","lalka","dzieci","wózek","niemowlę"
    ]):
        return "bambini_giocattoli"
    if any(w in t for w in [
        "book","novel","comic","dvd","bluray","cd","vinyl","game","board game","puzzle",
        "libro","romanzo","fumetto","gioco","puzzle","vinile",
        "livre","roman","bande dessinée","jeu","puzzle","vinyle",
        "buch","roman","comic","spiel","puzzle","vinyl",
        "książka","powieść","komiks","gra","puzzle","winyl"
    ]):
        return "libri_media"
    if any(w in t for w in [
        "dress","shirt","jeans","jacket","coat","hoodie","sweater","top","skirt","trousers","pants",
        "tshirt","t-shirt","blouse","suit","underwear","lingerie",
        "vestito","camicia","jeans","giacca","cappotto","felpa","maglione","gonna","pantaloni",
        "robe","chemise","jean","veste","manteau","pull","jupe","pantalon","sous-vêtements",
        "kleid","hemd","jeans","jacke","mantel","pullover","rock","hose","unterwäsche",
        "sukienka","koszula","dżinsy","kurtka","płaszcz","bluza","sweter","spódnica","spodnie"
    ]):
        return "abbigliamento"
    return "undefined"


NEGATIVE_REQUIRED = [
    # EN
    "scam","fraud","fake","empty","broken","damaged","missing","lost","stolen","wrong item",
    "not received","never arrived","not as described","dispute","refund","ripped off",
    "cheat","deceived","never got","didn't receive","do not use","avoid","terrible","awful",
    "worst","horrible","disgusting","outrageous","useless","impossible","nightmare",
    # IT
    "truffa","falso","truffato","fregato","pacco vuoto","non arrivato","mai arrivato",
    "non ricevuto","danneggiato","rotto","controversia","rimborso","vergogna","pessimo",
    "orribile","schifo","inutile","impossibile","disperato","sparito",
    # FR
    "arnaque","arnaqué","faux","colis vide","jamais reçu","pas reçu","endommagé","cassé",
    "litige","remboursement","escroquerie","horrible","nul","catastrophique","dégoûtant",
    # DE
    "betrug","betrogen","gefälscht","nie angekommen","beschädigt","kaputt","verloren",
    "erstattung","schrecklich","furchtbar","katastrophe","abzocke","verschwunden",
    # ES
    "estafa","estafado","falso","nunca llegó","dañado","roto","disputa","reembolso",
    "horrible","pésimo","terrible","catastrófico","fraude","engaño",
    # PL
    "oszustwo","oszukany","podróbka","nie dotarło","uszkodzone","zniszczone","zwrot",
    "okropny","koszmar","beznadziejny","zniknął",
]

def is_negative(text):
    t = text.lower()
    return any(k in t for k in NEGATIVE_REQUIRED)

def is_valid(text, date, issue=""):
    words = len(text.split())
    if words < MIN_WORDS:
        return False
    if date and date < MIN_DATE:
        # Ante-2022: accetta solo se testo lungo e problema preciso
        if words < PRE22_WORDS or issue == "altro":
            return False
    # Richiedi almeno un segnale negativo — filtra recensioni positive
    if not is_negative(text):
        return False
    return True


def add(state, source, username, date, country, rating, issue, text, url=""):
    if not is_valid(text, date, issue):
        return False
    # Cap totale per fonte: 1000 record (esistenti + nuovi del ciclo)
    total_for_source = _existing_counts.get(source, 0) + _cycle_counts.get(source, 0)
    if total_for_source >= SOURCE_HARD_CAP:
        return False
    limit = MAX_PER_SOURCE.get(source)
    if limit and _cycle_counts.get(source, 0) >= limit:
        return False
    uid = make_id(text, username, date)
    with _lock:
        if uid in state["seen_ids"]:
            return False
        state["seen_ids"].append(uid)
        _cycle_counts[source] = _cycle_counts.get(source, 0) + 1
        record = {
            "source": source, "username": username[:80], "date": date,
            "country": country, "rating": rating,
            "issue_type": issue,
            "product_category": classify_product(text),
            "platform": classify_platform(text, url),
            "text": text[:600].replace("\n"," ").replace("\r",""),
            "url": url, "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        write_record(record)
    return True

# ────────────────────────────────────────
# FONTE 1: REDDIT
# ────────────────────────────────────────
def run_reddit(state, config):
    if not config["sources_enabled"].get("reddit"): return
    log("Reddit: avvio")
    sess = requests.Session()
    sess.headers["User-Agent"] = "MelionResearch/2.0"
    count = 0

    subs = [
        # Vinted
        "vinted","VintedUK","vinted_fr","VintedItalia","Vinted","vinted_de","vinted_pl",
        # eBay
        "eBaySellerAdvice","Ebay","eBaySellers",
        # Depop
        "Depop","DepopUK",
        # Facebook Marketplace
        "FacebookMarketplace","marketplace",
        # Wallapop
        "wallapop",
        # Seconda mano generale EN
        "declutter","ThriftStoreHauls","Poshmark","BuyItForLife",
        "frugalmalefashion","RepLadies","fashionadvice",
        # UK
        "UKPersonalFinance","MSE_Forum","AskUK",
        # Forum IT (subito.it)
        "italy","italianproblems",
        # Forum FR (leboncoin)
        "france","AskFrance","viefrancaise",
        # Forum DE (kleinanzeigen)
        "de","germany","FragReddit",
        # Forum ES
        "es","spain","askspain",
        # Forum PL
        "poland","Polska","PolskaPomoc",
    ]

    for sub in subs:
        for sort in ["new","hot","top"]:
            after = state["reddit_after"].get(f"{sub}_{sort}")
            pages = 0
            while pages < 20:
                try:
                    params = {"limit":100,"t":"all"}
                    if after: params["after"] = after
                    r = sess.get(f"https://www.reddit.com/r/{sub}/{sort}.json",
                                params=params, timeout=15)
                    if r.status_code == 429: time.sleep(10); continue
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
                        if add(state,"reddit",d.get("author","anon"),date,"INT","",issue,txt,
                               f"https://reddit.com{d.get('permalink','')}"):
                            count += 1
                        # Commenti
                        try:
                            cr = sess.get(f"https://www.reddit.com/r/{sub}/comments/{d['id']}.json",
                                         params={"limit":50}, timeout=10)
                            if cr.status_code == 200 and len(cr.json()) > 1:
                                for c in cr.json()[1].get("data",{}).get("children",[]):
                                    cd = c.get("data",{})
                                    ct = cd.get("body","")
                                    if len(ct.split()) < MIN_WORDS: continue
                                    ci = classify(ct)
                                    if ci == "altro": continue
                                    cdate = datetime.utcfromtimestamp(cd.get("created_utc",0)).strftime("%Y-%m-%d")
                                    if add(state,"reddit_comment",cd.get("author","anon"),cdate,"INT","",ci,ct):
                                        count += 1
                        except: pass

                    after = data.get("after")
                    state["reddit_after"][f"{sub}_{sort}"] = after
                    save_state(state)
                    if not after: break
                    pages += 1
                    time.sleep(random.uniform(0.8,1.5))
                except Exception as e:
                    log(f"Reddit err {sub}/{sort}: {e}")
                    break

    # Keyword search — tutte le piattaforme, 6 lingue
    keywords = [
        # ── VINTED ──
        "vinted scam","vinted fake","vinted empty box","vinted wrong item",
        "vinted damaged","vinted fraud","vinted dispute","vinted refund denied",
        "vinted counterfeit","vinted lost package","vinted never arrived",
        "vinted not received","vinted not as described","vinted seller disappeared",
        "vinted truffa","vinted falso","vinted pacco vuoto","vinted non arrivato",
        "vinted rimborso negato","vinted truffato","vinted danneggiato",
        "vinted arnaque","vinted colis vide","vinted jamais reçu","vinted litige perdu",
        "vinted betrug","vinted gefälscht","vinted nie angekommen","vinted abzocke",
        "vinted estafa","vinted nunca llegó","vinted me estafaron","vinted fraude",
        "vinted oszustwo","vinted podróbka","vinted nie dotarło","vinted pusta paczka",
        # ── EBAY ──
        "ebay scam","ebay fake","ebay fraud","ebay empty box","ebay wrong item",
        "ebay not received","ebay never arrived","ebay counterfeit","ebay refund denied",
        "ebay lost dispute","ebay seller disappeared","ebay damaged","ebay not as described",
        "ebay truffa","ebay falso","ebay non arrivato","ebay rimborso negato","ebay truffato",
        "ebay arnaque","ebay colis vide","ebay jamais reçu","ebay contrefaçon",
        "ebay betrug","ebay gefälscht","ebay nie angekommen","ebay abzocke",
        "ebay estafa","ebay nunca llegó","ebay me estafaron",
        "ebay oszustwo","ebay podróbka","ebay nie dotarło",
        # ── DEPOP ──
        "depop scam","depop fake","depop fraud","depop not received","depop wrong item",
        "depop counterfeit","depop refund","depop dispute","depop not as described",
        "depop truffa","depop falso","depop non arrivato","depop arnaque","depop betrug",
        # ── FACEBOOK MARKETPLACE ──
        "facebook marketplace scam","facebook marketplace fake","facebook marketplace fraud",
        "facebook marketplace not received","facebook marketplace wrong item",
        "marketplace truffa","marketplace arnaque","marketplace betrug","marketplace estafa",
        "fb marketplace scam","fb marketplace fake",
        # ── WALLAPOP ──
        "wallapop scam","wallapop fake","wallapop fraud","wallapop estafa",
        "wallapop truffa","wallapop arnaque","wallapop betrug",
        "wallapop me estafaron","wallapop fraude","wallapop nunca llegó",
        "wallapop falso","wallapop non arrivato",
        # ── SUBITO.IT ──
        "subito truffa","subito.it truffa","subito falso","subito non arrivato",
        "subito fregato","subito truffato","subito scam","subito fake",
        # ── LEBONCOIN ──
        "leboncoin arnaque","leboncoin escroquerie","leboncoin faux","leboncoin jamais reçu",
        "leboncoin arnaqué","leboncoin fraude","leboncoin colis vide","leboncoin litige",
        # ── KLEINANZEIGEN ──
        "kleinanzeigen betrug","kleinanzeigen gefälscht","kleinanzeigen betrogen",
        "kleinanzeigen abzocke","kleinanzeigen fake","kleinanzeigen nie angekommen",
        "ebay kleinanzeigen betrug","ebay kleinanzeigen fake",
    ]
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
                    if add(state,"reddit_search",d.get("author","anon"),date,"INT","",issue,txt,
                           f"https://reddit.com{d.get('permalink','')}"):
                        count += 1
            time.sleep(random.uniform(1,2))
        except: pass

    log(f"Reddit: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 2: GOOGLE PLAY
# ────────────────────────────────────────
def run_google_play(state, config):
    if not config["sources_enabled"].get("google_play"): return
    log("Google Play: avvio")
    try:
        from google_play_scraper import reviews as gp, Sort
        count = 0
        configs = [
            ("it","it","IT"),("fr","fr","FR"),("en","gb","GB"),("de","de","DE"),
            ("es","es","ES"),("pl","pl","PL"),("nl","nl","NL"),("en","us","US"),
            ("pt","pt","PT"),("en","au","AU"),("cs","cz","CZ"),("sk","sk","SK"),
            ("lt","lt","LT"),("en","ie","IE"),("fi","fi","FI"),("sv","se","SE")
        ]
        for lang, country, cc in configs:
            if cc in state.get("gplay_done",[]): continue
            try:
                result, _ = gp("com.vinted.android", lang=lang, country=country,
                               sort=Sort.NEWEST, count=2000)
                for rev in result:
                    txt = rev.get("content","")
                    if len(txt) < 15: continue
                    issue = classify(txt)
                    at = rev.get("at")
                    date = at.strftime("%Y-%m-%d") if hasattr(at,"strftime") else str(at)[:10]
                    if add(state,"google_play",rev.get("userName","anon"),date,cc,
                           rev.get("score",0),issue,txt):
                        count += 1
                state.setdefault("gplay_done",[]).append(cc)
                save_state(state)
                log(f"  Google Play {cc}: {len(result)} rec")
                time.sleep(2)
            except Exception as e:
                log(f"  Google Play {cc} err: {e}")
        log(f"Google Play: +{count} nuovi record")
    except ImportError:
        log("google-play-scraper non disponibile")

# ────────────────────────────────────────
# FONTE 3: APP STORE
# ────────────────────────────────────────
def run_app_store(state, config):
    if not config["sources_enabled"].get("app_store"): return
    log("App Store: avvio")
    count = 0
    configs = [
        ("it","IT"),("fr","FR"),("gb","GB"),("de","DE"),("es","ES"),
        ("nl","NL"),("us","US"),("pl","PL"),("be","BE"),("pt","PT"),
        ("ie","IE"),("au","AU"),("ca","CA"),("se","SE"),("fi","FI")
    ]
    for country, cc in configs:
        if cc in state.get("appstore_done",[]): continue
        for page in range(1,11):
            try:
                url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id=1165726277/sortby=mostrecent/json"
                r = requests.get(url, timeout=15, headers={"User-Agent":"iTunes/12.0"})
                if r.status_code != 200: break
                entries = r.json().get("feed",{}).get("entry",[])
                if not entries: break
                for e in entries:
                    if not isinstance(e,dict): continue
                    txt = e.get("content",{}).get("label","")
                    if len(txt) < 15: continue
                    issue = classify(txt)
                    rating = e.get("im:rating",{}).get("label","")
                    author = e.get("author",{}).get("name",{}).get("label","anon")
                    date = e.get("updated",{}).get("label","")[:10]
                    if add(state,"app_store",author,date,cc,rating,issue,txt):
                        count += 1
                time.sleep(0.5)
            except Exception as e:
                log(f"  App Store {cc} p{page} err: {e}")
                break
        state.setdefault("appstore_done",[]).append(cc)
        save_state(state)
    log(f"App Store: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 4: PISSEDCONSUMER
# ────────────────────────────────────────
def run_pissedconsumer(state, config):
    if not config["sources_enabled"].get("pissedconsumer"): return
    log("PissedConsumer: avvio")
    count = 0
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    pc_sites = ["vinted","ebay","depop","wallapop","subito"]
    for site in pc_sites:
     for page in range(1,51):
        try:
            url = f"https://{site}.pissedconsumer.com/review.html?page={page}"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: break
            soup = BeautifulSoup(r.text,"lxml")
            reviews = soup.find_all("article") or soup.find_all(class_=lambda c: c and "review" in c.lower())
            found = 0
            for rev in reviews:
                txt_el = rev.find(class_=lambda c: c and "text" in c.lower()) or rev.find("p")
                txt = txt_el.get_text(separator=" ",strip=True) if txt_el else rev.get_text(separator=" ",strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                # Estrai data reale
                date_el = rev.find("time") or rev.find(class_=lambda c: c and "date" in (c or "").lower())
                if date_el:
                    date_str = date_el.get("datetime","") or date_el.get_text(strip=True)
                    date = date_str[:10] if len(date_str) >= 10 else datetime.now().strftime("%Y-%m-%d")
                else:
                    date = datetime.now().strftime("%Y-%m-%d")
                # Estrai autore
                author_el = rev.find(class_=lambda c: c and "author" in (c or "").lower())
                author = author_el.get_text(strip=True)[:80] if author_el else "anon"
                issue = classify(txt)
                if issue == "altro": continue
                if add(state,"pissedconsumer",author,date,"US","",issue,txt,url):
                    count += 1
                    found += 1
            if found == 0: break
            time.sleep(random.uniform(1,2))
        except Exception as e:
            log(f"  PissedConsumer {site} p{page} err: {e}")
            break
    log(f"PissedConsumer: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 5: SITEJABBER
# ────────────────────────────────────────
def run_sitejabber(state, config):
    if not config["sources_enabled"].get("sitejabber"): return
    log("Sitejabber: avvio")
    count = 0
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    platforms = [
        "vinted.com","ebay.com","depop.com","wallapop.com",
        "subito.it","leboncoin.fr","ebay-kleinanzeigen.de",
    ]
    for site in platforms:
        for page in range(1, 31):
            try:
                url = f"https://www.sitejabber.com/reviews/{site}?page={page}"
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code != 200: break
                soup = BeautifulSoup(r.text,"lxml")
                for rev in soup.find_all(attrs={"itemprop":"review"}) or soup.find_all(class_=lambda c: c and "review" in c.lower()):
                    txt_el = rev.find(attrs={"itemprop":"reviewBody"}) or rev.find("p")
                    if not txt_el: continue
                    txt = txt_el.get_text(strip=True)
                    if len(txt) < 20: continue
                    author_el = rev.find(attrs={"itemprop":"author"})
                    author = author_el.get_text(strip=True) if author_el else "anon"
                    date_el = rev.find("time")
                    date = date_el.get("datetime","")[:10] if date_el else datetime.now().strftime("%Y-%m-%d")
                    issue = classify(txt)
                    if issue == "altro": continue
                    if add(state,"sitejabber",author,date,"INT","",issue,txt,url):
                        count += 1
                time.sleep(random.uniform(1.5,2.5))
            except Exception as e:
                log(f"  Sitejabber {site} p{page} err: {e}")
                break
    log(f"Sitejabber: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 6: YOUTUBE (se API key disponibile)
# ────────────────────────────────────────
def run_youtube(state, config):
    key = config.get("youtube_api_key","")
    if not key or not config["sources_enabled"].get("youtube"): return
    log("YouTube: avvio")
    count = 0
    queries = [
        # ── VINTED ──
        "vinted scam","vinted fake","vinted fraud","vinted empty box","vinted dispute",
        "vinted truffa","vinted pacco vuoto","vinted falso","vinted non arrivato",
        "vinted arnaque","vinted colis vide","vinted jamais reçu","vinted escroquerie",
        "vinted betrug","vinted gefälscht","vinted nie angekommen",
        "vinted estafa","vinted nunca llegó","vinted me estafaron",
        "vinted oszustwo","vinted nie dotarło",
        # ── EBAY ──
        "ebay scam","ebay fake item","ebay fraud","ebay empty box","ebay dispute",
        "ebay counterfeit","ebay not received","ebay seller scam",
        "ebay truffa","ebay falso","ebay non arrivato",
        "ebay arnaque","ebay colis vide","ebay jamais reçu",
        "ebay betrug","ebay gefälscht","ebay nie angekommen",
        "ebay estafa","ebay nunca llegó",
        # ── DEPOP ──
        "depop scam","depop fake","depop fraud","depop not received","depop counterfeit",
        "depop truffa","depop arnaque","depop betrug",
        # ── FACEBOOK MARKETPLACE ──
        "facebook marketplace scam","facebook marketplace fake","facebook marketplace fraud",
        "marketplace truffa","marketplace arnaque","marketplace betrug","marketplace estafa",
        # ── WALLAPOP ──
        "wallapop estafa","wallapop scam","wallapop fraude","wallapop arnaque",
        "wallapop truffa","wallapop betrug",
        # ── SUBITO.IT ──
        "subito truffa","subito.it truffa","subito falso","subito fregato",
        # ── LEBONCOIN ──
        "leboncoin arnaque","leboncoin escroquerie","leboncoin fraude","leboncoin colis vide",
        # ── KLEINANZEIGEN ──
        "kleinanzeigen betrug","kleinanzeigen gefälscht","kleinanzeigen abzocke",
    ]
    for query in queries:
        try:
            r = requests.get("https://www.googleapis.com/youtube/v3/search", params={
                "q": query, "type": "video", "maxResults": 50,
                "key": key, "part": "snippet", "order": "relevance",
                "publishedAfter": "2022-01-01T00:00:00Z"
            }, timeout=15)
            if r.status_code != 200: continue
            videos = r.json().get("items",[])
            for video in videos:
                vid_id = video["id"].get("videoId","")
                if not vid_id: continue
                snip = video.get("snippet",{})
                vid_url = f"https://youtube.com/watch?v={vid_id}"
                # Salva titolo + descrizione del video come record
                vid_title = snip.get("title","")
                vid_desc = snip.get("description","")
                vid_txt = f"{vid_title} {vid_desc}".strip()
                vid_date = snip.get("publishedAt","")[:10]
                vid_author = snip.get("channelTitle","anon")
                if len(vid_txt.split()) >= MIN_WORDS:
                    vid_issue = classify(vid_txt)
                    if vid_issue != "altro":
                        add(state,"youtube_video",vid_author,vid_date,"INT","",vid_issue,vid_txt,vid_url)
                        count += 1
                # Commenti
                next_page = None
                pages = 0
                while pages < 5:
                    params = {
                        "videoId": vid_id, "part": "snippet",
                        "maxResults": 100, "key": key,
                        "textFormat": "plainText", "order": "relevance"
                    }
                    if next_page: params["pageToken"] = next_page
                    cr = requests.get("https://www.googleapis.com/youtube/v3/commentThreads",
                                     params=params, timeout=15)
                    if cr.status_code != 200: break
                    cdata = cr.json()
                    for item in cdata.get("items",[]):
                        comment = item["snippet"]["topLevelComment"]["snippet"]
                        txt = comment.get("textDisplay","")
                        if len(txt.split()) < MIN_WORDS: continue
                        issue = classify(txt)
                        if issue == "altro": continue
                        date = comment.get("publishedAt","")[:10]
                        author = comment.get("authorDisplayName","anon")
                        if add(state,"youtube_comment",author,date,"INT","",issue,txt,vid_url):
                            count += 1
                    next_page = cdata.get("nextPageToken")
                    if not next_page: break
                    pages += 1
                    time.sleep(0.3)
            time.sleep(1)
        except Exception as e:
            log(f"  YouTube err '{query}': {e}")
    log(f"YouTube: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 7: TRUSTPILOT (se API key disponibile)
# ────────────────────────────────────────
def run_trustpilot(state, config):
    key = config.get("trustpilot_api_key","")
    if not key or not config["sources_enabled"].get("trustpilot"): return
    log("Trustpilot: avvio")
    count = 0
    # Trova business unit ID di Vinted
    domains = ["vinted.it","vinted.fr","vinted.co.uk","vinted.de","vinted.es",
               "vinted.be","vinted.nl","vinted.pl","vinted.com"]
    country_map = {"vinted.it":"IT","vinted.fr":"FR","vinted.co.uk":"GB","vinted.de":"DE",
                   "vinted.es":"ES","vinted.be":"BE","vinted.nl":"NL","vinted.pl":"PL","vinted.com":"INT"}
    headers = {"apikey": key}

    for domain in domains:
        cc = country_map.get(domain,"INT")
        try:
            # Cerca business unit
            r = requests.get("https://api.trustpilot.com/v1/business-units/find",
                           params={"name": domain}, headers=headers, timeout=15)
            if r.status_code != 200: continue
            bu_id = r.json().get("id","")
            if not bu_id: continue

            # Scarica recensioni paginate
            page = 1
            while True:
                rev_r = requests.get(f"https://api.trustpilot.com/v1/business-units/{bu_id}/reviews",
                    params={"page":page,"perPage":100,"stars":"1,2","orderBy":"createdat.desc"},
                    headers=headers, timeout=15)
                if rev_r.status_code != 200: break
                data = rev_r.json()
                revs = data.get("reviews",[])
                if not revs: break
                for rev in revs:
                    txt = rev.get("text","")
                    if len(txt) < 15: continue
                    issue = classify(txt)
                    date = rev.get("createdAt","")[:10]
                    author = rev.get("consumer",{}).get("displayName","anon")
                    stars = rev.get("stars",1)
                    if add(state,"trustpilot",author,date,cc,stars,issue,txt,
                           f"https://trustpilot.com/review/{domain}"):
                        count += 1
                if page >= data.get("totalPages",1): break
                page += 1
                time.sleep(0.5)
        except Exception as e:
            log(f"  Trustpilot {domain} err: {e}")

    log(f"Trustpilot: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 8: TWITTER/X (se Bearer Token disponibile)
# ────────────────────────────────────────
def run_twitter(state, config):
    token = config.get("twitter_bearer_token","")
    if not token or not config["sources_enabled"].get("twitter"): return
    log("Twitter: avvio")
    count = 0
    headers = {"Authorization": f"Bearer {token}"}
    queries = [
        # EN
        "vinted scam","vinted fake","vinted fraud","vinted empty box",
        "vinted wrong item","vinted not received","vinted lost dispute","vinted no refund",
        # IT
        "vinted truffa","vinted pacco vuoto","vinted falso","vinted non arrivato",
        "vinted rimborso negato","vinted truffato","vinted fregato",
        # FR
        "vinted arnaque","vinted colis vide","vinted jamais reçu","vinted arnaqué",
        "vinted remboursement refusé","vinted litige perdu",
        # DE
        "vinted betrug","vinted nie angekommen","vinted gefälscht","vinted abzocke",
        # ES
        "vinted estafa","vinted nunca llegó","vinted me estafaron","vinted fraude",
        # PL
        "vinted oszustwo","vinted nie dotarło","vinted podróbka","vinted zostałem oszukany"
    ]
    for query in queries:
        try:
            r = requests.get("https://api.twitter.com/2/tweets/search/recent",
                params={
                    "query": f"{query} -is:retweet",
                    "max_results": 100,
                    "tweet.fields": "created_at,author_id,lang",
                    "expansions": "author_id",
                    "user.fields": "username"
                },
                headers=headers, timeout=15)
            if r.status_code != 200: continue
            data = r.json()
            users = {u["id"]:u["username"] for u in data.get("includes",{}).get("users",[])}
            for tweet in data.get("data",[]):
                txt = tweet.get("text","")
                if len(txt) < 20: continue
                issue = classify(txt)
                if issue == "altro": continue
                date = tweet.get("created_at","")[:10]
                author = users.get(tweet.get("author_id",""),"anon")
                lang = tweet.get("lang","")
                cc = {"it":"IT","fr":"FR","de":"DE","es":"ES","nl":"NL","pl":"PL"}.get(lang,"INT")
                if add(state,"twitter",author,date,cc,"",issue,txt,
                       f"https://twitter.com/i/web/status/{tweet['id']}"):
                    count += 1
            time.sleep(2)
        except Exception as e:
            log(f"  Twitter err '{query}': {e}")
    log(f"Twitter: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 9: TIKTOK (pubblico, nessun login)
# ────────────────────────────────────────
def run_tiktok(state, config):
    if not config["sources_enabled"].get("tiktok"): return
    log("TikTok: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        "Referer": "https://www.tiktok.com/",
    }

    hashtags = [
        "vintedscam","vintedtruffa","vintedfraud","vintedproblemi",
        "vintedarnaque","vintedfake","vintedescroquerie","vintedbetrug",
        "vintedproblem","vintedreview","vintedfail","vintedcomplaints"
    ]
    for tag in hashtags:
        try:
            r = requests.get(f"https://www.tiktok.com/tag/{tag}",
                            headers=headers, timeout=20)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for script in soup.find_all("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__"):
                try:
                    data = json.loads(script.string)
                    items = (data.get("__DEFAULT_SCOPE__",{})
                               .get("webapp.challenge-detail",{})
                               .get("itemList",[]))
                    for item in items:
                        desc = item.get("desc","")
                        if len(desc) < 20: continue
                        issue = classify(desc)
                        author = item.get("author",{}).get("uniqueId","anon")
                        ts = item.get("createTime",0)
                        date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d") if ts else datetime.now().strftime("%Y-%m-%d")
                        vid_id = item.get("id","")
                        vid_url = f"https://www.tiktok.com/@{author}/video/{vid_id}" if vid_id else f"https://www.tiktok.com/tag/{tag}"
                        if add(state,"tiktok",author,date,"INT","",issue,desc,vid_url):
                            count += 1
                except Exception as e:
                    log(f"  TikTok parse {tag}: {e}")
            time.sleep(random.uniform(2,4))
        except Exception as e:
            log(f"  TikTok err {tag}: {e}")

    keywords = [
        "vinted scam","vinted truffa","vinted fake","vinted arnaque",
        "vinted fraud","vinted pacco vuoto","vinted betrug","vinted dispute"
    ]
    for kw in keywords:
        try:
            r = requests.get("https://www.tiktok.com/search",
                            params={"q": kw}, headers=headers, timeout=20)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for script in soup.find_all("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__"):
                try:
                    data = json.loads(script.string)
                    items = (data.get("__DEFAULT_SCOPE__",{})
                               .get("webapp.search-result-list",{})
                               .get("itemList",[]))
                    for item in items:
                        desc = item.get("desc","")
                        if len(desc) < 20: continue
                        issue = classify(desc)
                        author = item.get("author",{}).get("uniqueId","anon")
                        ts = item.get("createTime",0)
                        date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d") if ts else datetime.now().strftime("%Y-%m-%d")
                        vid_id = item.get("id","")
                        vid_url = f"https://www.tiktok.com/@{author}/video/{vid_id}" if vid_id else ""
                        if add(state,"tiktok",author,date,"INT","",issue,desc,vid_url):
                            count += 1
                except: pass
            time.sleep(random.uniform(2,4))
        except Exception as e:
            log(f"  TikTok search '{kw}': {e}")

    log(f"TikTok: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 10: FACEBOOK (richiede cookie sessione)
# ────────────────────────────────────────
def run_facebook(state, config):
    cookies_str = config.get("facebook_cookies","")
    if not cookies_str or not config["sources_enabled"].get("facebook",False): return
    log("Facebook: avvio")
    count = 0
    # Parsing cookie string formato "nome=valore; nome2=valore2"
    cookies = {}
    for part in cookies_str.split(";"):
        part = part.strip()
        if "=" in part:
            k,v = part.split("=",1)
            cookies[k.strip()] = v.strip()

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
    }
    # Gruppi pubblici Vinted su Facebook
    groups = [
        ("groups/vinteditaliaofficial","IT"),
        ("groups/vinteduk","GB"),
        ("groups/vintedfrance","FR"),
        ("groups/vintedgermany","DE"),
    ]
    for group_path, cc in groups:
        try:
            r = requests.get(f"https://www.facebook.com/{group_path}",
                            headers=headers, cookies=cookies, timeout=20)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for el in soup.find_all(attrs={"data-testid": "post_message"}):
                txt = el.get_text(separator=" ", strip=True)
                if len(txt) < 30: continue
                issue = classify(txt)
                if add(state,"facebook","anon",datetime.now().strftime("%Y-%m-%d"),cc,"",issue,txt,
                       f"https://facebook.com/{group_path}"):
                    count += 1
            time.sleep(random.uniform(3,6))
        except Exception as e:
            log(f"  Facebook {group_path}: {e}")
    log(f"Facebook: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 11: CONSUMERAFFAIRS
# ────────────────────────────────────────
def run_consumeraffairs(state, config):
    if not config["sources_enabled"].get("consumeraffairs", True): return
    log("ConsumerAffairs: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for page in range(1, 21):
        try:
            url = f"https://www.consumeraffairs.com/online/vinted.html?page={page}"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: break
            soup = BeautifulSoup(r.text, "lxml")
            reviews = soup.find_all("div", class_=lambda c: c and "rvw" in (c or ""))
            if not reviews:
                reviews = soup.find_all(attrs={"itemprop": "review"})
            found = 0
            for rev in reviews:
                txt_el = rev.find(attrs={"itemprop": "reviewBody"}) or rev.find("p")
                if not txt_el: continue
                txt = txt_el.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                author_el = rev.find(attrs={"itemprop": "author"}) or rev.find(class_=lambda c: c and "name" in (c or ""))
                author = author_el.get_text(strip=True)[:80] if author_el else "anon"
                date_el = rev.find("time") or rev.find(attrs={"itemprop": "datePublished"})
                if date_el:
                    date_str = date_el.get("datetime", "") or date_el.get("content", "") or date_el.get_text(strip=True)
                    date = date_str[:10] if len(date_str) >= 10 else datetime.now().strftime("%Y-%m-%d")
                else:
                    date = datetime.now().strftime("%Y-%m-%d")
                issue = classify(txt)
                if add(state, "consumeraffairs", author, date, "US", "", issue, txt, url):
                    count += 1
                    found += 1
            if found == 0: break
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as e:
            log(f"  ConsumerAffairs p{page} err: {e}")
            break
    log(f"ConsumerAffairs: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 12: MONEYSAVINGEXPERT FORUM (UK)
# ────────────────────────────────────────
def run_mse(state, config):
    if not config["sources_enabled"].get("mse", True): return
    log("MoneySavingExpert Forum: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }
    keywords = ["vinted scam","vinted fake","vinted fraud","vinted empty","vinted not received","vinted dispute"]
    for kw in keywords:
        try:
            url = f"https://forums.moneysavingexpert.com/search?Search={kw.replace(' ','+')}&Type=Post"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for post in soup.find_all("div", class_=lambda c: c and "post" in (c or "").lower()):
                txt_el = post.find("div", class_=lambda c: c and "content" in (c or "").lower()) or post.find("p")
                if not txt_el: continue
                txt = txt_el.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                author_el = post.find(class_=lambda c: c and ("author" in (c or "") or "username" in (c or "")))
                author = author_el.get_text(strip=True)[:80] if author_el else "anon"
                date_el = post.find("time")
                date = date_el.get("datetime","")[:10] if date_el else datetime.now().strftime("%Y-%m-%d")
                issue = classify(txt)
                if add(state, "mse_forum", author, date, "GB", "", issue, txt, url):
                    count += 1
            time.sleep(random.uniform(2, 3))
        except Exception as e:
            log(f"  MSE err '{kw}': {e}")
    log(f"MSE Forum: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 13: REVIEWS.IO
# ────────────────────────────────────────
def run_reviewsio(state, config):
    if not config["sources_enabled"].get("reviewsio", True): return
    log("Reviews.io: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    # Reviews.io API pubblica (no auth richiesta per lettura)
    try:
        r = requests.get("https://api.reviews.io/company/reviews", params={
            "store": "vinted.com", "per_page": 100, "page": 1,
            "min_rating": 1, "max_rating": 2
        }, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            reviews = data.get("reviews", {}).get("data", [])
            for rev in reviews:
                txt = rev.get("comments", "")
                if len(txt.split()) < MIN_WORDS: continue
                author = rev.get("reviewer", {}).get("first_name", "anon")
                date = rev.get("date_created", "")[:10]
                country = rev.get("reviewer", {}).get("country", "INT")
                rating = rev.get("rating", "")
                issue = classify(txt)
                if add(state, "reviewsio", author, date, country, rating, issue, txt,
                       "https://www.reviews.io/company-reviews/store/vinted.com"):
                    count += 1
    except Exception as e:
        log(f"  Reviews.io err: {e}")
    log(f"Reviews.io: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 14: TIKTOK (potenziato, tutte le piattaforme)
# ────────────────────────────────────────
def run_tiktok_v2(state, config):
    if not config["sources_enabled"].get("tiktok", True): return
    log("TikTok: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8,fr;q=0.7",
        "Referer": "https://www.tiktok.com/",
    }
    hashtags = [
        # Vinted
        "vintedscam","vintedtruffa","vintedfraud","vintedarnaque","vintedbetrug",
        "vintedestafa","vintedproblemi","vintedfail","vintedfake",
        # eBay
        "ebayscam","ebayfraud","ebaytruffa","ebayarnaque","ebaybetrug",
        # Depop
        "depopscam","depopfraud","depoptruffa","depopfake",
        # Marketplace generico
        "marketplacescam","marketplacetruffa","onlineshoppingscam",
        "secondhandscam","resellscam","fakeseller",
        # Wallapop
        "wallapopestafa","wallapopscam","wallapopfraude",
        # Subito
        "subitotruffa","subitofake",
        # Leboncoin
        "leboncoinarnaque","leboncoinescroquerie",
        # Kleinanzeigen
        "kleinanzeigenbetrug",
    ]
    for tag in hashtags:
        try:
            r = requests.get(f"https://www.tiktok.com/tag/{tag}",
                            headers=headers, timeout=20)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for script in soup.find_all("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__"):
                try:
                    data = json.loads(script.string)
                    items = (data.get("__DEFAULT_SCOPE__",{})
                               .get("webapp.challenge-detail",{})
                               .get("itemList",[]))
                    for item in items:
                        desc = item.get("desc","")
                        if len(desc.split()) < MIN_WORDS: continue
                        issue = classify(desc)
                        if issue == "altro": continue
                        author = item.get("author",{}).get("uniqueId","anon")
                        ts = item.get("createTime",0)
                        date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d") if ts else datetime.now().strftime("%Y-%m-%d")
                        vid_id = item.get("id","")
                        vid_url = f"https://www.tiktok.com/@{author}/video/{vid_id}"
                        if add(state,"tiktok",author,date,"INT","",issue,desc,vid_url):
                            count += 1
                except: pass
            time.sleep(random.uniform(3,5))
        except Exception as e:
            log(f"  TikTok {tag}: {e}")

    # Keyword search
    kws = [
        "vinted scam","vinted truffa","vinted arnaque","vinted betrug","vinted estafa",
        "ebay scam","ebay truffa","ebay fake","depop scam","depop fake",
        "facebook marketplace scam","wallapop estafa","subito truffa",
        "leboncoin arnaque","kleinanzeigen betrug",
    ]
    for kw in kws:
        try:
            r = requests.get("https://www.tiktok.com/search",
                            params={"q": kw}, headers=headers, timeout=20)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for script in soup.find_all("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__"):
                try:
                    data = json.loads(script.string)
                    items = (data.get("__DEFAULT_SCOPE__",{})
                               .get("webapp.search-result-list",{})
                               .get("itemList",[]))
                    for item in items:
                        desc = item.get("desc","")
                        if len(desc.split()) < MIN_WORDS: continue
                        issue = classify(desc)
                        if issue == "altro": continue
                        author = item.get("author",{}).get("uniqueId","anon")
                        ts = item.get("createTime",0)
                        date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d") if ts else datetime.now().strftime("%Y-%m-%d")
                        vid_id = item.get("id","")
                        vid_url = f"https://www.tiktok.com/@{author}/video/{vid_id}" if vid_id else ""
                        if add(state,"tiktok",author,date,"INT","",issue,desc,vid_url):
                            count += 1
                except: pass
            time.sleep(random.uniform(2,4))
        except Exception as e:
            log(f"  TikTok kw '{kw}': {e}")

    log(f"TikTok: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 15: INSTAGRAM (hashtag pubblici)
# ────────────────────────────────────────
def run_instagram(state, config):
    if not config["sources_enabled"].get("instagram", True): return
    log("Instagram: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
    }
    hashtags = [
        "vintedscam","vintedtruffa","vintedfraud","vintedarnaque","vintedbetrug",
        "ebayscam","ebayfraud","ebaytruffa","ebayfake",
        "depopscam","depopfraud","depoptruffa",
        "marketplacescam","fakeseller","onlinescam","secondhandscam",
        "wallapopestafa","subitotruffa","leboncoinarnaque",
    ]
    for tag in hashtags:
        try:
            r = requests.get(f"https://www.instagram.com/explore/tags/{tag}/",
                            headers=headers, timeout=15)
            if r.status_code != 200: continue
            # Estrai JSON embeddato nella pagina
            import re
            matches = re.findall(r'"text":"((?:[^"\\]|\\.)*)"', r.text)
            for txt_raw in matches:
                try:
                    txt = txt_raw.encode().decode('unicode_escape')
                except:
                    txt = txt_raw
                txt = txt.replace("\\n"," ").strip()
                if len(txt.split()) < MIN_WORDS: continue
                if len(txt) > 600: txt = txt[:600]
                issue = classify(txt)
                if issue == "altro": continue
                if add(state,"instagram","anon",datetime.now().strftime("%Y-%m-%d"),"INT","",issue,txt,
                       f"https://instagram.com/explore/tags/{tag}/"):
                    count += 1
            time.sleep(random.uniform(4,7))
        except Exception as e:
            log(f"  Instagram {tag}: {e}")
    log(f"Instagram: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 16: QUORA (Q&A pubblico)
# ────────────────────────────────────────
def run_quora(state, config):
    if not config["sources_enabled"].get("quora", True): return
    log("Quora: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    queries = [
        "vinted scam","vinted fake","vinted fraud","vinted empty box",
        "ebay scam","ebay fake item","ebay fraud",
        "depop scam","depop fake",
        "facebook marketplace scam","wallapop estafa","subito truffa",
    ]
    for q in queries:
        try:
            r = requests.get("https://www.quora.com/search",
                            params={"q": q, "type": "answer"},
                            headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for answer in soup.find_all("div", class_=lambda c: c and "q-box" in (c or "")):
                txt = answer.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                issue = classify(txt)
                if issue == "altro": continue
                if add(state,"quora","anon",datetime.now().strftime("%Y-%m-%d"),"INT","",issue,txt):
                    count += 1
            time.sleep(random.uniform(2,4))
        except Exception as e:
            log(f"  Quora '{q}': {e}")
    log(f"Quora: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 17: TWITTER/X (v2 con fix)
# ────────────────────────────────────────
def run_twitter_v2(state, config):
    token = config.get("twitter_bearer_token","")
    if not token or not config["sources_enabled"].get("twitter", True): return
    log("Twitter: avvio")
    count = 0
    headers = {"Authorization": f"Bearer {token}"}
    queries = [
        # Vinted — 6 lingue
        "vinted scam -is:retweet lang:en",
        "vinted fake -is:retweet lang:en",
        "vinted fraud -is:retweet lang:en",
        "vinted truffa -is:retweet lang:it",
        "vinted falso -is:retweet lang:it",
        "vinted pacco vuoto -is:retweet lang:it",
        "vinted arnaque -is:retweet lang:fr",
        "vinted colis vide -is:retweet lang:fr",
        "vinted betrug -is:retweet lang:de",
        "vinted gefälscht -is:retweet lang:de",
        "vinted estafa -is:retweet lang:es",
        "vinted oszustwo -is:retweet lang:pl",
        # eBay
        "ebay scam -is:retweet lang:en",
        "ebay fake -is:retweet lang:en",
        "ebay truffa -is:retweet lang:it",
        "ebay arnaque -is:retweet lang:fr",
        "ebay betrug -is:retweet lang:de",
        "ebay estafa -is:retweet lang:es",
        # Depop
        "depop scam -is:retweet lang:en",
        "depop fake -is:retweet lang:en",
        "depop truffa -is:retweet lang:it",
        # Facebook Marketplace
        "facebook marketplace scam -is:retweet lang:en",
        "marketplace truffa -is:retweet lang:it",
        "marketplace arnaque -is:retweet lang:fr",
        "marketplace betrug -is:retweet lang:de",
        "marketplace estafa -is:retweet lang:es",
        # Wallapop
        "wallapop estafa -is:retweet lang:es",
        "wallapop scam -is:retweet lang:en",
        # Subito / Leboncoin / Kleinanzeigen
        "subito truffa -is:retweet lang:it",
        "leboncoin arnaque -is:retweet lang:fr",
        "kleinanzeigen betrug -is:retweet lang:de",
    ]
    random.shuffle(queries)
    for query in queries:
        try:
            r = requests.get("https://api.twitter.com/2/tweets/search/recent",
                params={
                    "query": query,
                    "max_results": 100,
                    "tweet.fields": "created_at,author_id,lang",
                    "expansions": "author_id",
                    "user.fields": "username"
                },
                headers=headers, timeout=15)
            if r.status_code == 429:
                time.sleep(60); continue
            if r.status_code != 200: continue
            data = r.json()
            users = {u["id"]:u["username"] for u in data.get("includes",{}).get("users",[])}
            for tweet in data.get("data",[]):
                txt = tweet.get("text","")
                if len(txt.split()) < MIN_WORDS: continue
                issue = classify(txt)
                if issue == "altro": continue
                date = tweet.get("created_at","")[:10]
                author = users.get(tweet.get("author_id",""),"anon")
                lang = tweet.get("lang","")
                cc = {"it":"IT","fr":"FR","de":"DE","es":"ES","nl":"NL","pl":"PL"}.get(lang,"INT")
                if add(state,"twitter",author,date,cc,"",issue,txt,
                       f"https://twitter.com/i/web/status/{tweet['id']}"):
                    count += 1
            time.sleep(3)
        except Exception as e:
            log(f"  Twitter '{query[:40]}': {e}")
    log(f"Twitter: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 18: WYKOP.PL (Reddit polacco)
# ────────────────────────────────────────
def run_wykop(state, config):
    if not config["sources_enabled"].get("wykop", True): return
    log("Wykop: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    keywords = [
        "vinted oszustwo","vinted podróbka","vinted nie dotarło","vinted pusta paczka",
        "ebay oszustwo","depop oszustwo","allegro oszustwo","olx oszustwo",
    ]
    for kw in keywords:
        try:
            r = requests.get("https://wykop.pl/szukaj/wyniki",
                            params={"q": kw, "type": "entries"},
                            headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for entry in soup.find_all("div", class_=lambda c: c and "entry" in (c or "").lower()):
                txt_el = entry.find("p") or entry.find("div", class_=lambda c: c and "content" in (c or ""))
                if not txt_el: continue
                txt = txt_el.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                issue = classify(txt)
                if issue == "altro": continue
                author_el = entry.find(class_=lambda c: c and "author" in (c or "").lower())
                author = author_el.get_text(strip=True)[:80] if author_el else "anon"
                date_el = entry.find("time")
                date = date_el.get("datetime","")[:10] if date_el else datetime.now().strftime("%Y-%m-%d")
                if add(state,"wykop",author,date,"PL","",issue,txt):
                    count += 1
            time.sleep(random.uniform(2,3))
        except Exception as e:
            log(f"  Wykop '{kw}': {e}")
    log(f"Wykop: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 19: GUTEFRAGE.NET (forum tedesco)
# ────────────────────────────────────────
def run_gutefrage(state, config):
    if not config["sources_enabled"].get("gutefrage", True): return
    log("Gutefrage: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "de-DE,de;q=0.9",
    }
    keywords = [
        "vinted betrug","vinted gefälscht","vinted nicht erhalten","vinted fake",
        "ebay betrug","ebay gefälscht","ebay fake","ebay abzocke",
        "kleinanzeigen betrug","kleinanzeigen fake","kleinanzeigen abzocke",
        "facebook marketplace betrug","wallapop betrug","fake pakete",
    ]
    for kw in keywords:
        try:
            r = requests.get(f"https://www.gutefrage.net/suche/{requests.utils.quote(kw)}",
                            headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            # Gutefrage usa data-testid per i risultati
            for item in soup.find_all(attrs={"data-testid": True}):
                txt = item.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                if len(txt) > 600: txt = txt[:600]
                issue = classify(txt)
                if issue == "altro": continue
                date_el = item.find("time")
                date = date_el.get("datetime","")[:10] if date_el else datetime.now().strftime("%Y-%m-%d")
                if add(state,"gutefrage","anon",date,"DE","",issue,txt,
                       f"https://www.gutefrage.net/suche/{requests.utils.quote(kw)}"):
                    count += 1
            time.sleep(random.uniform(2,3))
        except Exception as e:
            log(f"  Gutefrage '{kw}': {e}")
    log(f"Gutefrage: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 20: FOROCOCHES (forum spagnolo)
# ────────────────────────────────────────
def run_forocoches(state, config):
    if not config["sources_enabled"].get("forocoches", True): return
    log("Forocoches: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "es-ES,es;q=0.9",
    }
    keywords = [
        "vinted estafa","vinted fraude","vinted me han estafado",
        "ebay estafa","wallapop estafa","facebook marketplace estafa",
        "milanuncios estafa","compra segunda mano fraude",
    ]
    for kw in keywords:
        try:
            r = requests.get("https://www.forocoches.com/foro/search.php",
                            params={"searchid": "1", "query": kw, "titleonly": "0"},
                            headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for post in soup.find_all("div", class_=lambda c: c and "post" in (c or "").lower()):
                txt_el = post.find("div", class_=lambda c: c and "content" in (c or "")) or post.find("p")
                if not txt_el: continue
                txt = txt_el.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                issue = classify(txt)
                if issue == "altro": continue
                date_el = post.find("time") or post.find(class_=lambda c: c and "date" in (c or "").lower())
                date = date_el.get("datetime","")[:10] if date_el and date_el.get("datetime") else datetime.now().strftime("%Y-%m-%d")
                if add(state,"forocoches","anon",date,"ES","",issue,txt):
                    count += 1
            time.sleep(random.uniform(2,4))
        except Exception as e:
            log(f"  Forocoches '{kw}': {e}")
    log(f"Forocoches: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 21: GOOGLE NEWS RSS (multilingua, no blocco)
# ────────────────────────────────────────
def run_google_news(state, config):
    if not config["sources_enabled"].get("google_news", True): return
    log("Google News: avvio")
    import xml.etree.ElementTree as ET
    count = 0
    queries = [
        # Vinted
        "vinted scam","vinted truffa","vinted arnaque","vinted betrug","vinted estafa",
        "vinted fake","vinted fraud","vinted oszustwo",
        # eBay
        "ebay scam","ebay truffa","ebay arnaque","ebay betrug","ebay estafa","ebay fraud",
        # Depop
        "depop scam","depop fraud","depop fake","depop truffa",
        # Marketplace
        "facebook marketplace scam","marketplace truffa","marketplace arnaque",
        "wallapop estafa","subito truffa","leboncoin arnaque","kleinanzeigen betrug",
        # Generico
        "online marketplace scam","second hand scam","vendita online truffa",
        "achat revente arnaque","Kleinanzeigen Betrug pakete","fake online seller",
    ]
    headers = {"User-Agent": "Mozilla/5.0 (compatible; RSS reader)"}
    for q in queries:
        try:
            url = f"https://news.google.com/rss/search?q={requests.utils.quote(q)}&hl=en&gl=US&ceid=US:en"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: continue
            root = ET.fromstring(r.content)
            for item in root.findall(".//item"):
                title = item.findtext("title","")
                desc  = item.findtext("description","")
                # Pulisci HTML tags dalla description
                desc = BeautifulSoup(desc, "lxml").get_text(separator=" ", strip=True) if desc else ""
                txt = f"{title} {desc}".strip()
                if len(txt.split()) < MIN_WORDS: continue
                issue = classify(txt)
                if issue == "altro": continue
                pub = item.findtext("pubDate","")
                try:
                    from email.utils import parsedate_to_datetime
                    date = parsedate_to_datetime(pub).strftime("%Y-%m-%d") if pub else datetime.now().strftime("%Y-%m-%d")
                except:
                    date = datetime.now().strftime("%Y-%m-%d")
                link = item.findtext("link","")
                if add(state,"google_news","news",date,"INT","",issue,txt,link):
                    count += 1
            time.sleep(random.uniform(1,2))
        except Exception as e:
            log(f"  GoogleNews '{q}': {e}")
    log(f"Google News: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 22: FORUM ITALIANI (hwupgrade, tom's hardware IT)
# ────────────────────────────────────────
def run_forum_it(state, config):
    if not config["sources_enabled"].get("forum_it", True): return
    log("Forum IT: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "it-IT,it;q=0.9",
    }
    searches = [
        ("https://www.hwupgrade.it/forum/search.php?do=process&query={q}&titleonly=0&showposts=1", "hwupgrade"),
        ("https://www.tomshw.it/forum/search.php?do=process&query={q}&titleonly=0&showposts=1", "tomshw"),
    ]
    keywords = [
        "vinted truffa","vinted falso","vinted non arrivato","vinted fregato",
        "ebay truffa","ebay falso","subito truffa","subito falso",
        "facebook marketplace truffa","vendita online truffa","pacco vuoto truffa",
    ]
    for kw in keywords:
        for url_tpl, forum_name in searches:
            try:
                r = requests.get(url_tpl.format(q=requests.utils.quote(kw)),
                                headers=headers, timeout=15)
                if r.status_code != 200: continue
                soup = BeautifulSoup(r.text, "lxml")
                for post in soup.find_all(["div","td"], class_=lambda c: c and ("post" in (c or "") or "message" in (c or ""))):
                    txt = post.get_text(separator=" ", strip=True)
                    if len(txt.split()) < MIN_WORDS: continue
                    if len(txt) > 600: txt = txt[:600]
                    issue = classify(txt)
                    if issue == "altro": continue
                    date_el = post.find("time") or post.find(class_=lambda c: c and "date" in (c or ""))
                    date = date_el.get("datetime","")[:10] if date_el and date_el.get("datetime") else datetime.now().strftime("%Y-%m-%d")
                    if add(state,"forum_it","anon",date,"IT","",issue,txt):
                        count += 1
                time.sleep(random.uniform(2,3))
            except Exception as e:
                log(f"  Forum IT {forum_name} '{kw}': {e}")
    log(f"Forum IT: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 23: FORUM FRANCESI (commentcamarche, futura-sciences)
# ────────────────────────────────────────
def run_forum_fr(state, config):
    if not config["sources_enabled"].get("forum_fr", True): return
    log("Forum FR: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9",
    }
    keywords = [
        "vinted arnaque","vinted colis vide","vinted escroquerie","vinted jamais reçu",
        "ebay arnaque","leboncoin arnaque","leboncoin escroquerie",
        "facebook marketplace arnaque","vinted litige perdu","achat en ligne arnaque",
    ]
    for kw in keywords:
        try:
            url = f"https://forums.commentcamarche.net/search/?q={requests.utils.quote(kw)}"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for post in soup.find_all(["article","div"], class_=lambda c: c and ("post" in (c or "") or "message" in (c or "") or "result" in (c or ""))):
                txt = post.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                if len(txt) > 600: txt = txt[:600]
                issue = classify(txt)
                if issue == "altro": continue
                date_el = post.find("time")
                date = date_el.get("datetime","")[:10] if date_el and date_el.get("datetime") else datetime.now().strftime("%Y-%m-%d")
                if add(state,"forum_fr","anon",date,"FR","",issue,txt,url):
                    count += 1
            time.sleep(random.uniform(2,3))
        except Exception as e:
            log(f"  Forum FR '{kw}': {e}")
    log(f"Forum FR: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 24: FORUM TEDESCHI (gutefrage, forum.chip.de)
# ────────────────────────────────────────
def run_forum_de(state, config):
    if not config["sources_enabled"].get("forum_de", True): return
    log("Forum DE: avvio")
    count = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept-Language": "de-DE,de;q=0.9",
    }
    keywords = [
        "vinted betrug","vinted gefälscht","vinted nie angekommen","vinted abzocke",
        "ebay betrug","ebay gefälscht","kleinanzeigen betrug","kleinanzeigen fake",
        "facebook marketplace betrug","online kauf betrug paket",
    ]
    for kw in keywords:
        try:
            url = f"https://forum.chip.de/search?q={requests.utils.quote(kw)}"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "lxml")
            for post in soup.find_all(["article","div","li"], class_=lambda c: c and ("post" in (c or "") or "message" in (c or "") or "result" in (c or ""))):
                txt = post.get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                if len(txt) > 600: txt = txt[:600]
                issue = classify(txt)
                if issue == "altro": continue
                date_el = post.find("time")
                date = date_el.get("datetime","")[:10] if date_el and date_el.get("datetime") else datetime.now().strftime("%Y-%m-%d")
                if add(state,"forum_de","anon",date,"DE","",issue,txt,url):
                    count += 1
            time.sleep(random.uniform(2,3))
        except Exception as e:
            log(f"  Forum DE '{kw}': {e}")
    log(f"Forum DE: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 25: HACKER NEWS (Algolia API)
# ────────────────────────────────────────
def run_hackernews(state, config):
    if not config["sources_enabled"].get("hackernews", True): return
    log("Hacker News: avvio")
    count = 0
    queries = [
        "vinted scam", "vinted fraud", "vinted fake", "vinted problem",
        "vinted truffa", "vinted arnaque", "vinted betrug",
        "ebay scam", "ebay fraud", "ebay fake seller",
        "depop scam", "depop fraud", "depop fake",
        "facebook marketplace scam", "marketplace fraud",
        "wallapop scam", "second hand scam", "c2c fraud",
        "online marketplace scam", "fake online seller",
        "kleinanzeigen scam", "leboncoin arnaque",
    ]
    for q in queries:
        try:
            r = requests.get("https://hn.algolia.com/api/v1/search",
                params={"query": q, "tags": "comment", "hitsPerPage": 100},
                timeout=15)
            if r.status_code != 200: continue
            for hit in r.json().get("hits", []):
                raw = hit.get("comment_text") or hit.get("story_text") or hit.get("title") or ""
                txt = BeautifulSoup(raw, "lxml").get_text(separator=" ", strip=True)
                if len(txt.split()) < MIN_WORDS: continue
                if not is_negative(txt): continue
                issue = classify(txt)
                if issue == "altro": continue
                author = hit.get("author", "anon")
                date   = hit.get("created_at", "")[:10]
                url    = f"https://news.ycombinator.com/item?id={hit.get('objectID','')}"
                if add(state, "hackernews", author, date, "INT", "", issue, txt, url):
                    count += 1
            time.sleep(1)
        except Exception as e:
            log(f"  HN '{q}': {e}")
    log(f"Hacker News: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 26: APP STORE REVIEWS (iTunes RSS)
# ────────────────────────────────────────
def run_appstore(state, config):
    if not config["sources_enabled"].get("appstore", True): return
    log("App Store: avvio")
    count = 0
    apps = [
        ("632064380",  "vinted",   ["fr", "it", "de", "es", "pl", "gb"]),
        ("282614216",  "ebay",     ["us", "gb", "de", "fr", "it"]),
        ("570843012",  "depop",    ["us", "gb", "it"]),
        ("586348880",  "wallapop", ["es", "it"]),
    ]
    headers = {"User-Agent": "iTunes/12.0 (Macintosh; U; Mac OS X 10.15)"}
    for app_id, app_name, countries in apps:
        for cc in countries:
            try:
                url = (f"https://itunes.apple.com/{cc}/rss/customerreviews"
                       f"/id={app_id}/sortBy=mostRecent/json")
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code != 200: continue
                entries = r.json().get("feed", {}).get("entry", [])
                for entry in entries[1:]:  # primo entry è info app
                    title = entry.get("title", {}).get("label", "")
                    body  = entry.get("content", {}).get("label", "")
                    txt   = f"{title}. {body}".strip(". ")
                    if len(txt.split()) < MIN_WORDS: continue
                    if not is_negative(txt): continue
                    issue = classify(txt)
                    if issue == "altro": continue
                    rating_str = entry.get("im:rating", {}).get("label", "5")
                    try:
                        if int(rating_str) >= 4: continue
                    except: pass
                    author = entry.get("author", {}).get("name", {}).get("label", "anon")
                    date   = entry.get("updated", {}).get("label", "")[:10]
                    if add(state, "appstore", author, date, cc.upper(), rating_str, issue, txt,
                           f"https://apps.apple.com/{cc}/app/id{app_id}"):
                        count += 1
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                log(f"  AppStore {app_name}/{cc}: {e}")
    log(f"App Store: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 27: TRUSTPILOT (scraping __NEXT_DATA__)
# ────────────────────────────────────────
def run_trustpilot_v2(state, config):
    if not config["sources_enabled"].get("trustpilot", True): return
    log("Trustpilot: avvio")
    count = 0
    domains = {
        "vinted.fr": "FR", "vinted.de": "DE", "vinted.es": "ES",
        "vinted.co.uk": "GB", "vinted.it": "IT", "vinted.pl": "PL",
        "ebay.com": "INT", "depop.com": "INT", "wallapop.com": "ES",
    }
    headers = {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "DNT": "1",
    }
    for domain, cc in domains.items():
        for page in range(1, 6):
            try:
                url = (f"https://www.trustpilot.com/review/{domain}"
                       f"?languages=all&stars=1,2,3&page={page}")
                r = requests.get(url, headers=headers, timeout=20)
                if r.status_code != 200:
                    break
                soup   = BeautifulSoup(r.text, "lxml")
                script = soup.find("script", id="__NEXT_DATA__")
                if not script:
                    break
                data    = json.loads(script.string)
                reviews = (data.get("props", {})
                               .get("pageProps", {})
                               .get("reviews", []))
                if not reviews:
                    break
                for rev in reviews:
                    title  = rev.get("title", "") or ""
                    body   = rev.get("text", "") or ""
                    txt    = f"{title}. {body}".strip(". ")
                    if len(txt.split()) < MIN_WORDS: continue
                    issue  = classify(txt)
                    if issue == "altro": continue
                    rating = rev.get("rating", 5)
                    if rating >= 4: continue
                    date   = rev.get("dates", {}).get("publishedDate", "")[:10]
                    author = rev.get("consumer", {}).get("displayName", "anon")
                    if add(state, "trustpilot", author, date, cc, str(rating), issue, txt,
                           f"https://www.trustpilot.com/review/{domain}"):
                        count += 1
                time.sleep(random.uniform(4, 7))
            except Exception as e:
                log(f"  Trustpilot {domain} p{page}: {e}")
    log(f"Trustpilot: +{count} nuovi record")

# ────────────────────────────────────────
# FONTE 28: SEARCH ENGINES (DuckDuckGo dorking su FB/IG/TT)
# ────────────────────────────────────────
def _search_bing(q, headers):
    """Cerca su Bing, ritorna lista di (title, snippet, url)."""
    r = requests.get("https://www.bing.com/search",
                    params={"q": q, "count": 30}, headers=headers, timeout=15)
    if r.status_code != 200: return []
    soup = BeautifulSoup(r.text, "lxml")
    out = []
    for li in soup.find_all("li", class_="b_algo"):
        h2 = li.find("h2")
        link = h2.find("a") if h2 else None
        if not link: continue
        title = link.get_text(strip=True)
        url   = link.get("href", "")
        snip_el = li.find(class_=lambda c: c and ("b_lineclamp" in c or "b_caption" in c))
        snippet = snip_el.get_text(separator=" ", strip=True) if snip_el else ""
        out.append((title, snippet, url))
    return out

def _search_ddg(q, headers):
    """Fallback DuckDuckGo HTML."""
    r = requests.get("https://html.duckduckgo.com/html/",
                    params={"q": q}, headers=headers, timeout=10)
    if r.status_code != 200: return []
    soup = BeautifulSoup(r.text, "lxml")
    out = []
    for result in soup.find_all("div", class_="result"):
        title_el   = result.find("a", class_="result__a")
        snippet_el = result.find("a", class_="result__snippet") or \
                     result.find(class_="result__snippet")
        if not snippet_el: continue
        title   = title_el.get_text(strip=True) if title_el else ""
        snippet = snippet_el.get_text(strip=True)
        url     = title_el.get("href", "") if title_el else ""
        out.append((title, snippet, url))
    return out

def run_search_engines(state, config):
    if not config["sources_enabled"].get("search_engines", True): return
    log("Search Engines: avvio")
    count = 0
    headers = {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8,fr;q=0.7,de;q=0.6,es;q=0.5",
    }
    sites = {
        "facebook.com":   ("facebook_search",   300),
        "instagram.com":  ("instagram_search",  300),
        "tiktok.com":     ("tiktok_search",     300),
        "trustpilot.com": ("trustpilot_search", 300),
    }
    # Set keyword completo (stesso usato da Reddit) — 6 lingue × 8 piattaforme
    keywords = [
        # ── VINTED ──
        "vinted scam","vinted fake","vinted empty box","vinted fraud",
        "vinted not received","vinted counterfeit","vinted refund denied",
        "vinted seller disappeared","vinted not as described",
        "vinted truffa","vinted falso","vinted pacco vuoto","vinted non arrivato",
        "vinted rimborso negato","vinted truffato",
        "vinted arnaque","vinted colis vide","vinted jamais reçu","vinted litige perdu",
        "vinted betrug","vinted gefälscht","vinted nie angekommen","vinted abzocke",
        "vinted estafa","vinted nunca llegó","vinted me estafaron","vinted fraude",
        "vinted oszustwo","vinted podróbka","vinted pusta paczka",
        # ── EBAY ──
        "ebay scam","ebay fake","ebay fraud","ebay empty box",
        "ebay not received","ebay counterfeit","ebay refund denied",
        "ebay truffa","ebay falso","ebay non arrivato","ebay rimborso negato",
        "ebay arnaque","ebay colis vide","ebay contrefaçon",
        "ebay betrug","ebay gefälscht","ebay abzocke",
        "ebay estafa","ebay me estafaron",
        "ebay oszustwo","ebay podróbka",
        # ── DEPOP ──
        "depop scam","depop fake","depop fraud","depop not received",
        "depop counterfeit","depop not as described",
        "depop truffa","depop falso","depop arnaque","depop betrug",
        # ── FACEBOOK MARKETPLACE ──
        "facebook marketplace scam","facebook marketplace fake",
        "facebook marketplace fraud","facebook marketplace not received",
        "marketplace truffa","marketplace arnaque","marketplace betrug",
        "marketplace estafa","fb marketplace scam","fb marketplace fake",
        # ── WALLAPOP ──
        "wallapop scam","wallapop fake","wallapop fraud","wallapop estafa",
        "wallapop truffa","wallapop arnaque","wallapop betrug",
        "wallapop me estafaron","wallapop fraude",
        # ── SUBITO.IT ──
        "subito truffa","subito falso","subito non arrivato",
        "subito fregato","subito truffato","subito scam",
        # ── LEBONCOIN ──
        "leboncoin arnaque","leboncoin escroquerie","leboncoin faux",
        "leboncoin jamais reçu","leboncoin fraude","leboncoin colis vide","leboncoin litige",
        # ── KLEINANZEIGEN ──
        "kleinanzeigen betrug","kleinanzeigen gefälscht","kleinanzeigen betrogen",
        "kleinanzeigen abzocke","kleinanzeigen fake",
        "ebay kleinanzeigen betrug","ebay kleinanzeigen fake",
    ]
    # Costruisci tutte le query e shuffle per diversificare ogni ciclo
    queries = [(site, src, cap, kw) for site, (src, cap) in sites.items() for kw in keywords]
    random.shuffle(queries)

    site_counts = {src: 0 for src, _ in sites.values()}
    site_caps   = {src: cap for src, cap in sites.values()}

    bing_fail_streak = 0
    for site, src, cap, kw in queries:
        if site_counts[src] >= cap:
            continue  # raggiunto limite per questa piattaforma
        q = f"site:{site} {kw}"
        results = []
        engine = "bing"
        # 1. prova Bing (primario)
        if bing_fail_streak < 5:
            try:
                results = _search_bing(q, headers)
                bing_fail_streak = 0 if results else bing_fail_streak + 1
            except Exception as e:
                bing_fail_streak += 1
                log(f"  Bing '{q[:50]}': {e}")
        # 2. fallback DDG se Bing fallisce
        if not results:
            engine = "ddg"
            try:
                results = _search_ddg(q, headers)
            except Exception as e:
                log(f"  DDG '{q[:50]}': {e}")
        # 3. parsa risultati
        for title, snippet, url in results:
            txt = f"{title}. {snippet}".strip(". ")
            if len(txt.split()) < MIN_WORDS: continue
            if not is_negative(txt): continue
            issue = classify(txt)
            if issue == "altro": continue
            if add(state, src, engine, datetime.now().strftime("%Y-%m-%d"),
                   "INT", "", issue, txt, url):
                count += 1
                site_counts[src] += 1
        time.sleep(random.uniform(2, 4))
    log(f"Search Engines: +{count} nuovi record "
        f"(FB:{site_counts.get('facebook_search',0)} "
        f"IG:{site_counts.get('instagram_search',0)} "
        f"TT:{site_counts.get('tiktok_search',0)} "
        f"TP:{site_counts.get('trustpilot_search',0)})")

# ────────────────────────────────────────
# FONTE 29: GOOGLE PLAY STORE REVIEWS
# ────────────────────────────────────────
def run_playstore(state, config):
    if not config["sources_enabled"].get("playstore", True): return
    log("Play Store: avvio")
    try:
        from google_play_scraper import reviews, Sort
    except ImportError:
        log("  Play Store: libreria non installata (pip install google-play-scraper)")
        return
    count = 0
    apps = [
        ("com.vinted.android",  ["fr", "it", "de", "es", "pl", "en"]),
        ("com.ebay.mobile",     ["us", "de", "fr", "it"]),
        ("com.depop",           ["us", "gb", "it"]),
        ("com.wallapop",        ["es", "it"]),
        ("com.leboncoin.android.pro", ["fr"]),
    ]
    for pkg, langs in apps:
        for lang in langs:
            try:
                result, _ = reviews(
                    pkg,
                    lang=lang,
                    country=lang if lang not in ("en","gb") else "us",
                    sort=Sort.NEWEST,
                    count=100,
                    filter_score_with=None,
                )
                for rev in result:
                    if rev.get("score", 5) >= 4: continue
                    txt = rev.get("content", "") or ""
                    if len(txt.split()) < MIN_WORDS: continue
                    if not is_negative(txt): continue
                    issue = classify(txt)
                    if issue == "altro": continue
                    author = rev.get("userName", "anon")
                    at = rev.get("at")
                    date = at.strftime("%Y-%m-%d") if at else datetime.now().strftime("%Y-%m-%d")
                    rating = str(rev.get("score", ""))
                    cc = {"fr":"FR","it":"IT","de":"DE","es":"ES","pl":"PL","en":"INT"}.get(lang,"INT")
                    if add(state, "playstore", author, date, cc, rating, issue, txt,
                           f"https://play.google.com/store/apps/details?id={pkg}"):
                        count += 1
                time.sleep(random.uniform(2, 3))
            except Exception as e:
                log(f"  PlayStore {pkg}/{lang}: {e}")
    log(f"Play Store: +{count} nuovi record")

# ────────────────────────────────────────
# LOOP PRINCIPALE — gira 24/7
# ────────────────────────────────────────
def main():
    init_csv()
    log("="*50)
    log("MELION COLLECTOR — avvio ciclo continuo")
    log("="*50)

    RECORD_LIMIT = 15_000  # fermati qui finché non aggiustiamo le fonti

    cycle = 0
    while True:
        total = count_records()
        if total >= RECORD_LIMIT:
            log(f"\n🛑 Limite {RECORD_LIMIT} record raggiunto ({total} presenti). Collector in pausa.")
            log("Riavvia manualmente quando le fonti sono pronte.")
            break

        cycle += 1
        _cycle_counts.clear()  # reset contatori per-source
        _existing_counts.clear()
        _existing_counts.update(count_per_source())  # snapshot record per fonte
        state = load_state()
        config = load_config()

        saturated = [s for s, n in _existing_counts.items() if n >= SOURCE_HARD_CAP]
        log(f"\n── CICLO {cycle} | Record totali: {total} ──")
        if saturated:
            log(f"   Fonti saturate (>={SOURCE_HARD_CAP}): {', '.join(sorted(saturated))}")

        # Tutte le fonti in parallelo simultaneamente
        all_sources = [
            run_reddit,          # Reddit multi-subreddit
            run_youtube,         # YouTube API comments+videos
            run_google_news,     # Google News RSS
            run_pissedconsumer,  # PissedConsumer reviews
            run_sitejabber,      # Sitejabber reviews
            run_trustpilot_v2,   # Trustpilot scraping
            run_hackernews,      # Hacker News (Algolia API)
            run_appstore,        # App Store reviews (iTunes RSS)
            run_playstore,       # Play Store reviews (google-play-scraper)
            run_search_engines,  # DDG dorking su Facebook/Instagram/TikTok
            run_twitter_v2,      # Twitter/X API
            run_forum_it,        # Forum italiani
            run_forum_fr,        # Forum francesi
            run_forum_de,        # Forum tedeschi
        ]
        threads = [threading.Thread(target=fn, args=(state, config), daemon=True)
                   for fn in all_sources]
        for t in threads: t.start()
        for t in threads: t.join()

        total = count_records()
        log(f"\n✅ Fine ciclo {cycle} | TOTALE RECORD: {total}")
        log("Pausa 10 min prima del prossimo ciclo...")
        time.sleep(600)  # 10 minuti tra un ciclo e l'altro

if __name__ == "__main__":
    main()
