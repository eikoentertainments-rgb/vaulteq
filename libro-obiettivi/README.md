# Il mio libro sugli obiettivi

Web app minimale per costruire nel tempo un libro personale aggiungendo ogni
giorno qualche riga in un'interfaccia stile chat. Un agente AI smista da solo
il testo nei capitoli giusti, anche quando una nota parla di più temi insieme.

## Come funziona

- **Scrivi**: nella home scrivi una nota, come in una chat. Un agente AI
  (Claude Haiku) legge il testo e decide da solo in quale capitolo va —
  se la nota tocca più temi, la divide e la assegna a più capitoli. Le
  frasi brevi vengono anche "decorate": riscritte in una versione più
  distesa che sviluppa ciò che c'è dietro, senza inventare fatti non detti.
- **Leggi il libro**: la pagina `/libro` raccoglie tutte le note, capitolo per
  capitolo, in ordine cronologico.
- I dati vengono salvati in un database SQLite locale (`data/libro.db`, escluso da git).

## Capitoli inclusi

1. **Diario** — dove finisce tutto ciò che non rientra chiaramente negli altri capitoli.
2. **Capitolo 1 — Chi sono** — chi sei, da dove vieni, dove vuoi andare, come.
3. **Capitolo 2 — Insegnamenti di vita** — cosa hai imparato, cosa stai
   imparando, cosa devi ancora imparare.
4. **Capitolo 3 — Obiettivi** — quali obiettivi, come raggiungerli, in quanto
   tempo.

## Gli "agenti"

- **Classificatore** (`classificatore.py`, funzione `classifica_nota`): ad
  ogni nota inviata, chiama Claude Haiku per capire a quale/i capitolo/i
  appartiene, dividerla se è mista, e riscriverla in una versione più
  distesa quando è una frase breve — dando forma a ciò che c'è dietro senza
  inventare fatti che l'utente non ha detto. Serve una `ANTHROPIC_API_KEY`
  (vedi sotto); **senza key l'app funziona comunque**, ma ogni nota finisce
  intera e invariata nel Diario invece di essere smistata e riscritta.
- **Compilatore** (`agenti.py`, funzione `compila_capitolo`): a lettura,
  ordina le voci di ogni capitolo per data e calcola un riepilogo. È
  deterministico e gratuito — nessuna chiamata AI in questo passaggio.

### Costo

Il classificatore usa Claude Haiku (il modello più economico), con una nota
tipica il costo è nell'ordine di frazioni di centesimo per invio. Nessun
costo se non imposti `ANTHROPIC_API_KEY`.

## Avvio in locale

```bash
cd libro-obiettivi
pip install -r requirements.txt
cp .env.example .env   # poi inserisci la tua ANTHROPIC_API_KEY
python app.py
```

Apri `http://localhost:5001` dal browser (anche da telefono sulla stessa
rete, usando l'IP del computer al posto di `localhost`).

## Deploy gratuito (per usarlo da ogni dispositivo)

Il repo usa già Railway per il servizio `collector`. Per questa app puoi
creare un **secondo servizio Railway** nello stesso progetto (piano free),
puntato alla cartella `libro-obiettivi/`, con start command:

```
python app.py
```

e con la variabile d'ambiente `ANTHROPIC_API_KEY` impostata nel pannello
Railway (mai committarla nel repo).

**Nota sulla persistenza**: senza un volume Railway montato su `data/`, il
database SQLite viene azzerato a ogni nuovo deploy. Per la fase di test va
bene così; se decidi di tenerlo, aggiungi un volume persistente (o migra a un
database gestito, es. Postgres free tier) prima di scriverci contenuti a cui
tieni.

## Prossimi passi possibili

- Autenticazione (anche solo una password) prima di esporlo pubblicamente.
- Export del libro in PDF o Word.
- Volume persistente o database gestito per non perdere i dati ai deploy.
- Un agente di rilettura periodica che riscrive/fonde le voci di un capitolo
  invece di limitarsi ad affiancarle in ordine cronologico.
