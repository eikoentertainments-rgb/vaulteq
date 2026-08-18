# Il mio libro sugli obiettivi

Web app minimale, gratuita, per costruire nel tempo un libro personale
aggiungendo ogni giorno qualche riga in un'interfaccia stile chat.

## Come funziona

- **Scrivi**: nella home scegli il capitolo di destinazione e scrivi una nota,
  come in una chat. Viene salvata con la data di oggi.
- **Leggi il libro**: la pagina `/libro` raccoglie tutte le note, capitolo per
  capitolo, in ordine cronologico.
- I dati vengono salvati in un database SQLite locale (`data/libro.db`, escluso da git).

## Capitoli inclusi

1. **Diario** — note libere del giorno, da smistare poi nei capitoli tematici.
2. **Capitolo 1 — Chi sono** — chi sei, da dove vieni, dove vuoi andare, come.
3. **Capitolo 2 — Insegnamenti di vita** — cosa hai imparato, cosa stai
   imparando, cosa devi ancora imparare.
4. **Capitolo 3 — Obiettivi** — quali obiettivi, come raggiungerli, in quanto
   tempo.

## Cosa sono gli "agenti" in questa versione free

Ogni capitolo viene compilato da una funzione in `agenti.py`
(`compila_capitolo`): oggi è deterministica e gratuita, si limita a ordinare
le voci per data e a calcolare un piccolo riepilogo. È il punto di innesto
pensato per il passo successivo: quando vorrai passare a una versione con AI
vera (per riassumere, riorganizzare o riscrivere i contenuti), basterà
sostituire questa funzione con una chiamata a un modello — senza toccare il
resto dell'app.

## Avvio in locale

```bash
cd libro-obiettivi
pip install -r requirements.txt
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

**Nota sulla persistenza**: senza un volume Railway montato su `data/`, il
database SQLite viene azzerato a ogni nuovo deploy. Per la fase di test va
bene così; se decidi di tenerlo, aggiungi un volume persistente (o migra a un
database gestito, es. Postgres free tier) prima di scriverci contenuti a cui
tieni.

## Prossimi passi possibili

- Aggiungere un vero step di AI in `agenti.py` per riorganizzare/riassumere
  i contenuti invece di limitarsi a ordinarli.
- Autenticazione (anche solo una password) prima di esporlo pubblicamente.
- Export del libro in PDF o Word.
- Volume persistente o database gestito per non perdere i dati ai deploy.
