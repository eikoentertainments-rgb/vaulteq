import json
import os

from anthropic import Anthropic

MODELLO = "claude-haiku-4-5-20251001"

DESCRIZIONE_CAPITOLI = """
- "1": Capitolo 1 - Chi sono. Chi sei, da dove vieni, dove vuoi andare, come.
- "2": Capitolo 2 - Insegnamenti di vita. Cosa hai imparato, cosa stai imparando, cosa devi ancora imparare.
- "3": Capitolo 3 - Obiettivi. Quali obiettivi, come raggiungerli, in quanto tempo.
- "diario": tutto ciò che non rientra chiaramente in nessuno dei capitoli sopra.
""".strip()

CAPITOLI_VALIDI = {"1", "2", "3", "diario"}


def _client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    return Anthropic(api_key=api_key)


def classifica_nota(testo):
    """Smista e riscrive una nota per i capitoli pertinenti usando Claude.

    Ritorna una lista di coppie (capitolo_id, testo_riscritto). Senza
    ANTHROPIC_API_KEY, o se la chiamata fallisce per qualunque motivo,
    l'intera nota finisce nel diario così com'è: il salvataggio non si
    blocca mai.
    """
    client = _client()
    if client is None:
        return [("diario", testo)]

    prompt = f"""Sei l'assistente che scrive il libro personale di un utente, a
partire dalle sue note quotidiane.
Capitoli disponibili:
{DESCRIZIONE_CAPITOLI}

L'utente scrive spesso frasi brevi, che dicono poco in superficie ma
comunicano molto in profondità. Per la nota qui sotto:
1. individua a quali capitoli appartiene (anche più di uno, se il contenuto
   è misto);
2. per ciascun capitolo individuato, scrivi una versione abbellita e più
   distesa del relativo contenuto, che dia forma e voce a ciò che c'è dietro
   la frase breve — senza inventare fatti, nomi, eventi o dettagli che
   l'utente non ha fornito, ma approfondendo tono, riflessione e significato.

Nota dell'utente:
\"\"\"{testo}\"\"\"

Rispondi SOLO con JSON in questo formato, nessun testo fuori dal JSON:
{{"assegnazioni": [{{"capitolo": "1", "testo": "..."}}]}}"""

    try:
        risposta = client.messages.create(
            model=MODELLO,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        grezzo = risposta.content[0].text.strip()
        dati = json.loads(grezzo)
        risultato = [
            (a["capitolo"], a["testo"])
            for a in dati.get("assegnazioni", [])
            if a.get("capitolo") in CAPITOLI_VALIDI and a.get("testo")
        ]
        return risultato or [("diario", testo)]
    except Exception:
        return [("diario", testo)]
