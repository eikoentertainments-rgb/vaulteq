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
    """Divide una nota tra i capitoli pertinenti usando Claude.

    Ritorna una lista di coppie (capitolo_id, testo_estratto). Senza
    ANTHROPIC_API_KEY, o se la chiamata fallisce per qualunque motivo,
    l'intera nota finisce nel diario: il salvataggio non si blocca mai.
    """
    client = _client()
    if client is None:
        return [("diario", testo)]

    prompt = f"""Sei l'assistente che organizza il libro personale di un utente.
Capitoli disponibili:
{DESCRIZIONE_CAPITOLI}

L'utente ha scritto questa nota:
\"\"\"{testo}\"\"\"

Se la nota parla di più temi, dividila in più parti e assegna ciascuna al
capitolo giusto, riportando solo porzioni del testo originale (non
inventare, non riassumere). Se parla di un solo tema usa una sola voce.
Rispondi SOLO con JSON in questo formato, nessun testo fuori dal JSON:
{{"assegnazioni": [{{"capitolo": "1", "testo": "..."}}]}}"""

    try:
        risposta = client.messages.create(
            model=MODELLO,
            max_tokens=1024,
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
