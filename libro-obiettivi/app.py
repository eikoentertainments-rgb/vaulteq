import os
import sqlite3
from datetime import date

from dotenv import load_dotenv
from flask import Flask, redirect, render_template, request, url_for

from agenti import compila_capitolo
from classificatore import classifica_nota

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "libro.db")

CAPITOLI = {
    "diario": {
        "titolo": "Diario",
        "guida": "Note libere del giorno, da smistare poi nei capitoli tematici.",
    },
    "1": {
        "titolo": "Capitolo 1 — Chi sono",
        "guida": "Chi sei, da dove vieni, dove vuoi andare, come ci arrivi.",
    },
    "2": {
        "titolo": "Capitolo 2 — Insegnamenti di vita",
        "guida": "Cosa hai imparato, cosa stai imparando, cosa devi ancora imparare.",
    },
    "3": {
        "titolo": "Capitolo 3 — Obiettivi",
        "guida": "Quali obiettivi, come raggiungerli, in quanto tempo.",
    },
}


def get_db():
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capitolo TEXT NOT NULL,
            data TEXT NOT NULL,
            testo TEXT NOT NULL,
            creato_il TEXT NOT NULL
        )
        """
    )
    return conn


app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def chat():
    if request.method == "POST":
        testo = request.form.get("testo", "").strip()
        if testo:
            oggi = date.today().isoformat()
            conn = get_db()
            for capitolo, frammento in classifica_nota(testo):
                conn.execute(
                    "INSERT INTO entries (capitolo, data, testo, creato_il) VALUES (?, ?, ?, datetime('now'))",
                    (capitolo, oggi, frammento),
                )
            conn.commit()
            conn.close()
        return redirect(url_for("chat"))

    conn = get_db()
    righe = conn.execute(
        "SELECT capitolo, data, testo FROM entries ORDER BY id DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return render_template("chat.html", capitoli=CAPITOLI, righe=righe)


@app.route("/libro")
def libro():
    conn = get_db()
    capitoli_compilati = {}
    for cid, meta in CAPITOLI.items():
        voci = conn.execute(
            "SELECT data, testo FROM entries WHERE capitolo = ? ORDER BY data ASC, id ASC",
            (cid,),
        ).fetchall()
        capitoli_compilati[cid] = {**meta, **compila_capitolo(voci)}
    conn.close()
    return render_template("libro.html", capitoli=capitoli_compilati)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
