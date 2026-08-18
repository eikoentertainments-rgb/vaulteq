def compila_capitolo(voci):
    """Aggrega le voci di un capitolo in ordine cronologico.

    Deterministico e gratuito: nessuna chiamata a modelli esterni.
    Punto di innesto per una futura versione che usa un LLM per
    riassumere/riorganizzare il testo invece di limitarsi a ordinarlo.
    """
    if not voci:
        return {"riepilogo": "Ancora nessuna voce.", "voci": []}
    ultimo = voci[-1][0]
    riepilogo = f"{len(voci)} voci · ultimo aggiornamento {ultimo}"
    return {"riepilogo": riepilogo, "voci": voci}
