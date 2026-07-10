"""
Script una tantum: elimina dalla tabella `temperature` del database di
produzione (Postgres/Supabase) tutte le rilevazioni datate 09/07/2026 o
10/07/2026. Non tocca la tabella `apparecchi`.

Uso (da eseguire su Render, Shell o one-off job, con DATABASE_URL già
impostata nell'ambiente):

    python3 delete_temperature_09_10_luglio.py

Puoi eliminare questo file dal repo una volta eseguito.
"""
import sys

import database

TARGET_DATES = ('2026-07-09', '2026-07-10')


def main():
    if not database._USE_PG:
        print(
            "DATABASE_URL non è impostata in questo ambiente (o non è Postgres): "
            "interrompo per sicurezza, non voglio toccare per sbaglio un SQLite locale.",
            file=sys.stderr,
        )
        sys.exit(1)

    with database.get_conn() as conn:
        cur = conn.execute(
            "SELECT id, apparecchio, data, ora, temperatura, esito, operatore "
            "FROM temperature WHERE data IN (?, ?) ORDER BY data, ora",
            TARGET_DATES,
        )
        rows = database._rows(cur)

        print(f"Righe trovate con data in {TARGET_DATES}: {len(rows)}")
        for r in rows:
            print(f"  id={r['id']} {r['data']} {r['ora']} {r['apparecchio']} "
                  f"{r['temperatura']}°C {r['esito']} ({r['operatore']})")

        if not rows:
            print("Nessuna riga da eliminare.")
            return

        conn.execute("DELETE FROM temperature WHERE data IN (?, ?)", TARGET_DATES)
        print(f"Eliminate {len(rows)} righe dalla tabella temperature.")
        print("Tabella apparecchi non toccata.")


if __name__ == '__main__':
    main()
