#!/usr/bin/env python3
"""
Verificacion final de NULLs en todas las tablas del DWH.
Con registros centinela, NO debe haber NULLs en ninguna columna.
"""
from config import get_engine
from sqlalchemy import text

engine = get_engine()
conn = engine.connect()

tables = ['dim_date', 'dim_competitions', 'dim_clubs', 'dim_players', 'dim_games',
          'fact_games', 'fact_appearances', 'fact_game_events', 'fact_player_valuations', 'fact_transfers']

total_issues = 0

for table in tables:
    # Get columns.
    cols = conn.execute(text(
        f"SELECT column_name FROM information_schema.columns WHERE table_schema='dwh' AND table_name='{table}'"
    )).fetchall()
    col_names = [r[0] for r in cols]
    
    nulls_found = []
    for col in col_names:
        count = conn.execute(text(f'SELECT COUNT(*) FROM dwh.{table} WHERE "{col}" IS NULL')).fetchone()[0]
        if count > 0:
            nulls_found.append((col, count))
            total_issues += 1
    
    if nulls_found:
        print(f"{table}:")
        for col, count in nulls_found:
            print(f"   - {col}: {count} NULLs")
    else:
        print(f"{table}: 0 NULLs")

print(f"\n{'='*50}")
if total_issues == 0:
    print("VERIFICACION COMPLETA: 0 NULLs no deseados en todo el DWH")
else:
    print(f"{total_issues} columnas con NULLs no deseados")

conn.close()
