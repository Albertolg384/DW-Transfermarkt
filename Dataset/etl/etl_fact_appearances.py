#!/usr/bin/env python3

"""
ETL para fact_appearances (apariciones de jugadores en partidos)
Desnormaliza appearances.csv + game_lineups.csv
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE

def etl_fact_appearances():
    """Extrae, transforma y carga la tabla de hechos de apariciones"""
    print("👕 ETL: fact_appearances (appearances + game_lineups)")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo appearances.csv y game_lineups.csv...")
    appearances = pd.read_csv(CSV_FILES['appearances'], **PANDAS_READ_CONFIG)
    lineups = pd.read_csv(CSV_FILES['game_lineups'], **PANDAS_READ_CONFIG)
    print(f"   ✓ appearances: {len(appearances):,} registros")
    print(f"   ✓ game_lineups: {len(lineups):,} registros")
    
    # TRANSFORM
    print("2️⃣ Transformando y desnormalizando...")
    
    # Merge appearances con lineups
    fact_appearances = appearances.merge(
        lineups[['game_id', 'player_id', 'type', 'position', 'team_captain']],
        on=['game_id', 'player_id'],
        how='left'
    )
    
    # Obtener date_id y competition_id desde dim_games
    engine = get_engine()
    games_info = pd.read_sql(
        'SELECT game_id, competition_id, date FROM dwh.dim_games', 
        engine
    )
    games_info['date'] = pd.to_datetime(games_info['date'], errors='coerce')
    games_info['date_id'] = games_info['date'].dt.strftime('%Y%m%d').astype('Int64')
    
    fact_appearances = fact_appearances.merge(
        games_info[['game_id', 'competition_id', 'date_id']],
        on='game_id',
        how='left',
        suffixes=('_csv', '')
    )
    
    # Renombrar columnas si es necesario
    fact_appearances = fact_appearances.rename(columns={
        'player_club_id': 'club_id'
    })
    
    # Validación: eliminar registros con FK críticas NULL (ANTES de seleccionar columnas)
    critical_cols = ['appearance_id', 'game_id', 'player_id', 'club_id', 'competition_id', 'date_id']
    # Filtrar solo columnas críticas que existan
    critical_cols = [col for col in critical_cols if col in fact_appearances.columns]
    nulls_before = len(fact_appearances)
    fact_appearances = fact_appearances.dropna(subset=critical_cols)
    nulls_removed = nulls_before - len(fact_appearances)
    if nulls_removed > 0:
        print(f"   ⚠️ {nulls_removed} registros con FK NULL eliminados")
    
    # Seleccionar columnas finales
    columns_to_load = [
        'appearance_id', 'game_id', 'player_id', 'club_id', 'competition_id', 'date_id',
        'player_name', 'type', 'position', 'team_captain',
        'minutes_played', 'goals', 'assists', 'yellow_cards', 'red_cards'
    ]
    
    # Filtrar solo columnas que existan
    available_cols = [col for col in columns_to_load if col in fact_appearances.columns]
    fact_appearances = fact_appearances[available_cols].copy()
    
    # Convertir team_captain a boolean (viene como 0.0/1.0)
    if 'team_captain' in fact_appearances.columns:
        fact_appearances['team_captain'] = fact_appearances['team_captain'].apply(
            lambda x: None if pd.isna(x) else bool(int(x))
        )
    
    # Verificar integridad referencial
    print("   🔍 Verificando integridad referencial...")
    
    # Validar player_id
    valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
    invalid_players = ~fact_appearances['player_id'].isin(valid_players['player_id'])
    if invalid_players.any():
        print(f"   ⚠️ {invalid_players.sum()} registros con player_id inválido eliminados")
        fact_appearances = fact_appearances[~invalid_players]
    
    # Validar club_id
    valid_clubs = pd.read_sql('SELECT club_id FROM dwh.dim_clubs', engine)
    invalid_clubs = ~fact_appearances['club_id'].isin(valid_clubs['club_id'])
    if invalid_clubs.any():
        print(f"   ⚠️ {invalid_clubs.sum()} registros con club_id inválido eliminados")
        fact_appearances = fact_appearances[~invalid_clubs]
    
    print(f"   ✓ {len(fact_appearances):,} registros listos para carga")
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.fact_appearances)...")
    
    fact_appearances.to_sql(
        'fact_appearances',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method=None,
        chunksize=5000
    )
    
    print("✅ fact_appearances cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_appearances")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.fact_appearances\n")

if __name__ == "__main__":
    etl_fact_appearances()
