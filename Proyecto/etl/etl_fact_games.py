#!/usr/bin/env python3

"""
ETL para fact_games (tabla de hechos de partidos)
Desnormaliza games.csv + club_games.csv
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE
from null_handler import apply_null_rules, validate_no_nulls

def etl_fact_games():
    """Extrae, transforma y carga la tabla de hechos de partidos"""
    print("📊 ETL: fact_games (games + club_games desnormalizados)")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo games.csv y club_games.csv...")
    games = pd.read_csv(CSV_FILES['games'], **PANDAS_READ_CONFIG)
    club_games = pd.read_csv(CSV_FILES['club_games'], **PANDAS_READ_CONFIG)
    print(f"   ✓ games: {len(games):,} registros")
    print(f"   ✓ club_games: {len(club_games):,} registros")
    
    # TRANSFORM
    print("2️⃣ Transformando y desnormalizando...")
    
    # Eliminar columnas de posición de games si existen (usaremos las de club_games)
    games = games.drop(columns=['home_club_position', 'away_club_position'], errors='ignore')
    
    # Normalizar valores de 'hosting' por si vienen con espacios o mayus distinta.
    club_games['hosting'] = club_games['hosting'].str.strip()

    # Separar club_games en local y visitante
    home_games = club_games[club_games['hosting'] == 'Home'][['game_id', 'own_position']].rename(
        columns={'own_position': 'home_club_position'}
    )
    away_games = club_games[club_games['hosting'] == 'Away'][['game_id', 'own_position']].rename(
        columns={'own_position': 'away_club_position'}
    )
    
    # Merge con games
    fact_games = games.merge(home_games, on='game_id', how='left')
    fact_games = fact_games.merge(away_games, on='game_id', how='left')
    
    # Convertir fecha y generar date_id
    fact_games['date'] = pd.to_datetime(fact_games['date'], errors='coerce')
    fact_games['date_id'] = fact_games['date'].dt.strftime('%Y%m%d').astype('Int64')
    
    # Campos calculados (medidas adicionales)
    fact_games['goal_difference'] = fact_games['home_club_goals'] - fact_games['away_club_goals']
    fact_games['total_goals'] = fact_games['home_club_goals'] + fact_games['away_club_goals']
    fact_games['is_home_win'] = fact_games['goal_difference'] > 0
    fact_games['is_draw'] = fact_games['goal_difference'] == 0
    fact_games['is_away_win'] = fact_games['goal_difference'] < 0
    
    # Seleccionar columnas para la tabla de hechos
    columns_to_load = [
        'game_id', 'competition_id', 'home_club_id', 'away_club_id', 'date_id',
        'season', 'home_club_goals', 'away_club_goals',
        'home_club_position', 'away_club_position', 'attendance',
        'goal_difference', 'total_goals', 'is_home_win', 'is_draw', 'is_away_win'
    ]
    
    fact_games = fact_games[columns_to_load].copy()
    
    # ------------------------------------------------------------------
    # TRATAMIENTO CENTRALIZADO DE NULLs (módulo null_handler)
    # ------------------------------------------------------------------
    fact_games = apply_null_rules(fact_games, 'fact_games', is_dimension=False)
    validate_no_nulls(fact_games, 'fact_games')

    # Recalcular medidas derivadas (tras el tratamiento de NULLs)
    fact_games['goal_difference'] = fact_games['home_club_goals'] - fact_games['away_club_goals']
    fact_games['total_goals'] = fact_games['home_club_goals'] + fact_games['away_club_goals']
    fact_games['is_home_win'] = fact_games['goal_difference'] > 0
    fact_games['is_draw'] = fact_games['goal_difference'] == 0
    fact_games['is_away_win'] = fact_games['goal_difference'] < 0

    # Validación: eliminar registros con FK críticas NULL
    critical_cols = ['game_id', 'competition_id', 'home_club_id', 'away_club_id', 'date_id']
    nulls_before = len(fact_games)
    fact_games = fact_games.dropna(subset=critical_cols)
    nulls_removed = nulls_before - len(fact_games)
    if nulls_removed > 0:
        print(f"    {nulls_removed} registros con FK NULL eliminados")
    
    # Verificar que las FK existen en dimensiones (integridad referencial)
    print("   🔍 Verificando integridad referencial...")
    engine = get_engine()
    
    # Validar game_id
    valid_games = pd.read_sql('SELECT game_id FROM dwh.dim_games', engine)
    invalid_games = ~fact_games['game_id'].isin(valid_games['game_id'])
    if invalid_games.any():
        print(f"    {invalid_games.sum()} registros con game_id inválido eliminados")
        fact_games = fact_games[~invalid_games]
    
    # Validar competition_id
    valid_competitions = pd.read_sql('SELECT competition_id FROM dwh.dim_competitions', engine)
    invalid_comp = ~fact_games['competition_id'].isin(valid_competitions['competition_id'])
    if invalid_comp.any():
        print(f"    {invalid_comp.sum()} registros con competition_id inválido eliminados")
        fact_games = fact_games[~invalid_comp]
    
    # Validar clubs
    valid_clubs = pd.read_sql('SELECT club_id FROM dwh.dim_clubs', engine)
    invalid_home = ~fact_games['home_club_id'].isin(valid_clubs['club_id'])
    invalid_away = ~fact_games['away_club_id'].isin(valid_clubs['club_id'])
    if invalid_home.any() or invalid_away.any():
        total_invalid = invalid_home.sum() + invalid_away.sum()
        print(f"    {total_invalid} registros con club_id inválido eliminados")
        fact_games = fact_games[~(invalid_home | invalid_away)]
    
    # Validar date_id
    valid_dates = pd.read_sql('SELECT date_id FROM dwh.dim_date', engine)
    invalid_dates = ~fact_games['date_id'].isin(valid_dates['date_id'])
    if invalid_dates.any():
        print(f"    {invalid_dates.sum()} registros con date_id inválido eliminados")
        fact_games = fact_games[~invalid_dates]

    # Reporte de NULLs residuales (debe ser 0 en todo)
    nulls_remaining = fact_games.isnull().sum().sum()
    if nulls_remaining == 0:
        print(f"Sin NULLs residuales en la tabla de hechos")
    else:
        print(f"{nulls_remaining} NULLs residuales detectados (revisar)")
    
    print(f"   ✓ {len(fact_games):,} registros listos para carga")
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.fact_games)...")
    
    fact_games.to_sql(
        'fact_games',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method=None,
        chunksize=5000
    )
    
    print(" fact_games cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_games")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.fact_games\n")

if __name__ == "__main__":
    etl_fact_games()
