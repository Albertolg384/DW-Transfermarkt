#!/usr/bin/env python3

"""
ETL para fact_player_valuations (tabla de hechos de valoraciones)
PK artificial: valuation_id (autoincremental en PostgreSQL)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG
from null_handler import apply_null_rules, validate_no_nulls

def etl_fact_player_valuations():
    """Extrae, transforma y carga la tabla de hechos de valoraciones"""
    print("ETL: fact_player_valuations")
    print("=" * 60)
    
    # EXTRACT
    print("Extrayendo player_valuations.csv...")
    df = pd.read_csv(CSV_FILES['player_valuations'], **PANDAS_READ_CONFIG)
    print(f"{len(df):,} registros leidos")
    
    # TRANSFORM
    print("Transformando...")
    
    # Convertir fecha y generar date_id
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['date_id'] = df['date'].dt.strftime('%Y%m%d').astype('Int64')
    
    # Renombrar columnas para coincidir con el esquema
    fact_valuations = df.rename(columns={
        'current_club_id': 'club_id',
        'player_club_domestic_competition_id': 'competition_id'
    })
    
    # Seleccionar columnas (NO incluir valuation_id, se autogenera)
    columns_to_load = [
        'player_id', 'club_id', 'competition_id', 'date_id', 'market_value_in_eur'
    ]
    
    fact_valuations = fact_valuations[columns_to_load].copy()
    
    # TRATAMIENTO CENTRALIZADO DE NULLs (modulo null_handler)
    fact_valuations = apply_null_rules(fact_valuations, 'fact_player_valuations', is_dimension=False)
    validate_no_nulls(fact_valuations, 'fact_player_valuations')
    
    # Validacion: eliminar registros con FK criticas NULL
    critical_cols = ['player_id', 'club_id', 'date_id']
    nulls_before = len(fact_valuations)
    fact_valuations = fact_valuations.dropna(subset=critical_cols)
    nulls_removed = nulls_before - len(fact_valuations)
    if nulls_removed > 0:
        print(f"{nulls_removed} registros con FK NULL eliminados")
    
    # Verificar integridad referencial
    print("Verificando integridad referencial...")
    engine = get_engine()
    
    # Validar player_id
    valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
    invalid_players = ~fact_valuations['player_id'].isin(valid_players['player_id'])
    if invalid_players.any():
        print(f"{invalid_players.sum()} registros con player_id invalido eliminados")
        fact_valuations = fact_valuations[~invalid_players]
    
    # Validar club_id
    valid_clubs = pd.read_sql('SELECT club_id FROM dwh.dim_clubs', engine)
    invalid_clubs = ~fact_valuations['club_id'].isin(valid_clubs['club_id'])
    if invalid_clubs.any():
        print(f"{invalid_clubs.sum()} registros con club_id invalido eliminados")
        fact_valuations = fact_valuations[~invalid_clubs]
    
    # Validar date_id
    valid_dates = pd.read_sql('SELECT date_id FROM dwh.dim_date', engine)
    invalid_dates = ~fact_valuations['date_id'].isin(valid_dates['date_id'])
    if invalid_dates.any():
        print(f"{invalid_dates.sum()} registros con date_id invalido eliminados")
        fact_valuations = fact_valuations[~invalid_dates]
    
    print(f"{len(fact_valuations):,} registros listos para carga")
    
    # LOAD
    print("Cargando a PostgreSQL (dwh.fact_player_valuations)...")
    
    fact_valuations.to_sql(
        'fact_player_valuations',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method=None,
        chunksize=5000
    )
    
    print("fact_player_valuations cargada exitosamente")
    
    # Verificacion
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_player_valuations")).fetchone()[0]
        print(f"Verificacion: {count:,} registros en dwh.fact_player_valuations\n")

if __name__ == "__main__":
    etl_fact_player_valuations()
