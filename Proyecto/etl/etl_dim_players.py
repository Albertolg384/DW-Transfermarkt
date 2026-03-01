#!/usr/bin/env python3

"""
ETL para dim_players (tabla de jugadores)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE
from null_handler import apply_null_rules, validate_no_nulls

def etl_dim_players():
    """Extrae, transforma y carga la dimensión de jugadores"""
    print("👤 ETL: dim_players")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo players.csv...")
    df = pd.read_csv(CSV_FILES['players'], **PANDAS_READ_CONFIG)
    print(f"   ✓ {len(df):,} registros leídos")
    
    # TRANSFORM
    print("2️⃣ Transformando...")
    
    # Columnas esperadas (ajustar según el CSV real)
    columns_to_keep = [
        'player_id', 'first_name', 'last_name', 'name', 'last_season',
        'current_club_id', 'player_code', 'country_of_birth', 'city_of_birth',
        'country_of_citizenship', 'date_of_birth', 'sub_position', 'position',
        'foot', 'height_in_cm', 'contract_expiration_date', 'agent_name',
        'image_url', 'url', 'current_club_domestic_competition_id',
        'current_club_name', 'market_value_in_eur', 'highest_market_value_in_eur'
    ]
    
    # Filtrar solo columnas que existan
    available_cols = [col for col in columns_to_keep if col in df.columns]
    dim_players = df[available_cols].copy()
    
    # Convertir fechas
    date_columns = ['date_of_birth', 'contract_expiration_date']
    for col in date_columns:
        if col in dim_players.columns:
            dim_players[col] = pd.to_datetime(dim_players[col], errors='coerce')
    
    # Convertir numéricos
    numeric_cols = ['height_in_cm', 'market_value_in_eur', 'highest_market_value_in_eur', 'current_club_id']
    for col in numeric_cols:
        if col in dim_players.columns:
            dim_players[col] = pd.to_numeric(dim_players[col], errors='coerce')
    
    # Eliminar duplicados
    original_count = len(dim_players)
    dim_players = dim_players.drop_duplicates(subset=['player_id'])
    duplicates = original_count - len(dim_players)
    if duplicates > 0:
        print(f"    {duplicates} duplicados eliminados")
    
    # Validación: asegurar que player_id no sea nulo
    nulls = dim_players['player_id'].isna().sum()
    if nulls > 0:
        print(f"    {nulls} registros con player_id NULL eliminados")
        dim_players = dim_players[dim_players['player_id'].notna()]
    
    # TRATAMIENTO CENTRALIZADO DE NULLs (módulo null_handler)
    dim_players = apply_null_rules(dim_players, 'dim_players', is_dimension=True)
    validate_no_nulls(dim_players, 'dim_players')
    
    print(f"   ✓ {len(dim_players):,} registros listos para carga")
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.dim_players)...")
    engine = get_engine()
    
    dim_players.to_sql(
        'dim_players',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method=None,
        chunksize=5000
    )
    
    # Insertar registro centinela (Kimball: Unknown Member Row)
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO dwh.dim_players (player_id, first_name, last_name, name, last_season,
                current_club_id, player_code, country_of_birth, city_of_birth,
                country_of_citizenship, date_of_birth, sub_position, position,
                foot, height_in_cm, contract_expiration_date, agent_name,
                image_url, url, current_club_domestic_competition_id,
                current_club_name, market_value_in_eur, highest_market_value_in_eur)
            VALUES (-1, 'N/A', 'N/A', 'Desconocido', -1,
                -1, 'N/A', 'N/A', 'N/A',
                'N/A', '1900-01-01', 'N/A', 'N/A',
                'N/A', -1, '9999-12-31', 'N/A',
                'N/A', 'N/A', 'N/A',
                'N/A', -1, -1)
            ON CONFLICT (player_id) DO NOTHING
        """))
        conn.commit()
    print("   🏷️ Registro centinela (player_id=-1, 'Desconocido') insertado")
    
    print(" dim_players cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_players")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.dim_players\n")

if __name__ == "__main__":
    etl_dim_players()
