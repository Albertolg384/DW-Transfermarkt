#!/usr/bin/env python3

"""
ETL para dim_games (tabla de partidos como dimensión)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE
from null_handler import apply_null_rules, validate_no_nulls

def etl_dim_games():
    """Extrae, transforma y carga la dimensión de partidos"""
    print("⚽📅 ETL: dim_games")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo games.csv...")
    df = pd.read_csv(CSV_FILES['games'], **PANDAS_READ_CONFIG)
    print(f"   ✓ {len(df):,} registros leídos")
    
    # TRANSFORM
    print("2️⃣ Transformando...")
    
    columns_to_keep = [
        'game_id', 'competition_id', 'season', 'round', 'date',
        'home_club_id', 'away_club_id', 'home_club_name', 'away_club_name',
        'stadium', 'attendance', 'referee', 'url',
        'home_club_formation', 'away_club_formation',
        'home_club_manager_name', 'away_club_manager_name',
        'aggregate', 'competition_type'
    ]
    
    available_cols = [col for col in columns_to_keep if col in df.columns]
    dim_games = df[available_cols].copy()
    
    # Convertir fecha
    if 'date' in dim_games.columns:
        dim_games['date'] = pd.to_datetime(dim_games['date'], errors='coerce')
    
    # Convertir numéricos
    numeric_cols = ['game_id', 'season', 'home_club_id', 'away_club_id', 'attendance']
    for col in numeric_cols:
        if col in dim_games.columns:
            dim_games[col] = pd.to_numeric(dim_games[col], errors='coerce')
    
    # Truncar campos VARCHAR que pueden exceder límites del DDL
    varchar_limits = {
        'home_club_formation': 20,
        'away_club_formation': 20,
        'aggregate': 20,
        'round': 50,
        'competition_type': 50
    }
    
    for col, max_length in varchar_limits.items():
        if col in dim_games.columns:
            dim_games[col] = dim_games[col].astype(str).str[:max_length]
            # Reemplazar 'nan' string con None
            dim_games[col] = dim_games[col].replace('nan', None)
    
    # Eliminar duplicados
    original_count = len(dim_games)
    dim_games = dim_games.drop_duplicates(subset=['game_id'])
    duplicates = original_count - len(dim_games)
    if duplicates > 0:
        print(f"    {duplicates} duplicados eliminados")
    
    # Validación: registros con game_id, competition_id, date válidos
    critical_nulls = dim_games[['game_id', 'competition_id', 'date']].isna().any(axis=1).sum()
    if critical_nulls > 0:
        print(f"    {critical_nulls} registros con campos críticos NULL eliminados")
        dim_games = dim_games[
            dim_games['game_id'].notna() & 
            dim_games['competition_id'].notna() & 
            dim_games['date'].notna()
        ]
    
    # TRATAMIENTO CENTRALIZADO DE NULLs (módulo null_handler)
    dim_games = apply_null_rules(dim_games, 'dim_games', is_dimension=True)
    
    # Validar integridad referencial con dim_clubs
    print("   🔍 Verificando integridad referencial con dim_clubs...")
    engine = get_engine()
    valid_clubs = pd.read_sql('SELECT club_id FROM dwh.dim_clubs', engine)
    
    # Filtrar registros con clubs que no existen
    before_fk_filter = len(dim_games)
    dim_games = dim_games[
        dim_games['home_club_id'].isin(valid_clubs['club_id']) &
        dim_games['away_club_id'].isin(valid_clubs['club_id'])
    ]
    fk_removed = before_fk_filter - len(dim_games)
    if fk_removed > 0:
        print(f"    {fk_removed} registros con clubs inexistentes eliminados")
    
    # Validar competition_id
    valid_competitions = pd.read_sql('SELECT competition_id FROM dwh.dim_competitions', engine)
    before_comp_filter = len(dim_games)
    dim_games = dim_games[dim_games['competition_id'].isin(valid_competitions['competition_id'])]
    comp_removed = before_comp_filter - len(dim_games)
    if comp_removed > 0:
        print(f"    {comp_removed} registros con competiciones inexistentes eliminados")
    
    # Validación final de NULLs
    validate_no_nulls(dim_games, 'dim_games')

    print(f"   ✓ {len(dim_games):,} registros listos para carga")
        
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.dim_games)...")
    
    # Insertar en chunks más pequeños para evitar problemas de parámetros
    chunk_size = 1000  # Reducido para esta tabla por el número de columnas
    total_chunks = len(dim_games) // chunk_size + (1 if len(dim_games) % chunk_size else 0)
    
    print(f"   Insertando en {total_chunks} lotes de hasta {chunk_size} registros...")
    
    for i in range(0, len(dim_games), chunk_size):
        chunk = dim_games.iloc[i:i+chunk_size]
        chunk.to_sql(
            'dim_games',
            engine,
            schema='dwh',
            if_exists='append',
            index=False,
            method=None,  # Usar método por defecto en lugar de 'multi'
        )
        if (i // chunk_size + 1) % 10 == 0:
            print(f"   Progreso: {i + len(chunk):,}/{len(dim_games):,} registros")
    
    print(" dim_games cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_games")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.dim_games\n")

if __name__ == "__main__":
    etl_dim_games()
