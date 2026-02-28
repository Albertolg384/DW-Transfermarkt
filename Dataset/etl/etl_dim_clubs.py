#!/usr/bin/env python3

"""
ETL para dim_clubs (tabla de clubes)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE
from null_handler import apply_null_rules, validate_no_nulls

def etl_dim_clubs():
    """Extrae, transforma y carga la dimensión de clubes"""
    print("⚽ ETL: dim_clubs")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo clubs.csv...")
    df = pd.read_csv(CSV_FILES['clubs'], **PANDAS_READ_CONFIG)
    print(f"   ✓ {len(df):,} registros leídos")
    
    # TRANSFORM
    print("2️⃣ Transformando...")
    
    # Seleccionar columnas
    columns_to_keep = [
        'club_id', 'club_code', 'name', 'domestic_competition_id',
        'total_market_value', 'squad_size', 'average_age',
        'foreigners_number', 'foreigners_percentage', 'national_team_players',
        'stadium_name', 'stadium_seats', 'net_transfer_record',
        'coach_name', 'last_season', 'url'
    ]
    
    dim_clubs = df[columns_to_keep].copy()
    
    # Limpiar valores numéricos (total_market_value puede ser string con "€")
    # Por simplicidad, dejamos NaN si no se puede convertir
    numeric_cols = ['total_market_value', 'squad_size', 'average_age', 
                    'foreigners_number', 'foreigners_percentage', 
                    'national_team_players', 'stadium_seats']
    
    for col in numeric_cols:
        if col in dim_clubs.columns:
            dim_clubs[col] = pd.to_numeric(dim_clubs[col], errors='coerce')
    
    # Eliminar duplicados
    original_count = len(dim_clubs)
    dim_clubs = dim_clubs.drop_duplicates(subset=['club_id'])
    duplicates = original_count - len(dim_clubs)
    if duplicates > 0:
        print(f"   ⚠️ {duplicates} duplicados eliminados")
    
    # Validación: asegurar que club_id no sea nulo
    nulls = dim_clubs['club_id'].isna().sum()
    if nulls > 0:
        print(f"   ⚠️ {nulls} registros con club_id NULL eliminados")
        dim_clubs = dim_clubs[dim_clubs['club_id'].notna()]
    
    print(f"   ✓ {len(dim_clubs):,} registros listos para carga")
    # TRATAMIENTO CENTRALIZADO DE NULLs (módulo null_handler)
    dim_clubs = apply_null_rules(dim_clubs, 'dim_clubs', is_dimension=True)
    validate_no_nulls(dim_clubs, 'dim_clubs')
    
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.dim_clubs)...")
    engine = get_engine()
    
    dim_clubs.to_sql(
        'dim_clubs',
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
            INSERT INTO dwh.dim_clubs (club_id, club_code, name, domestic_competition_id,
                total_market_value, squad_size, average_age, foreigners_number,
                foreigners_percentage, national_team_players, stadium_name,
                stadium_seats, net_transfer_record, coach_name, last_season, url)
            VALUES (-1, 'N/A', 'Desconocido', 'N/A',
                -1, -1, -1, -1,
                -1, -1, 'N/A',
                -1, 'N/A', 'N/A', -1, 'N/A')
            ON CONFLICT (club_id) DO NOTHING
        """))
        conn.commit()
    print("   🏷️ Registro centinela (club_id=-1, 'Desconocido') insertado")
    
    print("✅ dim_clubs cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_clubs")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.dim_clubs\n")

if __name__ == "__main__":
    etl_dim_clubs()
