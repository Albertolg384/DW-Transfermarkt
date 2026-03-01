#!/usr/bin/env python3

"""
ETL para dim_competitions (tabla de competiciones)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE
from null_handler import apply_null_rules, validate_no_nulls

def etl_dim_competitions():
    """Extrae, transforma y carga la dimensión de competiciones"""
    print("🏆 ETL: dim_competitions")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo competitions.csv...")
    df = pd.read_csv(CSV_FILES['competitions'], **PANDAS_READ_CONFIG)
    print(f"   ✓ {len(df):,} registros leídos")
    
    # TRANSFORM
    print("2️⃣ Transformando...")
    
    # Seleccionar columnas relevantes (todas las del CSV)
    columns_to_keep = [
        'competition_id', 'competition_code', 'name', 'sub_type', 'type',
        'country_id', 'country_name', 'domestic_league_code', 'confederation',
        'url', 'is_major_national_league'
    ]
    
    dim_competitions = df[columns_to_keep].copy()
    
    # Eliminar duplicados
    original_count = len(dim_competitions)
    dim_competitions = dim_competitions.drop_duplicates(subset=['competition_id'])
    duplicates = original_count - len(dim_competitions)
    if duplicates > 0:
        print(f"    {duplicates} duplicados eliminados")
    
    # Validación: asegurar que competition_id no sea nulo
    nulls = dim_competitions['competition_id'].isna().sum()
    if nulls > 0:
        print(f"    {nulls} registros con competition_id NULL eliminados")
        dim_competitions = dim_competitions[dim_competitions['competition_id'].notna()]
    
    # TRATAMIENTO CENTRALIZADO DE NULLs (módulo null_handler)
    dim_competitions = apply_null_rules(dim_competitions, 'dim_competitions', is_dimension=True)
    validate_no_nulls(dim_competitions, 'dim_competitions')

    print(f"   ✓ {len(dim_competitions):,} registros listos para carga")
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.dim_competitions)...")
    engine = get_engine()
    
    dim_competitions.to_sql(
        'dim_competitions',
        engine,
        schema='dwh',
        if_exists='append',  # Cambiar a 'replace' si quieres recargar todo
        index=False,
        method=None,
        chunksize=5000
    )
    
    print(" dim_competitions cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_competitions")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.dim_competitions\n")

if __name__ == "__main__":
    etl_dim_competitions()
