#!/usr/bin/env python3

"""
ETL para dim_competitions (tabla de competiciones)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE

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
    
    # ------------------------------------------------------------------
    # TRATAMIENTO DE NULLs (Kimball: nunca NULLs en dimensiones)
    # ------------------------------------------------------------------

    # Campos de texto --> 'N/A'
    # Ocurre en competiciones internacionales (Champions, Mundial, etc.)
    # que no tienen pais, liga domestica ni confederacion asignada
    text_cols = [
        'competition_code', 'name', 'sub_type', 'type',
        'country_name', 'domestic_league_code', 'confederation', 'url'
    ]
    for col in text_cols:
        if col in dim_competitions.columns:
            dim_competitions[col] = dim_competitions[col].fillna('N/A')

    # Campos numericos enteros--> -1
    # country_id NULL significa competicion sin país asignado (ej: UEFA Champions)
    int_cols = ['country_id']
    for col in int_cols:
        if col in dim_competitions.columns:
            dim_competitions[col] = dim_competitions[col].fillna(-1).astype(int)

    # Booleanos --> False
    bool_cols = ['is_major_national_league']
    for col in bool_cols:
        if col in dim_competitions.columns:
            dim_competitions[col] = dim_competitions[col].fillna(False)
    
    # Eliminar duplicados
    original_count = len(dim_competitions)
    dim_competitions = dim_competitions.drop_duplicates(subset=['competition_id'])
    duplicates = original_count - len(dim_competitions)
    if duplicates > 0:
        print(f"   ⚠️ {duplicates} duplicados eliminados")
    
    # Validación: asegurar que competition_id no sea nulo
    nulls = dim_competitions['competition_id'].isna().sum()
    if nulls > 0:
        print(f"   ⚠️ {nulls} registros con competition_id NULL eliminados")
        dim_competitions = dim_competitions[dim_competitions['competition_id'].notna()]
    
    # Reporte de NULLs residuales (debe ser 0 en todo)
    nulls_remaining = dim_competitions.isnull().sum().sum()
    if nulls_remaining == 0:
        print(f"Sin NULLs residuales en la dimensión")
    else:
        print(f"{nulls_remaining} NULLs residuales detectados (revisar)")

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
    
    print("✅ dim_competitions cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_competitions")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.dim_competitions\n")

if __name__ == "__main__":
    etl_dim_competitions()
