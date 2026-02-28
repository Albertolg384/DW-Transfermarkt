#!/usr/bin/env python3

"""
ETL para fact_transfers (traspasos de jugadores)
PK artificial: transfer_id (autoincremental en PostgreSQL)
"""
import pandas as pd
import re
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE

def parse_transfer_fee(fee_str):
    """Convierte strings de transfer_fee a EUR numérico"""
    if pd.isna(fee_str) or fee_str == '':
        return None
    
    fee_str = str(fee_str).strip().lower()
    
    # Casos especiales
    if fee_str in ['free', 'loan', 'free transfer', '-', '?']:
        return 0
    
    # Parsear valores con M (millones) o k (miles)
    # Ejemplos: "€50.00m", "€1.5m", "€500k"
    match = re.search(r'([\d.]+)\s*([mk])?', fee_str, re.IGNORECASE)
    if match:
        value = float(match.group(1))
        unit = match.group(2)
        
        if unit and unit.lower() == 'm':
            return value * 1_000_000
        elif unit and unit.lower() == 'k':
            return value * 1_000
        else:
            return value
    
    return None

def parse_season(season_str):
    """Convierte '23/24' en 2023, '99/00' en 1999"""
    if pd.isna(season_str):
        return -1
    season_str = str(season_str).strip()
    parts = season_str.split('/')
    if len(parts) == 2:
        try:
            year_short = int(parts[0])
            year_full = 2000 + year_short if year_short < 50 else 1900 + year_short
            return year_full
        except ValueError:
            return -1
    return -1

def etl_fact_transfers():
    """Extrae, transforma y carga la tabla de hechos de traspasos"""
    print("🔄 ETL: fact_transfers")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo transfers.csv...")
    df = pd.read_csv(CSV_FILES['transfers'], **PANDAS_READ_CONFIG)
    print(f"   ✓ {len(df):,} registros leídos")
    
    # TRANSFORM
    print("2️⃣ Transformando...")
    
    # Convertir fecha de traspaso y generar date_id
    if 'transfer_date' in df.columns:
        df['transfer_date'] = pd.to_datetime(df['transfer_date'], errors='coerce')
        df['transfer_date_id'] = df['transfer_date'].dt.strftime('%Y%m%d').astype('Int64')
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['transfer_date_id'] = df['date'].dt.strftime('%Y%m%d').astype('Int64')
    
    # Parsear transfer_fee (puede venir como string "€50.00m")
    if 'transfer_fee' in df.columns:
        if df['transfer_fee'].dtype == object:
            df['transfer_fee'] = df['transfer_fee'].apply(parse_transfer_fee)
    
    # Transformar transfer_season de '26/27' a 2026 (año de inicio en formato completo)
    # Esto mantiene consistencia con dim_games.season y permite queries OLAP eficientes
    if 'transfer_season' in df.columns:
        df['transfer_season'] = df['transfer_season'].apply(parse_season)
    
    fact_transfers = df.copy()
    
    # Seleccionar columnas (NO incluir transfer_id, se autogenera)
    columns_to_load = [
        'player_id', 'from_club_id', 'to_club_id', 'transfer_date_id',
        'transfer_season', 'player_name', 'from_club_name', 'to_club_name',
        'transfer_fee', 'market_value_in_eur'
    ]
    
    available_cols = [col for col in columns_to_load if col in fact_transfers.columns]
    fact_transfers = fact_transfers[available_cols].copy()
    
    # Validación: eliminar registros con FK críticas NULL
    # Nota: from_club_id y to_club_id pueden ser NULL si vienen/van fuera del dataset
    critical_cols = ['player_id', 'transfer_date_id']
    nulls_before = len(fact_transfers)
    fact_transfers = fact_transfers.dropna(subset=critical_cols)
    nulls_removed = nulls_before - len(fact_transfers)
    if nulls_removed > 0:
        print(f"   ⚠️ {nulls_removed} registros con FK críticas NULL eliminados")
    
    # Verificar integridad referencial
    print("   🔍 Verificando integridad referencial...")
    engine = get_engine()
    
    # Validar player_id
    valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
    invalid_players = ~fact_transfers['player_id'].isin(valid_players['player_id'])
    if invalid_players.any():
        print(f"   ⚠️ {invalid_players.sum()} registros con player_id inválido eliminados")
        fact_transfers = fact_transfers[~invalid_players]
    
    # Validar from_club_id (solo si no es NULL)
  
    valid_clubs = pd.read_sql('SELECT club_id FROM dwh.dim_clubs', engine)

    # from_club_id: si no existe en dim_clubs es NULL (FK nullable en DDL)
    if 'from_club_id' in fact_transfers.columns:
        invalid_from = (
            fact_transfers['from_club_id'].notna() &
            ~fact_transfers['from_club_id'].isin(valid_clubs['club_id'])
        )
        if invalid_from.any():
            print(f"   ⚠️ {invalid_from.sum()} from_club_id fuera del dataset → NULL")
            fact_transfers.loc[invalid_from, 'from_club_id'] = None

    # to_club_id: si no existe en dim_clubs es NULL (FK nullable en DDL)
    if 'to_club_id' in fact_transfers.columns:
        invalid_to = (
            fact_transfers['to_club_id'].notna() &
            ~fact_transfers['to_club_id'].isin(valid_clubs['club_id'])
        )
        if invalid_to.any():
            print(f"   ⚠️ {invalid_to.sum()} to_club_id fuera del dataset → NULL")
            fact_transfers.loc[invalid_to, 'to_club_id'] = None
    
    # Validar date_id
    valid_dates = pd.read_sql('SELECT date_id FROM dwh.dim_date', engine)
    invalid_dates = ~fact_transfers['transfer_date_id'].isin(valid_dates['date_id'])
    if invalid_dates.any():
        print(f"   ⚠️ {invalid_dates.sum()} registros con date_id inválido eliminados")
        fact_transfers = fact_transfers[~invalid_dates]
    
    # ------------------------------------------------------------------
    # TRATAMIENTO DE NULLs
    # ------------------------------------------------------------------

    # from_club_id / to_club_id: se dejan NULL (FK nullable en DDL)
    # No se puede usar -1 porque violaría la FK constraint hacia dim_clubs
    # Semántica: NULL = club fuera del dataset (retiro, agente libre, liga menor)

    # transfer_season: -1 si no se pudo parsear
    if 'transfer_season' in fact_transfers.columns:
        fact_transfers['transfer_season'] = fact_transfers['transfer_season'].fillna(-1).astype(int)

    # transfer_fee: -1 si desconocida (distinto de 0 que significa traspaso gratuito)
    if 'transfer_fee' in fact_transfers.columns:
        fact_transfers['transfer_fee'] = fact_transfers['transfer_fee'].fillna(-1)

    # market_value_in_eur: -1 si sin valoracion registrada
    if 'market_value_in_eur' in fact_transfers.columns:
        fact_transfers['market_value_in_eur'] = fact_transfers['market_value_in_eur'].fillna(-1)

    # Textos --> 'N/A' si no hay nombre registrado
    for col in ['player_name', 'from_club_name', 'to_club_name']:
        if col in fact_transfers.columns:
            fact_transfers[col] = fact_transfers[col].fillna('N/A')

    # ------------------------------------------------------------------

    # Reporte de NULLs residuales (debería ser 0 en todo)
    # from_club_id y to_club_id PUEDEN ser NULL legítimamente (FK nullable)
    cols_no_nullable = ['player_id', 'transfer_date_id', 'transfer_season',
                        'player_name', 'from_club_name', 'to_club_name',
                        'transfer_fee', 'market_value_in_eur']
    cols_no_nullable = [c for c in cols_no_nullable if c in fact_transfers.columns]
    nulls_remaining = fact_transfers[cols_no_nullable].isnull().sum().sum()

    if nulls_remaining == 0:
        print(f"Sin NULLs residuales en la tabla de hechos")
    else:
        print(f" {nulls_remaining} NULLs residuales detectados (revisar)")
        print(fact_transfers[cols_no_nullable].isnull().sum()[fact_transfers[cols_no_nullable].isnull().sum() > 0])
    print(f"   ✓ {len(fact_transfers):,} registros listos para carga")
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.fact_transfers)...")
    
    fact_transfers.to_sql(
        'fact_transfers',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method=None,
        chunksize=5000
    )
    
    print("✅ fact_transfers cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_transfers")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.fact_transfers\n")

if __name__ == "__main__":
    etl_fact_transfers()
