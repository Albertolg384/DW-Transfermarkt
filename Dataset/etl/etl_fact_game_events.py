#!/usr/bin/env python3

"""
ETL para fact_game_events (eventos minuto a minuto en partidos)
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG, BATCH_SIZE

def etl_fact_game_events():
    """Extrae, transforma y carga la tabla de hechos de eventos"""
    print("⚡ ETL: fact_game_events")
    print("=" * 60)
    
    # EXTRACT
    print("1️⃣ Extrayendo game_events.csv...")
    df = pd.read_csv(CSV_FILES['game_events'], **PANDAS_READ_CONFIG)
    print(f"   ✓ {len(df):,} registros leídos")
    
    # TRANSFORM
    print("2️⃣ Transformando...")
    
    # Obtener date_id y competition_id desde dim_games
    engine = get_engine()
    games_info = pd.read_sql(
        'SELECT game_id, competition_id, date FROM dwh.dim_games', 
        engine
    )
    games_info['date'] = pd.to_datetime(games_info['date'], errors='coerce')
    games_info['date_id'] = games_info['date'].dt.strftime('%Y%m%d').astype('Int64')
    
    fact_events = df.merge(
        games_info[['game_id', 'competition_id', 'date_id']],
        on='game_id',
        how='left',
        suffixes=('_csv', '')
    )
    

    # Renombrar game_event_id --> event_id para coincidir con el DDL
    if 'game_event_id' in fact_events.columns:
        fact_events = fact_events.rename(columns={'game_event_id': 'event_id'})
    elif 'id' in fact_events.columns:
        fact_events = fact_events.rename(columns={'id': 'event_id'})
    else:
        fact_events['event_id'] = range(1, len(fact_events) + 1)
    
    # Seleccionar columnas
    columns_to_load = [
        'event_id', 'game_id', 'club_id', 'player_id', 'date_id', 'competition_id',
        'type', 'description', 'player_in_id', 'player_assist_id', 'minute'
    ]
    
    available_cols = [col for col in columns_to_load if col in fact_events.columns]
    fact_events = fact_events[available_cols].copy()

    # ------------------------------------------------------------------
    # TRATAMIENTO DE NULLs E INCONSISTENCIAS POR TIPO DE EVENTO
    # ------------------------------------------------------------------

    # type --> 'Unknown' si no esta registrado
    if 'type' in fact_events.columns:
        fact_events['type'] = fact_events['type'].fillna('Unknown')

    # description: limpiar coma/espacio inicial que viene del scraping
    # Ej: ", Not reported" --> "Not reported" | ", Header, ..." --> "Header, ..."
    if 'description' in fact_events.columns:
        fact_events['description'] = (
            fact_events['description']
            .fillna('Unknown')
            .str.lstrip(', ')
            .str.strip()
            .replace('', 'Unknown')
        )

    # minute --> -1 si no esta registrado
    if 'minute' in fact_events.columns:
        fact_events['minute'] = fact_events['minute'].fillna(-1).astype(int)

    # ------------------------------------------------------------------
    # player_in_id: SOLO aplica en Substitutions
    # Si viene relleno en Cards o Goals --> inconsistencia, poner NULL
    # ------------------------------------------------------------------
    if 'player_in_id' in fact_events.columns:
        inconsistencias_in = (
            fact_events['player_in_id'].notna() &
            (fact_events['type'] != 'Substitutions')
        )
        if inconsistencias_in.any():
            print(f"{inconsistencias_in.sum()} player_in_id en evento no-sustitución → NULL")
            fact_events.loc[inconsistencias_in, 'player_in_id'] = None

    # ------------------------------------------------------------------
    # player_assist_id: SOLO aplica en Goals
    # Si viene relleno en Cards o Substitutions --> inconsistencia, poner NULL
    # ------------------------------------------------------------------
    if 'player_assist_id' in fact_events.columns:
        inconsistencias_assist = (
            fact_events['player_assist_id'].notna() &
            (fact_events['type'] != 'Goals')
        )
        if inconsistencias_assist.any():
            print(f"{inconsistencias_assist.sum()} player_assist_id en evento no-gol → NULL")
            fact_events.loc[inconsistencias_assist, 'player_assist_id'] = None

    # player_id: puede ser NULL en eventos de equipo se deja NULL (FK nullable en DDL)

    
    # Validación: eliminar registros con FK críticas NULL (player_id puede ser NULL)
    critical_cols = ['event_id', 'game_id', 'club_id', 'date_id']
    # Filtrar solo columnas críticas que existan
    critical_cols = [col for col in critical_cols if col in fact_events.columns]
    nulls_before = len(fact_events)
    fact_events = fact_events.dropna(subset=critical_cols)
    nulls_removed = nulls_before - len(fact_events)
    if nulls_removed > 0:
        print(f"   ⚠️ {nulls_removed} registros con FK críticas NULL eliminados")
    
    # Verificar integridad referencial (solo para valores no NULL)
    print("   🔍 Verificando integridad referencial...")
    
    # Validar club_id
    valid_clubs = pd.read_sql('SELECT club_id FROM dwh.dim_clubs', engine)
    invalid_clubs = ~fact_events['club_id'].isin(valid_clubs['club_id'])
    if invalid_clubs.any():
        print(f"   ⚠️ {invalid_clubs.sum()} registros con club_id inválido eliminados")
        fact_events = fact_events[~invalid_clubs]
    
    # Validar player_id (solo si no es NULL)
    if 'player_id' in fact_events.columns:
        valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
        invalid_players = (
            fact_events['player_id'].notna() & 
            ~fact_events['player_id'].isin(valid_players['player_id'])
        )
        if invalid_players.any():
            print(f"   ⚠️ {invalid_players.sum()} registros con player_id inválido eliminados")
            fact_events = fact_events[~invalid_players]
    
    # Validar player_in_id (solo si no es NULL)
    if 'player_in_id' in fact_events.columns:
        valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
        invalid_in = (
            fact_events['player_in_id'].notna() & 
            ~fact_events['player_in_id'].isin(valid_players['player_id'])
        )
        if invalid_in.any():
            print(f"   ⚠️ {invalid_in.sum()} registros con player_in_id inválido eliminados")
            fact_events = fact_events[~invalid_in]
    
    # Validar player_assist_id (solo si no es NULL)
    if 'player_assist_id' in fact_events.columns:
        valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
        invalid_assist = (
            fact_events['player_assist_id'].notna() & 
            ~fact_events['player_assist_id'].isin(valid_players['player_id'])
        )
        if invalid_assist.any():
            print(f"   ⚠️ {invalid_assist.sum()} registros con player_assist_id inválido eliminados")
            fact_events = fact_events[~invalid_assist]
    
    # Reporte de NULLs residuales (solo en campos no-nullable)
    cols_no_nullable = ['event_id', 'game_id', 'club_id', 'date_id', 'competition_id',
                        'type', 'description', 'minute']
    cols_no_nullable = [c for c in cols_no_nullable if c in fact_events.columns]
    nulls_remaining = fact_events[cols_no_nullable].isnull().sum().sum()
    if nulls_remaining == 0:
        print(f"Sin NULLs en campos no-nullable")
    else:
        print(f"{nulls_remaining} NULLs residuales en campos no-nullable (revisar)")

    print(f"   ✓ {len(fact_events):,} registros listos para carga")
    
    # LOAD
    print("3️⃣ Cargando a PostgreSQL (dwh.fact_game_events)...")
    
    fact_events.to_sql(
        'fact_game_events',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method=None,
        chunksize=5000
    )
    
    print("✅ fact_game_events cargada exitosamente")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_game_events")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.fact_game_events\n")

if __name__ == "__main__":
    etl_fact_game_events()
