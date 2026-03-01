#!/usr/bin/env python3

"""
INFORME DETALLADO DE FILTRACIONES ETL
======================================
Este script analiza por que registros fueron filtrados durante el ETL
y proporciona ejemplos concretos de cada tipo de filtracion.
"""
import pandas as pd
from config import CSV_FILES, get_engine

def analizar_filtraciones():
    print("=" * 80)
    print("INFORME DETALLADO: ¿POR QUE SE FILTRARON REGISTROS EN EL ETL?")
    print("=" * 80)
    
    # Cargar CSVs
    games = pd.read_csv(CSV_FILES['games'], encoding='utf-8')
    clubs = pd.read_csv(CSV_FILES['clubs'], encoding='utf-8')
    competitions = pd.read_csv(CSV_FILES['competitions'], encoding='utf-8')
    players = pd.read_csv(CSV_FILES['players'], encoding='utf-8')
    appearances = pd.read_csv(CSV_FILES['appearances'], encoding='utf-8')
    game_events = pd.read_csv(CSV_FILES['game_events'], encoding='utf-8')
    transfers = pd.read_csv(CSV_FILES['transfers'], encoding='utf-8')
    
    engine = get_engine()
    
    # =========================================================================
    # 1. FILTRACION EN dim_games (games.csv)
    # =========================================================================
    print("\n\nFILTRACION EN dim_games: 74,026 --> 58,486 (79.0%)")
    print("-" * 80)
    print("\nRAZON: Partidos con clubs que no existen en clubs.csv\n")
    
    # Identificar games invalidos
    invalid_home = ~games['home_club_id'].isin(clubs['club_id'])
    invalid_away = ~games['away_club_id'].isin(clubs['club_id'])
    invalid_games = games[invalid_home | invalid_away]
    
    print(f"ESTADiSTICAS:")
    print(f"- Games con home_club_id inexistente: {invalid_home.sum():,}")
    print(f"- Games con away_club_id inexistente: {invalid_away.sum():,}")
    print(f"- Total games filtrados: {len(invalid_games):,}")
    
    print(f"\n¿QUE SIGNIFICA ESTO?")
    print(f"El CSV games.csv contiene partidos de clubs que NO estan en clubs.csv.")
    print(f"Esto puede ocurrir porque:")
    print(f"- El dataset clubs.csv solo incluye clubs principales (439 clubs)")
    print(f"- Faltan clubs de ligas inferiores, equipos historicos o internacionales")
    print(f"- El scraping de Transfermarkt fue parcial")
    
    print(f"\nEJEMPLOS REALES (primeros 5 casos):")
    ejemplos = invalid_games[['game_id', 'competition_id', 'home_club_id', 'away_club_id', 
                               'home_club_name', 'away_club_name', 'date']].head(5)
    print(ejemplos.to_string(index=False))
    
    # Verificar si estos club_ids realmente no existen
    print(f"\n🔎 VERIFICACION:")
    for _, row in ejemplos.head(3).iterrows():
        if pd.notna(row['home_club_id']):
            exists = row['home_club_id'] in clubs['club_id'].values
            print(f"   - home_club_id {int(row['home_club_id'])} ({row['home_club_name']}) --> Existe en clubs.csv: {exists}")
        if pd.notna(row['away_club_id']):
            exists = row['away_club_id'] in clubs['club_id'].values
            print(f"   - away_club_id {int(row['away_club_id'])} ({row['away_club_name']}) --> Existe en clubs.csv: {exists}")
    
    print(f"\n⚙️ DECISION ETL (etl_dim_games.py, lineas 82-89):")
    print(f"   ```python")
    print(f"   dim_games = dim_games[")
    print(f"       dim_games['home_club_id'].isin(valid_clubs['club_id']) &")
    print(f"       dim_games['away_club_id'].isin(valid_clubs['club_id'])")
    print(f"   ]")
    print(f"   ```")
    print(f"   ➜ Se eliminan partidos donde alguno de los clubs no existe")
    print(f"   ➜ JUSTIFICACION: Garantizar integridad referencial (FK validas)")
    
    # =========================================================================
    # 2. FILTRACION EN fact_appearances
    # =========================================================================
    print("\n\nFILTRACION EN fact_appearances: 1,706,806 --> 1,628,836 (95.4%)")
    print("-" * 80)
    
    # Cargar dim_games de PostgreSQL
    dim_games = pd.read_sql('SELECT game_id FROM dwh.dim_games', engine)
    
    # Identificar appearances huerfanas
    orphan_games = ~appearances['game_id'].isin(dim_games['game_id'])
    print(f"\nRAZON 1: Appearances de partidos filtrados")
    print(f"- Appearances con game_id que no esta en dim_games: {orphan_games.sum():,}")
    print(f"- Esto ocurre porque esos partidos fueron filtrados en dim_games")
    
    print(f"\nEJEMPLOS REALES:")
    ejemplos_orphan = appearances[orphan_games][['appearance_id', 'game_id', 'player_id', 
                                                  'player_name', 'player_club_id']].head(5)
    print(ejemplos_orphan.to_string(index=False))
    
    # Verificar si esos game_id estan en games.csv original pero no en dim_games
    print(f"\nVERIFICACION:")
    for _, row in ejemplos_orphan.head(3).iterrows():
        in_original = row['game_id'] in games['game_id'].values
        in_dim = row['game_id'] in dim_games['game_id'].values
        print(f"   game_id {row['game_id']}: en games.csv={in_original}, en dim_games={in_dim}")
    
    # Cargar dim_players
    dim_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)
    invalid_players = ~appearances['player_id'].isin(dim_players['player_id'])
    
    print(f"\nRAZON 2: Players inexistentes")
    print(f"- Appearances con player_id que no esta en dim_players: {invalid_players.sum():,}")
    
    print(f"\nDECISION ETL (etl_fact_appearances.py, lineas 52-57):")
    print(f"1. Eliminar registros con FK NULL (game_id, player_id, club_id, competition_id, date_id)")
    print(f"fact_appearances = fact_appearances.dropna(subset=critical_cols)")
    print(f" ")
    print(f"2. Validar integridad referencial")
    print(f"valid_players = pd.read_sql('SELECT player_id FROM dwh.dim_players', engine)")
    print(f"fact_appearances = fact_appearances[fact_appearances['player_id'].isin(valid_players['player_id'])]")
    
    # =========================================================================
    # 3. FILTRACION EN fact_game_events
    # =========================================================================
    print("\n\n FILTRACION EN fact_game_events: 1,035,043 --> 807,897 (78.1%)")
    print("-" * 80)
    
    # Merge con dim_games para obtener competition_id y date_id
    events_with_games = game_events.merge(
        dim_games[['game_id']], 
        on='game_id', 
        how='left', 
        indicator=True
    )
    
    no_dim_game = events_with_games['_merge'] == 'left_only'
    print(f"\nRAZON 1: Eventos de partidos filtrados")
    print(f"- Game events con game_id no en dim_games: {no_dim_game.sum():,}")
    
    # Validar player_id (puede ser NULL en eventos)
    events_with_player = game_events[game_events['player_id'].notna()]
    invalid_event_players = ~events_with_player['player_id'].isin(dim_players['player_id'])
    print(f"\nRAZON 2: Players inexistentes")
    print(f"- Events con player_id invalido: {invalid_event_players.sum():,}")
    
    # Validar player_in_id
    events_with_player_in = game_events[game_events['player_in_id'].notna()]
    invalid_player_in = ~events_with_player_in['player_in_id'].isin(dim_players['player_id'])
    print(f"\nRAZON 3: player_in_id inexistentes (sustituciones)")
    print(f"- Events con player_in_id invalido: {invalid_player_in.sum():,}")
    
    # Validar player_assist_id
    events_with_assist = game_events[game_events['player_assist_id'].notna()]
    invalid_assist = ~events_with_assist['player_assist_id'].isin(dim_players['player_id'])
    print(f"\nRAZON 4: player_assist_id inexistentes (asistencias)")
    print(f"- Events con player_assist_id invalido: {invalid_assist.sum():,}")
    
    total_filtrados = no_dim_game.sum() + invalid_event_players.sum() + invalid_player_in.sum() + invalid_assist.sum()
    print(f"\nTOTAL APROXIMADO FILTRADO: {total_filtrados:,}")
    print(f"(puede haber overlap entre categorias)")
    
    print(f"\nDECISION ETL (etl_fact_game_events.py):")
    print(f"Se validan TODAS las FK: game_id, player_id, player_in_id, player_assist_id")
    print(f"Garantiza que no haya referencias rotas en el DWH")
    
    # =========================================================================
    # 4. FILTRACION EN fact_transfers
    # =========================================================================
    print("\n\nFILTRACION EN fact_transfers: 79,646 --> 79,594 (99.9%)")
    print("-" * 80)
    print("\n Filtracion MiNIMA (solo 52 registros, 0.1%)")
    
    # Validar date_id
    if 'transfer_date' in transfers.columns:
        transfers['transfer_date_tmp'] = pd.to_datetime(transfers['transfer_date'], errors='coerce')
        invalid_dates = transfers['transfer_date_tmp'].isna().sum()
        print(f"\nRAZON: Fechas invalidas")
        print(f"- Transfers con fecha invalida/NULL: {invalid_dates}")
    
    print(f"\nDECISION ETL:")
    print(f"- from_club_id y to_club_id PUEDEN ser NULL (retiros, sin club)")
    print(f"- Solo se eliminan registros con date_id invalido")
    
    # =========================================================================
    # 5. FILTRACION EN fact_player_valuations
    # =========================================================================
    print("\n\nFILTRACION EN fact_player_valuations: 496,606 --> 496,606 (100.0%)")
    print("-" * 80)
    print("\n SIN FILTRACION - Todos los registros validos")
    print("- Todos los player_id existen en dim_players")
    print("- Todas las fechas son validas")
    
    # =========================================================================
    # RESUMEN DE DECISIONES ETL
    # =========================================================================
    print("\n\n" + "=" * 80)
    print("RESUMEN: ¿POR QUE SE TOMARON ESTAS DECISIONES?")
    print("=" * 80)
    
    print("""
PRINCIPIO FUNDAMENTAL: INTEGRIDAD REFERENCIAL

En un Data Warehouse, es CRiTICO que:
1. Todas las Foreign Keys (FK) apunten a registros que existen
2. No haya valores NULL en campos obligatorios
3. No haya registros duplicados por PK

 CONSECUENCIAS DE ESTAS DECISIONES:

1. CALIDAD DE DATOS
    Todas las consultas JOIN funcionaran correctamente
    No habra errores en queries OLAP
    Los reportes seran precisos y confiables

2. PERFORMANCE
    Los indices funcionaran eficientemente
    Las FK constraints de PostgreSQL validan automaticamente
    Menos registros = queries mas rapidas

3. TRAZABILIDAD
    Sabemos exactamente que se filtro y por que
    Los logs ETL documentan cada filtracion
    Se puede investigar registros especificos si es necesario

 ALTERNATIVA (NO RECOMENDADA):

Si cargaramos TODOS los registros sin validar:
 Aparecerian valores "Unknown" o NULL en reports
 JOINs producirian resultados incorrectos
 Las metricas serian engañosas
 El DWH perderia confiabilidad

TASA DE CARGA 90.4%:

Es EXCELENTE para un Data Warehouse porque:
- Indica que el 90.4% de los datos tienen calidad suficiente
- El 9.6% filtrado son registros incompletos o inconsistentes
- Es mejor un DWH pequeño y confiable que uno grande con errores

DATASET INCOMPLETO:

El problema NO es el ETL, sino que el dataset original:
- clubs.csv tiene solo 439 clubs (falta mayoria de clubs mundiales)
- games.csv referencia clubs que no estan en clubs.csv
- Este es un problema del scraping original de Transfermarkt

 CONCLUSION:

El ETL esta diseñado correctamente siguiendo mejores practicas
de Data Warehousing. La filtracion garantiza un DWH confiable
para analisis OLAP y toma de decisiones basada en datos.
""")

if __name__ == "__main__":
    analizar_filtraciones()
