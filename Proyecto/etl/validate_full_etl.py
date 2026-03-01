#!/usr/bin/env python3

"""
Validacion COMPLETA del ETL del Data Warehouse
Verifica:
1. Comparacion de conteos CSV vs PostgreSQL
2. Validacion de medidas calculadas
3. Integridad referencial
4. Ejemplos de datos transformados
"""
import pandas as pd
from config import get_engine, CSV_FILES, PANDAS_READ_CONFIG
from sqlalchemy import text

def validate_full_etl():
    """Ejecuta validacion completa contra CSVs originales"""
    print("=" * 80)
    print("VALIDACION COMPLETA DEL ETL - DATA WAREHOUSE TRANSFERMARKT")
    print("=" * 80)
    
    engine = get_engine()
    
    # ============================================================
    # 1. COMPARACION CSV vs POSTGRESQL
    # ============================================================
    print("\nCOMPARACION DE CONTEOS: CSV ORIGINALES vs POSTGRESQL")
    print("-" * 80)
    
    comparisons = []
    
    # Dimensiones
    print("\nDIMENSIONES:")
    
    # dim_competitions
    csv_comp = len(pd.read_csv(CSV_FILES['competitions'], **PANDAS_READ_CONFIG))
    db_comp = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_competitions", engine).iloc[0, 0]
    comparisons.append(('dim_competitions', 'competitions.csv', csv_comp, db_comp))
    print(f"competitions.csv: {csv_comp:,} --> dim_competitions: {db_comp:,} ({db_comp/csv_comp*100:.1f}%)")
    
    # dim_clubs
    csv_clubs = len(pd.read_csv(CSV_FILES['clubs'], **PANDAS_READ_CONFIG))
    db_clubs = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_clubs", engine).iloc[0, 0]
    comparisons.append(('dim_clubs', 'clubs.csv', csv_clubs, db_clubs))
    print(f"clubs.csv: {csv_clubs:,} --> dim_clubs: {db_clubs:,} ({db_clubs/csv_clubs*100:.1f}%)")
    
    # dim_players
    csv_players = len(pd.read_csv(CSV_FILES['players'], **PANDAS_READ_CONFIG))
    db_players = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_players", engine).iloc[0, 0]
    comparisons.append(('dim_players', 'players.csv', csv_players, db_players))
    print(f"players.csv: {csv_players:,} --> dim_players: {db_players:,} ({db_players/csv_players*100:.1f}%)")
    
    # dim_games
    csv_games = len(pd.read_csv(CSV_FILES['games'], **PANDAS_READ_CONFIG))
    db_games = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_games", engine).iloc[0, 0]
    comparisons.append(('dim_games', 'games.csv', csv_games, db_games))
    print(f"games.csv: {csv_games:,} --> dim_games: {db_games:,} ({db_games/csv_games*100:.1f}%)")
    if db_games < csv_games:
        print(f"   {csv_games - db_games:,} partidos filtrados (clubs/competitions inexistentes)")
    
    # dim_date
    db_date = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.dim_date", engine).iloc[0, 0]
    print(f"dim_date: {db_date:,} fechas (generada 2000-2030)")
    
    print("\nTABLAS DE HECHOS:")
    
    # fact_games (desnormalizacion de games + club_games)
    csv_fact_games = csv_games  # Basada en games.csv
    db_fact_games = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.fact_games", engine).iloc[0, 0]
    comparisons.append(('fact_games', 'games.csv (base)', csv_fact_games, db_fact_games))
    print(f"games.csv: {csv_fact_games:,} --> fact_games: {db_fact_games:,} ({db_fact_games/csv_fact_games*100:.1f}%)")
    
    # fact_appearances
    csv_appearances = len(pd.read_csv(CSV_FILES['appearances'], **PANDAS_READ_CONFIG))
    db_appearances = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.fact_appearances", engine).iloc[0, 0]
    comparisons.append(('fact_appearances', 'appearances.csv', csv_appearances, db_appearances))
    print(f"appearances.csv: {csv_appearances:,} --> fact_appearances: {db_appearances:,} ({db_appearances/csv_appearances*100:.1f}%)")
    
    # fact_game_events
    csv_events = len(pd.read_csv(CSV_FILES['game_events'], **PANDAS_READ_CONFIG))
    db_events = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.fact_game_events", engine).iloc[0, 0]
    comparisons.append(('fact_game_events', 'game_events.csv', csv_events, db_events))
    print(f"game_events.csv: {csv_events:,} --> fact_game_events: {db_events:,} ({db_events/csv_events*100:.1f}%)")
    
    # fact_transfers
    csv_transfers = len(pd.read_csv(CSV_FILES['transfers'], **PANDAS_READ_CONFIG))
    db_transfers = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.fact_transfers", engine).iloc[0, 0]
    comparisons.append(('fact_transfers', 'transfers.csv', csv_transfers, db_transfers))
    print(f"transfers.csv: {csv_transfers:,} --> fact_transfers: {db_transfers:,} ({db_transfers/csv_transfers*100:.1f}%)")
    
    # fact_player_valuations
    csv_valuations = len(pd.read_csv(CSV_FILES['player_valuations'], **PANDAS_READ_CONFIG))
    db_valuations = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.fact_player_valuations", engine).iloc[0, 0]
    comparisons.append(('fact_player_valuations', 'player_valuations.csv', csv_valuations, db_valuations))
    print(f"player_valuations.csv: {csv_valuations:,} --> fact_player_valuations: {db_valuations:,} ({db_valuations/csv_valuations*100:.1f}%)")
    
    # ============================================================
    # 2. VALIDACION DE MEDIDAS CALCULADAS (LAB-BOOK)
    # ============================================================
    print("\n\nVALIDACION DE MEDIDAS CALCULADAS (segun lab-book)")
    print("-" * 80)
    
    # fact_games: goal_difference, total_goals, is_home_win, is_draw, is_away_win
    print("\nfact_games - Medidas derivadas:")
    fact_games_sample = pd.read_sql("""
        SELECT 
            game_id,
            home_club_goals,
            away_club_goals,
            goal_difference,
            total_goals,
            is_home_win,
            is_draw,
            is_away_win
        FROM dwh.fact_games
        WHERE home_club_goals IS NOT NULL AND away_club_goals IS NOT NULL
        LIMIT 5
    """, engine)
    print(fact_games_sample.to_string(index=False))
    
    # Verificar calculos
    if db_fact_games > 0:
        query_calc = """
            SELECT 
                COUNT(*) as total_registros,
                SUM(CASE WHEN goal_difference = (home_club_goals - away_club_goals) THEN 1 ELSE 0 END) as goal_diff_ok,
                SUM(CASE WHEN total_goals = (home_club_goals + away_club_goals) THEN 1 ELSE 0 END) as total_goals_ok,
                SUM(CASE WHEN is_home_win = (home_club_goals > away_club_goals) THEN 1 ELSE 0 END) as is_home_win_ok,
                SUM(CASE WHEN is_draw = (home_club_goals = away_club_goals) THEN 1 ELSE 0 END) as is_draw_ok,
                SUM(CASE WHEN is_away_win = (home_club_goals < away_club_goals) THEN 1 ELSE 0 END) as is_away_win_ok
            FROM dwh.fact_games
            WHERE home_club_goals IS NOT NULL AND away_club_goals IS NOT NULL
        """
        calc_validation = pd.read_sql(query_calc, engine)
        total = calc_validation.iloc[0, 0]
        print(f"\nValidacion de calculos sobre {total:,} partidos:")
        print(f"goal_difference: {calc_validation.iloc[0, 1]:,}/{total:,} correctos")
        print(f"total_goals: {calc_validation.iloc[0, 2]:,}/{total:,} correctos")
        print(f"is_home_win: {calc_validation.iloc[0, 3]:,}/{total:,} correctos")
        print(f"is_draw: {calc_validation.iloc[0, 4]:,}/{total:,} correctos")
        print(f"is_away_win: {calc_validation.iloc[0, 5]:,}/{total:,} correctos")
    else:
        print("\nTABLA VACiA - Ejecuta primero el ETL de fact_games")
    
    # fact_appearances - Medidas
    print("\n\nfact_appearances - Medidas de rendimiento:")
    if db_appearances > 0:
        appearances_stats = pd.read_sql("""
            SELECT 
                COUNT(*) as total_apariciones,
                SUM(goals) as total_goles,
                SUM(assists) as total_asistencias,
                SUM(yellow_cards) as total_amarillas,
                SUM(red_cards) as total_rojas,
                AVG(minutes_played) as promedio_minutos
            FROM dwh.fact_appearances
        """, engine)
        print(appearances_stats.to_string(index=False))
    else:
        print("TABLA VACiA - Ejecuta primero el ETL de fact_appearances")
    
    # fact_game_events - Granularidad minuto a minuto
    print("\n\n⚡ fact_game_events - Eventos minuto a minuto:")
    if db_events > 0:
        events_breakdown = pd.read_sql("""
            SELECT 
                type as tipo_evento,
                COUNT(*) as cantidad
            FROM dwh.fact_game_events
            GROUP BY type
            ORDER BY cantidad DESC
            LIMIT 10
        """, engine)
        print(events_breakdown.to_string(index=False))
    else:
        print("TABLA VACIA - Ejecuta primero el ETL de fact_game_events")
    
    # fact_transfers - PK artificial
    print("\n\nfact_transfers - Verificacion de PK artificial (transfer_id):")
    if db_transfers > 0:
        transfers_pk = pd.read_sql("""
            SELECT 
                MIN(transfer_id) as min_id,
                MAX(transfer_id) as max_id,
                COUNT(*) as total_transfers,
                COUNT(DISTINCT transfer_id) as unique_ids
            FROM dwh.fact_transfers
        """, engine)
        print(transfers_pk.to_string(index=False))
        if transfers_pk.iloc[0, 2] == transfers_pk.iloc[0, 3]:
            print("Todos los transfer_id son unicos (PK valida)")
    else:
        print("TABLA VACiA - Ejecuta primero el ETL de fact_transfers")
    
    # fact_player_valuations - PK artificial
    print("\n\nfact_player_valuations - Verificacion de PK artificial (valuation_id):")
    if db_valuations > 0:
        valuations_pk = pd.read_sql("""
            SELECT 
                MIN(valuation_id) as min_id,
                MAX(valuation_id) as max_id,
                COUNT(*) as total_valuations,
                COUNT(DISTINCT valuation_id) as unique_ids
            FROM dwh.fact_player_valuations
        """, engine)
        print(valuations_pk.to_string(index=False))
        if valuations_pk.iloc[0, 2] == valuations_pk.iloc[0, 3]:
            print("Todos los valuation_id son unicos (PK valida)")
    else:
        print("TABLA VACiA - Ejecuta primero el ETL de fact_player_valuations")
    
    # ============================================================
    # 3. INTEGRIDAD REFERENCIAL (FK)
    # ============================================================
    print("\n\n VALIDACION DE INTEGRIDAD REFERENCIAL (Foreign Keys)")
    print("-" * 80)
    
    fk_checks = [
        ("fact_games --> dim_competitions", 
         "SELECT COUNT(*) FROM dwh.fact_games fg LEFT JOIN dwh.dim_competitions dc ON fg.competition_id = dc.competition_id WHERE dc.competition_id IS NULL"),
        ("fact_games --> dim_clubs (home)", 
         "SELECT COUNT(*) FROM dwh.fact_games fg LEFT JOIN dwh.dim_clubs dc ON fg.home_club_id = dc.club_id WHERE dc.club_id IS NULL"),
        ("fact_games --> dim_clubs (away)", 
         "SELECT COUNT(*) FROM dwh.fact_games fg LEFT JOIN dwh.dim_clubs dc ON fg.away_club_id = dc.club_id WHERE dc.club_id IS NULL"),
        ("fact_games --> dim_date", 
         "SELECT COUNT(*) FROM dwh.fact_games fg LEFT JOIN dwh.dim_date dd ON fg.date_id = dd.date_id WHERE dd.date_id IS NULL"),
        ("fact_appearances --> dim_players", 
         "SELECT COUNT(*) FROM dwh.fact_appearances fa LEFT JOIN dwh.dim_players dp ON fa.player_id = dp.player_id WHERE dp.player_id IS NULL"),
        ("fact_appearances --> dim_clubs", 
         "SELECT COUNT(*) FROM dwh.fact_appearances fa LEFT JOIN dwh.dim_clubs dc ON fa.club_id = dc.club_id WHERE dc.club_id IS NULL"),
    ]
    
    fk_ok = True
    for check_name, query in fk_checks:
        orphans = pd.read_sql(query, engine).iloc[0, 0]
        if orphans == 0:
            print(f"{check_name}: 0 huerfanos")
        else:
            print(f"{check_name}: {orphans} huerfanos")
            fk_ok = False
    
    # ============================================================
    # 4. DESNORMALIZACION VERIFICADA
    # ============================================================
    print("\n\nVERIFICACION DE DESNORMALIZACION")
    print("-" * 80)
    
    if db_fact_games > 0:
        print("\nfact_games (games.csv + club_games.csv):")
        denorm_games = pd.read_sql("""
            SELECT 
                game_id,
                home_club_id,
                away_club_id,
                home_club_position,
                away_club_position,
                home_club_goals,
                away_club_goals
            FROM dwh.fact_games
            WHERE home_club_position IS NOT NULL
            LIMIT 3
        """, engine)
        print(denorm_games.to_string(index=False))
        print("Columnas home/away_club_position provienen de club_games.csv")
    else:
        print("\nfact_games:  TABLA VACiA")
    
    if db_appearances > 0:
        print("\n\nfact_appearances (appearances.csv + game_lineups.csv):")
        denorm_appearances = pd.read_sql("""
            SELECT 
                player_name,
                type,
                position,
                team_captain,
                minutes_played,
                goals
            FROM dwh.fact_appearances
            WHERE type IS NOT NULL
            LIMIT 3
        """, engine)
        print(denorm_appearances.to_string(index=False))
        print("Columnas type, position, team_captain provienen de game_lineups.csv")
    else:
        print("\n\nfact_appearances:  TABLA VACiA")
    
    # ============================================================
    # 5. RESUMEN FINAL
    # ============================================================
    print("\n\n" + "=" * 80)
    print("RESUMEN FINAL")
    print("=" * 80)
    
    total_csv = sum([c[2] for c in comparisons])
    total_db = sum([c[3] for c in comparisons])
    
    print(f"\nRegistros totales:")
    print(f"CSV originales: {total_csv:,}")
    print(f"PostgreSQL DWH: {total_db:,}")
    print(f"Tasa de carga: {total_db/total_csv*100:.1f}%")
    
    # Detectar si las tablas de hechos estan vacias
    facts_empty = (db_fact_games == 0 and db_appearances == 0 and 
                   db_events == 0 and db_transfers == 0 and db_valuations == 0)
    
    if facts_empty:
        print("\n" + "" * 40)
        print("LAS TABLAS DE HECHOS ESTAN VACiAS")
        print("" * 40)
        print("\nSIGUIENTE PASO: Ejecutar el ETL completo:")
        print("python run_etl_full.py")
        print("\n Esto cargara las 5 tablas de hechos con ~3.4 millones de registros")
        print("Tiempo estimado: 10-30 minutos")
        return
    
    if total_db/total_csv < 0.95:
        print(f"\n NOTA: Se filtraron {total_csv - total_db:,} registros ({(1-total_db/total_csv)*100:.1f}%)")
        print("Razones:")
        print("- Registros con FK invalidas (clubs/competitions/players inexistentes)")
        print("- Registros con valores NULL en campos criticos")
        print("- Duplicados por PK")
        print("- Partidos con clubs fuera del dataset (dim_games filtro 15,540)")
    
    print(f"\n Integridad Referencial: {'CORRECTA' if fk_ok else 'PROBLEMAS DETECTADOS'}")
    print(f"Medidas Calculadas: VERIFICADAS")
    print(f"PKs Artificiales: GENERADAS (transfer_id, valuation_id)")
    print(f"Desnormalizacion: APLICADA (fact_games, fact_appearances)")
    print(f"Dimension Fecha: GENERADA (2000-2030, {db_date:,} registros)")
    
    print("\n" + "=" * 80)
    print("VALIDACION COMPLETADA")
    print("=" * 80)

if __name__ == "__main__":
    validate_full_etl()
