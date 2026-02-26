"""
Validación post-carga del Data Warehouse
Verifica conteos, integridad referencial y datos críticos
"""
import pandas as pd
from config import get_engine

def validate_dwh():
    """Ejecuta todas las validaciones del DWH"""
    print("=" * 70)
    print("🔍 VALIDACIÓN DEL DATA WAREHOUSE")
    print("=" * 70)
    
    engine = get_engine()
    
    # ============================================================
    # 1. CONTEO DE REGISTROS POR TABLA
    # ============================================================
    print("\n1️⃣ CONTEO DE REGISTROS POR TABLA")
    print("-" * 70)
    
    query_counts = """
        SELECT 'dim_competitions' AS tabla, COUNT(*) AS registros FROM dwh.dim_competitions
        UNION ALL
        SELECT 'dim_clubs', COUNT(*) FROM dwh.dim_clubs
        UNION ALL
        SELECT 'dim_players', COUNT(*) FROM dwh.dim_players
        UNION ALL
        SELECT 'dim_games', COUNT(*) FROM dwh.dim_games
        UNION ALL
        SELECT 'dim_date', COUNT(*) FROM dwh.dim_date
        UNION ALL
        SELECT 'fact_games', COUNT(*) FROM dwh.fact_games
        UNION ALL
        SELECT 'fact_appearances', COUNT(*) FROM dwh.fact_appearances
        UNION ALL
        SELECT 'fact_game_events', COUNT(*) FROM dwh.fact_game_events
        UNION ALL
        SELECT 'fact_transfers', COUNT(*) FROM dwh.fact_transfers
        UNION ALL
        SELECT 'fact_player_valuations', COUNT(*) FROM dwh.fact_player_valuations
        ORDER BY 1;
    """
    
    counts = pd.read_sql(query_counts, engine)
    print(counts.to_string(index=False))
    
    # ============================================================
    # 2. INTEGRIDAD REFERENCIAL (Registros huérfanos)
    # ============================================================
    print("\n2️⃣ INTEGRIDAD REFERENCIAL (detectar FK sin padre)")
    print("-" * 70)
    
    # fact_games → dim_competitions
    query_orphan_comp = """
        SELECT COUNT(*) AS huerfanos
        FROM dwh.fact_games fg
        LEFT JOIN dwh.dim_competitions dc ON fg.competition_id = dc.competition_id
        WHERE dc.competition_id IS NULL;
    """
    orphans_comp = pd.read_sql(query_orphan_comp, engine).iloc[0, 0]
    status_comp = "✅" if orphans_comp == 0 else f"❌ {orphans_comp}"
    print(f"   fact_games → dim_competitions: {status_comp}")
    
    # fact_games → dim_clubs (home)
    query_orphan_home = """
        SELECT COUNT(*) AS huerfanos
        FROM dwh.fact_games fg
        LEFT JOIN dwh.dim_clubs dc ON fg.home_club_id = dc.club_id
        WHERE dc.club_id IS NULL;
    """
    orphans_home = pd.read_sql(query_orphan_home, engine).iloc[0, 0]
    status_home = "✅" if orphans_home == 0 else f"❌ {orphans_home}"
    print(f"   fact_games → dim_clubs (home):  {status_home}")
    
    # fact_games → dim_clubs (away)
    query_orphan_away = """
        SELECT COUNT(*) AS huerfanos
        FROM dwh.fact_games fg
        LEFT JOIN dwh.dim_clubs dc ON fg.away_club_id = dc.club_id
        WHERE dc.club_id IS NULL;
    """
    orphans_away = pd.read_sql(query_orphan_away, engine).iloc[0, 0]
    status_away = "✅" if orphans_away == 0 else f"❌ {orphans_away}"
    print(f"   fact_games → dim_clubs (away):  {status_away}")
    
    # fact_games → dim_date
    query_orphan_date = """
        SELECT COUNT(*) AS huerfanos
        FROM dwh.fact_games fg
        LEFT JOIN dwh.dim_date dd ON fg.date_id = dd.date_id
        WHERE dd.date_id IS NULL;
    """
    orphans_date = pd.read_sql(query_orphan_date, engine).iloc[0, 0]
    status_date = "✅" if orphans_date == 0 else f"❌ {orphans_date}"
    print(f"   fact_games → dim_date:          {status_date}")
    
    # ============================================================
    # 3. VALIDACIÓN DE NULOS EN CAMPOS CRÍTICOS
    # ============================================================
    print("\n3️⃣ VALIDACIÓN DE NULOS EN PKs Y FKs CRÍTICAS")
    print("-" * 70)
    
    query_nulls = """
        SELECT 
            SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END) AS nulls_game_id,
            SUM(CASE WHEN competition_id IS NULL THEN 1 ELSE 0 END) AS nulls_competition_id,
            SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) AS nulls_date_id
        FROM dwh.fact_games;
    """
    nulls = pd.read_sql(query_nulls, engine)
    print("   fact_games:")
    for col, val in nulls.iloc[0].items():
        status = "✅" if val == 0 else f"❌ {int(val)}"
        print(f"      {col}: {status}")
    
    # ============================================================
    # 4. VALIDACIÓN DE RANGOS DE FECHAS
    # ============================================================
    print("\n4️⃣ RANGOS DE FECHAS")
    print("-" * 70)
    
    query_date_range = """
        SELECT 
            MIN(dd.full_date) AS fecha_min,
            MAX(dd.full_date) AS fecha_max,
            COUNT(DISTINCT dd.year) AS años_distintos
        FROM dwh.dim_date dd
        JOIN dwh.fact_games fg ON dd.date_id = fg.date_id;
    """
    date_range = pd.read_sql(query_date_range, engine)
    print(f"   Rango en fact_games: {date_range.iloc[0, 0]} → {date_range.iloc[0, 1]}")
    print(f"   Años distintos: {date_range.iloc[0, 2]}")
    
    # ============================================================
    # 5. ESTADÍSTICAS BÁSICAS
    # ============================================================
    print("\n5️⃣ ESTADÍSTICAS BÁSICAS")
    print("-" * 70)
    
    # Promedio de goles por partido
    query_avg_goals = """
        SELECT 
            ROUND(AVG(total_goals), 2) AS promedio_goles_por_partido,
            MAX(total_goals) AS max_goles_partido,
            SUM(is_home_win::int) AS victorias_local,
            SUM(is_draw::int) AS empates,
            SUM(is_away_win::int) AS victorias_visitante
        FROM dwh.fact_games;
    """
    stats = pd.read_sql(query_avg_goals, engine)
    print("   fact_games:")
    print(f"      Promedio goles/partido: {stats.iloc[0, 0]}")
    print(f"      Máximo goles en partido: {stats.iloc[0, 1]}")
    print(f"      Victorias local: {stats.iloc[0, 2]:,}")
    print(f"      Empates: {stats.iloc[0, 3]:,}")
    print(f"      Victorias visitante: {stats.iloc[0, 4]:,}")
    
    # ============================================================
    # RESUMEN FINAL
    # ============================================================
    print("\n" + "=" * 70)
    total_issues = orphans_comp + orphans_home + orphans_away + orphans_date + nulls.sum().sum()
    if total_issues == 0:
        print("✅ VALIDACIÓN COMPLETADA: Sin problemas detectados")
    else:
        print(f"⚠️ VALIDACIÓN COMPLETADA: {int(total_issues)} problema(s) detectado(s)")
        print("   Revisar detalles arriba y corregir antes de OLAP.")
    print("=" * 70)

if __name__ == "__main__":
    validate_dwh()
