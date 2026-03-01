-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║                                                                              ║
-- ║        CONSULTAS OLAP — DATA WAREHOUSE TRANSFERMARKT                         ║
-- ║        Esquema en Constelación Kimball | PostgreSQL 18                        ║
-- ║                                                                              ║
-- ║  Modelo:                                                                     ║
-- ║    Dimensiones: dim_date, dim_competitions, dim_clubs, dim_players, dwh.dim_games ║
-- ║    Hechos: fact_games, fact_appearances, fact_game_events,                    ║
-- ║            fact_player_valuations, dwh.fact_transfers                             ║
-- ║                                                                              ║
-- ║  Clasificación de operaciones OLAP:                                          ║
-- ║    1. SLICE        — Corte por una dimensión                                 ║
-- ║    2. DICE         — Subcubo multidimensional                                ║
-- ║    3. ROLL-UP      — Agregación ascendente (detalle → resumen)               ║
-- ║    4. DRILL-DOWN   — Desglose descendente (resumen → detalle)                ║
-- ║    5. PIVOT        — Rotación de ejes (tabulación cruzada)                   ║
-- ║    6. WINDOW       — Funciones ventana (rankings, acumulados, medias)        ║
-- ║    7. GROUPING SETS / CUBE / ROLLUP — Agregaciones multinivel               ║
-- ║                                                                              ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- Todas las consultas usan INNER JOIN gracias a los registros centinela
-- (club_id=-1 'Desconocido' en dim_clubs, player_id=-1 'Desconocido' en dwh.dim_players).
-- Esto garantiza que ninguna fila se pierda en los JOINs.

SET search_path TO dwh;


-- ============================================================================
-- 1. SLICE (CORTE)
-- ============================================================================
-- Operación que fija UNA dimensión en un valor concreto y muestra el resto
-- del cubo libremente. Equivale a "cortar una rebanada" del cubo OLAP.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 1.1  SLICE TEMPORAL — Actividad de transferencias en la temporada 2023
-- ---------------------------------------------------------------------------
-- Se fija la dimensión TIEMPO en transfer_season = 2023 y se analiza
-- el volumen y coste de fichajes por club de destino.
-- Útil para: "¿Qué clubes invirtieron más en el mercado de verano 2023?"
-- ---------------------------------------------------------------------------
SELECT
    c.name                                          AS club_destino,
    comp.name                                       AS liga,
    COUNT(*)                                        AS num_fichajes,
    SUM(t.transfer_fee)    FILTER (WHERE t.transfer_fee > 0) AS inversion_total,
    ROUND(AVG(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0), 2) AS coste_medio_fichaje
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_clubs c     ON t.to_club_id    = c.club_id
    INNER JOIN dwh.dim_clubs co    ON t.from_club_id  = co.club_id
    INNER JOIN dwh.dim_date  d     ON t.transfer_date_id = d.date_id
    INNER JOIN dwh.dim_players p   ON t.player_id     = p.player_id
    LEFT  JOIN dwh.dim_competitions comp ON c.domestic_competition_id = comp.competition_id
WHERE t.transfer_season = 2023                       -- ← SLICE: fija temporada
  AND c.club_id != -1                                -- Excluir centinela
GROUP BY c.name, comp.name
ORDER BY inversion_total DESC NULLS LAST
LIMIT 15;


-- ---------------------------------------------------------------------------
-- 1.2  SLICE POR COMPETICIÓN — Rendimiento de equipos en la Premier League
-- ---------------------------------------------------------------------------
-- Se fija la dimensión COMPETICIÓN en 'GB1' (Premier League) y se analiza
-- el rendimiento goleador local vs visitante de cada club.
-- Útil para: "¿Qué equipo es más fuerte en casa en la Premier?"
-- ---------------------------------------------------------------------------
SELECT
    g.home_club_name                                AS equipo,
    COUNT(*)                                        AS partidos_local,
    SUM(fg.home_club_goals)                         AS goles_favor_local,
    SUM(fg.away_club_goals)                         AS goles_contra_local,
    SUM(CASE WHEN fg.is_home_win THEN 1 ELSE 0 END) AS victorias_local,
    ROUND(AVG(fg.home_club_goals), 2)               AS promedio_goles_local,
    ROUND(
        100.0 * SUM(CASE WHEN fg.is_home_win THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                               AS pct_victorias_local
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_games g ON fg.game_id = g.game_id
WHERE fg.competition_id = 'GB1'                      -- ← SLICE: fija competición (Premier League)
GROUP BY g.home_club_name
HAVING COUNT(*) >= 20
ORDER BY pct_victorias_local DESC
LIMIT 15;


-- ---------------------------------------------------------------------------
-- 1.3  SLICE POR CLUB — Historial completo de transferencias del FC Barcelona
-- ---------------------------------------------------------------------------
-- Se fija la dimensión CLUB y se muestra cada transferencia de entrada/salida
-- con detalle temporal y monetario.
-- ---------------------------------------------------------------------------
SELECT
    d.full_date                                     AS fecha,
    d.year                                          AS anio,
    t.player_name,
    CASE 
        WHEN t.to_club_id = 131 THEN 'FICHAJE'
        WHEN t.from_club_id = 131 THEN 'VENTA'
    END                                             AS tipo_operacion,
    CASE 
        WHEN t.to_club_id = 131 THEN t.from_club_name
        ELSE t.to_club_name
    END                                             AS club_contraparte,
    t.transfer_fee,
    t.market_value_in_eur                           AS valor_mercado
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_date d ON t.transfer_date_id = d.date_id
WHERE (t.to_club_id = 131 OR t.from_club_id = 131)  -- ← SLICE: fija club (FC Barcelona = 131)
  AND t.transfer_fee > 0                             -- Solo operaciones con coste registrado
ORDER BY d.full_date DESC
LIMIT 30;


-- ============================================================================
-- 2. DICE (SUBCUBO)
-- ============================================================================
-- Operación que recorta el cubo filtrando por DOS O MÁS dimensiones
-- simultáneamente. El resultado es un "subcubo" más pequeño.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 2.1  DICE — Goles de jugadores españoles en las 5 grandes ligas (2020-2024)
-- ---------------------------------------------------------------------------
-- Dimensiones filtradas:
--   • JUGADOR: country_of_citizenship = 'Spain'
--   • COMPETICIÓN: las Big Five (ES1, GB1, IT1, FR1, L1)
--   • TIEMPO: temporadas 2020 a 2024
-- Útil para: "¿Quién es el máximo goleador español en Europa en los últimos 5 años?"
-- ---------------------------------------------------------------------------
SELECT
    p.name                                          AS jugador,
    p.position                                      AS posicion,
    comp.name                                       AS liga,
    SUM(fa.goals)                                   AS total_goles,
    SUM(fa.assists)                                 AS total_asistencias,
    SUM(fa.goals + fa.assists)                      AS participaciones_gol,
    COUNT(DISTINCT fa.game_id)                      AS partidos_jugados,
    ROUND(SUM(fa.goals)::NUMERIC / NULLIF(COUNT(DISTINCT fa.game_id), 0), 2) 
                                                    AS goles_por_partido
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_players p       ON fa.player_id = p.player_id
    INNER JOIN dwh.dim_date d          ON fa.date_id   = d.date_id
    INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
WHERE p.country_of_citizenship = 'Spain'             -- ← DICE dim 1: nacionalidad
  AND fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')  -- ← DICE dim 2: Big Five
  AND d.year BETWEEN 2020 AND 2024                   -- ← DICE dim 3: rango temporal
  AND p.player_id != -1
GROUP BY p.name, p.position, comp.name
HAVING SUM(fa.goals) > 0
ORDER BY total_goles DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 2.2  DICE — Transferencias millonarias entre clubes de la Bundesliga
--              y la Premier League (2018-2024)
-- ---------------------------------------------------------------------------
-- Dimensiones filtradas:
--   • CLUB ORIGEN: liga alemana (L1)
--   • CLUB DESTINO: liga inglesa (GB1)
--   • TIEMPO: 2018 a 2024
--   • MONTO: transfer_fee > 10.000.000
-- Útil para: "¿Cuánto talento exporta Alemania a Inglaterra?"
-- ---------------------------------------------------------------------------
SELECT
    t.player_name                                   AS jugador,
    t.from_club_name                                AS club_origen,
    t.to_club_name                                  AS club_destino,
    d.year                                          AS anio,
    t.transfer_fee                                  AS precio,
    t.market_value_in_eur                           AS valor_mercado,
    ROUND(t.transfer_fee / NULLIF(t.market_value_in_eur, 0), 2) 
                                                    AS ratio_precio_valor
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_date d     ON t.transfer_date_id  = d.date_id
    INNER JOIN dwh.dim_clubs co   ON t.from_club_id      = co.club_id
    INNER JOIN dwh.dim_clubs cd   ON t.to_club_id        = cd.club_id
WHERE co.domestic_competition_id = 'L1'              -- ← DICE dim 1: origen Bundesliga
  AND cd.domestic_competition_id = 'GB1'             -- ← DICE dim 2: destino Premier
  AND d.year BETWEEN 2018 AND 2024                   -- ← DICE dim 3: rango temporal
  AND t.transfer_fee > 10000000                      -- ← DICE dim 4: solo millonarios
ORDER BY t.transfer_fee DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 2.3  DICE — Tarjetas rojas en derbis de las 5 grandes ligas (fin de semana)
-- ---------------------------------------------------------------------------
-- Dimensiones filtradas:
--   • EVENTO: type = 'Cards' AND description LIKE '%Red%'
--   • COMPETICIÓN: Big Five
--   • TIEMPO: solo fines de semana
-- ---------------------------------------------------------------------------
SELECT
    comp.name                                       AS liga,
    g.home_club_name || ' vs ' || g.away_club_name  AS partido,
    d.full_date                                     AS fecha,
    d.day_name                                      AS dia_semana,
    p.name                                          AS jugador,
    ge.minute                                       AS minuto,
    ge.description                                  AS detalle
FROM dwh.fact_game_events ge
    INNER JOIN dwh.dim_games g       ON ge.game_id  = g.game_id
    INNER JOIN dwh.dim_date d        ON ge.date_id  = d.date_id
    INNER JOIN dwh.dim_players p     ON ge.player_id = p.player_id
    INNER JOIN dwh.dim_competitions comp ON ge.competition_id = comp.competition_id
WHERE ge.type = 'Cards'                              -- ← DICE dim 1: tipo evento
  AND ge.description ILIKE '%Red card%'              -- ← DICE dim 2: solo rojas
  AND ge.competition_id IN ('ES1','GB1','IT1','FR1','L1')  -- ← DICE dim 3: Big Five
  AND d.is_weekend = TRUE                            -- ← DICE dim 4: fin de semana
  AND p.player_id != -1
ORDER BY d.full_date DESC
LIMIT 25;


-- ============================================================================
-- 3. ROLL-UP (AGREGACIÓN ASCENDENTE)
-- ============================================================================
-- Operación que navega de un nivel de granularidad fino a uno más grueso,
-- colapsando una o más dimensiones. Se "sube" en la jerarquía dimensional.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 3.1  ROLL-UP TEMPORAL — Goles totales: Mes → Trimestre → Año → Total
-- ---------------------------------------------------------------------------
-- Partiendo del grano más fino (mes), se agregan los goles por trimestre,
-- luego por año, y finalmente el gran total. Usa ROLLUP() de SQL.
-- Jerarquía: mes → trimestre → año → total
-- ---------------------------------------------------------------------------
SELECT
    d.year                                          AS anio,
    d.quarter                                       AS trimestre,
    d.month                                         AS mes,
    SUM(fg.total_goals)                             AS goles_totales,
    COUNT(*)                                        AS partidos,
    ROUND(AVG(fg.total_goals), 2)                   AS promedio_goles_partido
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
WHERE d.year BETWEEN 2020 AND 2024
GROUP BY ROLLUP(d.year, d.quarter, d.month)          -- ← ROLL-UP jerárquico
ORDER BY d.year NULLS LAST, d.quarter NULLS LAST, d.month NULLS LAST;


-- ---------------------------------------------------------------------------
-- 3.2  ROLL-UP GEOGRÁFICO — Ingresos por transferencias:
--       Club → Liga → Confederación → Total
-- ---------------------------------------------------------------------------
-- Jerarquía geográfica: club individual → liga nacional → confederación continental
-- Muestra cuánto generó cada nivel vendiendo jugadores.
-- ---------------------------------------------------------------------------
SELECT
    comp.confederation                              AS confederacion,
    comp.name                                       AS liga,
    co.name                                         AS club,
    COUNT(*)                                        AS ventas,
    SUM(t.transfer_fee)   FILTER (WHERE t.transfer_fee > 0)  AS ingresos_totales,
    ROUND(AVG(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0), 2) AS ingreso_medio
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_clubs co        ON t.from_club_id = co.club_id
    LEFT  JOIN dwh.dim_competitions comp ON co.domestic_competition_id = comp.competition_id
WHERE co.club_id != -1
  AND t.transfer_fee > 0
GROUP BY ROLLUP(comp.confederation, comp.name, co.name) -- ← ROLL-UP geográfico
ORDER BY comp.confederation NULLS LAST, 
         comp.name NULLS LAST, 
         ingresos_totales DESC NULLS LAST;


-- ---------------------------------------------------------------------------
-- 3.3  ROLL-UP DEPORTIVO — Goleadores: Jugador → País → Posición → Total
-- ---------------------------------------------------------------------------
-- Jerarquía: jugador individual → su nacionalidad → su posición → gran total
-- Permite ver los goles agregados a diferentes niveles de detalle.
-- ---------------------------------------------------------------------------
SELECT
    p.position                                      AS posicion,
    p.country_of_citizenship                        AS pais,
    p.name                                          AS jugador,
    SUM(fa.goals)                                   AS goles,
    SUM(fa.assists)                                 AS asistencias,
    COUNT(DISTINCT fa.game_id)                      AS partidos
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_players p ON fa.player_id = p.player_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND p.player_id != -1
  AND p.position != 'N/A'
GROUP BY ROLLUP(p.position, p.country_of_citizenship, p.name) -- ← ROLL-UP deportivo
HAVING SUM(fa.goals) > 0
ORDER BY p.position NULLS LAST, 
         p.country_of_citizenship NULLS LAST, 
         goles DESC NULLS LAST;


-- ============================================================================
-- 4. DRILL-DOWN (DESGLOSE DESCENDENTE)
-- ============================================================================
-- Operación inversa al Roll-Up. Se parte de un nivel agregado y se
-- descompone en niveles más detallados ("bajar" en la jerarquía).
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 4.1  DRILL-DOWN TEMPORAL — De año a trimestre a mes:
--       Asistencia media a estadios en la Premier League
-- ---------------------------------------------------------------------------
-- Nivel 1: asistencia media por AÑO
-- Nivel 2: desglose por TRIMESTRE dentro de cada año
-- Nivel 3: desglose por MES dentro de cada trimestre
-- Útil para: "¿En qué meses hay más público en la Premier?"
-- ---------------------------------------------------------------------------

-- Nivel 1: Año
SELECT
    'Nivel 1: Año' AS nivel,
    d.year::TEXT AS periodo,
    ROUND(AVG(fg.attendance) FILTER (WHERE fg.attendance > 0), 0) AS asistencia_media,
    SUM(fg.attendance) FILTER (WHERE fg.attendance > 0) AS asistencia_total,
    COUNT(*) AS partidos
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
WHERE fg.competition_id = 'GB1'
  AND d.year BETWEEN 2018 AND 2024
GROUP BY d.year
ORDER BY d.year;

-- Nivel 2: Drill-down a Trimestre (dentro de 2023)
SELECT
    'Nivel 2: Trimestre' AS nivel,
    d.year || '-Q' || d.quarter AS periodo,
    ROUND(AVG(fg.attendance) FILTER (WHERE fg.attendance > 0), 0) AS asistencia_media,
    SUM(fg.attendance) FILTER (WHERE fg.attendance > 0) AS asistencia_total,
    COUNT(*) AS partidos
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
WHERE fg.competition_id = 'GB1'
  AND d.year = 2023                                  -- ← DRILL-DOWN: de año a trimestre
GROUP BY d.year, d.quarter
ORDER BY d.quarter;

-- Nivel 3: Drill-down a Mes (dentro de Q1 2023)
SELECT
    'Nivel 3: Mes' AS nivel,
    d.year || '-' || LPAD(d.month::TEXT, 2, '0') || ' (' || d.month_name || ')' AS periodo,
    ROUND(AVG(fg.attendance) FILTER (WHERE fg.attendance > 0), 0) AS asistencia_media,
    SUM(fg.attendance) FILTER (WHERE fg.attendance > 0) AS asistencia_total,
    COUNT(*) AS partidos
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
WHERE fg.competition_id = 'GB1'
  AND d.year = 2023
  AND d.quarter = 1                                  -- ← DRILL-DOWN: de trimestre a mes
GROUP BY d.year, d.month, d.month_name
ORDER BY d.month;


-- ---------------------------------------------------------------------------
-- 4.2  DRILL-DOWN DEPORTIVO — De liga a club a jugador:
--       Máximos goleadores de La Liga 2022/23
-- ---------------------------------------------------------------------------

-- Nivel 1: Total de goles por LIGA
SELECT
    'Nivel 1: Liga' AS nivel,
    comp.name AS liga,
    SUM(fa.goals) AS goles_totales
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_date d ON fa.date_id = d.date_id
    INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND d.season_start_year = 2022
GROUP BY comp.name
ORDER BY goles_totales DESC;

-- Nivel 2: Drill-down a CLUB (dentro de La Liga)
SELECT
    'Nivel 2: Club' AS nivel,
    c.name AS club,
    SUM(fa.goals) AS goles_totales,
    COUNT(DISTINCT fa.player_id) AS goleadores_distintos
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_date d    ON fa.date_id = d.date_id
    INNER JOIN dwh.dim_clubs c   ON fa.club_id = c.club_id
WHERE fa.competition_id = 'ES1'                      -- ← DRILL-DOWN: fija liga
  AND d.season_start_year = 2022
GROUP BY c.name
HAVING SUM(fa.goals) > 0
ORDER BY goles_totales DESC
LIMIT 15;

-- Nivel 3: Drill-down a JUGADOR (dentro del Real Madrid)
SELECT
    'Nivel 3: Jugador' AS nivel,
    p.name AS jugador,
    p.position AS posicion,
    SUM(fa.goals) AS goles,
    SUM(fa.assists) AS asistencias,
    COUNT(DISTINCT fa.game_id) AS partidos,
    SUM(fa.minutes_played) AS minutos_jugados
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_date d    ON fa.date_id = d.date_id
    INNER JOIN dwh.dim_clubs c   ON fa.club_id = c.club_id
    INNER JOIN dwh.dim_players p ON fa.player_id = p.player_id
WHERE fa.competition_id = 'ES1'
  AND d.season_start_year = 2022
  AND c.name ILIKE '%Real Madrid%'                    -- ← DRILL-DOWN: fija club
GROUP BY p.name, p.position
HAVING SUM(fa.goals) > 0
ORDER BY goles DESC;


-- ---------------------------------------------------------------------------
-- 4.3  DRILL-DOWN DE VALOR DE MERCADO — De confederación a liga a club a jugador
-- ---------------------------------------------------------------------------

-- Nivel 1: Valor total de mercado por CONFEDERACIÓN
SELECT
    'Nivel 1: Confederación' AS nivel,
    comp.confederation,
    SUM(pv.market_value_in_eur) FILTER (WHERE pv.market_value_in_eur > 0) AS valor_total,
    COUNT(DISTINCT pv.player_id) AS jugadores
FROM dwh.fact_player_valuations pv
    INNER JOIN dwh.dim_date d ON pv.date_id = d.date_id
    INNER JOIN dwh.dim_competitions comp ON pv.competition_id = comp.competition_id
WHERE d.year = 2024 AND d.month = 1
GROUP BY comp.confederation
ORDER BY valor_total DESC NULLS LAST;

-- Nivel 2: Drill-down a LIGA (dentro de Europa/UEFA)
SELECT
    'Nivel 2: Liga' AS nivel,
    comp.name AS liga,
    SUM(pv.market_value_in_eur) FILTER (WHERE pv.market_value_in_eur > 0) AS valor_total,
    COUNT(DISTINCT pv.player_id) AS jugadores,
    ROUND(AVG(pv.market_value_in_eur) FILTER (WHERE pv.market_value_in_eur > 0), 0) AS valor_medio
FROM dwh.fact_player_valuations pv
    INNER JOIN dwh.dim_date d ON pv.date_id = d.date_id
    INNER JOIN dwh.dim_competitions comp ON pv.competition_id = comp.competition_id
WHERE d.year = 2024 AND d.month = 1
  AND comp.confederation = 'europa'                  -- ← DRILL-DOWN: fija confederación
GROUP BY comp.name
ORDER BY valor_total DESC NULLS LAST
LIMIT 10;


-- ============================================================================
-- 5. PIVOT (ROTACIÓN / TABULACIÓN CRUZADA)
-- ============================================================================
-- Operación que rota los ejes del cubo para presentar los datos en formato
-- de tabla cruzada (una dimensión en filas, otra en columnas).
-- PostgreSQL no tiene PIVOT nativo, se simula con CASE WHEN / FILTER.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 5.1  PIVOT — Goles por temporada y liga (ligas como columnas)
-- ---------------------------------------------------------------------------
-- Filas: temporadas | Columnas: las 5 grandes ligas
-- Cada celda: total de goles de esa temporada en esa liga
-- ---------------------------------------------------------------------------
SELECT
    d.season_start_year                             AS temporada,
    SUM(fg.total_goals) FILTER (WHERE fg.competition_id = 'ES1') AS "La Liga",
    SUM(fg.total_goals) FILTER (WHERE fg.competition_id = 'GB1') AS "Premier League",
    SUM(fg.total_goals) FILTER (WHERE fg.competition_id = 'IT1') AS "Serie A",
    SUM(fg.total_goals) FILTER (WHERE fg.competition_id = 'L1')  AS "Bundesliga",
    SUM(fg.total_goals) FILTER (WHERE fg.competition_id = 'FR1') AS "Ligue 1",
    SUM(fg.total_goals)                             AS "TOTAL"
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
WHERE fg.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND d.season_start_year BETWEEN 2015 AND 2024
GROUP BY d.season_start_year
ORDER BY d.season_start_year;


-- ---------------------------------------------------------------------------
-- 5.2  PIVOT — Fichajes por club de destino y temporada (últimas 5 temporadas)
-- ---------------------------------------------------------------------------
-- Filas: clubes TOP 10 | Columnas: temporadas 2020-2024
-- Cada celda: gasto total en fichajes (millones €)
-- ---------------------------------------------------------------------------
SELECT
    c.name                                          AS club,
    ROUND(SUM(t.transfer_fee) FILTER (WHERE t.transfer_season = 2020 AND t.transfer_fee > 0) / 1e6, 1) AS "2020 (M€)",
    ROUND(SUM(t.transfer_fee) FILTER (WHERE t.transfer_season = 2021 AND t.transfer_fee > 0) / 1e6, 1) AS "2021 (M€)",
    ROUND(SUM(t.transfer_fee) FILTER (WHERE t.transfer_season = 2022 AND t.transfer_fee > 0) / 1e6, 1) AS "2022 (M€)",
    ROUND(SUM(t.transfer_fee) FILTER (WHERE t.transfer_season = 2023 AND t.transfer_fee > 0) / 1e6, 1) AS "2023 (M€)",
    ROUND(SUM(t.transfer_fee) FILTER (WHERE t.transfer_season = 2024 AND t.transfer_fee > 0) / 1e6, 1) AS "2024 (M€)",
    ROUND(SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) / 1e6, 1) AS "TOTAL (M€)"
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_clubs c ON t.to_club_id = c.club_id
WHERE t.transfer_season BETWEEN 2020 AND 2024
  AND c.club_id != -1
GROUP BY c.name
ORDER BY SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) DESC NULLS LAST
LIMIT 10;


-- ---------------------------------------------------------------------------
-- 5.3  PIVOT — Distribución de eventos por tipo y día de la semana
-- ---------------------------------------------------------------------------
-- Filas: tipo de evento (Goals, Cards, Substitutions)
-- Columnas: días de la semana
-- Útil para: "¿Hay más goles los sábados que entre semana?"
-- ---------------------------------------------------------------------------
SELECT
    ge.type                                         AS tipo_evento,
    COUNT(*) FILTER (WHERE d.day_name = 'Monday')    AS "Lunes",
    COUNT(*) FILTER (WHERE d.day_name = 'Tuesday')   AS "Martes",
    COUNT(*) FILTER (WHERE d.day_name = 'Wednesday') AS "Miercoles",
    COUNT(*) FILTER (WHERE d.day_name = 'Thursday')  AS "Jueves",
    COUNT(*) FILTER (WHERE d.day_name = 'Friday')    AS "Viernes",
    COUNT(*) FILTER (WHERE d.day_name = 'Saturday')  AS "Sabado",
    COUNT(*) FILTER (WHERE d.day_name = 'Sunday')    AS "Domingo",
    COUNT(*)                                        AS "TOTAL"
FROM dwh.fact_game_events ge
    INNER JOIN dwh.dim_date d ON ge.date_id = d.date_id
WHERE ge.competition_id IN ('ES1','GB1','IT1','FR1','L1')
GROUP BY ge.type
ORDER BY "TOTAL" DESC;


-- ============================================================================
-- 6. FUNCIONES VENTANA (WINDOW FUNCTIONS)
-- ============================================================================
-- Funciones analíticas que operan sobre un conjunto de filas relacionadas
-- (la "ventana") sin colapsar el resultado. Incluyen:
-- - RANK / DENSE_RANK / ROW_NUMBER → Rankings
-- - SUM() OVER / AVG() OVER       → Acumulados y medias móviles
-- - LAG / LEAD                     → Comparaciones con periodo anterior/siguiente
-- - NTILE                          → Distribución en cuantiles
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 6.1  ACUMULADO — Gasto acumulado en fichajes del Chelsea por temporada
-- ---------------------------------------------------------------------------
-- SUM() OVER con ORDER BY crea una suma acumulativa (running total).
-- Muestra cómo crece el gasto total del club a lo largo de los años.
-- ---------------------------------------------------------------------------
SELECT
    t.transfer_season                               AS temporada,
    COUNT(*)                                        AS num_fichajes,
    SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS gasto_temporada,
    SUM(SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0)) OVER (
        ORDER BY t.transfer_season
    )                                               AS gasto_acumulado,
    ROUND(AVG(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0), 0) AS gasto_medio_fichaje
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_clubs c ON t.to_club_id = c.club_id
WHERE c.name ILIKE '%Chelsea%'
  AND t.transfer_season BETWEEN 2015 AND 2024
GROUP BY t.transfer_season
ORDER BY t.transfer_season;


-- ---------------------------------------------------------------------------
-- 6.2  MEDIA MÓVIL — Promedio de goles por partido (media móvil de 3 meses)
--                     en La Liga
-- ---------------------------------------------------------------------------
-- AVG() OVER con ROWS BETWEEN crea una media móvil suavizada.
-- Elimina el ruido mensual y muestra tendencias reales.
-- ---------------------------------------------------------------------------
SELECT
    periodo,
    partidos,
    goles_totales,
    promedio_goles,
    ROUND(AVG(promedio_goles) OVER (
        ORDER BY periodo 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW    -- ← Ventana de 3 meses
    ), 2)                                           AS media_movil_3m
FROM (
    SELECT
        d.year || '-' || LPAD(d.month::TEXT, 2, '0') AS periodo,
        COUNT(*)                                    AS partidos,
        SUM(fg.total_goals)                         AS goles_totales,
        ROUND(AVG(fg.total_goals), 2)               AS promedio_goles
    FROM dwh.fact_games fg
        INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
    WHERE fg.competition_id = 'ES1'
      AND d.year BETWEEN 2020 AND 2024
    GROUP BY d.year, d.month
) mensual
ORDER BY periodo;


-- ---------------------------------------------------------------------------
-- 6.3  LAG — Variación año a año del valor de mercado de Mbappé
-- ---------------------------------------------------------------------------
-- LAG() accede al valor de la fila anterior (año previo) para calcular
-- el crecimiento porcentual interanual.
-- ---------------------------------------------------------------------------
SELECT
    anio,
    valor_mercado,
    valor_anio_anterior,
    ROUND(
        100.0 * (valor_mercado - valor_anio_anterior) / NULLIF(valor_anio_anterior, 0), 1
    )                                               AS variacion_pct
FROM (
    SELECT
        d.year                                      AS anio,
        MAX(pv.market_value_in_eur)                 AS valor_mercado,
        LAG(MAX(pv.market_value_in_eur)) OVER (
            ORDER BY d.year
        )                                           AS valor_anio_anterior
    FROM dwh.fact_player_valuations pv
        INNER JOIN dwh.dim_players p ON pv.player_id = p.player_id
        INNER JOIN dwh.dim_date d    ON pv.date_id   = d.date_id
    WHERE p.name ILIKE '%Mbapp%'
      AND pv.market_value_in_eur > 0
    GROUP BY d.year
) evolucion
ORDER BY anio;


-- ---------------------------------------------------------------------------
-- 6.4  NTILE — Distribución de jugadores en cuartiles por goles (Tier System)
-- ---------------------------------------------------------------------------
-- NTILE(4) divide a los jugadores en 4 cuartiles según su producción goleadora.
-- Tier 1 = Top 25% goleadores, Tier 4 = Bottom 25%.
-- ---------------------------------------------------------------------------
SELECT
    CASE tier
        WHEN 1 THEN 'Tier 1 (Elite)'
        WHEN 2 THEN 'Tier 2 (Alto)'
        WHEN 3 THEN 'Tier 3 (Medio)'
        WHEN 4 THEN 'Tier 4 (Bajo)'
    END                                             AS categoria,
    COUNT(*)                                        AS num_jugadores,
    MIN(total_goles)                                AS goles_minimo,
    MAX(total_goles)                                AS goles_maximo,
    ROUND(AVG(total_goles), 1)                      AS promedio_goles
FROM (
    SELECT
        p.name,
        SUM(fa.goals) AS total_goles,
        NTILE(4) OVER (ORDER BY SUM(fa.goals) DESC) AS tier
    FROM dwh.fact_appearances fa
        INNER JOIN dwh.dim_players p ON fa.player_id = p.player_id
    WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
      AND p.player_id != -1
      AND p.position IN ('Attack', 'Midfield')
    GROUP BY p.name
    HAVING SUM(fa.goals) > 0
) tiered
GROUP BY tier
ORDER BY tier;


-- ---------------------------------------------------------------------------
-- 6.5  RANKING POR PARTICIÓN — Mejor goleador de CADA club en CADA liga
-- ---------------------------------------------------------------------------
-- ROW_NUMBER() particionado por (liga, club) encuentra al máximo goleador
-- de cada equipo sin necesidad de subconsultas correlacionadas.
-- ---------------------------------------------------------------------------
SELECT
    liga, club, jugador, posicion, goles, partidos
FROM (
    SELECT
        comp.name                                   AS liga,
        c.name                                      AS club,
        p.name                                      AS jugador,
        p.position                                  AS posicion,
        SUM(fa.goals)                               AS goles,
        COUNT(DISTINCT fa.game_id)                  AS partidos,
        ROW_NUMBER() OVER (
            PARTITION BY comp.name, c.name 
            ORDER BY SUM(fa.goals) DESC
        )                                           AS rn
    FROM dwh.fact_appearances fa
        INNER JOIN dwh.dim_players p       ON fa.player_id     = p.player_id
        INNER JOIN dwh.dim_clubs c         ON fa.club_id       = c.club_id
        INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
    WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
      AND p.player_id != -1
      AND c.club_id != -1
    GROUP BY comp.name, c.name, p.name, p.position
    HAVING SUM(fa.goals) > 0
) top_scorers
WHERE rn = 1
ORDER BY liga, goles DESC;


-- ============================================================================
-- 7. GROUPING SETS / CUBE / ROLLUP (AGREGACIONES MULTINIVEL)
-- ============================================================================
-- Extensiones SQL que generan MÚLTIPLES niveles de agregación en una sola
-- consulta, evitando UNIONs manuales.
-- - GROUPING SETS: niveles específicos elegidos manualmente
-- - CUBE: todas las combinaciones posibles de dimensiones
-- - ROLLUP: jerarquía de mayor a menor detalle (ya visto, aquí con variantes)
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 7.1  GROUPING SETS — Goles por (liga), por (temporada), por (liga+temporada)
--                       y gran total, todo en una sola consulta
-- ---------------------------------------------------------------------------
-- Genera 4 niveles de agregación simultáneos sin UNION ALL:
--   1. Por cada combinación liga + temporada
--   2. Subtotal por liga (todas las temporadas)
--   3. Subtotal por temporada (todas las ligas)
--   4. Gran total global
-- ---------------------------------------------------------------------------
SELECT
    COALESCE(comp.name, '** TODAS LAS LIGAS **')   AS liga,
    COALESCE(d.season_start_year::TEXT, '** TODAS **') AS temporada,
    SUM(fg.total_goals)                             AS goles,
    COUNT(*)                                        AS partidos,
    ROUND(AVG(fg.total_goals), 2)                   AS promedio,
    GROUPING(comp.name)                             AS es_subtotal_liga,
    GROUPING(d.season_start_year)                   AS es_subtotal_temporada
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
    INNER JOIN dwh.dim_competitions comp ON fg.competition_id = comp.competition_id
WHERE fg.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND d.season_start_year BETWEEN 2020 AND 2024
GROUP BY GROUPING SETS (
    (comp.name, d.season_start_year),                -- Detalle liga+temporada
    (comp.name),                                     -- Subtotal por liga
    (d.season_start_year),                           -- Subtotal por temporada
    ()                                               -- Gran total
)
ORDER BY es_subtotal_liga, es_subtotal_temporada, liga, temporada;


-- ---------------------------------------------------------------------------
-- 7.2  CUBE — Análisis multidimensional completo de apariciones:
--             Posición × Tipo de aparición × Capitán
-- ---------------------------------------------------------------------------
-- CUBE genera TODAS las combinaciones posibles de las 3 dimensiones:
--   (posición, tipo, capitán), (posición, tipo), (posición, capitán),
--   (tipo, capitán), (posición), (tipo), (capitán), ()
-- Total: 2^3 = 8 niveles de agregación en una consulta.
-- ---------------------------------------------------------------------------
SELECT
    COALESCE(fa.position, '** TODAS **')            AS posicion,
    COALESCE(fa.type, '** TODOS **')                AS tipo_aparicion,
    CASE 
        WHEN GROUPING(fa.team_captain) = 1 THEN '** TODOS **'
        WHEN fa.team_captain THEN 'Sí'
        ELSE 'No'
    END                                             AS es_capitan,
    COUNT(*)                                        AS apariciones,
    SUM(fa.goals)                                   AS goles,
    SUM(fa.assists)                                 AS asistencias,
    ROUND(AVG(fa.minutes_played), 0)                AS minutos_promedio,
    SUM(fa.yellow_cards)                            AS amarillas,
    SUM(fa.red_cards)                               AS rojas
FROM dwh.fact_appearances fa
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND fa.position NOT IN ('Unknown', 'N/A')
GROUP BY CUBE(fa.position, fa.type, fa.team_captain) -- ← CUBE: todas las combinaciones
ORDER BY 
    GROUPING(fa.position), 
    GROUPING(fa.type), 
    GROUPING(fa.team_captain),
    posicion, tipo_aparicion;


-- ---------------------------------------------------------------------------
-- 7.3  GROUPING SETS — Balance de transferencias (compras - ventas) por liga
--                       con subtotales por confederación
-- ---------------------------------------------------------------------------
-- Combina compras y ventas en una sola consulta para calcular el
-- balance neto del mercado de fichajes por liga y confederación.
-- ---------------------------------------------------------------------------
WITH compras AS (
    SELECT
        comp.confederation,
        comp.name AS liga,
        SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS gasto_total,
        COUNT(*) AS num_compras
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_clubs c ON t.to_club_id = c.club_id
        LEFT JOIN dwh.dim_competitions comp ON c.domestic_competition_id = comp.competition_id
    WHERE c.club_id != -1
      AND t.transfer_season BETWEEN 2020 AND 2024
    GROUP BY comp.confederation, comp.name
),
ventas AS (
    SELECT
        comp.confederation,
        comp.name AS liga,
        SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS ingreso_total,
        COUNT(*) AS num_ventas
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_clubs co ON t.from_club_id = co.club_id
        LEFT JOIN dwh.dim_competitions comp ON co.domestic_competition_id = comp.competition_id
    WHERE co.club_id != -1
      AND t.transfer_season BETWEEN 2020 AND 2024
    GROUP BY comp.confederation, comp.name
)
SELECT
    COALESCE(c.confederacion, v.confederacion, '** TOTAL **') AS confederacion,
    COALESCE(c.liga, v.liga, '** TOTAL **')         AS liga,
    COALESCE(c.gasto_total, 0)                      AS gasto_fichajes,
    COALESCE(v.ingreso_total, 0)                    AS ingreso_ventas,
    COALESCE(v.ingreso_total, 0) - COALESCE(c.gasto_total, 0) AS balance_neto,
    CASE 
        WHEN COALESCE(v.ingreso_total, 0) - COALESCE(c.gasto_total, 0) > 0 THEN 'SUPERÁVIT'
        WHEN COALESCE(v.ingreso_total, 0) - COALESCE(c.gasto_total, 0) < 0 THEN 'DÉFICIT'
        ELSE 'EQUILIBRIO'
    END                                             AS estado
FROM (SELECT confederation AS confederacion, liga, gasto_total, num_compras FROM compras) c
    FULL OUTER JOIN (SELECT confederation AS confederacion, liga, ingreso_total, num_ventas FROM ventas) v
    ON c.liga = v.liga
ORDER BY balance_neto DESC NULLS LAST
LIMIT 20;


-- ============================================================================
-- 8. ANÁLISIS ESTRATÉGICOS COMBINADOS
-- ============================================================================
-- Consultas complejas que combinan múltiples operaciones OLAP para
-- casos de estudio específicos. Útiles para storytelling y análisis profundo.
-- Combinan: SLICE + WINDOW + LAG + PIVOT + CTEs + agregaciones avanzadas.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 8.1  SLICE + WINDOW — Evolución del valor de mercado del Top 5 jugadores
--                        más valiosos de La Liga, con media móvil
-- ---------------------------------------------------------------------------
-- Combina: SLICE (La Liga) + RANKING (top 5) + MEDIA MÓVIL (6 meses)
-- ---------------------------------------------------------------------------
WITH top5_players AS (
    SELECT player_id, p.name
    FROM dwh.fact_player_valuations pv
        INNER JOIN dwh.dim_players p USING (player_id)
    WHERE pv.competition_id = 'ES1'                  -- SLICE: La Liga
      AND pv.market_value_in_eur > 0
    GROUP BY player_id, p.name
    ORDER BY MAX(pv.market_value_in_eur) DESC
    LIMIT 5
)
SELECT
    t5.name                                         AS jugador,
    d.year || '-' || LPAD(d.month::TEXT, 2, '0')    AS periodo,
    MAX(pv.market_value_in_eur)                     AS valor_mercado,
    ROUND(AVG(MAX(pv.market_value_in_eur)) OVER (
        PARTITION BY t5.name
        ORDER BY d.year, d.month
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ), 0)                                           AS media_movil_6m
FROM dwh.fact_player_valuations pv
    INNER JOIN top5_players t5 ON pv.player_id = t5.player_id
    INNER JOIN dwh.dim_date d      ON pv.date_id   = d.date_id
WHERE d.year BETWEEN 2022 AND 2024
GROUP BY t5.name, d.year, d.month
ORDER BY t5.name, periodo;


-- ---------------------------------------------------------------------------
-- 8.2  DICE + ROLLUP — Efectividad de las sustituciones:
--       ¿Los jugadores que entran de cambio marcan más por minuto?
-- ---------------------------------------------------------------------------
-- Compara la productividad goleadora (goles/90 min) entre titulares
-- y suplentes en las Big Five, con roll-up por liga.
-- ---------------------------------------------------------------------------
SELECT
    COALESCE(comp.name, '** TOTAL BIG FIVE **')     AS liga,
    fa.type                                         AS tipo_aparicion,
    COUNT(*)                                        AS apariciones,
    SUM(fa.goals)                                   AS goles,
    SUM(fa.minutes_played)                          AS minutos_totales,
    ROUND(
        90.0 * SUM(fa.goals) / NULLIF(SUM(fa.minutes_played), 0), 3
    )                                               AS goles_por_90min
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')  -- DICE: Big Five
  AND fa.type IN ('starting_lineup', 'substitutes')    -- DICE: solo titulares/suplentes
  AND fa.minutes_played > 0
GROUP BY ROLLUP(comp.name), fa.type                  -- ROLLUP: liga → total
ORDER BY GROUPING(comp.name), liga, tipo_aparicion;


-- ---------------------------------------------------------------------------
-- 8.3  DRILL-DOWN + LAG — Análisis interanual de fichajes con variación
--       porcentual por liga
-- ---------------------------------------------------------------------------
-- Combina: Drill-down temporal (temporada) + LAG (comparar con año anterior)
-- Responde: "¿Qué liga incrementó más su gasto en fichajes año a año?"
-- ---------------------------------------------------------------------------
SELECT
    liga,
    temporada,
    gasto_total,
    gasto_anterior,
    ROUND(
        100.0 * (gasto_total - gasto_anterior) / NULLIF(gasto_anterior, 0), 1
    )                                               AS variacion_pct,
    CASE 
        WHEN gasto_total > gasto_anterior THEN 'SUBE'
        WHEN gasto_total < gasto_anterior THEN 'BAJA'
        ELSE '→ ESTABLE'
    END                                             AS tendencia
FROM (
    SELECT
        comp.name                                   AS liga,
        t.transfer_season                           AS temporada,
        SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS gasto_total,
        LAG(SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0)) OVER (
            PARTITION BY comp.name
            ORDER BY t.transfer_season
        )                                           AS gasto_anterior
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_clubs c ON t.to_club_id = c.club_id
        LEFT JOIN dwh.dim_competitions comp ON c.domestic_competition_id = comp.competition_id
    WHERE t.transfer_season BETWEEN 2016 AND 2024
      AND c.club_id != -1
      AND comp.competition_id IN ('ES1','GB1','IT1','FR1','L1')
    GROUP BY comp.name, t.transfer_season
) datos
WHERE gasto_anterior IS NOT NULL
ORDER BY liga, temporada;


-- ---------------------------------------------------------------------------
-- 8.4  CUBE + RANKING — Análisis completo de tarjetas:
--       Dimensiones posición × liga × día_semana con TOP infractores
-- ---------------------------------------------------------------------------
-- Parte 1: CUBE de tarjetas amarillas por posición y liga
-- Parte 2: TOP 10 jugadores más amonestados
-- ---------------------------------------------------------------------------

-- Parte 1: CUBE de tarjetas
SELECT
    COALESCE(fa.position, '** TODAS **')            AS posicion,
    COALESCE(comp.name, '** TODAS LAS LIGAS **')   AS liga,
    SUM(fa.yellow_cards)                            AS amarillas,
    SUM(fa.red_cards)                               AS rojas,
    SUM(fa.yellow_cards + fa.red_cards)             AS total_tarjetas,
    COUNT(*)                                        AS apariciones,
    ROUND(100.0 * SUM(fa.yellow_cards) / NULLIF(COUNT(*), 0), 2) AS pct_partido_con_amarilla
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND fa.position NOT IN ('Unknown', 'N/A')
GROUP BY CUBE(fa.position, comp.name)
HAVING SUM(fa.yellow_cards + fa.red_cards) > 0
ORDER BY GROUPING(fa.position), GROUPING(comp.name), total_tarjetas DESC;

-- Parte 2: TOP 10 jugadores más amonestados (ranking)
SELECT
    jugador, posicion, liga, amarillas, rojas, total_tarjetas, partidos,
    ROUND(100.0 * amarillas / NULLIF(partidos, 0), 1) AS pct_partidos_amarilla
FROM (
    SELECT
        p.name                                      AS jugador,
        p.position                                  AS posicion,
        comp.name                                   AS liga,
        SUM(fa.yellow_cards)                        AS amarillas,
        SUM(fa.red_cards)                           AS rojas,
        SUM(fa.yellow_cards + fa.red_cards)         AS total_tarjetas,
        COUNT(DISTINCT fa.game_id)                  AS partidos,
        DENSE_RANK() OVER (ORDER BY SUM(fa.yellow_cards + fa.red_cards) DESC) AS rk
    FROM dwh.fact_appearances fa
        INNER JOIN dwh.dim_players p       ON fa.player_id = p.player_id
        INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
    WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
      AND p.player_id != -1
    GROUP BY p.name, p.position, comp.name
) ranked
WHERE rk <= 10
ORDER BY total_tarjetas DESC;


-- ---------------------------------------------------------------------------
-- 8.5  SLICE + PIVOT + WINDOW — Matriz de rendimiento mensual del
--       Real Madrid en La Liga 2023/24 con acumulados
-- ---------------------------------------------------------------------------
-- Combina: SLICE (club + liga + temporada) + PIVOT (meses como columnas)
--        + WINDOW (victorias acumuladas)
-- ---------------------------------------------------------------------------
WITH madrid_monthly AS (
    SELECT
        d.month,
        d.month_name,
        COUNT(*) AS partidos,
        SUM(CASE WHEN fg.is_home_win AND fg.home_club_id = (SELECT club_id FROM dwh.dim_clubs WHERE name ILIKE '%Real Madrid%' LIMIT 1) THEN 1
                 WHEN fg.is_away_win AND fg.away_club_id = (SELECT club_id FROM dwh.dim_clubs WHERE name ILIKE '%Real Madrid%' LIMIT 1) THEN 1
                 ELSE 0 END) AS victorias,
        SUM(CASE WHEN fg.is_draw THEN 1 ELSE 0 END) AS empates,
        SUM(CASE WHEN fg.home_club_id = (SELECT club_id FROM dwh.dim_clubs WHERE name ILIKE '%Real Madrid%' LIMIT 1) THEN fg.home_club_goals
                 ELSE fg.away_club_goals END) AS goles_favor,
        SUM(CASE WHEN fg.home_club_id = (SELECT club_id FROM dwh.dim_clubs WHERE name ILIKE '%Real Madrid%' LIMIT 1) THEN fg.away_club_goals
                 ELSE fg.home_club_goals END) AS goles_contra
    FROM dwh.fact_games fg
        INNER JOIN dwh.dim_date d ON fg.date_id = d.date_id
    WHERE fg.competition_id = 'ES1'
      AND d.season_start_year = 2023
      AND (fg.home_club_id = (SELECT club_id FROM dwh.dim_clubs WHERE name ILIKE '%Real Madrid%' LIMIT 1)
        OR fg.away_club_id = (SELECT club_id FROM dwh.dim_clubs WHERE name ILIKE '%Real Madrid%' LIMIT 1))
    GROUP BY d.month, d.month_name
)
SELECT
    month_name                                      AS mes,
    partidos,
    victorias,
    empates,
    partidos - victorias - empates                  AS derrotas,
    goles_favor,
    goles_contra,
    goles_favor - goles_contra                      AS diferencia_goles,
    SUM(victorias) OVER (ORDER BY month)            AS victorias_acumuladas,
    SUM(goles_favor) OVER (ORDER BY month)          AS goles_acumulados
FROM madrid_monthly
ORDER BY month;


-- ============================================================================
-- 9. CONSULTAS ESTRELLA — RESULTADOS PRESENTABLES
-- ============================================================================
-- Consultas diseñadas para generar insights directamente presentables.
-- Cada consulta combina múltiples operaciones OLAP (SLICE + WINDOW + DICE)
-- para responder preguntas analíticas concretas del negocio.
-- 
-- Incluye recomendaciones de visualización para la presentación.
-- Cada consulta tiene un insight clave esperado.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 9.1 TOP 25 GOLEADORES HISTÓRICOS DE LAS 5 GRANDES LIGAS
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Quién ha marcado más goles en las Big Five desde que tenemos datos?"
-- Operaciones OLAP: SLICE (Big Five) + WINDOW (ROW_NUMBER) + Agregaciones
-- Visualización: Gráfico de barras horizontal con banderas nacionalidad
-- Insight esperado: Lewandowski domina, seguido Messi/Benzema. 
--    Posición Attack representa >90% del top 25.
-- ---------------------------------------------------------------------------
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(fa.goals) DESC) AS ranking,
    p.name                                          AS jugador,
    p.position                                      AS posicion,
    p.country_of_citizenship                        AS nacionalidad,
    STRING_AGG(DISTINCT comp.name, ', ')            AS ligas,
    STRING_AGG(DISTINCT c.name, ', ')               AS clubes,
    SUM(fa.goals)                                   AS goles,
    SUM(fa.assists)                                 AS asistencias,
    SUM(fa.goals + fa.assists)                      AS participaciones_gol,
    COUNT(DISTINCT fa.game_id)                      AS partidos,
    ROUND(SUM(fa.goals)::NUMERIC / NULLIF(COUNT(DISTINCT fa.game_id), 0), 2) 
                                                    AS goles_por_partido,
    SUM(fa.minutes_played)                          AS minutos_totales,
    ROUND(SUM(fa.goals)::NUMERIC * 90 / NULLIF(SUM(fa.minutes_played), 0), 2) 
                                                    AS goles_por_90min
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_players p       ON fa.player_id     = p.player_id
    INNER JOIN dwh.dim_clubs c         ON fa.club_id       = c.club_id
    INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND p.player_id != -1
GROUP BY p.name, p.position, p.country_of_citizenship
HAVING SUM(fa.goals) > 0
ORDER BY goles DESC
LIMIT 25;


-- ---------------------------------------------------------------------------
-- 9.2  TOP 25 MÁXIMOS ASISTENTES DE LAS BIG FIVE
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Quién ha dado más asistencias en las 5 grandes ligas?"
-- Operaciones OLAP: SLICE (Big Five) + WINDOW (ROW_NUMBER) + Agregaciones
-- Visualización: Gráfico de barras horizontal, destacar midfielders
-- Insight esperado: Messi domina asistencias. Midfield y Attack casi 100%.
-- ---------------------------------------------------------------------------
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(fa.assists) DESC) AS ranking,
    p.name                                          AS jugador,
    p.position                                      AS posicion,
    p.country_of_citizenship                        AS nacionalidad,
    STRING_AGG(DISTINCT c.name, ', ')               AS clubes,
    SUM(fa.assists)                                 AS asistencias,
    SUM(fa.goals)                                   AS goles,
    COUNT(DISTINCT fa.game_id)                      AS partidos,
    ROUND(SUM(fa.assists)::NUMERIC / NULLIF(COUNT(DISTINCT fa.game_id), 0), 2)
                                                    AS asistencias_por_partido
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_players p ON fa.player_id = p.player_id
    INNER JOIN dwh.dim_clubs c   ON fa.club_id   = c.club_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND p.player_id != -1
GROUP BY p.name, p.position, p.country_of_citizenship
HAVING SUM(fa.assists) > 0
ORDER BY asistencias DESC
LIMIT 25;


-- ---------------------------------------------------------------------------
-- 9.3  TOP 20 FICHAJES MÁS CAROS DE LA HISTORIA (en el dataset)
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Cuáles han sido las transferencias más caras?"
-- Operaciones OLAP: WINDOW (ROW_NUMBER) + Cálculos derivados (ratio, valoración)
-- Visualización: Tabla con colores según valoración (rojo=sobrepagado, verde=ganga)
-- Insight esperado: Mbappé #1. Muchos fichajes ingleses sobrepagados.
-- ---------------------------------------------------------------------------
SELECT
    ROW_NUMBER() OVER (ORDER BY t.transfer_fee DESC) AS ranking,
    t.player_name                                   AS jugador,
    t.from_club_name                                AS de_club,
    t.to_club_name                                  AS a_club,
    d.year                                          AS anio,
    t.transfer_fee / 1e6                            AS precio_millones,
    t.market_value_in_eur / 1e6                     AS valor_mercado_millones,
    ROUND(t.transfer_fee / NULLIF(t.market_value_in_eur, 0), 2) 
                                                    AS ratio_precio_valor,
    CASE
        WHEN t.transfer_fee > t.market_value_in_eur * 1.3 THEN 'SOBREPAGADO'
        WHEN t.transfer_fee < t.market_value_in_eur * 0.7 THEN 'GANGA'
        ELSE 'PRECIO JUSTO'
    END                                             AS valoracion
FROM dwh.fact_transfers t
    INNER JOIN dwh.dim_date d ON t.transfer_date_id = d.date_id
WHERE t.transfer_fee > 0
  AND t.market_value_in_eur > 0
ORDER BY t.transfer_fee DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 9.4 VENTAJA DE JUGAR EN CASA — ¿Existe el factor campo?
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Gana más el equipo local? ¿En qué liga es más fuerte el factor campo?"
-- Operaciones OLAP: SLICE (Big Five) + Agregaciones condicionales + PIVOT-like
-- Visualización: Gráfico de columnas apiladas 100% (Local/Empate/Visitante)
-- Insight esperado: ~45% victorias local, ~27% empates. Bundesliga más ofensiva.
-- ---------------------------------------------------------------------------
SELECT
    comp.name                                       AS liga,
    COUNT(*)                                        AS partidos_totales,
    SUM(CASE WHEN fg.is_home_win THEN 1 ELSE 0 END) AS victorias_local,
    SUM(CASE WHEN fg.is_draw THEN 1 ELSE 0 END)     AS empates,
    SUM(CASE WHEN fg.is_away_win THEN 1 ELSE 0 END) AS victorias_visitante,
    ROUND(100.0 * SUM(CASE WHEN fg.is_home_win THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                    AS pct_victoria_local,
    ROUND(100.0 * SUM(CASE WHEN fg.is_draw THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                    AS pct_empate,
    ROUND(100.0 * SUM(CASE WHEN fg.is_away_win THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                    AS pct_victoria_visitante,
    ROUND(AVG(fg.home_club_goals), 2)               AS goles_local_promedio,
    ROUND(AVG(fg.away_club_goals), 2)               AS goles_visitante_promedio
FROM dwh.fact_games fg
    INNER JOIN dwh.dim_competitions comp ON fg.competition_id = comp.competition_id
WHERE fg.competition_id IN ('ES1','GB1','IT1','FR1','L1')
GROUP BY comp.name
ORDER BY pct_victoria_local DESC;


-- ---------------------------------------------------------------------------
-- 9.5 PICHICHI POR TEMPORADA — Máximo goleador de cada liga cada año
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Quién fue el Pichichi/Capocannoniere/Torjäger cada temporada?"
-- Operaciones OLAP: SLICE + DRILL-DOWN (liga→temporada) + WINDOW (RANK)
-- Visualización: Heatmap o matriz (ligas en columnas, años en filas, foto jugador)
-- Insight esperado: Lewandowski/Messi dominan 2015-2020. Haaland emerge 2023+.
-- ---------------------------------------------------------------------------
SELECT
    liga, temporada, jugador, club, goles, partidos, goles_por_partido
FROM (
    SELECT
        comp.name                                   AS liga,
        d.season_start_year                         AS temporada,
        p.name                                      AS jugador,
        STRING_AGG(DISTINCT c.name, ', ')           AS club,
        SUM(fa.goals)                               AS goles,
        COUNT(DISTINCT fa.game_id)                  AS partidos,
        ROUND(SUM(fa.goals)::NUMERIC / NULLIF(COUNT(DISTINCT fa.game_id), 0), 2) 
                                                    AS goles_por_partido,
        RANK() OVER (
            PARTITION BY comp.name, d.season_start_year 
            ORDER BY SUM(fa.goals) DESC
        )                                           AS rk
    FROM dwh.fact_appearances fa
        INNER JOIN dwh.dim_players p       ON fa.player_id     = p.player_id
        INNER JOIN dwh.dim_clubs c         ON fa.club_id       = c.club_id
        INNER JOIN dwh.dim_date d          ON fa.date_id       = d.date_id
        INNER JOIN dwh.dim_competitions comp ON fa.competition_id = comp.competition_id
    WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
      AND p.player_id != -1
      AND d.season_start_year BETWEEN 2015 AND 2024
    GROUP BY comp.name, d.season_start_year, p.name
    HAVING SUM(fa.goals) > 0
) goleadores
WHERE rk = 1
ORDER BY liga, temporada;


-- ---------------------------------------------------------------------------
-- 9.6 EL CLUB QUE MÁS GASTA vs EL QUE MÁS GANA EN FICHAJES (Big Five)
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Qué club tiene el mayor déficit y cuál el mayor superávit 
--            en el mercado de fichajes?"
-- Operaciones OLAP: DICE (Big Five) + CTEs + FULL OUTER JOIN + Balance neto
-- Visualización: Gráfico de barras divergentes (déficit izq, superávit der)
-- Insight esperado: Chelsea mayor déficit (-1.591M€). Clubes menores venden.
-- ---------------------------------------------------------------------------
WITH gastos AS (
    SELECT
        c.name AS club,
        comp.name AS liga,
        SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS total_gastado
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_clubs c ON t.to_club_id = c.club_id
        LEFT JOIN dwh.dim_competitions comp ON c.domestic_competition_id = comp.competition_id
    WHERE c.club_id != -1
      AND comp.competition_id IN ('ES1','GB1','IT1','FR1','L1')
    GROUP BY c.name, comp.name
),
ingresos AS (
    SELECT
        c.name AS club,
        comp.name AS liga,
        SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS total_ingresado
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_clubs c ON t.from_club_id = c.club_id
        LEFT JOIN dwh.dim_competitions comp ON c.domestic_competition_id = comp.competition_id
    WHERE c.club_id != -1
      AND comp.competition_id IN ('ES1','GB1','IT1','FR1','L1')
    GROUP BY c.name, comp.name
)
SELECT
    COALESCE(g.club, i.club)                        AS club,
    COALESCE(g.liga, i.liga)                        AS liga,
    ROUND(COALESCE(g.total_gastado, 0) / 1e6, 1)   AS gastado_M,
    ROUND(COALESCE(i.total_ingresado, 0) / 1e6, 1) AS ingresado_M,
    ROUND((COALESCE(i.total_ingresado, 0) - COALESCE(g.total_gastado, 0)) / 1e6, 1)
                                                    AS balance_neto_M,
    CASE
        WHEN COALESCE(i.total_ingresado, 0) > COALESCE(g.total_gastado, 0) THEN 'SUPERÁVIT'
        ELSE 'DÉFICIT'
    END                                             AS estado
FROM gastos g
    FULL OUTER JOIN ingresos i ON g.club = i.club
ORDER BY balance_neto_M ASC
LIMIT 30;


-- ---------------------------------------------------------------------------
-- 9.7 ¿EN QUÉ MINUTO SE MARCAN MÁS GOLES? — Distribución por franja
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Se marcan más goles al final de cada parte?"
-- Operaciones OLAP: SLICE (Big Five) + PIVOT (franjas minuto) + Agregación
-- Visualización: Gráfico de área o histograma con picos destacados
-- Insight esperado: Picos en minutos 31-45 y 76-90. Fatiga aumenta goles.
-- ---------------------------------------------------------------------------
SELECT
    CASE
        WHEN ge.minute BETWEEN 1  AND 15  THEN '01-15 (Inicio 1ª parte)'
        WHEN ge.minute BETWEEN 16 AND 30  THEN '16-30 (Media 1ª parte)'
        WHEN ge.minute BETWEEN 31 AND 45  THEN '31-45 (Final 1ª parte)'
        WHEN ge.minute BETWEEN 46 AND 60  THEN '46-60 (Inicio 2ª parte)'
        WHEN ge.minute BETWEEN 61 AND 75  THEN '61-75 (Media 2ª parte)'
        WHEN ge.minute BETWEEN 76 AND 90  THEN '76-90 (Final 2ª parte)'
        WHEN ge.minute > 90                THEN '90+   (Tiempo añadido)'
        ELSE 'Desconocido'
    END                                             AS franja_minutos,
    COUNT(*)                                        AS total_goles,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS porcentaje
FROM dwh.fact_game_events ge
WHERE ge.type = 'Goals'
  AND ge.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND ge.minute > 0
GROUP BY
    CASE
        WHEN ge.minute BETWEEN 1  AND 15  THEN '01-15 (Inicio 1ª parte)'
        WHEN ge.minute BETWEEN 16 AND 30  THEN '16-30 (Media 1ª parte)'
        WHEN ge.minute BETWEEN 31 AND 45  THEN '31-45 (Final 1ª parte)'
        WHEN ge.minute BETWEEN 46 AND 60  THEN '46-60 (Inicio 2ª parte)'
        WHEN ge.minute BETWEEN 61 AND 75  THEN '61-75 (Media 2ª parte)'
        WHEN ge.minute BETWEEN 76 AND 90  THEN '76-90 (Final 2ª parte)'
        WHEN ge.minute > 90                THEN '90+   (Tiempo añadido)'
        ELSE 'Desconocido'
    END
ORDER BY MIN(ge.minute);


-- ---------------------------------------------------------------------------
-- 9.8  TOP 10 NACIONALIDADES MÁS GOLEADORAS EN LAS BIG FIVE
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Qué país produce más goleadores en las grandes ligas europeas?"
-- Operaciones OLAP: DICE (Big Five + nacionalidad) + WINDOW + Agregaciones
-- Visualización: Mapa de calor mundial o barras con bandera de país
-- Insight esperado: España #1 (7.527 goles). Brasil, Francia, Alemania top 5.
-- ---------------------------------------------------------------------------
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(fa.goals) DESC) AS ranking,
    p.country_of_citizenship                        AS pais,
    COUNT(DISTINCT p.player_id)                     AS num_jugadores,
    SUM(fa.goals)                                   AS goles_totales,
    SUM(fa.assists)                                 AS asistencias_totales,
    ROUND(SUM(fa.goals)::NUMERIC / NULLIF(COUNT(DISTINCT p.player_id), 0), 1)
                                                    AS goles_por_jugador,
    ROUND(SUM(fa.goals)::NUMERIC / NULLIF(COUNT(DISTINCT fa.game_id), 0), 2)
                                                    AS goles_por_partido
FROM dwh.fact_appearances fa
    INNER JOIN dwh.dim_players p ON fa.player_id = p.player_id
WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
  AND p.player_id != -1
  AND p.country_of_citizenship != 'N/A'
GROUP BY p.country_of_citizenship
HAVING SUM(fa.goals) > 0
ORDER BY goles_totales DESC
LIMIT 10;


-- ---------------------------------------------------------------------------
-- 9.9 INFLACIÓN DEL MERCADO — Precio medio de fichaje por año
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Los fichajes son cada vez más caros? ¿Cuánto ha crecido el precio?"
-- Operaciones OLAP: ROLL-UP (temporal) + WINDOW (LAG) + PERCENTILE_CONT (mediana)
-- Visualización: Gráfico de líneas con 2 ejes (precio medio + % variación)
-- Insight esperado: Crecimiento exponencial hasta 2019. Caída COVID 2020-21.
-- ---------------------------------------------------------------------------
WITH base AS (
    SELECT
        d.year                                      AS anio,
        COUNT(*) FILTER (WHERE t.transfer_fee > 0)  AS num_fichajes_con_precio,
        ROUND((AVG(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0))::NUMERIC / 1e6, 2) 
                                                    AS precio_medio_M,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.transfer_fee) 
              FILTER (WHERE t.transfer_fee > 0))::NUMERIC / 1e6, 2)
                                                    AS mediana_M,
        ROUND(MAX(t.transfer_fee)::NUMERIC / 1e6, 1) AS maximo_M,
        ROUND((SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0))::NUMERIC / 1e6, 1) 
                                                    AS total_mercado_M
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_date d ON t.transfer_date_id = d.date_id
    WHERE d.year BETWEEN 2012 AND 2025
    GROUP BY d.year
)
SELECT
    anio,
    num_fichajes_con_precio,
    precio_medio_M,
    mediana_M,
    maximo_M,
    total_mercado_M,
    ROUND(100.0 * (precio_medio_M - LAG(precio_medio_M) OVER (ORDER BY anio))
        / NULLIF(LAG(precio_medio_M) OVER (ORDER BY anio), 0), 1) AS variacion_pct
FROM base
ORDER BY anio;


-- ---------------------------------------------------------------------------
-- 9.10 COMPARATIVA LIGAS — Resumen ejecutivo de las Big Five
-- ---------------------------------------------------------------------------
-- Pregunta: "¿Cuál es la liga con más goles, más público, más fichajes?"
-- Operaciones OLAP: DICE (Big Five) + Múltiples CTEs + JOIN de hechos diferentes
-- Visualización: Tabla resumen multicolor o radar chart (5 ligas, 6 KPIs)
-- Insight esperado: Premier más goles y asistencia. Bundesliga menos tarjetas.
-- ---------------------------------------------------------------------------
WITH goles_liga AS (
    SELECT
        fg.competition_id,
        COUNT(*)                                    AS partidos,
        SUM(fg.total_goals)                         AS goles,
        ROUND(AVG(fg.total_goals)::NUMERIC, 2)       AS goles_por_partido,
        ROUND((AVG(fg.attendance) FILTER (WHERE fg.attendance > 0))::NUMERIC, 0) AS asistencia_media
    FROM dwh.fact_games fg
    WHERE fg.competition_id IN ('ES1','GB1','IT1','FR1','L1')
    GROUP BY fg.competition_id
),
fichajes_liga AS (
    SELECT
        comp.competition_id,
        COUNT(*) FILTER (WHERE t.transfer_fee > 0)  AS num_fichajes,
        SUM(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0) AS gasto_total,
        ROUND((AVG(t.transfer_fee) FILTER (WHERE t.transfer_fee > 0))::NUMERIC / 1e6, 2) AS fichaje_medio_M
    FROM dwh.fact_transfers t
        INNER JOIN dwh.dim_clubs c ON t.to_club_id = c.club_id
        LEFT JOIN dwh.dim_competitions comp ON c.domestic_competition_id = comp.competition_id
    WHERE comp.competition_id IN ('ES1','GB1','IT1','FR1','L1')
      AND c.club_id != -1
    GROUP BY comp.competition_id
),
tarjetas_liga AS (
    SELECT
        fa.competition_id,
        SUM(fa.yellow_cards)                        AS amarillas,
        SUM(fa.red_cards)                           AS rojas,
        ROUND(100.0 * SUM(fa.yellow_cards) / COUNT(*), 2) AS pct_amarilla_por_aparicion
    FROM dwh.fact_appearances fa
    WHERE fa.competition_id IN ('ES1','GB1','IT1','FR1','L1')
    GROUP BY fa.competition_id
)
SELECT
    comp.name                                       AS liga,
    g.partidos,
    g.goles,
    g.goles_por_partido,
    g.asistencia_media,
    f.num_fichajes,
    ROUND(f.gasto_total::NUMERIC / 1e9, 2)          AS gasto_total_B,
    f.fichaje_medio_M,
    t.amarillas,
    t.rojas,
    t.pct_amarilla_por_aparicion
FROM goles_liga g
    INNER JOIN dwh.dim_competitions comp ON g.competition_id = comp.competition_id
    LEFT JOIN fichajes_liga f ON g.competition_id = f.competition_id
    LEFT JOIN tarjetas_liga t ON g.competition_id = t.competition_id
ORDER BY g.goles DESC;


-- ============================================================================
-- FIN DE LAS CONSULTAS OLAP
-- ============================================================================
-- Resumen de operaciones demostradas:
--
-- | #   | Operación        | Consultas  | Descripción                          |
-- |-----|------------------|------------|--------------------------------------|
-- | 1   | SLICE            | 1.1–1.3    | Corte por tiempo, competición, club  |
-- | 2   | DICE             | 2.1–2.3    | Subcubos multidimensionales          |
-- | 3   | ROLL-UP          | 3.1–3.3    | Agregación temporal, geo, deportiva  |
-- | 4   | DRILL-DOWN       | 4.1–4.3    | Desglose temporal, deportivo, valor  |
-- | 5   | PIVOT            | 5.1–5.3    | Tabulaciones cruzadas                |
-- | 6   | WINDOW           | 6.1–6.5    | Acumulados, medias móviles, LAG, NTILE, rankings |
-- | 7   | GROUPING SETS    | 7.1–7.3    | Agregaciones multinivel + CUBE       |
-- | 8   | COMBINADAS       | 8.1–8.5    | Casos de estudio con múltiples OLAP  |
-- | 9   | ESTRELLA         | 9.1–9.10   | Hallazgos presentables (con metadata)|
-- | 10  | APÉNDICE TÉCNICO | 10.1       | Auditoría centinelas Kimball         |
-- |-----|------------------|------------|--------------------------------------|
-- |     | **TOTAL**        | **39**     | **Consultas numeradas**              |
-- ============================================================================
--
-- TOTAL: 39 consultas optimizadas
--   • Operaciones OLAP puras (1-7): 23 consultas
--   • Análisis combinados (8): 5 consultas
--   • Consultas estrella presentables (9): 10 consultas
--   • Apéndice técnico (10): 1 consulta de auditoría
-- ============================================================================


-- ============================================================================
-- 10. APÉNDICE TÉCNICO — AUDITORÍA Y CALIDAD DE DATOS
-- ============================================================================
-- Consultas de validación técnica del Data Warehouse.
-- NO son análisis de negocio, sino verificaciones de arquitectura Kimball.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- 10.1  AUDITORÍA DE REGISTROS CENTINELA (Unknown Member Rows)
-- ---------------------------------------------------------------------------
-- Verifica el funcionamiento del patrón Kimball de registros centinela (-1).
-- Muestra qué porcentaje de datos usa el centinela vs registros reales.
-- 
-- Útil para: Validar la estrategia de NULLs y demostrar que NO hay NULLs
--            absolutos en las FKs opcionales (se usan centinelas en su lugar).
-- ---------------------------------------------------------------------------
SELECT
    'fact_transfers' AS tabla,
    'from_club_id' AS fk_columna,
    COUNT(*) FILTER (WHERE t.from_club_id = -1)     AS registros_centinela,
    COUNT(*) FILTER (WHERE t.from_club_id != -1)    AS registros_reales,
    COUNT(*)                                        AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE t.from_club_id = -1) / COUNT(*), 1) AS pct_desconocido
FROM dwh.fact_transfers t

UNION ALL

SELECT
    'fact_transfers', 'to_club_id',
    COUNT(*) FILTER (WHERE t.to_club_id = -1),
    COUNT(*) FILTER (WHERE t.to_club_id != -1),
    COUNT(*),
    ROUND(100.0 * COUNT(*) FILTER (WHERE t.to_club_id = -1) / COUNT(*), 1)
FROM dwh.fact_transfers t

UNION ALL

SELECT
    'fact_game_events', 'player_in_id',
    COUNT(*) FILTER (WHERE ge.player_in_id = -1),
    COUNT(*) FILTER (WHERE ge.player_in_id != -1),
    COUNT(*),
    ROUND(100.0 * COUNT(*) FILTER (WHERE ge.player_in_id = -1) / COUNT(*), 1)
FROM dwh.fact_game_events ge

UNION ALL

SELECT
    'fact_game_events', 'player_assist_id',
    COUNT(*) FILTER (WHERE ge.player_assist_id = -1),
    COUNT(*) FILTER (WHERE ge.player_assist_id != -1),
    COUNT(*),
    ROUND(100.0 * COUNT(*) FILTER (WHERE ge.player_assist_id = -1) / COUNT(*), 1)
FROM dwh.fact_game_events ge

ORDER BY tabla, fk_columna;


-- ============================================================================
-- FIN DEL APÉNDICE TÉCNICO
-- ============================================================================
