-- ============================================================
-- FOOTBALL DATA WAREHOUSE - PostgreSQL DDL
-- Esquema en Constelación (5 dimensiones + 5 hechos)
-- Proyecto: Transfermarkt Dataset
-- ============================================================

-- Crear esquema dedicado para el DWH
CREATE SCHEMA IF NOT EXISTS dwh;
SET search_path TO dwh;

-- ============================================================
-- DIMENSIONES (Tablas compartidas)
-- ============================================================

-- Dimensión: Jugadores
DROP TABLE IF EXISTS dim_players CASCADE;
CREATE TABLE dim_players (
    player_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    name VARCHAR(200),
    last_season INTEGER,
    current_club_id INTEGER,
    player_code VARCHAR(100),
    country_of_birth VARCHAR(100),
    city_of_birth VARCHAR(100),
    country_of_citizenship VARCHAR(100),
    date_of_birth DATE,
    sub_position VARCHAR(50),
    position VARCHAR(50),
    foot VARCHAR(20),
    height_in_cm NUMERIC(5,2),
    contract_expiration_date DATE,
    agent_name VARCHAR(200),
    image_url TEXT,
    url TEXT,
    current_club_domestic_competition_id VARCHAR(10),
    current_club_name VARCHAR(200),
    market_value_in_eur NUMERIC(15,2),
    highest_market_value_in_eur NUMERIC(15,2)
);

-- Dimensión: Clubes
DROP TABLE IF EXISTS dim_clubs CASCADE;
CREATE TABLE dim_clubs (
    club_id INTEGER PRIMARY KEY,
    club_code VARCHAR(100),
    name VARCHAR(200),
    domestic_competition_id VARCHAR(10),
    total_market_value NUMERIC(15,2),
    squad_size INTEGER,
    average_age NUMERIC(4,2),
    foreigners_number INTEGER,
    foreigners_percentage NUMERIC(5,2),
    national_team_players INTEGER,
    stadium_name VARCHAR(200),
    stadium_seats INTEGER,
    net_transfer_record VARCHAR(50),
    coach_name VARCHAR(200),
    last_season INTEGER,
    url TEXT
);

-- Dimensión: Competiciones
DROP TABLE IF EXISTS dim_competitions CASCADE;
CREATE TABLE dim_competitions (
    competition_id VARCHAR(10) PRIMARY KEY,
    competition_code VARCHAR(100),
    name VARCHAR(200),
    sub_type VARCHAR(50),
    type VARCHAR(50),
    country_id INTEGER,
    country_name VARCHAR(100),
    domestic_league_code VARCHAR(10),
    confederation VARCHAR(50),
    url TEXT,
    is_major_national_league BOOLEAN
);

-- Dimensión: Partidos (base para dim, también aparece como PK en fact_games)
DROP TABLE IF EXISTS dim_games CASCADE;
CREATE TABLE dim_games (
    game_id INTEGER PRIMARY KEY,
    competition_id VARCHAR(10),
    season INTEGER,
    round VARCHAR(50),
    date DATE,
    home_club_id INTEGER,
    away_club_id INTEGER,
    home_club_name VARCHAR(200),
    away_club_name VARCHAR(200),
    stadium VARCHAR(200),
    attendance INTEGER,
    referee VARCHAR(200),
    url TEXT,
    home_club_formation VARCHAR(20),
    away_club_formation VARCHAR(20),
    home_club_manager_name VARCHAR(200),
    away_club_manager_name VARCHAR(200),
    aggregate VARCHAR(20),
    competition_type VARCHAR(50),
    FOREIGN KEY (competition_id) REFERENCES dim_competitions(competition_id),
    FOREIGN KEY (home_club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (away_club_id) REFERENCES dim_clubs(club_id)
);

-- Dimensión: Fecha (tabla generada - junk dimension)
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,  -- formato YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    season_start_year INTEGER  -- Año de inicio de temporada (ej: 2013 para 2013/14)
);

-- ============================================================
-- HECHOS (Tablas de medidas)
-- ============================================================

-- HECHO 1: Partidos (games + club_games desnormalizados)
DROP TABLE IF EXISTS fact_games CASCADE;
CREATE TABLE fact_games (
    game_id INTEGER PRIMARY KEY,  -- PK natural
    competition_id VARCHAR(10) NOT NULL,
    home_club_id INTEGER NOT NULL,
    away_club_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    season INTEGER,
    -- Medidas
    home_club_goals INTEGER,
    away_club_goals INTEGER,
    home_club_position INTEGER,  -- posición en tabla local
    away_club_position INTEGER,  -- posición en tabla visitante
    attendance INTEGER,
    -- Derivadas
    goal_difference INTEGER,  -- goles_local - goles_visitante
    total_goals INTEGER,      -- goles_local + goles_visitante
    is_home_win BOOLEAN,
    is_draw BOOLEAN,
    is_away_win BOOLEAN,
    -- FK
    FOREIGN KEY (game_id) REFERENCES dim_games(game_id),
    FOREIGN KEY (competition_id) REFERENCES dim_competitions(competition_id),
    FOREIGN KEY (home_club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (away_club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- HECHO 2: Apariciones de jugadores (appearances + game_lineups desnormalizados)
DROP TABLE IF EXISTS fact_appearances CASCADE;
CREATE TABLE fact_appearances (
    appearance_id VARCHAR(50) PRIMARY KEY,  -- PK natural del CSV
    game_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    club_id INTEGER NOT NULL,
    competition_id VARCHAR(10) NOT NULL,
    date_id INTEGER NOT NULL,
    -- Atributos degenerados
    player_name VARCHAR(200),
    type VARCHAR(20),  -- titular/suplente
    position VARCHAR(50),
    team_captain BOOLEAN,
    -- Medidas
    minutes_played INTEGER,
    goals INTEGER,
    assists INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    -- FK
    FOREIGN KEY (game_id) REFERENCES dim_games(game_id),
    FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
    FOREIGN KEY (club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (competition_id) REFERENCES dim_competitions(competition_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- HECHO 3: Eventos en partidos (minuto a minuto)
DROP TABLE IF EXISTS fact_game_events CASCADE;
CREATE TABLE fact_game_events (
    event_id VARCHAR(50) PRIMARY KEY,  -- PK natural del CSV
    game_id INTEGER NOT NULL,
    club_id INTEGER NOT NULL,
    player_id INTEGER,  -- NULL si es evento de equipo
    date_id INTEGER NOT NULL,
    competition_id VARCHAR(10) NOT NULL,
    -- Atributos degenerados
    type VARCHAR(50),  -- 'Goals', 'Cards', 'Substitutions'
    description TEXT,
    player_in_id INTEGER,  -- Para sustituciones
    player_assist_id INTEGER,  -- Para goles
    -- Medidas
    minute INTEGER,
    -- FK
    FOREIGN KEY (game_id) REFERENCES dim_games(game_id),
    FOREIGN KEY (club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
    FOREIGN KEY (player_in_id) REFERENCES dim_players(player_id),
    FOREIGN KEY (player_assist_id) REFERENCES dim_players(player_id),
    FOREIGN KEY (competition_id) REFERENCES dim_competitions(competition_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- HECHO 4: Transferencias (PK artificial)
DROP TABLE IF EXISTS fact_transfers CASCADE;
CREATE TABLE fact_transfers (
    transfer_id SERIAL PRIMARY KEY,  -- PK artificial autoincremental
    player_id INTEGER NOT NULL,
    from_club_id INTEGER,  -- NULL si viene de fuera del dataset
    to_club_id INTEGER,    -- NULL si sale fuera del dataset
    transfer_date_id INTEGER NOT NULL,
    -- Atributos degenerados
    transfer_season INTEGER,
    player_name VARCHAR(200),
    from_club_name VARCHAR(200),
    to_club_name VARCHAR(200),
    -- Medidas
    transfer_fee NUMERIC(15,2),  -- En EUR
    market_value_in_eur NUMERIC(15,2),  -- Valor de mercado en momento del traspaso
    -- FK
    FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
    FOREIGN KEY (from_club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (to_club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (transfer_date_id) REFERENCES dim_date(date_id)
);

-- HECHO 5: Valoraciones de mercado (PK artificial)
DROP TABLE IF EXISTS fact_player_valuations CASCADE;
CREATE TABLE fact_player_valuations (
    valuation_id SERIAL PRIMARY KEY,  -- PK artificial autoincremental
    player_id INTEGER NOT NULL,
    club_id INTEGER NOT NULL,  -- Club actual en momento de valoración
    competition_id VARCHAR(10),
    date_id INTEGER NOT NULL,
    -- Medidas
    market_value_in_eur NUMERIC(15,2),
    -- FK
    FOREIGN KEY (player_id) REFERENCES dim_players(player_id),
    FOREIGN KEY (club_id) REFERENCES dim_clubs(club_id),
    FOREIGN KEY (competition_id) REFERENCES dim_competitions(competition_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- ============================================================
-- ÍNDICES PARA OPTIMIZACIÓN OLAP
-- ============================================================

-- Índices en dimensiones (búsquedas frecuentes)
CREATE INDEX idx_players_position ON dim_players(position);
CREATE INDEX idx_players_country ON dim_players(country_of_citizenship);
CREATE INDEX idx_players_club ON dim_players(current_club_id);
CREATE INDEX idx_clubs_competition ON dim_clubs(domestic_competition_id);
CREATE INDEX idx_competitions_type ON dim_competitions(type);
CREATE INDEX idx_games_season ON dim_games(season);
CREATE INDEX idx_games_competition ON dim_games(competition_id);
CREATE INDEX idx_date_year ON dim_date(year);
CREATE INDEX idx_date_season ON dim_date(season_start_year);

-- Índices en hechos (FK para joins)
CREATE INDEX idx_fact_games_date ON fact_games(date_id);
CREATE INDEX idx_fact_games_comp ON fact_games(competition_id);
CREATE INDEX idx_fact_games_home ON fact_games(home_club_id);
CREATE INDEX idx_fact_games_away ON fact_games(away_club_id);

CREATE INDEX idx_fact_appearances_game ON fact_appearances(game_id);
CREATE INDEX idx_fact_appearances_player ON fact_appearances(player_id);
CREATE INDEX idx_fact_appearances_club ON fact_appearances(club_id);
CREATE INDEX idx_fact_appearances_date ON fact_appearances(date_id);

CREATE INDEX idx_fact_events_game ON fact_game_events(game_id);
CREATE INDEX idx_fact_events_player ON fact_game_events(player_id);
CREATE INDEX idx_fact_events_date ON fact_game_events(date_id);
CREATE INDEX idx_fact_events_type ON fact_game_events(type);

CREATE INDEX idx_fact_transfers_player ON fact_transfers(player_id);
CREATE INDEX idx_fact_transfers_from ON fact_transfers(from_club_id);
CREATE INDEX idx_fact_transfers_to ON fact_transfers(to_club_id);
CREATE INDEX idx_fact_transfers_date ON fact_transfers(transfer_date_id);
CREATE INDEX idx_fact_transfers_season ON fact_transfers(transfer_season);

CREATE INDEX idx_fact_valuations_player ON fact_player_valuations(player_id);
CREATE INDEX idx_fact_valuations_club ON fact_player_valuations(club_id);
CREATE INDEX idx_fact_valuations_date ON fact_player_valuations(date_id);

-- ============================================================
-- COMENTARIOS (Documentación en BD)
-- ============================================================

COMMENT ON SCHEMA dwh IS 'Data Warehouse Transfermarkt - Esquema en Constelación';
COMMENT ON TABLE fact_games IS 'Tabla de hechos: resumen de partidos por equipo';
COMMENT ON TABLE fact_appearances IS 'Tabla de hechos: apariciones de jugadores en partidos';
COMMENT ON TABLE fact_game_events IS 'Tabla de hechos: eventos minuto a minuto en partidos';
COMMENT ON TABLE fact_transfers IS 'Tabla de hechos: histórico de traspasos';
COMMENT ON TABLE fact_player_valuations IS 'Tabla de hechos: histórico de valoraciones de mercado';
