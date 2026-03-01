#!/usr/bin/env python3
"""
Módulo centralizado para normalización de NULLs en el DWH
Estrategia Kimball: 
- Dimensiones: textos='N/A', numéricos según naturaleza
- Hechos: textos='Unknown', numéricos según naturaleza
- Medidas agregables (goals, assists) = 0
- Atributos/clasificadores (season, minute, attendance) = -1
- Monetarios = -1
- FK opcionales = -1
"""
import pandas as pd
from datetime import datetime

# ============================================================
# CONSTANTES
# ============================================================
DIMENSION_TEXT_DEFAULT = 'N/A'
FACT_TEXT_DEFAULT = 'Unknown'
MEASURE_DEFAULT = 0           # Para métricas agregables (goals, assists, minutes_played)
ATTRIBUTE_DEFAULT = -1        # Para clasificadores (season, minute, attendance, height)
MONETARY_DEFAULT = -1         # Para valores monetarios
FK_OPTIONAL_DEFAULT = -1      # Para FK opcionales: apunta a registro centinela (-1) en la dimensión
DATE_UNKNOWN = '1900-01-01'   # Fecha desconocida
DATE_NO_EXPIRY = '9999-12-31' # Sin vencimiento

# ============================================================
# REGLAS POR TABLA
# ============================================================

NULL_RULES = {
    # ========================================
    # DIMENSIONES
    # ========================================
    'dim_date': {
        # Tabla generada, no debería tener NULLs
    },
    
    'dim_competitions': {
        'text': ['competition_code', 'name', 'sub_type', 'type', 'country_name', 
                 'domestic_league_code', 'confederation', 'url'],
        'numeric_measure': [],
        'numeric_attribute': ['country_id'],
        'boolean': ['is_major_national_league']
    },
    
    'dim_clubs': {
        'text': ['club_code', 'name', 'domestic_competition_id', 'stadium_name', 
                 'net_transfer_record', 'coach_name', 'url'],
        'numeric_measure': ['average_age', 'foreigners_percentage'],  # Agregables (AVG, SUM)
        'numeric_attribute': ['squad_size', 'foreigners_number', 'national_team_players', 
                              'stadium_seats', 'last_season'],
        'monetary': ['total_market_value']
    },
    
    'dim_players': {
        'text': ['first_name', 'last_name', 'name', 'player_code', 'country_of_birth', 
                 'city_of_birth', 'country_of_citizenship', 'sub_position', 'position', 
                 'foot', 'agent_name', 'image_url', 'url', 'current_club_domestic_competition_id',
                 'current_club_name'],
        'numeric_measure': [],
        'numeric_attribute': ['last_season', 'current_club_id', 'height_in_cm'],
        'monetary': ['market_value_in_eur', 'highest_market_value_in_eur'],
        'date_unknown': ['date_of_birth'],
        'date_no_expiry': ['contract_expiration_date']
    },
    
    'dim_games': {
        'text': ['competition_id', 'round', 'home_club_name', 'away_club_name', 'stadium', 
                 'referee', 'url', 'home_club_formation', 'away_club_formation', 
                 'home_club_manager_name', 'away_club_manager_name', 'aggregate', 'competition_type'],
        'numeric_measure': [],
        'numeric_attribute': ['game_id', 'season', 'home_club_id', 'away_club_id', 'attendance'],
        'date_unknown': ['date']
    },
    
    # ========================================
    # HECHOS
    # ========================================
    'fact_appearances': {
        'text': ['appearance_id', 'competition_id', 'player_name', 'type', 'position'],
        'numeric_measure': ['minutes_played', 'goals', 'assists', 'yellow_cards', 'red_cards'],
        'numeric_attribute': ['game_id', 'player_id', 'club_id', 'date_id'],
        'boolean': ['team_captain']
    },
    
    'fact_game_events': {
        'text': ['event_id', 'competition_id', 'type', 'description'],
        'numeric_measure': [],
        'numeric_attribute': ['game_id', 'club_id', 'player_id', 'date_id', 'minute'],
        'fk_optional': ['player_assist_id', 'player_in_id']  # No todos los goles tienen asistencia
    },
    
    'fact_games': {
        'text': ['competition_id'],
        'numeric_measure': ['home_club_goals', 'away_club_goals', 'goal_difference', 
                           'total_goals', 'home_club_position', 'away_club_position'],
        'numeric_attribute': ['game_id', 'home_club_id', 'away_club_id', 'date_id', 
                             'season', 'attendance'],
        'boolean': ['is_home_win', 'is_draw', 'is_away_win']
    },
    
    'fact_player_valuations': {
        'text': ['competition_id'],
        'numeric_attribute': ['valuation_id', 'player_id', 'club_id', 'date_id'],
        'monetary': ['market_value_in_eur']
    },
    
    'fact_transfers': {
        'text': ['player_name', 'from_club_name', 'to_club_name'],
        'numeric_attribute': ['transfer_id', 'player_id', 'transfer_date_id', 'transfer_season'],
        'fk_optional': ['from_club_id', 'to_club_id'],  # Cantera o retiro sin club registrado
        'monetary': ['transfer_fee', 'market_value_in_eur']
    }
}

# ============================================================
# FUNCIONES DE NORMALIZACIÓN
# ============================================================

def apply_null_rules(df: pd.DataFrame, table_name: str, is_dimension: bool = True) -> pd.DataFrame:
    """
    Aplica reglas de normalización de NULLs según la tabla.
    
    Args:
        df: DataFrame con los datos
        table_name: Nombre de la tabla (debe estar en NULL_RULES)
        is_dimension: True si es dimensión (textos='N/A'), False si es hecho (textos='Unknown')
    
    Returns:
        DataFrame con NULLs normalizados
    """
    if table_name not in NULL_RULES:
        print(f"  Tabla '{table_name}' no tiene reglas definidas en null_handler.py")
        return df
    
    rules = NULL_RULES[table_name]
    text_default = DIMENSION_TEXT_DEFAULT if is_dimension else FACT_TEXT_DEFAULT
    
    # Textos
    for col in rules.get('text', []):
        if col in df.columns:
            df[col] = df[col].fillna(text_default)
    
    # Medidas (agregables)
    for col in rules.get('numeric_measure', []):
        if col in df.columns:
            df[col] = df[col].fillna(MEASURE_DEFAULT)
    
    # Atributos/clasificadores
    for col in rules.get('numeric_attribute', []):
        if col in df.columns:
            df[col] = df[col].fillna(ATTRIBUTE_DEFAULT)
    
    # Monetarios
    for col in rules.get('monetary', []):
        if col in df.columns:
            df[col] = df[col].fillna(MONETARY_DEFAULT)
    
    # FK opcionales: apuntan al registro centinela (-1) en la dimensión
    for col in rules.get('fk_optional', []):
        if col in df.columns:
            df[col] = df[col].fillna(FK_OPTIONAL_DEFAULT)
    
    # Booleanos
    for col in rules.get('boolean', []):
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)
    
    # Fechas desconocidas
    for col in rules.get('date_unknown', []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].astype(object).where(df[col].notna(), pd.Timestamp(DATE_UNKNOWN))
    
    # Fechas sin vencimiento (9999-12-31 desborda nanosegundos de pandas)
    for col in rules.get('date_no_expiry', []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            mask = df[col].isna()
            df[col] = df[col].astype(object)
            df.loc[mask, col] = DATE_NO_EXPIRY
    
    return df


def validate_no_nulls(df: pd.DataFrame, table_name: str) -> bool:
    """
    Valida que no queden NULLs en el DataFrame.
    Con registros centinela, NINGUNA columna debería tener NULLs.
    
    Args:
        df: DataFrame a validar
        table_name: Nombre de la tabla (para logging)
    
    Returns:
        True si no hay NULLs, False en caso contrario
    """
    null_counts = df.isnull().sum()
    nulls_found = null_counts[null_counts > 0]
    
    if len(nulls_found) > 0:
        print(f" Tabla '{table_name}' aún contiene NULLs:")
        for col, count in nulls_found.items():
            print(f"   - {col}: {count} NULLs")
        return False
    
    print(f" Tabla '{table_name}': 0 NULLs (normalización completa)")
    return True


def get_null_summary(df: pd.DataFrame) -> dict:
    """
    Retorna resumen de NULLs por columna.
    
    Args:
        df: DataFrame a analizar
    
    Returns:
        Dict con {columna: cantidad_nulls}
    """
    null_counts = df.isnull().sum()
    return {col: int(count) for col, count in null_counts.items() if count > 0}


# ============================================================
# EJEMPLO DE USO
# ============================================================
if __name__ == "__main__":
    print("=== NULL HANDLER - REGLAS CONFIGURADAS ===\n")
    
    for table_name, rules in NULL_RULES.items():
        is_dim = table_name.startswith('dim_')
        text_default = DIMENSION_TEXT_DEFAULT if is_dim else FACT_TEXT_DEFAULT
        
        print(f"📋 {table_name} ({'DIMENSIÓN' if is_dim else 'HECHO'})")
        print(f"   Textos --> '{text_default}'")
        
        if rules.get('numeric_measure'):
            print(f"   Medidas agregables --> {MEASURE_DEFAULT}: {rules['numeric_measure']}")
        if rules.get('numeric_attribute'):
            print(f"   Atributos/clasificadores --> {ATTRIBUTE_DEFAULT}: {rules['numeric_attribute']}")
        if rules.get('monetary'):
            print(f"   Monetarios --> {MONETARY_DEFAULT}: {rules['monetary']}")
        if rules.get('fk_optional'):
            print(f"   FK opcionales --> {FK_OPTIONAL_DEFAULT}: {rules['fk_optional']}")
        if rules.get('date_unknown'):
            print(f"   Fechas desconocidas --> '{DATE_UNKNOWN}': {rules['date_unknown']}")
        if rules.get('date_no_expiry'):
            print(f"   Fechas sin vencimiento --> '{DATE_NO_EXPIRY}': {rules['date_no_expiry']}")
        
        print()
