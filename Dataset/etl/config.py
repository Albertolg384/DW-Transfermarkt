#!/usr/bin/env python3

"""
Configuración centralizada para el ETL del Data Warehouse Transfermarkt
"""
import os
from sqlalchemy import create_engine, text
BASE_DIR = r"/home/alberto/Documentos/DW-Transfermarkt_Dataset/Dataset/Formato_csv"

CSV_FILES = {
    'players': os.path.join(BASE_DIR, 'players.csv'),
    'clubs': os.path.join(BASE_DIR, 'clubs.csv'),
    'competitions': os.path.join(BASE_DIR, 'competitions.csv'),
    'games': os.path.join(BASE_DIR, 'games.csv'),
    'club_games': os.path.join(BASE_DIR, 'club_games.csv'),
    'appearances': os.path.join(BASE_DIR, 'appearances.csv'),
    'game_lineups': os.path.join(BASE_DIR, 'game_lineups.csv'),
    'game_events': os.path.join(BASE_DIR, 'game_events.csv'),
    'transfers': os.path.join(BASE_DIR, 'transfers.csv'),
    'player_valuations': os.path.join(BASE_DIR, 'player_valuations.csv')
}

# ============================================================
# CONFIGURACIÓN DE BASE DE DATOS
# ============================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'football_dwh',
    'user': 'postgres',
    'password': 'pass'  # ⚠️ CAMBIAR antes de ejecutar
}

# Construcción de URI de conexión para SQLAlchemy
DB_URI = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Motor SQLAlchemy (reutilizable en todos los scripts)
def get_engine():
    """Retorna un engine SQLAlchemy configurado"""
    return create_engine(DB_URI, echo=False)

# ============================================================
# CONFIGURACIÓN DE PANDAS
# ============================================================
PANDAS_READ_CONFIG = {
    'encoding': 'utf-8',
    'low_memory': False,  # Para CSVs grandes
    'na_values': ['', 'NA', 'N/A', 'null', 'None']
}

# ============================================================
# LIMITES Y OPCIONES
# ============================================================
BATCH_SIZE = 10000  # Insertar en lotes de 10K registros (to_sql chunksize)
LOG_LEVEL = 'INFO'  # 'DEBUG', 'INFO', 'WARNING', 'ERROR'

# ============================================================
# VALIDACIÓN
# ============================================================
def validate_csv_files():
    """Verifica que todos los CSV existan"""
    missing = []
    for name, path in CSV_FILES.items():
        if not os.path.exists(path):
            missing.append(f"{name}: {path}")
    
    if missing:
        raise FileNotFoundError(f"Archivos CSV no encontrados:\n" + "\n".join(missing))
    print("✅ Todos los archivos CSV encontrados")

def test_db_connection():
    """Prueba la conexión a PostgreSQL"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✅ Conexión exitosa a PostgreSQL: {version[:50]}...")
            return True
    except Exception as e:
        print(f"❌ Error de conexión a PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    print("=== VALIDACIÓN DE CONFIGURACIÓN ===")
    validate_csv_files()
    test_db_connection()
