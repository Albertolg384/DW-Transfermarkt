"""
Script de utilidad: resetear/limpiar tablas del DWH
CUIDADO: Borra TODOS los datos (pero mantiene estructura)
"""
from config import get_engine
from sqlalchemy import text
import sys

def reset_dwh():
    """Borra todos los datos de las tablas pero mantiene la estructura"""
    
    print("=" * 70)
    print("⚠️  ADVERTENCIA: RESETEAR DATA WAREHOUSE")
    print("=" * 70)
    print("\nEsto BORRARÁ todos los datos de las siguientes tablas:")
    print("   - Todas las tablas de hechos (fact_*)")
    print("   - Todas las tablas de dimensiones (dim_*)")
    print("\nLa estructura (tablas, columnas, índices) se mantiene.")
    print("\nEsta operación NO SE PUEDE DESHACER sin backup.\n")
    
    response = input("¿Estás SEGURO de continuar? Escribe 'BORRAR' para confirmar: ")
    
    if response != 'BORRAR':
        print("❌ Operación cancelada. Ningún dato fue borrado.")
        return
    
    print("\n🔄 Iniciando limpieza del DWH...")
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # Desactivar temporalmente las FK constraints para evitar errores
            print("   1️⃣ Desactivando constraints...")
            conn.execute(text("SET session_replication_role = 'replica';"))
            conn.commit()
            
            # Borrar hechos (primero, por dependencias)
            print("   2️⃣ Borrando tablas de hechos...")
            tables_fact = [
                'fact_game_events',
                'fact_appearances',
                'fact_transfers',
                'fact_player_valuations',
                'fact_games'
            ]
            
            for table in tables_fact:
                try:
                    conn.execute(text(f"TRUNCATE TABLE dwh.{table} CASCADE;"))
                    conn.commit()
                    print(f"      ✓ {table} limpiada")
                except Exception as e:
                    print(f"      ⚠️ Error en {table}: {e}")
            
            # Borrar dimensiones
            print("   3️⃣ Borrando tablas de dimensiones...")
            tables_dim = [
                'dim_games',
                'dim_players',
                'dim_clubs',
                'dim_competitions',
                'dim_date'
            ]
            
            for table in tables_dim:
                try:
                    conn.execute(text(f"TRUNCATE TABLE dwh.{table} CASCADE;"))
                    conn.commit()
                    print(f"      ✓ {table} limpiada")
                except Exception as e:
                    print(f"      ⚠️ Error en {table}: {e}")
            
            # Reactivar constraints
            print("   4️⃣ Reactivando constraints...")
            conn.execute(text("SET session_replication_role = 'origin';"))
            conn.commit()
            
        print("\n✅ DWH reseteado exitosamente")
        print("   Todas las tablas están vacías pero la estructura se mantiene.")
        print("   Puedes ejecutar el ETL nuevamente con: python run_etl_full.py\n")
        
    except Exception as e:
        print(f"\n❌ ERROR durante el reseteo: {e}")
        print("   Verifica la conexión a PostgreSQL y los permisos.")
        sys.exit(1)

if __name__ == "__main__":
    reset_dwh()
