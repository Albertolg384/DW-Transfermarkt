#!/usr/bin/env python3

"""
Orquestador ETL: ejecuta todos los scripts en el orden correcto
"""
import sys
import subprocess
from datetime import datetime
import os

# Directorio donde estan todos los scripts ETL
SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))

def print_header(title):
    """Imprime encabezado formateado"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")

def truncate_all_tables():
    """Limpia todas las tablas respetando el orden de dependencias"""
    from config import get_engine
    from sqlalchemy import text
    
    print("Limpiando/Vaciando los datos de las tablas del DWH " \
        "por si ya estaban generadas dicha tablas.\n")
    engine = get_engine()
    
    # Orden: primero facts (dependen de dims), luego dims
    tables = [
        # Facts primero (tienen FK hacia dims)
        'fact_game_events',
        'fact_appearances',
        'fact_transfers',
        'fact_player_valuations',
        'fact_games',
        # Luego dims
        'dim_games',
        'dim_players',
        'dim_clubs',
        'dim_competitions',
        'dim_date',
    ]
    
    with engine.connect() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE dwh.{table} CASCADE;"))
            print(f"{table} limpiada")
        conn.commit()
    
    print("Todas las tablas limpias\n")

def run_script(script_name):
    """Ejecuta un script Python y captura excepciones"""
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    print(f"Ejecutando: {script_name}")
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=False,
            text=True
        )
        print(f" {script_name} completado exitosamente\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR en {script_name}")
        print(f"Codigo de salida: {e.returncode}\n")
        return False
    except Exception as e:
        print(f"EXCEPCION en {script_name}: {e}\n")
        return False

def run_full_etl():
    """Ejecuta el ETL completo en orden"""
    start_time = datetime.now()
    
    print_header("INICIANDO ETL COMPLETO - DATA WAREHOUSE TRANSFERMARKT")
    print(f"Fecha/Hora inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Limpiar/Vaciar los datos de las tablas por si ya estaban generadas dicha tablas.
    truncate_all_tables()
    
    # Lista de scripts en orden de ejecucion
    scripts = [
        # FASE 0: Validar configuracion
        ("config.py", "Validacion de configuracion"),
        
        # FASE 1: Generar dimension fecha
        ("generate_dim_date.py", "Generacion de dim_date"),
        
        # FASE 2: Cargar dimensiones
        ("etl_dim_competitions.py", "ETL dim_competitions"),
        ("etl_dim_clubs.py", "ETL dim_clubs"),
        ("etl_dim_players.py", "ETL dim_players"),
        ("etl_dim_games.py", "ETL dim_games"),
        
        # FASE 3: Cargar hechos
        ("etl_fact_games.py", "ETL fact_games"),
        ("etl_fact_appearances.py", "ETL fact_appearances (si existe)"),
        ("etl_fact_game_events.py", "ETL fact_game_events (si existe)"),
        ("etl_fact_transfers.py", "ETL fact_transfers (si existe)"),
        ("etl_fact_player_valuations.py", "ETL fact_player_valuations"),
        
        # FASE 4: Validacion final
        ("validate_dwh.py", "Validacion del DWH"),
    ]
    
    results = []
    
    for script, description in scripts:
        print_header(f"FASE: {description}")
        success = run_script(script)
        results.append((script, success))
        
        if not success:
            print("\n ERROR CRiTICO: Deteniendo ETL")
            print(f"   Script fallido: {script}")
            print("   Revisa los logs y corrige antes de continuar.\n")
            break
    
    # Resumen final
    end_time = datetime.now()
    duration = end_time - start_time
    
    print_header("RESUMEN DE EJECUCION ETL")
    
    print("Estado por script:")
    for script, success in results:
        status = " OK" if success else " FALLO"
        print(f"   {status}  {script}")
    
    print(f"\nFecha/Hora fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duracion total: {duration}")
    
    success_count = sum(1 for _, s in results if s)
    total_count = len(results)
    
    if success_count == total_count:
        print("\nETL COMPLETADO EXITOSAMENTE")
        print("Todas las tablas cargadas y validadas.")
        print("Siguiente paso: Consultas OLAP y optimizaciones.\n")
    else:
        print(f"\n ETL INCOMPLETO: {success_count}/{total_count} scripts exitosos")
        print("Revisa los errores antes de continuar.\n")

if __name__ == "__main__":
    print("""
    {================================================================}
    |          ETL DATA WAREHOUSE - TRANSFERMARKT DATASET            |
    |                                                                |
    |  ADVERTENCIA:                                                  |
    |  - Asegurate de haber ejecutado el DDL (ddl_dwh_schema.sql)    |
    |  - Configura la password en config.py antes de ejecutar        |
    |  - Este proceso puede tardar varios minutos                    |
    {================================================================}
    """)
    
    response = input("¿Continuar con el ETL completo? (s/n): ")
    if response.lower() in ['s', 'si', 'yes', 'y']:
        run_full_etl()
    else:
        print("ETL cancelado por el usuario.")
