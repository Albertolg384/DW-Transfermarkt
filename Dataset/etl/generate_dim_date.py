"""
Generador de la tabla de dimensión dim_date
Crea registros desde 2000-01-01 hasta 2030-12-31
"""
import pandas as pd
from sqlalchemy import text
from config import get_engine

def generate_dim_date():
    """Genera y carga la dimensión fecha"""
    print("📅 Generando dimensión dim_date...")
    
    # Rango de fechas (30 años de datos)
    date_range = pd.date_range(start='2000-01-01', end='2030-12-31', freq='D')
    
    # Construcción del DataFrame
    dim_date = pd.DataFrame({
        'date_id': date_range.strftime('%Y%m%d').astype(int),
        'full_date': date_range.date,  # Solo fecha, sin hora
        'year': date_range.year,
        'quarter': date_range.quarter,
        'month': date_range.month,
        'month_name': date_range.strftime('%B'),
        'week': date_range.isocalendar().week,
        'day_of_year': date_range.dayofyear,
        'day_of_month': date_range.day,
        'day_of_week': date_range.dayofweek,  # 0=Lunes, 6=Domingo
        'day_name': date_range.strftime('%A'),
        'is_weekend': date_range.dayofweek.isin([5, 6]),
        'season_start_year': date_range.year - (date_range.month < 7).astype(int)
    })
    
    print(f"   ✓ {len(dim_date):,} fechas generadas")
    print(f"   Rango: {dim_date['full_date'].min()} → {dim_date['full_date'].max()}")
    
    # Cargar a PostgreSQL
    engine = get_engine()
    dim_date.to_sql(
        'dim_date',
        engine,
        schema='dwh',
        if_exists='append',  # o 'replace' si quieres sobrescribir
        index=False,
        method=None,
        chunksize=5000
    )
    
    print("✅ dim_date cargada exitosamente\n")
    
    # Verificación
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dwh.dim_date")).fetchone()[0]
        print(f"   Verificación: {count:,} registros en dwh.dim_date")

if __name__ == "__main__":
    generate_dim_date()
