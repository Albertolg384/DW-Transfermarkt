"""
Comparación entre partidos FILTRADOS vs VÁLIDOS
Para entender por qué unos sí y otros no
"""
import pandas as pd
from config import CSV_FILES, DB_CONFIG
import psycopg2

print("=" * 100)
print("🔍 COMPARACIÓN: PARTIDOS FILTRADOS vs PARTIDOS VÁLIDOS")
print("=" * 100)

# Cargar datos
games = pd.read_csv(CSV_FILES['games'], encoding='utf-8')
clubs = pd.read_csv(CSV_FILES['clubs'], encoding='utf-8')
valid_club_ids = set(clubs['club_id'].values)

# Conectar a PostgreSQL para ver qué quedó en dim_games
conn = psycopg2.connect(**DB_CONFIG)
dim_games_db = pd.read_sql("SELECT game_id FROM dwh.dim_games", conn)
loaded_game_ids = set(dim_games_db['game_id'].values)
conn.close()

# Clasificar partidos
games['home_exists'] = games['home_club_id'].isin(valid_club_ids)
games['away_exists'] = games['away_club_id'].isin(valid_club_ids)
games['both_valid'] = games['home_exists'] & games['away_exists']
games['in_dwh'] = games['game_id'].isin(loaded_game_ids)

# Partidos VÁLIDOS (ambos clubs existen)
valid_games = games[games['both_valid']].head(5)
# Partidos FILTRADOS (al menos un club no existe)
filtered_games = games[~games['both_valid']].head(5)

print("\n" + "=" * 100)
print("✅ EJEMPLOS DE PARTIDOS VÁLIDOS (SÍ cargados en DWH)")
print("=" * 100)

for idx, game in valid_games.iterrows():
    print(f"\n📌 Partido #{idx + 1}:")
    print(f"   game_id: {game['game_id']}")
    print(f"   date: {game['date']}")
    print(f"   competition: {game['competition_id']}")
    print(f"   ")
    print(f"   🏠 HOME: {game['home_club_name']} (ID: {int(game['home_club_id']) if pd.notna(game['home_club_id']) else 'NULL'})")
    
    if pd.notna(game['home_club_id']):
        home_club = clubs[clubs['club_id'] == game['home_club_id']]
        if len(home_club) > 0:
            print(f"      ✅ Existe en clubs.csv: {home_club.iloc[0]['name']}")
        else:
            print(f"      ❌ NO existe en clubs.csv")
    
    print(f"   ")
    print(f"   ✈️  AWAY: {game['away_club_name']} (ID: {int(game['away_club_id']) if pd.notna(game['away_club_id']) else 'NULL'})")
    
    if pd.notna(game['away_club_id']):
        away_club = clubs[clubs['club_id'] == game['away_club_id']]
        if len(away_club) > 0:
            print(f"      ✅ Existe en clubs.csv: {away_club.iloc[0]['name']}")
        else:
            print(f"      ❌ NO existe en clubs.csv")
    
    print(f"   ")
    print(f"   📊 Estado: {'✅ CARGADO en dim_games' if game['in_dwh'] else '❌ FILTRADO'}")
    print(f"   💡 Razón: Ambos clubs existen → PASA el filtro")

print("\n\n" + "=" * 100)
print("❌ EJEMPLOS DE PARTIDOS FILTRADOS (NO cargados en DWH)")
print("=" * 100)

for idx, game in filtered_games.iterrows():
    print(f"\n📌 Partido #{idx + 1}:")
    print(f"   game_id: {game['game_id']}")
    print(f"   date: {game['date']}")
    print(f"   competition: {game['competition_id']}")
    print(f"   ")
    print(f"   🏠 HOME: {game['home_club_name'] if pd.notna(game['home_club_name']) else '❓ DESCONOCIDO'} (ID: {int(game['home_club_id']) if pd.notna(game['home_club_id']) else 'NULL'})")
    
    if pd.notna(game['home_club_id']):
        home_club = clubs[clubs['club_id'] == game['home_club_id']]
        if len(home_club) > 0:
            print(f"      ✅ Existe en clubs.csv: {home_club.iloc[0]['name']}")
        else:
            print(f"      ❌ NO existe en clubs.csv")
    else:
        print(f"      ⚠️  home_club_id es NULL")
    
    print(f"   ")
    print(f"   ✈️  AWAY: {game['away_club_name'] if pd.notna(game['away_club_name']) else '❓ DESCONOCIDO'} (ID: {int(game['away_club_id']) if pd.notna(game['away_club_id']) else 'NULL'})")
    
    if pd.notna(game['away_club_id']):
        away_club = clubs[clubs['club_id'] == game['away_club_id']]
        if len(away_club) > 0:
            print(f"      ✅ Existe en clubs.csv: {away_club.iloc[0]['name']}")
        else:
            print(f"      ❌ NO existe en clubs.csv")
    else:
        print(f"      ⚠️  away_club_id es NULL")
    
    print(f"   ")
    print(f"   📊 Estado: {'✅ CARGADO en dim_games' if game['in_dwh'] else '❌ FILTRADO'}")
    
    # Razón específica
    reasons = []
    if not game['home_exists']:
        reasons.append("home_club_id no existe")
    if not game['away_exists']:
        reasons.append("away_club_id no existe")
    
    print(f"   💡 Razón: {' Y '.join(reasons)} → NO pasa el filtro")

print("\n\n" + "=" * 100)
print("📊 RESUMEN ESTADÍSTICO")
print("=" * 100)

total_games = len(games)
games_both_valid = games['both_valid'].sum()
games_home_invalid = (~games['home_exists']).sum()
games_away_invalid = (~games['away_exists']).sum()
games_both_invalid = (~games['home_exists'] & ~games['away_exists']).sum()

print(f"\n📈 Total partidos en games.csv: {total_games:,}")
print(f"   ")
print(f"   ✅ Ambos clubs válidos: {games_both_valid:,} ({100*games_both_valid/total_games:.1f}%)")
print(f"   ❌ Home club inválido: {games_home_invalid:,} ({100*games_home_invalid/total_games:.1f}%)")
print(f"   ❌ Away club inválido: {games_away_invalid:,} ({100*games_away_invalid/total_games:.1f}%)")
print(f"   ❌❌ Ambos clubs inválidos: {games_both_invalid:,} ({100*games_both_invalid/total_games:.1f}%)")

print(f"\n🎯 Cargados en DWH (dim_games): {len(loaded_game_ids):,}")
print(f"🚫 Filtrados: {total_games - len(loaded_game_ids):,}")

print("\n\n" + "=" * 100)
print("💡 CONCLUSIONES")
print("=" * 100)

print("""
1️⃣  PARTIDOS VÁLIDOS tienen:
   • home_club_id ∈ clubs.csv ✅
   • away_club_id ∈ clubs.csv ✅
   • Se pueden cargar en dim_games sin problemas
   • Sus JOIN funcionan correctamente

2️⃣  PARTIDOS FILTRADOS tienen:
   • Al menos un club_id que NO existe en clubs.csv ❌
   • No se pueden crear FKs válidas a dim_clubs
   • Si se cargaran, los JOIN fallarían
   • Producirían datos incompletos en reports

3️⃣  ¿POR QUÉ PASA?
   • clubs.csv tiene solo 439 clubs (ligas principales)
   • games.csv incluye partidos de divisiones inferiores
   • Esos clubs "menores" no fueron scrapeados

4️⃣  DECISIÓN CORRECTA:
   • Es mejor tener un DWH pequeño pero confiable
   • Que un DWH grande con datos rotos
   • 79% de carga es EXCELENTE para este caso
""")

print("=" * 100)
