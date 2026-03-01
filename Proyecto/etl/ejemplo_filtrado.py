#!/usr/bin/env python3

"""
Ejemplo concreto de un partido filtrado
"""
import pandas as pd
from config import CSV_FILES

games = pd.read_csv(CSV_FILES['games'], encoding='utf-8')
clubs = pd.read_csv(CSV_FILES['clubs'], encoding='utf-8')
appearances = pd.read_csv(CSV_FILES['appearances'], encoding='utf-8')

print("=" * 80)
print("CASO CONCRETO: ¿Por que se filtro el game_id 2320450?")
print("=" * 80)

# Buscar el partido
game = games[games['game_id'] == 2320450].iloc[0]

print(f"\nPASO 1: DATOS DEL PARTIDO EN games.csv")
print(f"game_id: {game['game_id']}")
print(f"competition_id: {game['competition_id']} (DFB-Pokal)")
print(f"date: {game['date']}")
print(f"home_club_id: {game['home_club_id']}")
print(f"away_club_id: {game['away_club_id']}")
print(f"home_club_name: {game['home_club_name']}")
print(f"away_club_name: {game['away_club_name']}")

print(f"\nPASO 2: VERIFICAR SI LOS CLUBS EXISTEN EN clubs.csv")
home_exists = game['home_club_id'] in clubs['club_id'].values
away_exists = game['away_club_id'] in clubs['club_id'].values

print(f"¿home_club_id {int(game['home_club_id'])} existe en clubs.csv? {home_exists}")
print(f"¿away_club_id {int(game['away_club_id'])} existe en clubs.csv? {away_exists}")

if away_exists:
    away_club = clubs[clubs['club_id'] == game['away_club_id']].iloc[0]
    print(f"\nAway club encontrado:")
    print(f"club_id: {away_club['club_id']}")
    print(f"name: {away_club['name']}")
    print(f"domestic_competition_id: {away_club['domestic_competition_id']}")

if not home_exists:
    print(f"\nHome club NO encontrado:")
    print(f"club_id {int(game['home_club_id'])} no esta en clubs.csv")
    print(f"home_club_name es NaN (vacio) --> Confirma que el club no existe")

print(f"\nPASO 3: DECISIoN DEL ETL")
print(f"Codigo en etl_dim_games.py:")
print(f"dim_games = dim_games[")
print(f"dim_games['home_club_id'].isin(valid_clubs['club_id']) &")
print(f"dim_games['away_club_id'].isin(valid_clubs['club_id'])")

print(f"Este partido NO pasa el filtro porque:")
print(f"home_club_id {int(game['home_club_id'])} no pertenece clubs.csv")

print(f"\nPASO 4: IMPACTO EN CASCADA")
# Buscar appearances de este partido
game_appearances = appearances[appearances['game_id'] == 2320450]
print(f"- Este partido tiene {len(game_appearances)} appearances en appearances.csv")
print(f"- Como el partido se filtra de dim_games...")
print(f"- ...esas {len(game_appearances)} appearances tambien se filtran de fact_appearances")
print(f"- (No se pueden tener appearances de un partido que no existe)")

if len(game_appearances) > 0:
    print(f"\n Ejemplo de appearances que se perdieron:")
    for _, app in game_appearances.head(3).iterrows():
        print(f"- {app['player_name']} (player_id: {app['player_id']})")

print(f"\nPASO 5: ¿POR QUE PASA ESTO?")
print(f"clubs.csv tiene solo 439 clubs principales")
print(f"games.csv incluye partidos de clubs de ligas menores")
print(f"El home_club_id {int(game['home_club_id'])} probablemente es:")
print(f"- Un equipo de division inferior")
print(f"- Un club amateur")
print(f"- Un equipo que no fue scrapeado en clubs.csv")

print(f"\nCONCLUSION:")
print(f"Es correcto filtrar este partido porque:")
print(f"1. No podemos tener FK apuntando a clubs inexistentes")
print(f"2. Los queries JOIN fallarian")
print(f"3. Los reports mostrarian datos incompletos")
print(f"4. Violamos integridad referencial del DWH")

print("\n" + "=" * 80)
