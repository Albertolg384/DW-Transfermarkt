# Estrategia de Tratamiento de NULLs en el Data Warehouse de Transfermarkt

## 1. Contexto y Problemática

El Data Warehouse de Transfermarkt se construye a partir de 7 ficheros CSV del dataset público de Transfermarkt en Kaggle, modelado bajo un **esquema en constelación** (variante Kimball) con **5 tablas de dimensiones** y **5 tablas de hechos** sobre PostgreSQL 18.

### Esquema del DWH

| Tipo | Tabla | Registros |
|------|-------|-----------|
| Dimensión | `dim_date` | 11.323 |
| Dimensión | `dim_competitions` | 44 |
| Dimensión | `dim_clubs` | 440 (439 + 1 centinela) |
| Dimensión | `dim_players` | 32.602 (32.601 + 1 centinela) |
| Dimensión | `dim_games` | 58.495 |
| Hecho | `fact_games` | 58.495 |
| Hecho | `fact_appearances` | 1.628.836 |
| Hecho | `fact_game_events` | 809.989 |
| Hecho | `fact_player_valuations` | 496.606 |
| Hecho | `fact_transfers` | 79.594 |

Los CSVs de origen contenían **NULLs masivos** en columnas como `coach_name` (439 nulos), `agent_name` (16.019), `contract_expiration_date` (12.091), `player_assist_id` (681.472), `from_club_id` (51.226), entre muchas otras. Si estos NULLs llegaban al DWH sin tratamiento, cualquier consulta OLAP con `JOIN`, `GROUP BY` o funciones de agregación produciría resultados incorrectos o incompletos.

---

## 2. Fundamento Teórico: Ralph Kimball y el Tratamiento de NULLs

Ralph Kimball, en *The Data Warehouse Toolkit* (3rd Edition, 2013), establece que:

> *"NULLs should be avoided in the data warehouse. Every dimension should contain a row to represent the unknown or not applicable condition."*

Los principios clave que guían nuestra estrategia son:

1. **Evitar NULLs en dimensiones**: un NULL en un `GROUP BY` crea un grupo fantasma que confunde al analista. Kimball recomienda reemplazarlos por valores explícitos como `"N/A"` o `"Unknown"`.

2. **Distinguir la naturaleza de cada columna**: no todas las columnas nulas significan lo mismo. Un **gol sin asistencia** no es un error, es un evento legítimo. Un **jugador sin nombre de agente** es un dato faltante en el catálogo.

3. **Preservar la integridad referencial**: las Foreign Keys deben apuntar a registros reales en las dimensiones, o ser explícitamente nulas si la relación es opcional.

4. **Hacer la ausencia de dato visible y consultable**: el analista debe poder filtrar por `WHERE agent_name = 'N/A'` para encontrar jugadores sin agente registrado, en lugar de usar `WHERE agent_name IS NULL` (que se pierde en JOINs).

---

## 3. Taxonomía de Columnas y Valores por Defecto

Tras analizar las 141 columnas del DWH, clasificamos cada una en una de estas **7 categorías**:

### 3.1. Campos de Texto

| Contexto | Valor por defecto | Justificación Kimball |
|----------|------------------|-----------------------|
| **Dimensiones** | `'N/A'` | *"Not Available"* - El dato no existe en el catálogo origen. Ej: un club sin `coach_name` registrado. |
| **Hechos** | `'Unknown'` | *"Unknown"* - El detalle del evento se perdió. Ej: una aparición sin `position` del lineup. |

**¿Por qué distinguir `N/A` vs `Unknown`?** Porque representan semánticas distintas:
- `N/A` en una dimensión = **el atributo nunca se registró** en la fuente (catálogo incompleto).
- `Unknown` en un hecho = **el detalle del evento existió pero no se capturó** (dato transaccional perdido).

Esta distinción permite a los analistas segregar el análisis: `WHERE position != 'Unknown'` filtra apariciones sin posición registrada, mientras que `WHERE coach_name != 'N/A'` filtra clubs con entrenador conocido.

### 3.2. Medidas Agregables --> `0`

Columnas que participan en `SUM()`, `AVG()`, `COUNT()` y cuya ausencia significa "no ocurrió":

| Tabla | Columnas | Ejemplo |
|-------|----------|---------|
| `fact_appearances` | `goals`, `assists`, `minutes_played`, `yellow_cards`, `red_cards` | Un jugador sin goles registrados = 0 goles, no dato desconocido |
| `fact_games` | `home_club_goals`, `away_club_goals`, `goal_difference`, `total_goals`, `home_club_position`, `away_club_position` | Si no se registraron goles, la medida es 0 |
| `dim_clubs` | `average_age`, `foreigners_percentage` | Métricas que se agregan con AVG |

**Justificación Kimball**: *"A null in a fact table measurement column should be avoided because it will distort every aggregate calculation."* Un `NULL` en `goals` haría que `SUM(goals)` lo ignore silenciosamente, subestimando el total.

#### ¿Por qué `minutes_played = 0` pero `minute = -1`?

Esta es una distinción fundamental:

- **`minutes_played`** es una **medida** (se suma: `SUM(minutes_played)` = total de minutos jugados por un equipo). Si no jugó, el valor correcto es `0`.
- **`minute`** es un **atributo clasificador** (indica en qué minuto del partido ocurrió un evento). No tiene sentido sumar minutos de eventos. Un valor `0` significaría "minuto 0 del partido", que es un dato real. Por eso usamos `-1` como centinela.

### 3.3. Atributos/Clasificadores --> `-1`

Columnas numéricas que **no se agregan**, sino que se usan para filtrar, agrupar o clasificar:

| Tabla | Columnas | Ejemplo |
|-------|----------|---------|
| `dim_competitions` | `country_id` | Competiciones internacionales sin país |
| `dim_clubs` | `squad_size`, `foreigners_number`, `national_team_players`, `stadium_seats`, `last_season` | Atributos descriptivos del club |
| `dim_players` | `last_season`, `current_club_id`, `height_in_cm` | Datos del perfil del jugador |
| `dim_games` | `season`, `attendance` | Clasificadores del partido |
| `fact_game_events` | `minute` | Minuto del evento en el partido |

**Justificación**: Se usa `-1` (valor imposible en el dominio) como centinela distinguible. Un `WHERE attendance = -1` identifica partidos sin asistencia registrada. Si usáramos `0`, sería ambiguo: ¿se jugó a puerta cerrada o no se registró?

### 3.4. Valores Monetarios --> `-1`

| Tabla | Columnas |
|-------|----------|
| `dim_clubs` | `total_market_value` |
| `dim_players` | `market_value_in_eur`, `highest_market_value_in_eur` |
| `fact_player_valuations` | `market_value_in_eur` |
| `fact_transfers` | `transfer_fee`, `market_value_in_eur` |

**Justificación**: Un `transfer_fee = 0` es un dato real (cesión gratuita, fin de contrato). Un `transfer_fee = -1` indica que la información del coste no está disponible. Sin esta distinción, sería imposible diferenciar "fichaje gratis" de "dato no registrado" en las consultas.

### 3.5. Foreign Keys Opcionales --> `-1` (Registro Centinela / Unknown Member Row)

| Tabla | Columnas | Significado semántico | Centinela en |
|-------|----------|----------------------|-------------|
| `fact_game_events` | `player_assist_id` | Gol sin asistencia (disparo directo, penalti, autogol) | `dim_players` (player_id = -1) |
| `fact_game_events` | `player_in_id` | Evento que no es sustitución (gol, tarjeta) | `dim_players` (player_id = -1) |
| `fact_transfers` | `from_club_id` | Jugador sin club previo registrado (cantera, equipo amateur fuera del dataset) | `dim_clubs` (club_id = -1) |
| `fact_transfers` | `to_club_id` | Jugador sin destino registrado (retirado, equipo fuera del dataset) | `dim_clubs` (club_id = -1) |

**Decisión crítica**: Se evaluaron dos alternativas para tratar las FK opcionales:

| Opción | Pros | Contras |
|--------|------|---------|
| **A) Registros centinela** (insertar `club_id=-1, name='Desconocido'` en dim_clubs y `player_id=-1` en dim_players) | Patrón Kimball puro. `INNER JOIN` siempre resuelve. 0 NULLs absolutos en todo el DWH. Consultas más simples. | Un registro ficticio por dimensión afectada. Requiere `WHERE club_id != -1` en conteos. |
| **B) NULL nativo** (PostgreSQL permite NULL en FKs) | Sin registros ficticios. SQL estándar. | Requiere `LEFT JOIN` siempre. NULLs se propagan en cascada. `GROUP BY` genera grupos fantasma. Kimball lo desaconseja explícitamente. |

**Decisión final: Opción A (Registros Centinela)** - el enfoque Kimball puro, porque:

1. **0 NULLs absolutos** en todas las columnas de todas las tablas --> cualquier `INNER JOIN` funciona sin pérdida de datos.
2. `INNER JOIN` es más eficiente que `LEFT JOIN` en el optimizador de PostgreSQL.
3. Kimball lo prescribe: *"Use special rows in dimension tables to handle unknown or not applicable conditions"* (Cap. 21).
4. El analista puede filtrar con `WHERE club_name != 'Desconocido'` (más legible que `WHERE from_club_id IS NOT NULL`).
5. Las consultas `GROUP BY` agrupan explícitamente los desconocidos bajo `'Desconocido'` en vez de bajo un grupo `NULL` invisible.

#### Registros centinela insertados

**`dim_clubs`** (club_id = -1):
```sql
INSERT INTO dwh.dim_clubs (club_id, name, domestic_competition_id, total_market_value,
    squad_size, average_age, foreigners_number, foreigners_percentage,
    national_team_players, stadium_name, stadium_seats, net_transfer_record,
    coach_name, last_season, url, filename)
VALUES (-1, 'Desconocido', 'N/A', -1, -1, 0, -1, 0, -1,
    'N/A', -1, 'N/A', 'N/A', -1, 'N/A', 'N/A')
ON CONFLICT (club_id) DO NOTHING;
```

**`dim_players`** (player_id = -1):
```sql
INSERT INTO dwh.dim_players (player_id, name, last_season, current_club_id,
    current_club_name, country_of_birth, city_of_birth, country_of_citizenship,
    date_of_birth, sub_position, position, foot, height_in_cm,
    contract_expiration_date, agent_name, image_url, url, current_club_domestic_competition_id,
    filename, market_value_in_eur, highest_market_value_in_eur, first_name, last_name)
VALUES (-1, 'Desconocido', -1, -1, 'N/A', 'N/A', 'N/A', 'N/A',
    '1900-01-01', 'N/A', 'N/A', 'N/A', -1, '9999-12-31', 'N/A',
    'N/A', 'N/A', 'N/A', 'N/A', -1, -1, 'N/A', 'N/A')
ON CONFLICT (player_id) DO NOTHING;
```

Ambos usan `ON CONFLICT DO NOTHING` para ser **idempotentes**: pueden ejecutarse múltiples veces sin error.

### 3.6. Booleanos --> `False`

| Tabla | Columnas |
|-------|----------|
| `dim_competitions` | `is_major_national_league` |
| `fact_appearances` | `team_captain` |
| `fact_games` | `is_home_win`, `is_draw`, `is_away_win` |

**Justificación**: Un booleano NULL es ambiguo. Si no sabemos si un jugador fue capitán, la asunción conservadora es `False`. Esto evita que `SUM(CASE WHEN team_captain THEN 1 END)` ignore registros.

**Nota técnica**: pandas carga los booleanos de CSVs como `1.0`/`0.0` (float) cuando hay NULLs en la columna. Se aplica `.astype(bool)` explícitamente para que PostgreSQL los acepte como tipo `BOOLEAN`.

### 3.7. Fechas

| Tipo | Valor | Columnas | Justificación |
|------|-------|----------|---------------|
| Fecha desconocida | `1900-01-01` | `date_of_birth`, `date` (dim_games) | Fecha fuera del rango del dataset (2000-2030) que señaliza dato ausente |
| Sin vencimiento | `9999-12-31` | `contract_expiration_date` | Contrato indefinido o sin fecha de expiración registrada |

**Nota técnica**: `pd.Timestamp('9999-12-31')` desborda la resolución nanosegundos de pandas (límite: ~2262). Se resolvió convirtiendo la columna a `object` y asignando la fecha como string, que PostgreSQL acepta nativamente como `DATE`.

---

## 4. Arquitectura de la Solución: `null_handler.py`

### 4.1. Problema Inicial: Tratamiento Manual

Originalmente, cada archivo ETL contenía su propio bloque de `fillna()` columna por columna:

```python
# Ejemplo en etl_dim_competitions.py (ANTES)
dim_competitions['country_name'] = dim_competitions['country_name'].fillna('N/A')
dim_competitions['domestic_league_code'] = dim_competitions['domestic_league_code'].fillna('N/A')
dim_competitions['confederation'] = dim_competitions['confederation'].fillna('N/A')
dim_competitions['country_id'] = dim_competitions['country_id'].fillna(-1).astype(int)
dim_competitions['is_major_national_league'] = dim_competitions['is_major_national_league'].fillna(False)
```

**Problemas**:
- **Sin fuente de verdad única**: Si cambiaba la estrategia (ej: de `-1` a `NULL` en FK opcionales), había que modificar 9 archivos.
- **Inconsistencias**: `fact_game_events` y `fact_transfers` usaban `'N/A'` en vez de `'Unknown'` para campos de texto.
- **Error-prone**: Fácil olvidar columnas nuevas o aplicar el valor incorrecto.
- **No validable**: No había verificación post-tratamiento de que realmente no quedaran NULLs.

### 4.2. Solución: Módulo Centralizado

Se creó `null_handler.py` como **Single Source of Truth** con tres componentes:

```
null_handler.py
├── CONSTANTES (7 valores por defecto)
├── NULL_RULES (diccionario: tabla --> {categoría --> [columnas]})
├── apply_null_rules(df, table_name, is_dimension)
├── validate_no_nulls(df, table_name)
└── get_null_summary(df)
```

**Flujo en cada ETL**:

```python
from null_handler import apply_null_rules, validate_no_nulls

# 1. Extraer y transformar
df = pd.read_csv(...)
# ... transformaciones específicas ...

# 2. Normalizar NULLs (una sola línea)
df = apply_null_rules(df, 'fact_transfers', is_dimension=False)

# 3. Validar (automáticamente excluye FK opcionales)
validate_no_nulls(df, 'fact_transfers')
```

### 4.3. Validación Estricta

`validate_no_nulls()` verifica que **todas** las columnas de la tabla tengan **0 NULLs**, sin excepciones. Gracias a los registros centinela, las FK opcionales ahora contienen `-1` en lugar de NULL, por lo que la validación es uniforme:

```
 Tabla 'fact_transfers': 0 NULLs (normalización completa)
 Tabla 'fact_game_events': 0 NULLs (normalización completa)
```

Esto simplifica enormemente la verificación: si `validate_no_nulls()` pasa sin error, **no hay NULLs de ningún tipo** en el DWH.

---

## 5. Integridad Referencial

Además del tratamiento de NULLs, cada ETL de hechos valida que las Foreign Keys **obligatorias** existan en sus dimensiones respectivas:

| Fact Table | FKs Validadas | Registros Eliminados |
|------------|---------------|---------------------|
| `fact_games` | `game_id`, `competition_id`, `home_club_id`, `away_club_id` | 15.531 eliminados |
| `fact_appearances` | `game_id`, `player_id`, `club_id` | 77.969 eliminados + 2 player_id inválidos |
| `fact_game_events` | `game_id`, `club_id`, `player_id` | 220.952 eliminados + 4.102 player_id inválidos |
| `fact_game_events` | `player_in_id`, `player_assist_id` | 1.887 + 205 --> **redirigidos a centinela (-1)** |
| `fact_player_valuations` | Validación en ETL existente | - |
| `fact_transfers` | `from_club_id`, `to_club_id` | 51.278 + 43.253 --> **redirigidos a centinela (-1)** |
| `fact_transfers` | `transfer_date_id` | 52 eliminados |

**Nota clave**: Las FK opcionales (`player_in_id`, `player_assist_id`, `from_club_id`, `to_club_id`) **no eliminan registros** - los redirigen al registro centinela (`-1`) en su dimensión correspondiente. Esto preserva el 100% de los hechos, a diferencia del enfoque con NULLs que requería eliminar o dejar huecos.

La causa principal de eliminaciones en FK obligatorias es que `dim_games` filtra partidos cuyos `home_club_id` o `away_club_id` no existen en `dim_clubs` (15.531 partidos de ligas menores). En cascada, los hechos referenciando esos `game_id` también se eliminan.

---

## 6. Resumen de Reglas por Tabla

### Dimensiones

| Tabla | Textos-->`N/A` | Medidas-->`0` | Atributos-->`-1` | Monetarios-->`-1` | Booleanos-->`False` | Fechas |
|-------|:-----------:|:----------:|:--------------:|:---------------:|:-----------------:|:------:|
| `dim_competitions` | 8 cols | - | 1 col | - | 1 col | - |
| `dim_clubs` | 7 cols | 2 cols | 5 cols | 1 col | - | - |
| `dim_players` | 15 cols | - | 3 cols | 2 cols | - | 2 cols |
| `dim_games` | 13 cols | - | 4 cols | - | - | 1 col |

### Hechos

| Tabla | Textos-->`Unknown` | Medidas-->`0` | Atributos-->`-1` | Monetarios-->`-1` | FK Opcionales-->`-1` (centinela) | Booleanos-->`False` |
|-------|:----------------:|:----------:|:--------------:|:---------------:|:------------------------------:|:-----------------:|
| `fact_games` | 1 col | 6 cols | 6 cols | - | - | 3 cols |
| `fact_appearances` | 4 cols | 5 cols | 4 cols | - | - | 1 col |
| `fact_game_events` | 4 cols | - | 5 cols | - | 2 cols | - |
| `fact_player_valuations` | 1 col | - | 4 cols | 1 col | - | - |
| `fact_transfers` | 3 cols | - | 4 cols | 2 cols | 2 cols | - |

---

## 7. Resultado Final

```
 dim_date:                0 NULLs
 dim_competitions:        0 NULLs
 dim_clubs:               0 NULLs  (incluye 1 registro centinela)
 dim_players:             0 NULLs  (incluye 1 registro centinela)
 dim_games:               0 NULLs
 fact_games:              0 NULLs
 fact_appearances:        0 NULLs
 fact_game_events:        0 NULLs  (FK opcionales --> centinela -1)
 fact_player_valuations:  0 NULLs
 fact_transfers:          0 NULLs  (FK opcionales --> centinela -1)

================================================
 VERIFICACIÓN COMPLETA: 0 NULLs ABSOLUTOS en todo el DWH
```

**0 NULLs en absolutamente todas las columnas de todas las tablas.** Gracias a los registros centinela en `dim_clubs` y `dim_players`, las FK opcionales apuntan a `id = -1` (`'Desconocido'`) en vez de ser NULL.

El Data Warehouse está preparado para consultas OLAP seguras: cualquier `INNER JOIN`, `GROUP BY`, `SUM()`, `AVG()` o `COUNT()` operará sobre datos completos sin riesgo de resultados silenciosamente incorrectos. No se requieren `LEFT JOIN` - todos los JOINs resuelven con `INNER JOIN`.

---

## 8. Referencia Bibliográfica

- Kimball, R., & Ross, M. (2013). *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling* (3rd ed.). Wiley.
  - Cap. 2: "Null-valued dimension attributes result in a row that cannot be joined"
  - Cap. 3: "Nulls in fact table measurements should be avoided"
  - Cap. 21: "Use special rows in dimension tables to handle unknown or not applicable conditions"
